package dashboard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/policy"
)

// APIVersion is the wire contract version carried by EVERY response body and every
// stream frame (§9.4a). It is frozen at 1 for Phases 5-6.
//
// The client compares it to its own compiled-in constant: higher than expected means
// "this dashboard is older than the server", and the client stops applying deltas
// rather than guessing at a schema it does not know. That skew rule only works if the
// field is genuinely present everywhere, which is why it is on the error envelope too.
const APIVersion = 1

// eventBuffer is how many frames may queue for ONE event-stream connection before the
// server starts dropping them.
//
// 32, the same depth signaling uses for a peer's outbound queue, and for the same
// reason: it absorbs a normal burst (a topology change fans out a handful of frames at
// once) without letting a wedged consumer grow the server's memory without bound. The
// choice of drop-over-buffer is §9.4's, and the client recovers through the seq gap it
// creates — which is exactly why seq is minted when a frame is PRODUCED and not when it
// is written.
const eventBuffer = 32

// maxTrackedMeets bounds the per-meet stream state (subscriber set, stale-rejection
// count, last-seen epoch/rev, per-node health history) this package keeps.
//
// The arbiter caps live meets at MaxMeets and tombstones at MaxEndedMeets, so no more
// than their sum can ever be interesting at once; a reaped meet, though, leaves an
// entry here that nothing else would ever remove. An unbounded map fed from an
// unauthenticated surface is a slow leak with a name, so the least-recently-touched
// stream that nobody is watching is evicted at the cap.
const maxTrackedMeets = arbiter.MaxMeets + arbiter.MaxEndedMeets

// ErrMemberNotFound is the sentinel a DemoControl returns when the eviction target is
// not a member of the meet. It is declared HERE, with the interface, because the
// mapping from "the target does not exist" to §9.4a's member_not_found belongs at the
// boundary that owns the status codes — and because cmd/server's Evict, which closes a
// Hub socket, has no arbiter error to reuse for it.
var ErrMemberNotFound = errors.New("dashboard: member not found")

// MeetSource is what the dashboard needs from the arbiter.
//
// Every signature names ARBITER-OWNED types, which is what lets *arbiter.Arbiter
// satisfy it without importing this package. The interface still earns its place: it is
// what lets these handler tests run against a fake with no arbiter at all.
type MeetSource interface {
	// ListMeets returns the live registry. The dashboard re-sorts the result into
	// §8.1's order rather than trusting the caller's, so the ordering is a property
	// of the boundary and not of one implementation.
	ListMeets(ctx context.Context) ([]arbiter.Meet, error)
	// ListEndedMeets returns the tombstone ring (§6.9). §9.3 mandates an ended[]
	// array that the v1 interface gave the dashboard no way to reach.
	ListEndedMeets(ctx context.Context) ([]arbiter.EndedMeet, error)
	// CreateMeet registers a meet. An EMPTY id must be passed straight through: the
	// arbiter generates ids from crypto/rand precisely so live meets are not
	// enumerable, and a dashboard-side generator would undo that.
	CreateMeet(ctx context.Context, id string) (arbiter.Meet, error)
	// GetMeet returns one meet, or an error wrapping arbiter.ErrMeetNotFound.
	GetMeet(ctx context.Context, id string) (arbiter.Meet, error)
}

// SubnetSource is what the dashboard needs from the coordinator: the INTENDED tree,
// which is a different answer from the arbiter's REALIZED one (§9.4b) and is served by
// different endpoints for exactly that reason.
//
// A nil SubnetSource is legal and means this process runs no coordinator (-coordinate
// off). The detail endpoint still answers, with an empty tree, because "there is no
// coordinator here" is information an operator needs rather than a reason to 404.
type SubnetSource interface {
	Snapshot(ctx context.Context, roomID string) (coordinator.RoomSnapshot, error)
}

// DemoControl is the gated destructive surface (§9.6). cmd/server passes nil when
// -demo is off and the dashboard then does not register the routes at all — so "off"
// is expressed as an ABSENT DEPENDENCY rather than a boolean a handler has to remember
// to check. An unregistered route cannot be reached by a bug in a permission check;
// the guard is structural, not conditional.
type DemoControl interface {
	// Evict closes the named peer's WebSocket, producing exactly the failure the
	// liveness path is meant to handle. It must return an error wrapping
	// ErrMemberNotFound when name is not in the meet, and one wrapping
	// arbiter.ErrMeetNotFound when the meet is not there — the boundary maps those to
	// §9.4a's 404s and everything else to a 500.
	Evict(ctx context.Context, roomID, name string) error
	// ForceElection forces an arbiter.ReasonManual announcement, bypassing
	// ElectionDwell and MinTermDuration. An EMPTY name means "the best candidate".
	ForceElection(ctx context.Context, roomID, name string) error
}

// Config parameterises the dashboard. Every dependency is injected; there are no
// globals and nothing is constructed here that cmd/server could not see.
type Config struct {
	// Meets is required.
	Meets MeetSource
	// Subnet may be nil (-coordinate off).
	Subnet SubnetSource
	// Demo nil means the destructive routes do not exist (§9.6).
	Demo DemoControl
	// Origins is the ONE allow-list, shared with the peer WebSocket's upgrade check.
	// Empty means same-origin only — which must deny a cross-origin browser, never
	// allow one; getting that polarity backwards is the silently-permissive failure
	// internal/policy exists to prevent.
	Origins policy.Origins
	// PublicURL is the externally reachable base URL used to build the join
	// rendezvous. Empty derives it from Addr (§9.4a).
	PublicURL string
	// Addr is the server's -addr value, used only for that derivation.
	Addr string
	// Clock is the time seam; nil means clock.System().
	Clock clock.Clock
}

// Server is the dashboard: an http.Handler plus the fan-out state behind the event
// streams. It implements coordinator.Publisher and arbiter.Publisher, so cmd/server
// hands the same value to both.
type Server struct {
	log     *slog.Logger
	cfg     Config
	clk     clock.Clock
	origins policy.Origins
	handler http.Handler

	// httpBase and wsBase are derived once at construction because they depend only
	// on flags. Building them per request would let two responses disagree about the
	// same server, which is the one thing the join block must never do.
	httpBase string
	wsBase   string

	// mu guards everything below. It is held for map lookups and channel sends ONLY:
	// no call into MeetSource, SubnetSource or DemoControl ever happens under it (see
	// the package doc's second rule).
	mu     sync.Mutex
	meets  map[string]*meetStream
	closed bool
}

// meetStream is the per-meet fan-out state.
type meetStream struct {
	subs map[*subscription]struct{}
	// stale is the INTERIM fence-rejection counter, superseded by
	// RoomSnapshot.StaleRejected once §15.14's carrier lands (see meetBody).
	// coordinator.RoomSnapshot carries no such field as of this commit, so the
	// dashboard owns the number §9.3 puts in every snapshot; capturing it when a
	// snapshot frame is MINTED (not when it is materialised) is what keeps it
	// consistent with the deltas that follow — each rejection counted exactly once.
	//
	// It is monotonic and per-process, and the real counter is neither, which is why
	// this is replaced rather than reconciled when the carrier arrives.
	stale uint64
	// epoch and rev are the last control-plane version seen for this meet, from an
	// event or a snapshot. They stamp frames that carry no version of their own (a
	// demo action, a pong). Stamping those with zero would be worse than omitting
	// them: web/js/state.js copies frame.epoch onto its snapshot for every delta, so
	// a zero would blank the epoch the operator is watching.
	epoch, rev uint64
	// health is the INTERIM last-published Health per node, superseded by
	// coordinator.Event.PrevHealth once §15.14's plumbing lands (see healthData).
	// coordinator.Event carries only the new value as of this commit, so the dashboard
	// remembers the series it is the only component to see in full. Bounded by live
	// membership: an entry is dropped when the member leaves.
	health map[string]coordinator.Health
	// touched drives the maxTrackedMeets eviction.
	touched time.Time
}

// New constructs a Server. It fails loud on a missing MeetSource or an unusable
// -public-url / -addr, in the same startup-validation discipline the rest of the
// process uses: a boundary that cannot build a correct join command should refuse to
// start rather than hand every operator a subtly wrong one.
func New(log *slog.Logger, cfg Config) (*Server, error) {
	if log == nil {
		return nil, errors.New("dashboard: logger is required")
	}
	if cfg.Meets == nil {
		return nil, errors.New("dashboard: Config.Meets is required")
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.System()
	}
	httpBase, wsBase, err := deriveBase(cfg.PublicURL, cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("dashboard: %w", err)
	}
	s := &Server{
		log:      log.With(slog.String("service", "dashboard")),
		cfg:      cfg,
		clk:      cfg.Clock,
		origins:  cfg.Origins,
		httpBase: httpBase,
		wsBase:   wsBase,
		meets:    map[string]*meetStream{},
	}
	s.handler = s.withCORS(s.routes())
	return s, nil
}

// routes builds the mux.
//
// Every pattern is registered WITHOUT a method and dispatches on r.Method itself,
// which is not the idiomatic Go 1.22 shape and is deliberate: §9.4a requires the
// {code, message, details} envelope on EVERY non-2xx, and ServeMux's own 404 and 405
// responses are plain text. Owning the dispatch is the only way to guarantee the
// envelope, and it also keeps the "/api/" catch-all in charge of anything unrouted.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/meets", s.handleMeets)
	mux.HandleFunc("/api/meets/{id}", s.handleMeet)
	mux.HandleFunc("/api/meets/{id}/events", s.handleEvents)
	if s.cfg.Demo != nil {
		// §9.6: registered ONLY when the operator explicitly asked for them.
		mux.HandleFunc("/api/demo/meets/{id}/evict", s.handleDemoEvict)
		mux.HandleFunc("/api/demo/meets/{id}/elect", s.handleDemoElect)
	}
	mux.HandleFunc("/api/", s.handleUnknown)
	return mux
}

// Handler returns the http.Handler for the whole /api/ surface. Mount it so that
// requests keep their full path — mux.Handle("/api/", dash.Handler()) — because the
// patterns above are absolute.
func (s *Server) Handler() http.Handler { return s.handler }

// Close tears every live event stream down with close code 1000 ("server shutting
// down"), which is the code that tells a client to reconnect with backoff rather than
// give up. It is idempotent, and after it a new subscription is refused.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, ms := range s.meets {
		for sub := range ms.subs {
			close(sub.stop)
		}
		ms.subs = map[*subscription]struct{}{}
	}
}

// streamLocked returns the meet's fan-out state, creating it if needed. Caller holds mu.
func (s *Server) streamLocked(meetID string) *meetStream {
	ms := s.meets[meetID]
	if ms == nil {
		s.evictLocked()
		ms = &meetStream{
			subs:   map[*subscription]struct{}{},
			health: map[string]coordinator.Health{},
		}
		s.meets[meetID] = ms
	}
	ms.touched = s.clk.Now()
	return ms
}

// evictLocked drops the least-recently-touched UNWATCHED stream once the map is at the
// cap. A watched stream is never evicted, because its subscribers' seq numbering lives
// in it; if every tracked meet is being watched the map is allowed to exceed the cap,
// since that bound is a leak guard and not a subscriber limit.
func (s *Server) evictLocked() {
	if len(s.meets) < maxTrackedMeets {
		return
	}
	var oldestID string
	var oldest time.Time
	for id, ms := range s.meets {
		if len(ms.subs) > 0 {
			continue
		}
		if oldestID == "" || ms.touched.Before(oldest) {
			oldestID, oldest = id, ms.touched
		}
	}
	if oldestID != "" {
		delete(s.meets, oldestID)
	}
}

// noteVersion records the control-plane version a snapshot reported, so frames that
// carry no version of their own can be stamped with something true.
func (s *Server) noteVersion(meetID string, epoch, rev uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := s.streamLocked(meetID)
	if epoch != 0 {
		ms.epoch = epoch
	}
	if rev != 0 {
		ms.rev = rev
	}
}

// staleCount reads the meet's observed fence-rejection count.
func (s *Server) staleCount(meetID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamLocked(meetID).stale
}
