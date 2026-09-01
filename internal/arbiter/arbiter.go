package arbiter

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/policy"
)

// The election policy constants. Every one of them is a brake, and each is sized by
// how expensive the action it gates is.
const (
	// ElectionDwell is the sustained-condition window for a VOLUNTARY handover. It
	// is twice the tree's degradation dwell on purpose: re-parenting one subtree
	// costs one stream a keyframe, but migrating the coordinator stalls the entire
	// control loop for the rebuild window. The more expensive the action, the more
	// evidence it should require.
	ElectionDwell = 20 * time.Second

	// PromoteMarginScore is how much fitter a challenger must be before it is worth
	// the handover. 0.20 on a [0,1] scale is a large, visible gap — roughly "the
	// incumbent has lost most of its CPU headroom AND its link got worse". A
	// marginal improvement is not worth a control-plane stall; this is the
	// election-layer twin of the overlay's re-parent stickiness.
	PromoteMarginScore = 0.20

	// DemoteBelowScore is the absolute floor below which the incumbent is replaced
	// by the best available candidate even without a large margin. 0.35 sits below
	// what a healthy peer on a mediocre home link scores (~0.55-0.7) and above what
	// a saturated or half-broken one scores.
	DemoteBelowScore = 0.35

	// MinTermDuration is the hard floor on how often the role may move voluntarily.
	//
	// It gates ONLY transitions that could flap, which means peer-to-peer promotion
	// and demotion. It does not gate the failure path (a dead coordinator is replaced
	// instantly, always), and it does not gate the arbiter's bootstrap handover to
	// the first fit peer: the arbiter does not compete for the role, so arbiter->peer
	// happens at most once per meet and cannot flap. Gating it would buy nothing and
	// cost a minute of running in a configuration that is explicitly a stepping
	// stone. The general rule is the thing to carry forward — anti-flap machinery
	// belongs only where flapping is possible.
	//
	// One minute makes a flapping-election bug obvious (a once-a-minute dashboard
	// event) rather than invisible (a hundred handovers a second).
	MinTermDuration = 60 * time.Second

	// MeetTTL is how long a meet may sit EMPTY before it is reaped.
	//
	// 5 minutes: comfortably longer than any reconnect storm a real meeting produces
	// — a laptop lid closing, a Wi-Fi roam, everyone refreshing a browser tab — so a
	// meet does not evaporate under its participants; and far shorter than any
	// interval over which an unauthenticated caller could accumulate meaningful
	// state, since MaxMeets caps the live set regardless.
	MeetTTL = 5 * time.Minute

	// MaxEndedMeets is the size of the tombstone ring (FIFO, oldest evicted).
	//
	// 20. A dashboard's value here is substantially retrospective — an operator wants
	// to review the failover they just watched — and a meet vanishing the instant its
	// last peer disconnects destroys exactly that, at exactly the wrong moment. An
	// unbounded history would be the same denial of service in a different map, so
	// the ring is bounded by construction rather than by policy.
	MaxEndedMeets = 20

	// MaxMeets caps the live registry, bounding the only unauthenticated write
	// surface in the system. 100 is far above any plausible single-process demo and
	// far below anything that could exhaust memory; it is also why the listing needs
	// no pagination.
	//
	// Honest limitation: this bounds STATE, not request rate. A caller can still
	// churn create/reap indefinitely. Rate limiting is out of scope for a system with
	// no authentication at all.
	MaxMeets = 100

	// DefaultArbiterID is the reserved coordinator id used when the arbiter itself
	// holds the role. It must equal signaling.ServerID, and it is a literal here
	// only because the dependency graph forbids arbiter -> signaling. cmd/server
	// passes signaling.ServerID into Config.ArbiterID explicitly so the two cannot
	// drift in production; this default exists for tests and for a zero Config.
	DefaultArbiterID = "_server"
)

// The errors a caller is expected to branch on. The dashboard maps them to HTTP
// statuses, which is why they are values and not strings: the mapping belongs at the
// boundary, and a boundary that has to match on message text is a boundary that
// breaks when someone improves a message.
var (
	// ErrInvalidMeetID: the id does not match policy.MeetIDPattern. HTTP 400.
	ErrInvalidMeetID = errors.New("arbiter: invalid meet id")
	// ErrMeetExists: a meet with that id is already registered. HTTP 409.
	ErrMeetExists = errors.New("arbiter: meet already exists")
	// ErrMeetNotFound: no meet with that id. HTTP 404.
	ErrMeetNotFound = errors.New("arbiter: meet not found")
	// ErrNoCandidate: the requested peer is not a member, or no peer can hold the
	// role. HTTP 409 on the demo surface.
	ErrNoCandidate = errors.New("arbiter: no eligible coordinator candidate")
	// ErrTooManyMeets: the live registry is at MaxMeets. HTTP 429.
	ErrTooManyMeets = errors.New("arbiter: too many meets")
	// ErrNotRunning: the arbiter's Run loop has stopped. Every query returns this
	// rather than blocking forever, so a late dashboard request during shutdown
	// fails fast instead of hanging a connection.
	ErrNotRunning = errors.New("arbiter: not running")
)

// Config parameterises the arbiter. Every zero value is a working default, so a
// zero Config is a legal (if inert) arbiter.
//
// The duration and margin fields follow one convention: ZERO means "use the frozen
// default"; NEGATIVE means "disable this brake entirely". Negative is reachable only
// from a test — cmd/server's startup validation rejects a non-positive duration
// flag — and it exists so a test can isolate one brake from the other instead of
// asserting on their sum.
type Config struct {
	// Clock is the time seam. nil means clock.System().
	Clock clock.Clock

	// Elect turns on arbitration of the role among peers (-elect). With it off the
	// arbiter never hands the role to a peer.
	Elect bool
	// Coordinate says this process may host the coordinator itself (-coordinate).
	// With both set, the arbiter takes the role at bootstrap and hands it to the
	// first peer that clears DemoteBelowScore — the posture that makes a meet
	// usable immediately and still migrates.
	Coordinate bool
	// CoordinatorConfig is the tuning every coordinator of every meet in this process
	// serves under, announced to whichever node holds the role. cmd/server builds it
	// from the same flags that build its OWN coordinator.Config, so the server-hosted
	// and peer-hosted coordinators are configured identically by construction rather
	// than by two code paths agreeing.
	//
	// It is not defaulted here on purpose: see CoordinatorConfig.Validate.
	CoordinatorConfig CoordinatorConfig

	// ArbiterID is the coordinator id to announce when the arbiter holds the role.
	// Empty means DefaultArbiterID. cmd/server passes signaling.ServerID.
	ArbiterID string

	// ElectionDwell, MinTerm, PromoteMargin, and DemoteBelow override the constants
	// of the same name. See the zero/negative convention above.
	ElectionDwell time.Duration
	MinTerm       time.Duration
	PromoteMargin float64
	DemoteBelow   float64

	// ValidMeetID is the boundary predicate for a caller-supplied meet id. nil means
	// policy.ValidMeetID, which is the single source of truth for the whole process:
	// two copies of a boundary matcher drift, and the drift's failure mode is silent.
	// The seam exists so a handler test can narrow it, never so a surface can differ.
	ValidMeetID func(id string) bool
	// NewMeetID generates an id for a create request that did not supply one. nil
	// means "m-" plus 8 characters of Crockford base32 from crypto/rand.
	//
	// Random rather than a counter, deliberately: a meet id is the only thing
	// standing between a stranger and a meeting, so a guessable id is a join
	// capability anyone can enumerate. The cost is that generated ids are the one
	// non-replayable thing this package produces, which is exactly why the generator
	// is a seam a deterministic test can replace.
	NewMeetID func() string
}

// eventKind discriminates the arbiter's single input channel.
type eventKind int

const (
	evJoin eventKind = iota
	evLeave
	evReport
	evBeat
	evQuery
)

// event is one input to the Run goroutine. A single struct with unused fields beats
// an interface per kind: the loop stays one switch, and adding a kind never touches
// a caller.
type event struct {
	kind   eventKind
	roomID string
	peerID string
	name   string

	report metrics.Report
	beat   metrics.Heartbeat

	// run is the closure a query executes ON the Run goroutine, so a caller sees a
	// consistent snapshot without a single lock existing anywhere in this package.
	// done is closed once it has run.
	run  func()
	done chan struct{}
}

// eventBuffer is how many frames may queue before an enqueuing Hub goroutine blocks.
// Generous: events are tiny and the loop drains them immediately, so this is only a
// cushion for a burst of simultaneous joins in a large meet.
const eventBuffer = 256

// Arbiter is the central authority over who coordinates each meet.
//
// All state lives behind one goroutine (Run) and every input arrives on one channel.
// That is not merely a concurrency style here: it is what makes the epoch a
// single-writer value, which is the entire safety argument for the fence. There is
// no lock to get wrong and no distributed agreement to reach, because there is
// exactly one writer.
type Arbiter struct {
	log  *slog.Logger
	cfg  Config
	clk  clock.Clock
	ann  Announcer
	pub  Publisher
	self string

	// Resolved policy, so the election path never re-derives a default.
	dwell         time.Duration
	minTerm       time.Duration
	promoteMargin float64
	demoteBelow   float64
	validMeetID   func(string) bool
	newMeetID     func() string

	events chan event
	done   chan struct{} // closed when Run returns; makes a late callback non-blocking

	// Owned by Run.
	meets map[string]*meetState
	// ended is the tombstone ring, oldest first. A slice rather than a container/ring
	// because it is 20 elements and the only operations are append-and-trim.
	ended   []EndedMeet
	meetSeq uint64
}

// New builds an Arbiter. Run must be called to start the owning goroutine; until
// then inputs buffer.
//
// A nil Announcer becomes a no-op, and a nil Publisher means "publish nothing", so
// neither seam has to be nil-checked on the hot path.
func New(log *slog.Logger, cfg Config, ann Announcer, pub Publisher) *Arbiter {
	a := &Arbiter{
		log:    log.With(slog.String("component", "arbiter")),
		cfg:    cfg,
		clk:    cfg.Clock,
		ann:    ann,
		pub:    pub,
		self:   cfg.ArbiterID,
		events: make(chan event, eventBuffer),
		done:   make(chan struct{}),
		meets:  map[string]*meetState{},
	}
	if a.clk == nil {
		a.clk = clock.System()
	}
	if ann == nil {
		a.ann = nopAnnouncer{}
	}
	if a.self == "" {
		a.self = DefaultArbiterID
	}
	a.dwell = orDefault(cfg.ElectionDwell, ElectionDwell)
	a.minTerm = orDefault(cfg.MinTerm, MinTermDuration)
	a.promoteMargin = orDefaultF(cfg.PromoteMargin, PromoteMarginScore)
	a.demoteBelow = orDefaultF(cfg.DemoteBelow, DemoteBelowScore)
	a.validMeetID = cfg.ValidMeetID
	if a.validMeetID == nil {
		a.validMeetID = policy.ValidMeetID
	}
	a.newMeetID = cfg.NewMeetID
	// Warn rather than refuse. An elected peer builds its coordinator from what we
	// announce, so an unusable configuration means it adopts the role, runs, and
	// publishes nothing — a silent failure whose symptom (a tree that is never
	// repaired after a handover) points nowhere near its cause. cmd/server's startup
	// validation is what makes this fatal; here it is a breadcrumb for the case where
	// something else constructed the arbiter.
	if cfg.Elect {
		if err := cfg.CoordinatorConfig.Validate(); err != nil {
			a.log.Warn("coordinator configuration is unusable; an elected peer will publish no tree",
				slog.Any("error", err))
		}
	}
	return a
}

// orDefault applies the zero-means-default, negative-means-disabled convention.
func orDefault(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	if v < 0 {
		return 0
	}
	return v
}

func orDefaultF(v, def float64) float64 {
	if v == 0 {
		return def
	}
	if v < 0 {
		return 0
	}
	return v
}

// Run owns all arbiter state until ctx is cancelled, processing one event at a time.
// Callers run it on its own goroutine; it returns ctx.Err() on shutdown.
//
// There is deliberately no timer in this loop. The arbiter is purely event-driven,
// and that is sound rather than a shortcut: every action it can take requires at
// least one live peer to promote, demote, or fail over to, and a live peer is by
// definition producing heartbeats at the configured cadence. A meet in which nobody
// is speaking is a meet in which there is nothing to decide. Not arming a timer also
// means the whole election replays exactly under a virtual clock.
func (a *Arbiter) Run(ctx context.Context) error {
	defer close(a.done)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-a.events:
			a.handle(ev)
		}
	}
}

// PeerJoined, PeerLeft, Metrics, and Heartbeat mirror the signaling Observer surface.
// cmd/server fans the Hub's callbacks to BOTH the arbiter and the coordinator, which
// is what gives the arbiter a liveness view that does not depend on the coordinator
// being alive to process it. All four are called from Hub goroutines, are safe
// concurrently, and only enqueue.

// PeerJoined registers a member, creating the meet if a peer reached it before the
// dashboard did. A rejoin resets the peer's uptime and its telemetry: a peer that
// dropped and came back has proven nothing about staying, and its old declaration
// belongs to a connection that no longer exists.
func (a *Arbiter) PeerJoined(roomID, peerID, name string) {
	a.enqueue(event{kind: evJoin, roomID: roomID, peerID: peerID, name: name})
}

// PeerLeft removes a member. A closed socket is definitive evidence of death, so a
// departing coordinator is failed over immediately with no dwell.
func (a *Arbiter) PeerLeft(roomID, peerID string) {
	a.enqueue(event{kind: evLeave, roomID: roomID, peerID: peerID})
}

// Metrics ingests a telemetry report: the fitness inputs, and the peer's own
// declaration that it is willing to be elected.
func (a *Arbiter) Metrics(roomID, peerID string, payload []byte) {
	var rep metrics.Report
	if err := json.Unmarshal(payload, &rep); err != nil {
		a.log.Debug("bad metrics payload", slog.String("peer_id", peerID), slog.Any("error", err))
		return
	}
	a.enqueue(event{kind: evReport, roomID: roomID, peerID: peerID, report: rep})
}

// Heartbeat ingests a liveness beat. This is the arbiter's own failure detector and
// the only one that can observe a frozen coordinator, so it is deliberately not
// routed through anything the judged peer runs.
func (a *Arbiter) Heartbeat(roomID, peerID string, payload []byte) {
	var hb metrics.Heartbeat
	if err := json.Unmarshal(payload, &hb); err != nil {
		a.log.Debug("bad heartbeat payload", slog.String("peer_id", peerID), slog.Any("error", err))
		return
	}
	a.enqueue(event{kind: evBeat, roomID: roomID, peerID: peerID, beat: hb})
}

// enqueue hands an event to the Run goroutine, giving up if the arbiter has shut
// down so a late Hub callback can never block forever on a drained loop.
func (a *Arbiter) enqueue(ev event) {
	select {
	case a.events <- ev:
	case <-a.done:
	}
}

// Sync blocks until every event enqueued before the call has been processed. It is
// the deterministic-testing barrier and the quiescence primitive a caller uses
// before reading a snapshot.
func (a *Arbiter) Sync(ctx context.Context) error { return a.query(ctx, func() {}) }

// query runs fn on the Run goroutine and waits for it. Every read of arbiter state
// goes through here, which is why no field in this package needs a mutex.
func (a *Arbiter) query(ctx context.Context, fn func()) error {
	done := make(chan struct{})
	select {
	case a.events <- event{kind: evQuery, run: fn, done: done}:
	case <-a.done:
		return ErrNotRunning
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-a.done:
		return ErrNotRunning
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ListMeets returns every meet, newest first and then by id ascending — the order the
// dashboard renders, computed once here rather than in every client.
func (a *Arbiter) ListMeets(ctx context.Context) ([]Meet, error) {
	var out []Meet
	err := a.query(ctx, func() {
		a.reap(a.clk.Now())
		out = make([]Meet, 0, len(a.meets))
		for _, ms := range a.meets {
			out = append(out, ms.snapshot(a.self))
		}
		sort.Slice(out, func(i, j int) bool {
			if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
				return out[i].CreatedAt.After(out[j].CreatedAt)
			}
			return out[i].ID < out[j].ID
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListEndedMeets returns the tombstone ring in the contract's order: EndedAt
// descending, then id ascending — newest first, so an operator reviewing the failover
// they just watched sees it at the top.
//
// It is a separate method rather than a second return value from ListMeets because the
// dashboard serves both in one body but a handler test wants to narrow each seam on its
// own, and because "list what is live" and "list what is gone" are different questions.
func (a *Arbiter) ListEndedMeets(ctx context.Context) ([]EndedMeet, error) {
	var out []EndedMeet
	err := a.query(ctx, func() {
		a.reap(a.clk.Now())
		out = append(make([]EndedMeet, 0, len(a.ended)), a.ended...)
		// Sorted, not merely reversed. Insertion order is REAP order, and a reap
		// sweeps by id — so a batch that ended at different times would come back in
		// the wrong order, and a batch that ended at the same time in the reverse of
		// the one the contract asks for.
		sort.Slice(out, func(i, j int) bool {
			if !out[i].EndedAt.Equal(out[j].EndedAt) {
				return out[i].EndedAt.After(out[j].EndedAt)
			}
			return out[i].ID < out[j].ID
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// reap removes every meet that has been continuously empty for MeetTTL and files a
// tombstone for it.
//
// Reaping is LAZY — driven by registry mutations and listings, never by a timer. A
// server with no traffic cannot grow, so it does not need sweeping; and staying
// timer-free is what keeps the whole package replayable under a virtual clock.
func (a *Arbiter) reap(now time.Time) {
	var due []string
	for id, ms := range a.meets {
		if len(ms.members) == 0 && !ms.emptyAt.IsZero() && now.Sub(ms.emptyAt) >= MeetTTL {
			due = append(due, id)
		}
	}
	if len(due) == 0 {
		return
	}
	// Sorted before acting: the map iteration above is randomised, and the order
	// meets enter the bounded ring decides which one is evicted from it.
	sort.Strings(due)
	for _, id := range due {
		ms := a.meets[id]
		delete(a.meets, id)
		a.ended = append(a.ended, ms.tombstone())
		if len(a.ended) > MaxEndedMeets {
			a.ended = a.ended[len(a.ended)-MaxEndedMeets:]
		}
		a.log.Info("meet reaped",
			slog.String("meet_id", id), slog.Uint64("final_epoch", ms.epoch))
	}
}

// GetMeet returns one meet, or ErrMeetNotFound.
func (a *Arbiter) GetMeet(ctx context.Context, id string) (Meet, error) {
	var out Meet
	var found bool
	if err := a.query(ctx, func() {
		ms, ok := a.meets[id]
		if !ok {
			return
		}
		out, found = ms.snapshot(a.self), true
	}); err != nil {
		return Meet{}, err
	}
	if !found {
		return Meet{}, fmt.Errorf("%w: %q", ErrMeetNotFound, id)
	}
	return out, nil
}

// CreateMeet registers an empty meet. An empty id is generated rather than rejected,
// because the dashboard's "create a meet" button has no id to offer.
func (a *Arbiter) CreateMeet(ctx context.Context, id string) (Meet, error) {
	var out Meet
	var cerr error
	if err := a.query(ctx, func() {
		now := a.clk.Now()
		// Reap first: a caller at the cap deserves the room an expired meet already
		// freed, and this is the mutation the lazy sweep rides on.
		a.reap(now)
		if id == "" {
			id = a.generateMeetID()
		}
		if !a.validMeetID(id) {
			cerr = fmt.Errorf("%w: %q must match %s", ErrInvalidMeetID, id, policy.MeetIDPattern)
			return
		}
		if _, ok := a.meets[id]; ok {
			cerr = fmt.Errorf("%w: %q", ErrMeetExists, id)
			return
		}
		if len(a.meets) >= MaxMeets {
			cerr = fmt.Errorf("%w: %d live meets", ErrTooManyMeets, len(a.meets))
			return
		}
		ms := newMeetState(id, now)
		// A meet created through the API starts empty, so its TTL starts now. An
		// abandoned create is reaped on the same clock as an abandoned meeting.
		ms.emptyAt = now
		a.meets[id] = ms
		a.log.Info("meet created", slog.String("meet_id", id))
		out = ms.snapshot(a.self)
	}); err != nil {
		return Meet{}, err
	}
	return out, cerr
}

// crockford is Crockford base32 in lowercase: the digits plus the alphabet minus
// i, l, o and u, so an id read aloud or copied from a screenshot cannot be
// mistranscribed into a different meet. Exactly 32 symbols, so each character is
// 5 bits and eight of them are 40.
const crockford = "0123456789abcdefghjkmnpqrstvwxyz"

// generatedIDLen is 8 characters, i.e. 40 bits. Collision-safe well past MaxMeets
// with room to spare, and short enough to type.
const generatedIDLen = 8

// generateMeetID produces an unused "m-XXXXXXXX". Runs on the Run goroutine, so the
// fallback counter needs no synchronisation.
func (a *Arbiter) generateMeetID() string {
	for {
		id := a.newMeetID
		var candidate string
		if id != nil {
			candidate = id()
		} else {
			candidate = a.randomMeetID()
		}
		if _, taken := a.meets[candidate]; !taken {
			return candidate
		}
	}
}

// randomMeetID draws from crypto/rand. A meet id is the only thing standing between a
// stranger and a meeting, so it must not be guessable — a counter would let anyone
// enumerate every live meet on the server.
//
// If the system entropy source fails, it falls back to a counter rather than
// returning an error: an unguessable id is a defence in depth, but a create request
// that cannot be served at all is a broken product. The fallback is logged loudly
// because it is a real (if remote) weakening.
func (a *Arbiter) randomMeetID() string {
	buf := make([]byte, generatedIDLen)
	if _, err := rand.Read(buf); err != nil {
		a.meetSeq++
		a.log.Error("system entropy unavailable; generating a GUESSABLE meet id",
			slog.Any("error", err))
		return fmt.Sprintf("m-%08d", a.meetSeq)
	}
	out := make([]byte, 0, 2+generatedIDLen)
	out = append(out, 'm', '-')
	for _, b := range buf {
		out = append(out, crockford[int(b)%len(crockford)])
	}
	return string(out)
}

// ForceElection moves the role now, bypassing both ElectionDwell and
// MinTermDuration. name may be empty, meaning "the best candidate".
//
// A named peer is NOT required to be eligible: this is the demo control surface, and
// forcing the role onto a peer that should not have it is precisely the failure a
// demo wants to produce. It is only required to be a member, because announcing a
// coordinator that is not in the meet would fence every peer to nothing.
func (a *Arbiter) ForceElection(ctx context.Context, roomID, name string) error {
	var ferr error
	if err := a.query(ctx, func() {
		now := a.clk.Now()
		ms, ok := a.meets[roomID]
		if !ok {
			ferr = fmt.Errorf("%w: %q", ErrMeetNotFound, roomID)
			return
		}
		target, ok := a.forceTarget(ms, name, now)
		if !ok {
			ferr = fmt.Errorf("%w: meet %q, name %q", ErrNoCandidate, roomID, name)
			return
		}
		a.announce(ms, target, ReasonManual, now)
	}); err != nil {
		return err
	}
	return ferr
}

// forceTarget resolves a manual election request to a holder.
func (a *Arbiter) forceTarget(ms *meetState, name string, now time.Time) (holder, bool) {
	if name == "" {
		cands := a.candidates(ms, now)
		if len(cands) == 0 {
			return holder{}, false
		}
		return peerHolder(cands[0]), true
	}
	// Members are scanned in id order so a (impossible, but cheap to defend
	// against) duplicate name resolves the same way every time.
	for _, id := range sortedIDs(ms) {
		if ms.members[id].name == name {
			return holder{kind: holderPeer, id: id, name: name}, true
		}
	}
	return holder{}, false
}

// handle applies one event to the owned state.
//
// Every input follows the same tail: apply the fact, run the election, re-announce if
// the election produced nothing, then re-check the two latched operator-facing states
// (fence repair, and "nobody volunteered"). Keeping the tail common is what stops a new
// event kind from silently skipping one of them.
func (a *Arbiter) handle(ev event) {
	if ev.kind == evQuery {
		ev.run()
		close(ev.done)
		return
	}
	now := a.clk.Now()

	var ms *meetState
	var ps *peerState
	membershipChanged := false

	switch ev.kind {
	case evJoin:
		ms = a.meets[ev.roomID]
		if ms == nil {
			// A peer reached this meet before the dashboard did. The Hub has already
			// admitted it, so refusing to track it here would leave a meet with live
			// members the arbiter cannot see — silently uncoordinated, which is worse
			// than exceeding a bound aimed at the unauthenticated REST surface.
			if len(a.meets) >= MaxMeets {
				a.log.Warn("tracking a meet past MaxMeets: a peer is already in it",
					slog.String("meet_id", ev.roomID), slog.Int("meets", len(a.meets)))
			}
			ms = newMeetState(ev.roomID, now)
			a.meets[ev.roomID] = ms
		}
		ps = &peerState{id: ev.peerID, name: ev.name, joinedAt: now, lastSeen: now}
		ms.members[ev.peerID] = ps
		ms.emptyAt = time.Time{} // emptiness must be CONTINUOUS to reap
		if len(ms.members) > ms.peakMembers {
			ms.peakMembers = len(ms.members)
		}
		membershipChanged = true
		a.log.Debug("member joined", slog.String("meet_id", ms.id), slog.String("name", ev.name))
		a.reap(now)

	case evLeave:
		ms = a.meets[ev.roomID]
		if ms == nil {
			return
		}
		if _, ok := ms.members[ev.peerID]; !ok {
			return
		}
		delete(ms.members, ev.peerID)
		if len(ms.members) == 0 {
			ms.emptyAt = now
		}
		membershipChanged = true
		a.reap(now)

	case evReport:
		ps = a.member(ev.roomID, ev.peerID)
		if ps == nil {
			return
		}
		ms = a.meets[ev.roomID]
		ps.report, ps.reported, ps.lastSeen = ev.report, true, now

	case evBeat:
		ps = a.member(ev.roomID, ev.peerID)
		if ps == nil {
			return
		}
		ms = a.meets[ev.roomID]
		ps.beat, ps.hasBeat, ps.lastSeen = ev.beat, true, now
	}

	elected := a.elect(ms, now)
	if membershipChanged && !elected {
		a.reannounce(ms)
	}
	// After the election, never before: an election resets termStart, which is what
	// suppresses a repair during the legitimate propagation window that follows one.
	if ev.kind == evBeat {
		a.checkFence(ms, ps, now)
	}
	a.checkVolunteers(ms)
}

// member resolves a frame's sender, or nil when it names a peer or meet the arbiter
// does not know. A frame must never conjure a member: the Hub's join handshake is
// the only thing that may, and trusting a payload instead would let a peer file
// telemetry into a meet it is not in.
func (a *Arbiter) member(roomID, peerID string) *peerState {
	ms, ok := a.meets[roomID]
	if !ok {
		return nil
	}
	return ms.members[peerID]
}

// holderKind distinguishes the three things that can hold the coordinator role. It is
// an explicit kind rather than "is this string empty" because two of the three states
// have an empty peer name and conflating them is exactly how a vacancy gets rendered
// as "the arbiter is coordinating".
type holderKind uint8

const (
	holderNone    holderKind = iota // vacant
	holderArbiter                   // the arbiter process itself (-coordinate)
	holderPeer                      // an elected peer
)

// holder names whoever is to hold the coordinator role.
type holder struct {
	kind holderKind
	id   string
	name string
}

// candidate is one peer ranked for the role.
type candidate struct {
	id    string
	name  string
	score float64
}

// candidates returns the eligible members, fittest first.
//
// The member map is iterated, which Go randomises — so the result is sorted by a
// TOTAL order before it is returned, and nothing reads it before then. The tiebreak
// is peer NAME ascending: names are unique within a meet (the Hub rejects a
// duplicate), so it is total; and unlike a peer id, a name does not depend on join
// order, so two runs of the same scenario elect the same peer even if the peers
// connected in a different order. The id is a final tiebreak for defence only.
func (a *Arbiter) candidates(ms *meetState, now time.Time) []candidate {
	out := make([]candidate, 0, len(ms.members))
	for id, ps := range ms.members {
		f := a.fitness(ps, now)
		if !eligible(f) {
			continue
		}
		out = append(out, candidate{id: id, name: ps.name, score: Score(f)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		if out[i].name != out[j].name {
			return out[i].name < out[j].name
		}
		return out[i].id < out[j].id
	})
	return out
}

// sortedIDs is the member id list in ascending order, for the few places that must
// scan members and whose result a caller can observe.
func sortedIDs(ms *meetState) []string {
	ids := make([]string, 0, len(ms.members))
	for id := range ms.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// fitness assembles the control-plane evidence about one peer.
func (a *Arbiter) fitness(ps *peerState, now time.Time) Fitness {
	return Fitness{
		CPUFreePct:  100 - ps.report.CPUPct,
		RTTServerMs: ps.report.RTTServerMs,
		LossPct:     ps.report.LossPct,
		UptimeSec:   now.Sub(ps.joinedAt).Seconds(),
		NAT:         ps.report.NAT,
		Live:        a.live(ps, now),
		// eligible == reported && Coordinatable. The wire field is a bare bool, so
		// absent reads as false, and that conservative direction is deliberate:
		// silently electing a peer that never opted in is worse than leaving a meet
		// uncoordinated. Gating on reported is what keeps "nobody volunteered"
		// distinguishable from "nobody has spoken yet" — see checkVolunteers.
		Coordinatable: ps.reported && ps.report.Coordinatable,
	}
}

// live is the arbiter's OWN liveness verdict: has this peer said anything within the
// gone threshold for the cadence it declared?
//
// This is the detector that has to work when the sitting coordinator freezes, and it
// is why it reads only from frames the Hub terminates. Anything routed through the
// coordinator's own health machinery would be asking a dead process to report its
// own death.
func (a *Arbiter) live(ps *peerState, now time.Time) bool {
	return now.Sub(ps.lastSeen) < metrics.GoneAfter(ps.interval())
}

// score is the fitness score of one member by id, 0 if it is gone.
func (a *Arbiter) score(ms *meetState, id string, now time.Time) float64 {
	ps, ok := ms.members[id]
	if !ok {
		return 0
	}
	return Score(a.fitness(ps, now))
}

// elect runs the trigger table for one meet and returns whether it announced.
//
// Precedence is failure, then bootstrap, then the voluntary moves — and among the
// voluntary moves, demotion before promotion. Failure first because a meet with a
// dead coordinator is receiving nothing and every other consideration is a
// preference. Demotion before promotion because the two produce the same ACTION but a
// different Reason, and Reason is operator-facing: an incumbent below the floor is a
// problem whether or not a challenger happens to exist, so the absolute fact outranks
// the relative one.
func (a *Arbiter) elect(ms *meetState, now time.Time) bool {
	if ms == nil {
		return false
	}
	// An empty meet has nobody to coordinate and nobody to announce to. Dropping the
	// retained announcement (rather than announcing a vacancy, which is the right move
	// while members remain) is safe precisely because there is no one left to mislead,
	// and it stops the next joiner being fenced to a departed coordinator.
	if len(ms.members) == 0 {
		ms.abandon()
		ms.clearPending()
		return false
	}

	cands := a.candidates(ms, now)

	// Failure. A peer coordinator that has left, or that the arbiter can no longer
	// hear, is replaced immediately: no dwell, no minimum term.
	if ms.who.kind == holderPeer {
		ps, ok := ms.members[ms.who.id]
		if !ok || !a.live(ps, now) {
			ms.clearPending()
			a.log.Warn("coordinator lost",
				slog.String("meet_id", ms.id), slog.String("coordinator", ms.who.name))
			if len(cands) > 0 {
				return a.announce(ms, peerHolder(cands[0]), ReasonFailover, now)
			}
			if a.cfg.Coordinate {
				return a.announce(ms, holder{kind: holderArbiter}, ReasonFailover, now)
			}
			// Nobody can hold the role. Announce the VACANCY rather than going
			// silent: silence cannot tell a live-but-demoted coordinator to stop, and
			// a joiner fenced to nothing rejects everything forever. A bumped epoch
			// naming nobody does both jobs, and the meet keeps running on the tree it
			// already realized — the data plane does not need a coordinator.
			return a.announce(ms, holder{}, ReasonVacated, now)
		}
	}

	// Bootstrap.
	if !ms.hasHolder() {
		ms.clearPending()
		switch {
		case a.cfg.Coordinate:
			return a.announce(ms, holder{kind: holderArbiter}, ReasonBootstrap, now)
		case a.cfg.Elect && len(cands) > 0:
			return a.announce(ms, peerHolder(cands[0]), ReasonBootstrap, now)
		default:
			return false
		}
	}

	// Everything below is a VOLUNTARY handover and requires -elect.
	if !a.cfg.Elect {
		ms.clearPending()
		return false
	}

	// The best challenger is the fittest eligible peer that is not the incumbent.
	var chal *candidate
	for i := range cands {
		if cands[i].id != ms.who.id {
			chal = &cands[i]
			break
		}
	}
	if chal == nil {
		ms.clearPending()
		return false
	}

	want := a.wantedMove(ms, chal.score, now)
	if want == "" {
		ms.clearPending()
		return false
	}

	// Dwell: the same wanted move, at the same target, sustained. A change of either
	// restarts the clock, which is what stops a flapping metric from moving the role.
	if ms.pendingReason != want || ms.pendingTarget != chal.id {
		ms.pendingReason, ms.pendingTarget, ms.pendingSince = want, chal.id, now
	}
	if now.Sub(ms.pendingSince) < a.dwell {
		return false
	}
	// The term floor applies only where flapping is possible, i.e. peer to peer. The
	// arbiter does not compete for the role, so its handover to the first fit peer
	// happens at most once per meet and is not gated.
	if ms.who.kind == holderPeer && now.Sub(ms.termStart) < a.minTerm {
		return false
	}
	return a.announce(ms, peerHolder(*chal), want, now)
}

// peerHolder lifts a ranked candidate into a holder.
func peerHolder(c candidate) holder {
	return holder{kind: holderPeer, id: c.id, name: c.name}
}

// wantedMove decides which voluntary transition the current evidence argues for, or
// "" for none. It does not consider the dwell or the term floor; those are brakes on
// acting, not on wanting.
func (a *Arbiter) wantedMove(ms *meetState, chalScore float64, now time.Time) Reason {
	if ms.who.kind == holderArbiter {
		// The arbiter is a stand-in, not a peer, so it has no Fitness and the margin
		// rule cannot apply to it. It holds the meet until a peer is good enough to
		// take over on its own merits — the absolute floor, not a margin.
		if chalScore > a.demoteBelow {
			return ReasonPromotion
		}
		return ""
	}
	inc := a.score(ms, ms.who.id, now)
	switch {
	case inc < a.demoteBelow && chalScore > inc:
		// Below the floor, and there is somewhere better to go. Requiring a strictly
		// fitter replacement is what stops a meet of uniformly bad peers from
		// churning the role forever: a bad coordinator beats no coordinator.
		return ReasonDemotion
	case chalScore-inc > a.promoteMargin:
		return ReasonPromotion
	default:
		return ""
	}
}

// announce mints the next epoch for this meet and broadcasts it. It returns false
// without minting when the named holder already holds the role, so an idempotent
// caller — a forced election naming the incumbent, or a second membership change while
// a meet is already vacant — cannot burn an epoch.
//
// Note what does NOT happen on a broadcast failure: the epoch is not rolled back.
// Reissuing a consumed epoch is the one thing that could put two coordinators in the
// same term, which is the only state the fence cannot survive. A failed broadcast is
// repaired instead by the next membership change, or by the fence-repair path once the
// lagging peer's next heartbeat arrives.
func (a *Arbiter) announce(ms *meetState, h holder, reason Reason, now time.Time) bool {
	if ms.hasAnn && ms.who == h {
		return false
	}
	prev := ms.who.name

	ms.epoch++
	ms.elections++
	ms.who = h
	ms.termStart = now
	ms.clearPending()

	coordID := h.id
	if h.kind == holderArbiter {
		coordID = a.self
	}
	ann := Announcement{
		RoomID:        ms.id,
		Epoch:         ms.epoch,
		Coordinator:   h.name,
		CoordinatorID: coordID,
		Prev:          prev,
		Reason:        reason,
		Config:        a.cfg.CoordinatorConfig,
		IssuedAt:      now,
	}
	ms.lastAnn, ms.hasAnn = ann, true

	a.log.Info("coordinator announced",
		slog.String("meet_id", ms.id),
		slog.Uint64("epoch", ann.Epoch),
		slog.String("coordinator", ann.CoordinatorID),
		slog.String("reason", string(reason)))

	if err := a.ann.AnnounceCoordinator(ms.id, ann); err != nil {
		// Transient by contract. The epoch stands; the next membership change or the
		// first lagging heartbeat re-delivers it.
		a.log.Warn("announcement broadcast failed",
			slog.String("meet_id", ms.id), slog.Uint64("epoch", ann.Epoch), slog.Any("error", err))
	}
	if a.pub != nil {
		a.pub.PublishElection(ann)
	}
	return true
}

// reannounce replays the standing announcement, unchanged, after a membership change.
//
// This is the fix for the quietest failure in the system: a peer joining a meet that
// already had a coordinator was never told who it was, so it sat at epoch 0 rejecting
// every topology push forever — receiving no media, while the dashboard rendered it
// healthy and heartbeating.
//
// It is broadcast rather than unicast to the newcomer because broadcasting is
// idempotent and therefore self-healing: a peer already at that epoch ignores it at
// zero cost, while ANY peer that has lost its fence is repaired by the next membership
// change. It replays a VACANCY too — a vacancy is the true state, and fencing a joiner
// to it is strictly better than fencing it to nothing.
func (a *Arbiter) reannounce(ms *meetState) {
	if ms == nil || !ms.hasAnn {
		return
	}
	if err := a.ann.AnnounceCoordinator(ms.id, ms.lastAnn); err != nil {
		a.log.Warn("re-announcement failed",
			slog.String("meet_id", ms.id), slog.Uint64("epoch", ms.lastAnn.Epoch), slog.Any("error", err))
	}
}

// checkFence repairs a peer whose epoch has fallen behind the meet's.
//
// This closes the hole that re-announce-on-membership-change leaves open: if the
// broadcast is lost — the Announcer errors, or the Hub drops the frame on a full send
// buffer — the peer stays fenced out, rejecting every push and receiving nothing,
// until somebody happens to join or leave. In a static meet that is indefinite, which
// makes packet loss a route to the same critical failure as never announcing at all.
//
// Three properties, each load-bearing:
//
//   - VERBATIM. The peer must adopt exactly the announcement it missed. Minting a
//     fresh one with a different Reason would leave two peers disagreeing about why
//     the current coordinator holds the role.
//   - UNICAST. Only the lagging peer needs it; broadcasting would turn one wedged peer
//     into N frames per second.
//   - NO CAP, NO BACKOFF. The condition is indefinite, so the repair must be. It costs
//     ~150 bytes per lagging peer per second, which is nothing beside a 2 Mbit/s video
//     stream — whereas giving up leaves a participant permanently dark.
//
// The RebuildWindow guard suppresses repair during the legitimate propagation window
// right after an election, so a normal handover never triggers one. The dashboard
// event, unlike the wire frame, fires only on the TRANSITION into and out of lagging.
func (a *Arbiter) checkFence(ms *meetState, ps *peerState, now time.Time) {
	if ms == nil || ps == nil || !ms.hasAnn {
		return
	}
	behind := ps.beat.Epoch < ms.lastAnn.Epoch
	settled := now.Sub(ms.termStart) >= metrics.RebuildWindow

	switch {
	case behind && settled:
		if err := a.ann.RepairCoordinator(ms.id, ps.id, ms.lastAnn); err != nil {
			a.log.Debug("fence repair send failed",
				slog.String("meet_id", ms.id), slog.String("name", ps.name), slog.Any("error", err))
		}
		if ps.lagging {
			a.log.Debug("fence still lagging",
				slog.String("meet_id", ms.id), slog.String("name", ps.name),
				slog.Uint64("peer_epoch", ps.beat.Epoch), slog.Uint64("meet_epoch", ms.lastAnn.Epoch))
			return
		}
		ps.lagging = true
		a.log.Info("peer fence is behind; repairing",
			slog.String("meet_id", ms.id), slog.String("name", ps.name),
			slog.Uint64("peer_epoch", ps.beat.Epoch), slog.Uint64("meet_epoch", ms.lastAnn.Epoch))
		if a.pub != nil {
			a.pub.PublishRepair(ms.id, ps.name, ps.beat.Epoch, ms.lastAnn.Epoch, false)
		}
	case !behind && ps.lagging:
		// Only a genuine catch-up clears the latch. Note this is deliberately NOT
		// gated on settled: an election resets termStart, and a peer that has adopted
		// the new epoch has caught up regardless of how recently the term began.
		ps.lagging = false
		a.log.Info("peer fence recovered",
			slog.String("meet_id", ms.id), slog.String("name", ps.name),
			slog.Uint64("epoch", ps.beat.Epoch))
		if a.pub != nil {
			a.pub.PublishRepair(ms.id, ps.name, ps.beat.Epoch, ms.lastAnn.Epoch, true)
		}
	}
}

// checkVolunteers warns, once per episode, when a meet has telemetry in hand and not
// one peer willing to be elected.
//
// This is the difference between failing closed and failing closed SILENTLY. With no
// volunteer every peer scores 0, no election ever fires, and the meet sits on the
// arbiter forever — which is the correct direction to fail in, but is indistinguishable
// from a bug unless something says so. The two states are separable because eligibility
// is reported && Coordinatable: "nobody has spoken yet" (no reports) is a young meet and
// must stay quiet; "everyone spoke and all said no" is a configuration an operator can
// act on.
//
// Latched, like the fence repair and the health FSM, so it is one line per episode
// rather than one per telemetry tick.
func (a *Arbiter) checkVolunteers(ms *meetState) {
	if ms == nil || !a.cfg.Elect {
		return
	}
	reported, volunteer := false, false
	for _, ps := range ms.members {
		if !ps.reported {
			continue
		}
		reported = true
		if ps.report.Coordinatable {
			volunteer = true
			break
		}
	}
	state := reported && !volunteer
	if state && !ms.noVolunteers {
		a.log.Warn("no peer has volunteered to coordinate; election cannot run",
			slog.String("meet_id", ms.id), slog.Int("members", len(ms.members)))
	}
	ms.noVolunteers = state
}
