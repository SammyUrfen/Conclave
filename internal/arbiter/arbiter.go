package arbiter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/metrics"
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
	// It does NOT apply to the failure path — a dead coordinator is replaced
	// instantly regardless. One minute makes a flapping-election bug obvious (it
	// becomes a once-a-minute event in the dashboard) rather than invisible (a
	// hundred handovers a second).
	MinTermDuration = 60 * time.Second

	// RebuildWindow bounds how long a freshly promoted coordinator waits for peers
	// to report in before it publishes its first tree. 3 s is about three heartbeats
	// plus a metrics tick, enough for every present peer to have spoken once.
	//
	// It is declared here because it is part of the election contract, but note that
	// the coordinator CANNOT consume it: the dependency graph forbids
	// coordinator -> arbiter. The coordinator declares its own copy; see the report
	// accompanying this package.
	RebuildWindow = 3 * time.Second

	// DefaultArbiterID is the reserved coordinator id used when the arbiter itself
	// holds the role. It must equal signaling.ServerID, and it is a literal here
	// only because the dependency graph forbids arbiter -> signaling. cmd/server
	// passes signaling.ServerID into Config.ArbiterID explicitly so the two cannot
	// drift in production; this default exists for tests and for a zero Config.
	DefaultArbiterID = "_server"
)

// MeetIDPattern constrains a meet id at the boundary rather than escaping it at
// every use: an id is interpolated into a `?room=` query value AND into a URL path
// segment, so it is restricted to characters unambiguous in both. The 64-character
// bound is the longest single DNS label — far above any human-typed room name and
// far below anything that could bloat a log line.
//
// This duplicates the signaling package's identical rule, which is a known defect
// rather than a choice: the shared internal/policy leaf that both surfaces are meant
// to delegate to has not landed yet. Wire Config.ValidMeetID to policy.ValidMeetID
// and delete this the moment it does.
const MeetIDPattern = `^[a-z0-9][a-z0-9_-]{0,63}$`

var meetIDRe = regexp.MustCompile(MeetIDPattern)

// The errors a caller is expected to branch on. The dashboard maps them to HTTP
// statuses, which is why they are values and not strings: the mapping belongs at the
// boundary, and a boundary that has to match on message text is a boundary that
// breaks when someone improves a message.
var (
	// ErrInvalidMeetID: the id does not match MeetIDPattern. HTTP 400.
	ErrInvalidMeetID = errors.New("arbiter: invalid meet id")
	// ErrMeetExists: a meet with that id is already registered. HTTP 409.
	ErrMeetExists = errors.New("arbiter: meet already exists")
	// ErrNoSuchMeet: no meet with that id. HTTP 404.
	ErrNoSuchMeet = errors.New("arbiter: no such meet")
	// ErrNoCandidate: the requested peer is not a member, or no peer can hold the
	// role. HTTP 409 on the demo surface.
	ErrNoCandidate = errors.New("arbiter: no eligible coordinator candidate")
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
	// MeetIDPattern. It is injected rather than imported so that when internal/policy
	// lands there is exactly one matcher in the process, not two that drift.
	ValidMeetID func(id string) bool
	// NewMeetID generates an id for a create request that did not supply one. nil
	// means a monotonic "meet-N", which is deterministic on purpose: a random id
	// would make a replayed scenario produce different meets.
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

	report        metrics.Report
	coordinatable bool
	beat          metrics.Heartbeat

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
	meets   map[string]*meetState
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
		a.validMeetID = meetIDRe.MatchString
	}
	a.newMeetID = cfg.NewMeetID
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
	// metrics.Report does not (yet) carry the coordinatable declaration, so it is
	// read alongside it from the same frame. Collapse this into metrics.Report as
	// soon as that field lands; the JSON key is identical either way.
	var wire struct {
		metrics.Report
		Coordinatable bool `json:"coordinatable"`
	}
	if err := json.Unmarshal(payload, &wire); err != nil {
		a.log.Debug("bad metrics payload", slog.String("peer_id", peerID), slog.Any("error", err))
		return
	}
	a.enqueue(event{
		kind: evReport, roomID: roomID, peerID: peerID,
		report: wire.Report, coordinatable: wire.Coordinatable,
	})
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

// GetMeet returns one meet, or ErrNoSuchMeet.
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
		return Meet{}, fmt.Errorf("%w: %q", ErrNoSuchMeet, id)
	}
	return out, nil
}

// CreateMeet registers an empty meet. An empty id is generated rather than rejected,
// because the dashboard's "create a meet" button has no id to offer.
func (a *Arbiter) CreateMeet(ctx context.Context, id string) (Meet, error) {
	var out Meet
	var cerr error
	if err := a.query(ctx, func() {
		if id == "" {
			id = a.generateMeetID()
		}
		if !a.validMeetID(id) {
			cerr = fmt.Errorf("%w: %q must match %s", ErrInvalidMeetID, id, MeetIDPattern)
			return
		}
		if _, ok := a.meets[id]; ok {
			cerr = fmt.Errorf("%w: %q", ErrMeetExists, id)
			return
		}
		ms := newMeetState(id, a.clk.Now())
		a.meets[id] = ms
		a.log.Info("meet created", slog.String("meet_id", id))
		out = ms.snapshot(a.self)
	}); err != nil {
		return Meet{}, err
	}
	return out, cerr
}

// generateMeetID produces the next unused "meet-N". Deterministic on purpose: a
// random id would make an otherwise replayable scenario diverge. Runs on the Run
// goroutine, so the counter needs no synchronisation.
func (a *Arbiter) generateMeetID() string {
	for {
		a.meetSeq++
		id := fmt.Sprintf("meet-%d", a.meetSeq)
		if a.newMeetID != nil {
			id = a.newMeetID()
		}
		if _, taken := a.meets[id]; !taken {
			return id
		}
	}
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
			ferr = fmt.Errorf("%w: %q", ErrNoSuchMeet, roomID)
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
		return holder{id: cands[0].id, name: cands[0].name}, true
	}
	// Members are scanned in id order so a (impossible, but cheap to defend
	// against) duplicate name resolves the same way every time.
	for _, id := range sortedIDs(ms) {
		if ms.members[id].name == name {
			return holder{id: id, name: name}, true
		}
	}
	return holder{}, false
}

// handle applies one event to the owned state. Membership changes run an election
// and, if that produced nothing, re-announce the standing term — which is what stops
// a joiner from sitting at epoch 0 rejecting every push forever.
func (a *Arbiter) handle(ev event) {
	if ev.kind == evQuery {
		ev.run()
		close(ev.done)
		return
	}
	now := a.clk.Now()
	switch ev.kind {
	case evJoin:
		ms, ok := a.meets[ev.roomID]
		if !ok {
			ms = newMeetState(ev.roomID, now)
			a.meets[ev.roomID] = ms
		}
		ms.members[ev.peerID] = &peerState{
			id: ev.peerID, name: ev.name, joinedAt: now, lastSeen: now,
		}
		a.log.Debug("member joined", slog.String("meet_id", ms.id), slog.String("name", ev.name))
		if !a.elect(ms, now) {
			a.reannounce(ms)
		}
	case evLeave:
		ms, ok := a.meets[ev.roomID]
		if !ok {
			return
		}
		if _, ok := ms.members[ev.peerID]; !ok {
			return
		}
		delete(ms.members, ev.peerID)
		if !a.elect(ms, now) {
			a.reannounce(ms)
		}
	case evReport:
		ps := a.member(ev.roomID, ev.peerID)
		if ps == nil {
			return
		}
		ps.report, ps.coordinatable, ps.reported = ev.report, ev.coordinatable, true
		ps.lastSeen = now
		a.elect(a.meets[ev.roomID], now)
	case evBeat:
		ps := a.member(ev.roomID, ev.peerID)
		if ps == nil {
			return
		}
		ps.beat, ps.hasBeat = ev.beat, true
		ps.lastSeen = now
		a.elect(a.meets[ev.roomID], now)
	}
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

// holder names whoever is to hold the coordinator role.
type holder struct {
	id        string
	name      string
	isArbiter bool
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
		// A peer that has never reported is treated as unwilling. The wire field is
		// a bare bool, so absent reads as false, and that conservative direction is
		// deliberate: silently electing a peer that never opted in is worse than
		// leaving a meet uncoordinated for one more telemetry tick.
		Coordinatable: ps.reported && ps.coordinatable,
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
// preference. Demotion before promotion because when both apply the incumbent's
// unfitness is the more urgent fact, and it is the reason a human wants to read in
// the event log.
func (a *Arbiter) elect(ms *meetState, now time.Time) bool {
	if ms == nil {
		return false
	}
	// An empty meet has no coordinator and nobody to announce to. Clearing the
	// holder (rather than leaving it stale) is what makes the next joiner bootstrap
	// instead of adopting a term whose coordinator left.
	if len(ms.members) == 0 {
		ms.clearHolder()
		ms.clearPending()
		return false
	}

	cands := a.candidates(ms, now)

	// Failure. A peer coordinator that has left, or that the arbiter can no longer
	// hear, is replaced immediately: no dwell, no minimum term.
	if ms.coordID != "" {
		ps, ok := ms.members[ms.coordID]
		if !ok || !a.live(ps, now) {
			ms.clearPending()
			a.log.Warn("coordinator lost",
				slog.String("meet_id", ms.id), slog.String("coordinator", ms.coordName))
			if len(cands) > 0 {
				return a.announce(ms, holder{id: cands[0].id, name: cands[0].name}, ReasonFailover, now)
			}
			if a.cfg.Coordinate {
				return a.announce(ms, holder{isArbiter: true}, ReasonFailover, now)
			}
			// Nothing can hold the role. The meet keeps running on the tree it
			// already realized — the data plane does not need a coordinator — and
			// the next eligible peer to appear bootstraps a fresh term.
			ms.clearHolder()
			a.log.Warn("meet has no eligible coordinator", slog.String("meet_id", ms.id))
			return false
		}
	}

	// Bootstrap.
	if !ms.hasHolder() {
		ms.clearPending()
		switch {
		case a.cfg.Coordinate:
			return a.announce(ms, holder{isArbiter: true}, ReasonBootstrap, now)
		case a.cfg.Elect && len(cands) > 0:
			return a.announce(ms, holder{id: cands[0].id, name: cands[0].name}, ReasonBootstrap, now)
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
		if cands[i].id != ms.coordID {
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
	if now.Sub(ms.termStart) < a.minTerm {
		return false
	}
	return a.announce(ms, holder{id: chal.id, name: chal.name}, want, now)
}

// wantedMove decides which voluntary transition the current evidence argues for, or
// "" for none. It does not consider the dwell or the term floor; those are brakes on
// acting, not on wanting.
func (a *Arbiter) wantedMove(ms *meetState, chalScore float64, now time.Time) Reason {
	if ms.arbiterIsCoord {
		// The arbiter is a stand-in, not a peer, so it has no Fitness and the
		// margin rule cannot apply to it. It holds the meet until a peer is good
		// enough to take over on its own merits — the absolute floor, not a margin.
		if chalScore > a.demoteBelow {
			return ReasonPromotion
		}
		return ""
	}
	inc := a.score(ms, ms.coordID, now)
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
// caller (a forced election naming the incumbent) cannot burn an epoch.
//
// Note what does NOT happen on a broadcast failure: the epoch is not rolled back.
// Reissuing a consumed epoch is the one thing that could put two coordinators in the
// same term, which is the only state the fence cannot survive. A failed broadcast is
// repaired instead by the next membership change, which replays the announcement.
func (a *Arbiter) announce(ms *meetState, h holder, reason Reason, now time.Time) bool {
	if ms.hasHolder() && ms.coordID == h.id && ms.arbiterIsCoord == h.isArbiter {
		return false
	}
	prev := ms.coordName

	ms.epoch++
	ms.coordID, ms.coordName, ms.arbiterIsCoord = h.id, h.name, h.isArbiter
	ms.termStart = now
	ms.clearPending()

	coordID := h.id
	if h.isArbiter {
		coordID = a.self
	}
	ann := Announcement{
		RoomID:         ms.id,
		Epoch:          ms.epoch,
		Coordinator:    h.name,
		CoordinatorID:  coordID,
		Prev:           prev,
		Reason:         reason,
		IssuedAtUnixMs: now.UnixMilli(),
	}
	ms.lastAnn, ms.hasAnn = ann, true

	a.log.Info("coordinator elected",
		slog.String("meet_id", ms.id),
		slog.Uint64("epoch", ann.Epoch),
		slog.String("coordinator", ann.CoordinatorID),
		slog.String("reason", string(reason)))

	if err := a.ann.AnnounceCoordinator(ms.id, ann); err != nil {
		// Transient by contract. The epoch stands; the next membership change
		// re-broadcasts it.
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
// zero cost, while ANY peer that has lost its fence — a rejoin, a missed frame, a
// partition that healed — is repaired by the next membership change. One mechanism
// covering the known case and the unknown ones beats a targeted fix for the known one.
func (a *Arbiter) reannounce(ms *meetState) {
	if !ms.hasAnn || !ms.hasHolder() {
		return
	}
	if err := a.ann.AnnounceCoordinator(ms.id, ms.lastAnn); err != nil {
		a.log.Warn("re-announcement failed",
			slog.String("meet_id", ms.id), slog.Uint64("epoch", ms.lastAnn.Epoch), slog.Any("error", err))
	}
}
