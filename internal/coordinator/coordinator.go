package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// Sender is how the coordinator pushes a computed tree to a peer. It is
// consumer-defined here (the coordinator says what it needs, not how signaling
// provides it) so this package never imports signaling — cmd/server supplies an
// adapter over the Hub.
//
// SendTopology MUST NOT BLOCK and MUST NOT perform network I/O on the calling
// goroutine. The existing hub adapter already satisfies this: hub.SendTo writes to a
// 32-deep buffered channel and drops on full rather than blocking. The requirement
// is stated on the interface so a future adapter (an HTTP push, a retrying sender)
// cannot quietly reintroduce a stall. An implementation that must do real I/O
// enqueues and returns.
//
// Defence in depth: this package calls SendTopology from a DEDICATED outbound
// goroutine, never from the Run loop, so an adapter that violates the rule degrades
// one meet's push latency instead of freezing every meet in the process.
//
// A returned error is transient and never fatal (the peer may have just left).
type Sender interface {
	SendTopology(roomID, peerID string, topo *overlay.Topology) error
}

// Member is one meet participant as the coordinator's membership seam sees it. It
// is coordinator-owned precisely so SetRoster adds no import: cmd/peer translates
// []signaling.Peer into []Member at the edge.
type Member struct {
	ID   string
	Name string
}

// eventBuffer is how many membership/telemetry events may queue before an enqueuing
// Hub goroutine blocks. Generous: events are tiny and the Run loop drains them
// quickly, so this is only a cushion for a burst of simultaneous joins.
const eventBuffer = 256

// sendBuffer is how deep the outbound queue is before a push is dropped.
//
// 1024 is ~one full fan-out for 100 meets of 10 peers, i.e. far beyond anything this
// system runs. Dropping rather than blocking is the deliberate choice: a wedged
// adapter must degrade to "this peer misses a tree" (self-healing, since every push
// is a complete state snapshot and the next threshold event re-pushes) rather than
// to "every meet in the process stops".
const sendBuffer = 1024

// maxDeadlineRounds bounds one advance pass. Every deadline this package arms is
// strictly in the future once it has been fired, so the loop below always
// terminates; the bound exists so a future handler that re-arms at a non-positive
// delay fails loudly instead of spinning a goroutine forever. A wedged test is
// strictly worse than a failing one.
const maxDeadlineRounds = 10_000

// Coordinator ingests room membership and telemetry, computes a forwarding tree on
// every threshold event, and pushes it to the peers.
//
// Concurrency model: ALL state lives in one goroutine (Run), and every input (peer
// joined/left, a metrics report, a heartbeat, a reparent, a roster, an epoch) is
// funnelled to it as an event on a channel. That is the "share memory by
// communicating" side of the trade-off, chosen over a mutex-guarded shared map
// because the coordinator is a control loop, not a hot path: one owner serialising
// everything means there are no locks to reason about and a recompute always sees a
// consistent snapshot. The alternative — an RWMutex around state read by many hub
// goroutines — would be faster under contention we do not have and harder to keep
// correct.
//
// Two things deliberately do NOT run on that goroutine:
//
//   - Outbound sends, which run on their own goroutine behind a bounded queue, so a
//     blocking Sender cannot stall the control plane (see Sender).
//   - Nothing else. In particular every timer in this package is multiplexed onto
//     ONE clock.Timer (see advance), because a dynamic set of timer channels cannot
//     be selected on, and because a single wake channel is what makes Sync's drain
//     — and therefore the whole deterministic test strategy — provably sound.
type Coordinator struct {
	log *slog.Logger
	cfg Config
	pub Publisher

	events chan event
	done   chan struct{} // closed when Run returns; makes enqueue non-blocking after shutdown

	// The outbound plane. sendQ is drained by one goroutine started in Run.
	send  Sender
	sendQ chan sendOp

	// Everything below is owned solely by the Run goroutine.
	rooms map[string]*roomState

	// wake is the single multiplexed deadline timer. wakeArmed/wakeAt track what it
	// is currently armed for so a re-arm that would not move the deadline is
	// skipped — which keeps the virtual clock's tiebreak sequence stable across
	// events that change nothing.
	wake      clock.Timer
	wakeArmed bool
	wakeAt    time.Time
	dropped   uint64 // outbound pushes discarded because sendQ was full
}

// roomState is one meet's control state. The three trees of the contract are kept
// distinct here, and conflating any two is a silent correctness bug rather than a
// compile error:
//
//   - published — the last tree actually SENT to peers. It carries the (Epoch, Rev)
//     peers are fencing against and, load-bearing, the Backups assignment they
//     promoted under. It is ValidateLocalRepair's `prev` and the dashboard's
//     reference.
//   - working — published, patched with everything the coordinator already knows
//     changed: every ratified promotion and the removal of every departed/gone node.
//     Derived fresh each round (see deriveWorking) and never sent. It is
//     BuildTree's `prev`.
//   - next — this round's output; on publish it BECOMES published.
//
// The asymmetry is the point: the BUILDER is told what peers have already DONE, so
// stickiness protects it; the ORACLE is told what peers were last INSTRUCTED to do,
// so it can check that what they did was allowed.
type roomState struct {
	id    string
	nodes map[string]*nodeState // keyed by server-assigned peer id

	epoch uint64
	rev   uint64 // the last PUBLISHED rev in this epoch; 0 before the first tree

	// serving is whether THIS node is the coordinator for this meet. It defaults to
	// true, which is the Phase 5 arrangement — the coordinator runs inside the arbiter
	// process and is simply told which term it serves. Yield clears it; adopting a
	// strictly higher epoch sets it again.
	serving bool
	// gate is the current term's outbound gate. Replaced on every epoch adoption and
	// ended on Yield, so a push computed under a term that has ended never leaves.
	gate *term

	// The rebuild window (§6.6). rebuilding is set on adopting a new epoch and holds
	// every publication until either every present member has been HEARD FROM or
	// rebuildUntil passes. baseline is the stickiness baseline reconstructed from
	// those heartbeats, consumed by the first build of the term.
	rebuilding   bool
	rebuildUntil time.Time
	heard        map[string]bool
	baseline     *overlay.Topology
	// handoverNote records WHY the baseline is missing, when it is missing for a bad
	// reason. It rides the term's first EventTopology, because that tree — the one
	// where everybody moves — is exactly where the explanation is needed.
	handoverNote string

	published *overlay.Topology
	working   *overlay.Topology

	// promotions maps a node name to the parent it re-parented ITSELF onto and the
	// coordinator has ratified but not yet folded into a published tree.
	promotions map[string]string

	// settleUntil is the end of the current membership-growth episode's settle
	// window. In the past (or zero) means no window is running.
	settleUntil time.Time
	// settlingNow records that the meet is currently ineligible, so EventSettling
	// is emitted once per TRANSITION into ineligibility rather than per suppressed
	// event.
	settlingNow bool

	// urgent survives across suppressed recomputes until the next publish. Urgency
	// is a property of the ROOM's pending state, not of one event: a join arms the
	// settle, so the build that finally places the joiner happens on a LATER event
	// (its first report, or the settle timer) which would otherwise be cooled down
	// for up to RecomputeCooldown — the 5s of black screen the bypass exists to
	// prevent.
	urgent bool

	lastBuild time.Time
	built     bool
	dirty     bool // a recompute was suppressed by the cooldown and must run when it closes
}

// nodeState is the coordinator's record of one peer.
type nodeState struct {
	id       string
	name     string
	report   metrics.Report
	reported bool // gates the "first report is a threshold event" rule

	health Health
	// lastSeen is the arrival instant of the peer's last frame OF ANY KIND. All
	// three inbound frames are proof of a live socket, so all three refresh it.
	lastSeen time.Time
	// interval is the cadence the peer DECLARED on its heartbeats. Zero means it
	// has not said, and the thresholds fall back to metrics.HeartbeatInterval.
	interval time.Duration

	// dwellSince is when this node's telemetry first crossed a degradation
	// threshold without recovering. Zero means it is not currently bad.
	dwellSince time.Time
	// impaired is set when the dwell FIRES: an evidence-backed classification of
	// sustained degradation, not one bad sample. It flows into overlay.Node.
	impaired bool

	// Realized state, from the peer's last heartbeat — what it has actually
	// connected, as opposed to what the coordinator believes it told it to.
	beatSeq      uint64
	lastBeatAt   time.Time
	realParent   string
	realChildren []string
}

// event kinds funnelled to the Run goroutine.
type eventKind int

const (
	evJoin eventKind = iota
	evLeave
	evReport
	evBeat
	evReparent
	evRoster
	evEpoch
	evYield
	evSnapshot
	evSync
)

type event struct {
	kind   eventKind
	roomID string
	peerID string
	name   string
	report metrics.Report
	beat   metrics.Heartbeat
	rep    metrics.Reparented
	roster []Member
	epoch  uint64
	snap   chan RoomSnapshot
	ack    chan struct{}
}

// sendOp is one item on the outbound queue: either a topology push or the Sync
// marker that makes Sync a barrier over the outbound plane as well as the loop.
type sendOp struct {
	roomID string
	peerID string
	topo   *overlay.Topology
	gate   *term         // nil on a barrier marker; otherwise the term that produced this push
	ack    chan struct{} // non-nil ⇒ this is a barrier marker, not a push
}

// term is one coordinator term for one meet, reduced to the single fact the OUTBOUND
// plane needs: is this term still current. Every push carries a pointer to the term
// that computed it, and the sender drops any push whose term has ended.
//
// This is the coordinator's half of the ambiguity window (§6.7). The contract's safety
// argument is that a fenced peer refuses a stale-epoch push, and that argument holds
// with or without this gate — but "the peer will reject it anyway" is a poor reason to
// send it. A push already inside the network call cannot be recalled, so it is
// delivered (and fenced); everything still queued behind it is dropped here.
//
// An atomic rather than a mutex because there is exactly one writer (the Run goroutine,
// on Yield or on adopting a new epoch), one reader (the sender goroutine), and the
// value is one bit. A mutex would be a heavier expression of the same thing and would
// put a lock back into a design whose whole point is not having one.
type term struct{ ended atomic.Bool }

func (t *term) end()          { t.ended.Store(true) }
func (t *term) isEnded() bool { return t.ended.Load() }

// New builds a Coordinator that pushes through send and reports through pub. Either
// may be nil for a coordinator that only computes (pub nil means "publish
// nothing"). Run must be called to start the owning goroutines; until then events
// buffer.
func New(log *slog.Logger, cfg Config, send Sender, pub Publisher) *Coordinator {
	return &Coordinator{
		log:    log.With(slog.String("component", "coordinator")),
		cfg:    normalize(cfg),
		pub:    pub,
		send:   send,
		sendQ:  make(chan sendOp, sendBuffer),
		events: make(chan event, eventBuffer),
		done:   make(chan struct{}),
		rooms:  make(map[string]*roomState),
	}
}

// Run owns all coordinator state until ctx is cancelled, processing one event at a
// time. Callers run it on its own goroutine; it returns ctx.Err() on shutdown.
//
// The select has exactly two live inputs — the event channel and the single
// multiplexed deadline timer — and the sync branch DRAINS the timer before acking.
// That drain is not an optimisation: a fired timer and a sync arriving at one parked
// select are resolved by Go's uniform-random choice, so a loop that acked without
// draining could report quiescent while the reaction the barrier exists to wait for
// had not happened. That is a permanently-flaky-test generator, and it gets blamed
// on the harness.
func (c *Coordinator) Run(ctx context.Context) error {
	defer close(c.done)

	c.wake = c.cfg.Clock.NewTimer(time.Hour)
	c.wake.Stop()
	c.wakeArmed = false

	sendDone := make(chan struct{})
	stopSender := make(chan struct{})
	go c.runSender(stopSender, sendDone)
	defer func() {
		close(stopSender)
		<-sendDone
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.wake.C():
			c.wakeArmed = false
			c.advance()
		case ev := <-c.events:
			c.handle(ev)
		}
	}
}

// runSender drains the outbound queue on its own goroutine. It is the reason a
// blocking Sender cannot stall the control loop.
func (c *Coordinator) runSender(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-stop:
			return
		case op := <-c.sendQ:
			if op.ack != nil {
				close(op.ack)
				continue
			}
			if c.send == nil {
				continue
			}
			if op.gate != nil && op.gate.isEnded() {
				// This node is no longer the coordinator for that meet, or is now
				// serving a later term. §6.7.
				c.log.Debug("dropped a push from an ended term",
					slog.String("room_id", op.roomID), slog.String("peer_id", op.peerID))
				continue
			}
			if err := c.send.SendTopology(op.roomID, op.peerID, op.topo); err != nil {
				// Transient by contract: the peer may have just left.
				c.log.Debug("push topology failed",
					slog.String("room_id", op.roomID), slog.String("peer_id", op.peerID),
					slog.Any("error", err))
			}
		}
	}
}

// enqueueSend hands one push to the outbound goroutine, dropping it if the queue is
// full rather than blocking the Run loop. See sendBuffer for why dropping is the
// right degradation.
func (c *Coordinator) enqueueSend(op sendOp) bool {
	select {
	case c.sendQ <- op:
		return true
	default:
		c.dropped++
		c.log.Warn("outbound queue full; dropped a topology push",
			slog.String("room_id", op.roomID), slog.String("peer_id", op.peerID),
			slog.Uint64("dropped_total", c.dropped))
		return false
	}
}

// enqueue hands an event to the Run goroutine, giving up if the coordinator has shut
// down so a late hub callback can never block forever on a drained loop.
func (c *Coordinator) enqueue(ev event) {
	select {
	case c.events <- ev:
	case <-c.done:
	}
}

// PeerJoined reports a new member. It is part of the signaling.Observer surface,
// satisfied STRUCTURALLY so this package never imports signaling; it is called from
// Hub goroutines and only enqueues.
func (c *Coordinator) PeerJoined(roomID, peerID, name string) {
	c.enqueue(event{kind: evJoin, roomID: roomID, peerID: peerID, name: name})
}

// PeerLeft reports a member whose socket has closed. It is the ONLY input that
// deletes a member's record — a gone verdict never does, because the peer may come
// back on the same socket (see Heartbeat).
func (c *Coordinator) PeerLeft(roomID, peerID string) {
	c.enqueue(event{kind: evLeave, roomID: roomID, peerID: peerID})
}

// Metrics delivers one peer's telemetry. A node's FIRST report is a threshold event
// (it is when the node's real capacity is learned); later reports refresh the stored
// numbers without rebuilding, or a chatty sensor would thrash the tree.
func (c *Coordinator) Metrics(roomID, peerID string, payload []byte) {
	var rep metrics.Report
	if err := json.Unmarshal(payload, &rep); err != nil {
		c.log.Debug("bad metrics payload", slog.String("peer_id", peerID), slog.Any("error", err))
		return
	}
	c.enqueue(event{kind: evReport, roomID: roomID, peerID: peerID, report: rep})
}

// Heartbeat delivers one peer's liveness beat and its REALIZED topology state.
//
// RESURRECTION — the coordinator's half of the two-party contract documented on
// signaling.Observer.Heartbeat. The Hub holds no health state and invokes this for
// every LIVE socket regardless of what the control plane believes, so a beat from a
// peer this coordinator has declared gone — or has no record of at all — is PROOF
// that peer is back, and is treated as a re-join. Dropping it is how a 9-second
// Wi-Fi roam permanently ejects a peer from a meet it believes it is still in, with
// no error anywhere and a dashboard cheerfully showing the meet healthy.
func (c *Coordinator) Heartbeat(roomID, peerID string, payload []byte) {
	var hb metrics.Heartbeat
	if err := json.Unmarshal(payload, &hb); err != nil {
		c.log.Debug("bad heartbeat payload", slog.String("peer_id", peerID), slog.Any("error", err))
		return
	}
	hb.Normalize()
	c.enqueue(event{kind: evBeat, roomID: roomID, peerID: peerID, beat: hb})
}

// Reparented delivers a peer's report that it changed its own parent without being
// told to. The coordinator RATIFIES a successful promotion (patching its working
// copy so stickiness defends the peer's choice) and treats a failed one — a stranded
// peer receiving nothing — as an urgent threshold event.
func (c *Coordinator) Reparented(roomID, peerID string, payload []byte) {
	var rp metrics.Reparented
	if err := json.Unmarshal(payload, &rp); err != nil {
		c.log.Debug("bad reparented payload", slog.String("peer_id", peerID), slog.Any("error", err))
		return
	}
	c.enqueue(event{kind: evReparent, roomID: roomID, peerID: peerID, rep: rp})
}

// SetEpoch tells the coordinator which term it is serving FOR ONE MEET.
//
// The roomID parameter is not optional: Epoch is minted per meet, so a
// process-global setter would stamp both meets of a two-meet server with whichever
// epoch was set last and silently mis-fence every peer in the other one. A call with
// a HIGHER epoch resets that meet's Rev to 0, so its next published tree is rev 1.
// A coordinator never raises its own epoch; it only adopts what the arbiter minted.
func (c *Coordinator) SetEpoch(roomID string, epoch uint64) {
	c.enqueue(event{kind: evEpoch, roomID: roomID, epoch: epoch})
}

// SetRoster reconciles this coordinator's membership for roomID against the
// arbiter's authoritative roster. Members in the roster but unknown here are added
// as provisional (and count as joins, settle included); members known here but
// absent from the roster are treated as left.
//
// It is the seam that makes "every member the arbiter says is present" reachable
// from an ELECTED PEER coordinator, which has no Hub callbacks of its own.
func (c *Coordinator) SetRoster(roomID string, members []Member) {
	c.enqueue(event{kind: evRoster, roomID: roomID, roster: append([]Member(nil), members...)})
}

// Yield stops the coordinator from publishing anything further for roomID: it is no
// longer the coordinator there. Idempotent.
//
// newEpoch is the term under which this node was replaced, and it is not decoration —
// it raises the meet's epoch floor, so the announcement that DEMOTED this node can
// never also be the one that resumes it. Only a strictly higher SetEpoch does that.
//
// What it drops and what it keeps is the whole design of the epoch boundary: this node
// stops being authoritative about the TREE, which was a belief, so the tree is
// discarded and every push still queued for the old term is dropped. It does NOT stop
// being a process watching the meet, so telemetry, health, and membership — all of
// which came off the wire and are still true — survive. Discarding those would lose
// real observations and make a re-election pay a full settle for nothing.
//
// A yielded coordinator therefore keeps running its health FSM and keeps answering
// Snapshot: the dashboard still reads it, and a re-elected node starts warm. Only the
// tree stops.
func (c *Coordinator) Yield(roomID string, newEpoch uint64) {
	c.enqueue(event{kind: evYield, roomID: roomID, epoch: newEpoch})
}

// Sync blocks until every event enqueued before the call has been processed AND
// every push those events produced has been handed to the Sender. It is the
// deterministic-testing barrier and the quiescence primitive the dashboard uses
// before serving a snapshot. Returns ctx.Err() if the context ends first.
//
// It rides two queues in order — the event channel, then the outbound queue — which
// is what makes an assertion about what was PUSHED safe and not merely an assertion
// about what was decided. Snapshot deliberately rides only the first, so it still
// answers while the outbound plane is wedged.
func (c *Coordinator) Sync(ctx context.Context) error {
	ack := make(chan struct{})
	select {
	case c.events <- event{kind: evSync, ack: ack}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errStopped
	}
	select {
	case <-ack:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errStopped
	}

	sendAck := make(chan struct{})
	select {
	case c.sendQ <- sendOp{ack: sendAck}:
	default:
		// The outbound queue is full, which means the Sender is wedged. Acking here
		// keeps the control plane answerable instead of inheriting the stall; the
		// barrier then covers the loop only, which is the honest degradation.
		return nil
	}
	select {
	case <-sendAck:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errStopped
	}
}

// errStopped is returned by Sync and Snapshot once Run has exited.
var errStopped = errors.New("coordinator: stopped")

// handle applies one event to the owned state. Every branch ends by re-arming the
// deadline timer through advance, so there is exactly one place that decides when
// the loop next wakes.
func (c *Coordinator) handle(ev event) {
	switch ev.kind {
	case evSync:
		// Drain first: see Run's comment. Without this the ack can outrun a
		// reaction that has already been triggered.
		c.drainWake()
		close(ev.ack)
		return
	case evSnapshot:
		ev.snap <- c.snapshotOf(ev.roomID)
		return
	case evJoin:
		c.onJoin(ev.roomID, ev.peerID, ev.name)
	case evLeave:
		c.onLeave(ev.roomID, ev.peerID)
	case evReport:
		c.onReport(ev.roomID, ev.peerID, ev.report)
	case evBeat:
		c.onBeat(ev.roomID, ev.peerID, ev.beat)
	case evReparent:
		c.onReparented(ev.roomID, ev.peerID, ev.rep)
	case evRoster:
		c.onRoster(ev.roomID, ev.roster)
	case evEpoch:
		c.onEpoch(ev.roomID, ev.epoch)
	case evYield:
		c.onYield(ev.roomID, ev.epoch)
	}
	c.advance()
}

// drainWake consumes an already-fired deadline and reacts to it. One pass suffices —
// advance always re-arms strictly in the future — but the loop is written defensively
// because a missed fire is exactly the failure this exists to prevent.
func (c *Coordinator) drainWake() {
	for {
		select {
		case <-c.wake.C():
			c.wakeArmed = false
			c.advance()
		default:
			return
		}
	}
}

// advance fires every deadline that is due at the clock's current instant, then arms
// the single wake timer at the earliest remaining one.
//
// One timer for every deadline in the process, rather than one timer per node per
// kind, for two reasons. A `select` cannot watch a dynamic set of channels, so the
// alternative is a polling ticker (which quantises every threshold to the tick) or a
// goroutine per timer (which puts state mutation back on many goroutines). And a
// single wake channel is what makes the Sync drain above a COMPLETE barrier: "a
// deadline is due" and "the wake channel has a value" are the same statement, so
// draining one drains all.
func (c *Coordinator) advance() {
	for i := 0; ; i++ {
		if i > maxDeadlineRounds {
			c.log.Error("deadline loop did not converge; a handler is re-arming in the past",
				slog.Int("rounds", i))
			return
		}
		if c.fireDue() {
			continue
		}
		next, ok := c.nextDeadline()
		if !ok {
			if c.wakeArmed {
				c.wake.Stop()
				c.wakeArmed = false
				c.wakeAt = time.Time{}
			}
			return
		}
		d := next.Sub(c.cfg.Clock.Now())
		if d <= 0 {
			// Virtual time moved under us between fireDue and here. Go fire it.
			continue
		}
		if c.wakeArmed && c.wakeAt.Equal(next) {
			return
		}
		c.wake.Stop()
		c.wake.Reset(d)
		c.wakeArmed, c.wakeAt = true, next
		return
	}
}

// deadlineKind ranks the deadline classes so two due at the same virtual instant
// fire in a fixed order rather than in map order. Gone before degraded because a
// node that crossed both while the clock jumped is gone, and its degraded
// transition would be noise.
type deadlineKind int

const (
	dlGone deadlineKind = iota
	dlDegraded
	dlDwell
	dlRebuild
	dlSettle
	dlCooldown
)

type deadline struct {
	at     time.Time
	kind   deadlineKind
	roomID string
	peerID string
}

// less is the total order deadlines fire in: time, then class, then room, then peer.
// Every component is needed for a replayable trace.
func (d deadline) less(o deadline) bool {
	if !d.at.Equal(o.at) {
		return d.at.Before(o.at)
	}
	if d.kind != o.kind {
		return d.kind < o.kind
	}
	if d.roomID != o.roomID {
		return d.roomID < o.roomID
	}
	return d.peerID < o.peerID
}

// eachDeadline visits every armed deadline in a deterministic order (rooms then
// peers, both sorted — never a map range).
func (c *Coordinator) eachDeadline(visit func(deadline)) {
	roomIDs := make([]string, 0, len(c.rooms))
	for id := range c.rooms {
		roomIDs = append(roomIDs, id)
	}
	sort.Strings(roomIDs)

	for _, rid := range roomIDs {
		rs := c.rooms[rid]
		if rs.rebuilding {
			visit(deadline{at: rs.rebuildUntil, kind: dlRebuild, roomID: rid})
		}
		if !rs.settleUntil.IsZero() {
			visit(deadline{at: rs.settleUntil, kind: dlSettle, roomID: rid})
		}
		if rs.dirty && rs.built {
			visit(deadline{at: rs.lastBuild.Add(c.cfg.RecomputeCooldown), kind: dlCooldown, roomID: rid})
		}
		peerIDs := make([]string, 0, len(rs.nodes))
		for id := range rs.nodes {
			peerIDs = append(peerIDs, id)
		}
		sort.Strings(peerIDs)
		for _, pid := range peerIDs {
			ns := rs.nodes[pid]
			if ns.health != HealthGone {
				visit(deadline{at: ns.lastSeen.Add(c.goneThreshold(ns)), kind: dlGone, roomID: rid, peerID: pid})
			}
			if ns.health == HealthHealthy {
				visit(deadline{at: ns.lastSeen.Add(c.degradedThreshold(ns)), kind: dlDegraded, roomID: rid, peerID: pid})
			}
			if !ns.dwellSince.IsZero() && !ns.impaired {
				visit(deadline{at: ns.dwellSince.Add(c.cfg.Dwell), kind: dlDwell, roomID: rid, peerID: pid})
			}
		}
	}
}

// nextDeadline returns the earliest armed deadline, if any.
func (c *Coordinator) nextDeadline() (time.Time, bool) {
	var best deadline
	found := false
	c.eachDeadline(func(d deadline) {
		if !found || d.less(best) {
			best, found = d, true
		}
	})
	return best.at, found
}

// fireDue fires the single earliest deadline that is due now and reports whether it
// did. One at a time, so a handler that arms an earlier deadline still gets it
// serviced in the right order.
func (c *Coordinator) fireDue() bool {
	now := c.cfg.Clock.Now()
	var best deadline
	found := false
	c.eachDeadline(func(d deadline) {
		if d.at.After(now) {
			return
		}
		if !found || d.less(best) {
			best, found = d, true
		}
	})
	if !found {
		return false
	}
	c.fire(best)
	return true
}

// fire dispatches one due deadline.
func (c *Coordinator) fire(d deadline) {
	rs := c.rooms[d.roomID]
	if rs == nil {
		return
	}
	switch d.kind {
	case dlSettle:
		// Threshold event 8. The window closed; whatever the fleet has said by now
		// is what the tree is built from.
		rs.settleUntil = time.Time{}
		c.recompute(rs, "join settle expired")
	case dlRebuild:
		// The bound the "heard from everyone" fast path lacks: one silent member must
		// not be able to hold a whole meet dark.
		c.recompute(rs, "rebuild window expired")
	case dlCooldown:
		rs.dirty = false
		c.recompute(rs, "recompute cooldown expired")
	case dlGone:
		c.onGone(rs, rs.nodes[d.peerID])
	case dlDegraded:
		ns := rs.nodes[d.peerID]
		if ns == nil || ns.health != HealthHealthy {
			return
		}
		ns.health = HealthDegraded
		// Advisory ONLY: this colours the dashboard and never rebuilds a tree.
		c.publish(rs, Event{Kind: EventHealth, Node: ns.name, Health: HealthDegraded,
			Reason: "missed heartbeats"})
	case dlDwell:
		ns := rs.nodes[d.peerID]
		if ns == nil || ns.impaired {
			return
		}
		ns.impaired = true
		c.publish(rs, Event{Kind: EventHealth, Node: ns.name, Health: ns.health,
			Reason: "sustained degradation"})
		// Threshold event 5. An impaired node keeps its children only if there is
		// nowhere better for them, and loses incumbency protection over them — which
		// is what makes a fired dwell produce a materially different tree.
		c.recompute(rs, "degradation dwell fired")
	}
}

// onGone applies the gone verdict: the node leaves the tree projection, repair runs,
// and — critically — its RECORD SURVIVES so a returning peer is cheap to re-admit.
func (c *Coordinator) onGone(rs *roomState, ns *nodeState) {
	if ns == nil || ns.health == HealthGone {
		return
	}
	ns.health = HealthGone
	c.log.Info("member declared gone", slog.String("room_id", rs.id),
		slog.String("peer_id", ns.id), slog.String("name", ns.name))
	c.publish(rs, Event{Kind: EventHealth, Node: ns.name, Health: HealthGone,
		Reason: "no frames within the gone threshold"})
	c.publishFailover(rs, ns.name, "declared gone")
	// Threshold event 3.
	c.recompute(rs, "member gone")
}

// publishFailover reports what one departure took down with it, computed against the
// PUBLISHED tree — the only artifact that describes what the peers were actually
// running. Reroot marks the one legitimately global repair, so an operator can see
// why everything moved instead of inferring it.
func (c *Coordinator) publishFailover(rs *roomState, name, reason string) {
	if rs.published == nil || rs.published.Depth(name) < 0 {
		return
	}
	orphans := make([]string, 0)
	for _, n := range rs.published.Subtree(name) {
		if n != name {
			orphans = append(orphans, n)
		}
	}
	sort.Strings(orphans)
	c.publish(rs, Event{
		Kind:       EventFailover,
		Node:       name,
		PrevParent: rs.published.ParentOf(name),
		Orphans:    orphans,
		Reroot:     name == rs.published.Root,
		Reason:     reason,
	})
}

// room returns the meet's state, creating it on first sight.
//
// A new meet starts at epoch 1 rather than 0 because overlay.BuildTree refuses an
// unstamped tree, and in Phase 5 the coordinator runs inside the arbiter process and
// simply reads the current term. SetEpoch raises it; nothing lowers it.
func (c *Coordinator) room(roomID string) *roomState {
	rs := c.rooms[roomID]
	if rs == nil {
		rs = &roomState{
			id:         roomID,
			nodes:      make(map[string]*nodeState),
			epoch:      1,
			serving:    true,
			gate:       &term{},
			promotions: make(map[string]string),
		}
		c.rooms[roomID] = rs
	}
	return rs
}

// touch records that a frame arrived from this node and, if the node had been
// written off, resurrects it. It returns true when this was a resurrection, which
// the caller treats as a JOIN for threshold purposes.
func (c *Coordinator) touch(rs *roomState, ns *nodeState) bool {
	ns.lastSeen = c.cfg.Clock.Now()
	if ns.health == HealthHealthy {
		return false
	}
	was := ns.health
	ns.health = HealthHealthy
	c.publish(rs, Event{Kind: EventHealth, Node: ns.name, Health: HealthHealthy,
		Reason: "frame received"})
	if was == HealthDegraded {
		return false // never left the tree; nothing to re-admit
	}
	// A gone node returning is a membership-growth episode: it must be re-admitted,
	// re-placed, and it arms the settle exactly like a fresh join.
	c.log.Info("member resurrected", slog.String("room_id", rs.id),
		slog.String("peer_id", ns.id), slog.String("name", ns.name))
	c.publish(rs, Event{Kind: EventMember, Node: ns.name, Present: true})
	return true
}

// ensure finds or CREATES the record for a peer id, reporting whether it created
// one. Creating rather than dropping is the whole of the resurrection contract's
// second row: a frame from a peer with no record at all — deleted by PeerLeft, or
// never seen because this coordinator was just elected — must admit it. The Phase 4
// code this replaces returned early on an unknown peer, which was correct for Phase
// 4 and is a permanent blackhole from Phase 5 on.
//
// It returns nil for an unknown peer whose frame carries no NAME. The name is the
// tree's only vocabulary, so a nameless record could never be placed, and admitting
// one would publish a membership event for the empty string. A named peer that later
// sends a nameless frame keeps the name it announced.
func (c *Coordinator) ensure(rs *roomState, peerID, name string) (*nodeState, bool) {
	if ns := rs.nodes[peerID]; ns != nil {
		if ns.name == "" && name != "" {
			ns.name = name
		}
		return ns, false
	}
	if name == "" {
		c.log.Debug("dropping a nameless frame from an unknown peer",
			slog.String("room_id", rs.id), slog.String("peer_id", peerID))
		return nil, false
	}
	ns := &nodeState{id: peerID, name: name, health: HealthHealthy, lastSeen: c.cfg.Clock.Now()}
	rs.nodes[peerID] = ns
	c.log.Info("member admitted from a frame", slog.String("room_id", rs.id),
		slog.String("peer_id", peerID), slog.String("name", name))
	c.publish(rs, Event{Kind: EventMember, Node: name, Present: true})
	return ns, true
}

// dropIfEmpty forgets a meet with no members. Every inbound frame names a room, so
// without this a stray frame for a meet nobody is in would leave a record behind for
// the life of the process.
func (c *Coordinator) dropIfEmpty(rs *roomState) {
	if len(rs.nodes) == 0 {
		delete(c.rooms, rs.id)
	}
}

// armSettle starts (or extends) the membership-growth window. A burst of joins keeps
// pushing it out, so the burst coalesces into ONE build. Only joins arm it — leave,
// gone, dwell, reparent, and the settle timer itself must act now.
func (c *Coordinator) armSettle(rs *roomState) {
	rs.settleUntil = c.cfg.Clock.Now().Add(c.cfg.JoinSettle)
}

func (c *Coordinator) onJoin(roomID, peerID, name string) {
	rs := c.room(roomID)
	if ns := rs.nodes[peerID]; ns != nil {
		// A duplicate join for a live id: refresh the label and treat it as proof of
		// life rather than resetting telemetry we already have.
		ns.name = name
		c.touch(rs, ns)
	} else {
		rs.nodes[peerID] = &nodeState{
			id: peerID, name: name, health: HealthHealthy, lastSeen: c.cfg.Clock.Now(),
		}
		c.log.Info("member joined", slog.String("room_id", roomID),
			slog.String("peer_id", peerID), slog.String("name", name),
			slog.Int("members", len(rs.nodes)))
		c.publish(rs, Event{Kind: EventMember, Node: name, Present: true})
	}
	c.armSettle(rs)
	c.markUrgentIfUnplaced(rs)
	c.recompute(rs, "member joined")
}

func (c *Coordinator) onLeave(roomID, peerID string) {
	rs := c.rooms[roomID]
	if rs == nil {
		return
	}
	ns := rs.nodes[peerID]
	if ns == nil {
		return
	}
	delete(rs.nodes, peerID)
	delete(rs.promotions, ns.name)
	c.log.Info("member left", slog.String("room_id", roomID),
		slog.String("peer_id", peerID), slog.String("name", ns.name),
		slog.Int("members", len(rs.nodes)))
	c.publish(rs, Event{Kind: EventMember, Node: ns.name, Present: false})
	if rs.published != nil && rs.published.IsRelay(ns.name) {
		// A departing relay orphans a subtree; a departing leaf orphans nobody, and
		// reporting a failover for it would be noise.
		c.publishFailover(rs, ns.name, "left the meet")
	}
	if len(rs.nodes) == 0 {
		delete(c.rooms, roomID) // don't leak a record per meeting ever held
		return
	}
	// Threshold event 2. Not urgent: nobody is left waiting for media because of it.
	c.recompute(rs, "member left")
}

func (c *Coordinator) onReport(roomID, peerID string, rep metrics.Report) {
	rs := c.room(roomID)
	ns, created := c.ensure(rs, peerID, rep.Name)
	if ns == nil {
		c.dropIfEmpty(rs)
		return
	}
	resurrected := c.touch(rs, ns)
	if created || resurrected {
		c.armSettle(rs)
	}
	first := !ns.reported
	ns.report = rep
	ns.reported = true
	c.trackDwell(rs, ns, rep)

	if first || created || resurrected {
		// Threshold event 4 (and the resurrection's join).
		c.markUrgentIfUnplaced(rs)
		c.recompute(rs, "first report")
		return
	}
	// A routine tick refreshes the stored numbers for whenever the next threshold
	// event rebuilds, and does not itself rebuild — that is the anti-thrash rule.
	c.advanceNothing()
}

// advanceNothing documents the deliberate no-op branches: an event that updates
// state without being a threshold event. It exists so a reader can tell "nothing
// happens here" from "someone forgot the recompute".
func (c *Coordinator) advanceNothing() {}

// trackDwell arms, holds, or resets a node's sustained-degradation timer. A single
// bad sample resets nothing; a single GOOD sample resets everything, which is what
// makes the dwell fire only on an unbroken run.
func (c *Coordinator) trackDwell(rs *roomState, ns *nodeState, rep metrics.Report) {
	if isDegradedSample(rep) {
		if ns.dwellSince.IsZero() {
			ns.dwellSince = c.cfg.Clock.Now()
		}
		return
	}
	ns.dwellSince = time.Time{}
	if ns.impaired {
		ns.impaired = false
		// Recovery is NOT on the threshold-event list: it updates state, publishes,
		// and is folded into whatever rebuild happens next. Rebuilding here would
		// let a flapping link drive the tree, which is the thing the dwell exists to
		// prevent in the other direction.
		c.publish(rs, Event{Kind: EventHealth, Node: ns.name, Health: ns.health,
			Reason: "degradation cleared"})
	}
}

func (c *Coordinator) onBeat(roomID, peerID string, hb metrics.Heartbeat) {
	rs := c.room(roomID)
	ns, created := c.ensure(rs, peerID, hb.Name)
	if ns == nil {
		c.dropIfEmpty(rs)
		return
	}
	resurrected := c.touch(rs, ns)
	if iv := hb.Interval(); iv > 0 {
		ns.interval = iv
	}
	ns.beatSeq = hb.Seq
	ns.lastBeatAt = ns.lastSeen
	ns.realParent = hb.Parent
	ns.realChildren = ns.realChildren[:0]
	for _, ch := range hb.Children {
		ns.realChildren = append(ns.realChildren, ch.Name)
	}
	if rs.rebuilding && ns.name != "" {
		// A heartbeat is the ONLY frame carrying realized topology, so the rebuild
		// window waits on heartbeats specifically — a metrics report says nothing about
		// the thing being reconstructed. Marking and re-checking here is what gives the
		// window its fast exit; it is scoped to the window, so this is not a ninth
		// threshold event.
		rs.heard[ns.name] = true
	}
	if created || resurrected {
		c.armSettle(rs)
		c.markUrgentIfUnplaced(rs)
		c.recompute(rs, "member returned")
		return
	}
	if rs.rebuilding {
		c.recompute(rs, "rebuild heartbeat")
		return
	}
	// A routine beat is not a threshold event.
	c.advanceNothing()
}

func (c *Coordinator) onReparented(roomID, peerID string, rp metrics.Reparented) {
	rs := c.room(roomID)
	ns, created := c.ensure(rs, peerID, rp.Name)
	if ns == nil {
		c.dropIfEmpty(rs)
		return
	}
	resurrected := c.touch(rs, ns)
	if created || resurrected {
		c.armSettle(rs)
	}

	if !rp.OK {
		// A stranded peer is receiving nothing, so the anti-thrash cooldown is the
		// wrong trade. This is one of the only two frozen bypasses.
		c.log.Warn("peer is stranded", slog.String("room_id", roomID),
			slog.String("name", ns.name), slog.String("from", rp.From),
			slog.String("reason", rp.Reason))
		c.publish(rs, Event{Kind: EventReparent, Node: ns.name, PrevParent: rp.From,
			Reason: rp.Reason})
		rs.urgent = true
		c.recompute(rs, "peer stranded")
		return
	}

	// Ratify only a promotion made under the tree the peers are actually running. A
	// success reported against a tree already replaced says nothing about the
	// current one, and patching it in would defend an edge from a dead topology.
	current := rs.published != nil && rp.Epoch == rs.published.Epoch && rp.Rev == rs.published.Rev
	if current && rp.To != "" {
		rs.promotions[ns.name] = rp.To
		c.log.Info("ratified self-promotion", slog.String("room_id", roomID),
			slog.String("name", ns.name), slog.String("from", rp.From), slog.String("to", rp.To))
	} else {
		c.log.Debug("self-promotion not ratified (stale or parentless)",
			slog.String("room_id", roomID), slog.String("name", ns.name),
			slog.Uint64("reported_rev", rp.Rev))
	}
	c.publish(rs, Event{Kind: EventReparent, Node: ns.name, Parent: rp.To,
		PrevParent: rp.From, Reason: rp.Reason})
	// Threshold event 6.
	c.recompute(rs, "peer self-promoted")
}

func (c *Coordinator) onRoster(roomID string, members []Member) {
	rs := c.room(roomID)
	inRoster := make(map[string]bool, len(members))
	for _, m := range members {
		inRoster[m.ID] = true
	}

	// Departures first, so a name freed by a leave can be reused by an arrival in
	// the same reconciliation.
	stale := make([]string, 0)
	for id := range rs.nodes {
		if !inRoster[id] {
			stale = append(stale, id)
		}
	}
	sort.Strings(stale)
	for _, id := range stale {
		ns := rs.nodes[id]
		delete(rs.nodes, id)
		delete(rs.promotions, ns.name)
		c.publish(rs, Event{Kind: EventMember, Node: ns.name, Present: false})
	}

	added := false
	for _, m := range members {
		if rs.nodes[m.ID] != nil {
			continue
		}
		rs.nodes[m.ID] = &nodeState{
			id: m.ID, name: m.Name, health: HealthHealthy, lastSeen: c.cfg.Clock.Now(),
		}
		c.publish(rs, Event{Kind: EventMember, Node: m.Name, Present: true})
		added = true
	}
	if added {
		c.armSettle(rs)
	}
	if len(rs.nodes) == 0 {
		delete(c.rooms, roomID)
		return
	}
	c.markUrgentIfUnplaced(rs)
	c.recompute(rs, "roster reconciled")
}

func (c *Coordinator) onEpoch(roomID string, epoch uint64) {
	rs := c.room(roomID)
	if epoch <= rs.epoch {
		// A coordinator never raises its own term; re-adopting the current one is a
		// no-op rather than a reset that would re-stamp trees peers already hold; and
		// — the case that matters — an announcement this node has already superseded
		// must never put it back in charge. Authority only moves forward.
		return
	}
	c.log.Info("adopted epoch", slog.String("room_id", roomID),
		slog.Uint64("epoch", epoch), slog.Uint64("was", rs.epoch))
	rs.epoch = epoch
	rs.serving = true

	// End the previous term's outbound gate and start a fresh one, so anything still
	// queued from before the handover is dropped rather than delivered under a term
	// that no longer exists.
	rs.gate.end()
	rs.gate = &term{}

	// REBUILD-FROM-PEERS, and only that (§6.6). Snapshot-and-ship is not implemented,
	// not even as a fast path: a handover route that runs only on GRACEFUL handovers is
	// a route that is never exercised and is therefore broken on the crash it exists
	// for. So a new term always starts from the peers' realized state — including when
	// the "new" coordinator is this same object, which is what keeps the single path
	// exercised on every handover and in every test.
	c.abandonTerm(rs)
	rs.rebuilding = true
	rs.rebuildUntil = c.cfg.Clock.Now().Add(metrics.RebuildWindow)
	rs.heard = make(map[string]bool)
	// Recompute now rather than waiting for an event: a bootstrap term over a meet with
	// no members has vacuously heard from everyone, so its window must close at once
	// instead of costing the first tree of every meet a RebuildWindow with nothing to
	// rebuild from.
	c.recompute(rs, "adopted a new epoch")
}

// onYield applies Yield on the Run goroutine. See Yield for what is dropped and why.
func (c *Coordinator) onYield(roomID string, newEpoch uint64) {
	rs := c.rooms[roomID]
	if rs == nil {
		// Remember the demotion even for a meet this node has not seen yet, because
		// the alternative is worse: roomState defaults to SERVING (the Phase 5
		// arrangement), so a frame arriving afterwards would silently start this node
		// coordinating a meet it was explicitly told it does not own.
		rs = c.room(roomID)
	}
	if newEpoch > rs.epoch {
		rs.epoch = newEpoch
	}
	if !rs.serving {
		return // idempotent: a re-broadcast announcement naming someone else is normal
	}
	c.log.Info("yielded coordination", slog.String("room_id", roomID),
		slog.Uint64("epoch", rs.epoch))
	rs.serving = false
	rs.gate.end()
	c.abandonTerm(rs)
}

// abandonTerm discards everything this node BELIEVED as coordinator of roomID and
// leaves everything it OBSERVED. Called on both ends of a term boundary — Yield and
// epoch adoption — so the two cannot drift apart.
//
// Nothing here touches nodes, their telemetry, their health, or their realized state:
// that is the line the boundary is cut along.
func (c *Coordinator) abandonTerm(rs *roomState) {
	rs.published, rs.working, rs.baseline = nil, nil, nil
	rs.promotions = make(map[string]string)
	rs.rev = 0
	rs.built = false
	rs.dirty = false
	rs.urgent = false
	rs.settleUntil = time.Time{}
	rs.settlingNow = false
	rs.rebuilding = false
	rs.rebuildUntil = time.Time{}
	rs.heard = nil
	rs.handoverNote = ""
}

// markUrgentIfUnplaced sets the room's cooldown bypass when some named, live member
// is not in the published tree. That is the "unplaced joiner" case: joining is the
// most common user action, and it must not cost up to RecomputeCooldown of black
// screen. It is deliberately a QUERY over state rather than a flag set by the join
// handler, because the build that finally places a joiner usually happens on a later
// event (its first report, or the settle timer).
func (c *Coordinator) markUrgentIfUnplaced(rs *roomState) {
	for _, ns := range rs.nodes {
		if ns.name == "" || ns.health == HealthGone {
			continue
		}
		if rs.published == nil || rs.published.Depth(ns.name) < 0 {
			rs.urgent = true
			return
		}
	}
}
