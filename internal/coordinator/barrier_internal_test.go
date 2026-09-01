package coordinator

// The one property everything else in this repo's deterministic test strategy rests
// on: Sync is a SOUND quiescence barrier.
//
// It is proved here, white-box and single-goroutine, rather than by racing a real
// loop. A concurrent test of this cannot discriminate: a fired timer and a sync
// arriving at one parked select are resolved by Go's uniform-random choice, but on a
// multi-core machine the Run goroutine is almost always already awake and reacting
// by the time the sync is enqueued — so a broken loop passes anyway, and the test
// reads as coverage while proving nothing. (surface_test.go's 200-trial race test is
// kept as an end-to-end check of the same claim, but THIS is the discriminator.)
//
// The construction: drive handle() directly on the test goroutine, fire a deadline
// into the wake channel with nobody consuming it, and then hand the loop a sync. The
// sync branch must react to the pending deadline BEFORE it acks.

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// stubClock is a minimal manual clock: time moves only when advance is called, and
// advance delivers due timers without running any loop. It is deliberately NOT
// simnet.VirtualClock — this file must not import simnet, because docs/PLAN.md §2.3
// permits simnet to import coordinator and an internal test file doing the reverse
// would become an import cycle the moment it does.
//
// Single-goroutine by construction, so it needs no lock.
type stubClock struct {
	now    time.Time
	timers []*stubTimer
}

func newStubClock() *stubClock {
	return &stubClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *stubClock) Now() time.Time { return c.now }

func (c *stubClock) NewTimer(d time.Duration) clock.Timer {
	t := &stubTimer{clk: c, ch: make(chan time.Time, 1), deadline: c.now.Add(d), armed: true}
	c.timers = append(c.timers, t)
	return t
}

func (c *stubClock) NewTicker(time.Duration) clock.Ticker {
	panic("stubClock: tickers are not used by the coordinator")
}

func (c *stubClock) After(d time.Duration) <-chan time.Time { return c.NewTimer(d).C() }

// advance moves time forward and delivers every deadline that becomes due. Nothing
// consumes the delivery: that is the whole point — it leaves the wake channel HOT so
// the next sync has something to drain.
func (c *stubClock) advance(d time.Duration) {
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if !t.armed || t.deadline.After(c.now) {
			continue
		}
		t.armed = false
		select {
		case t.ch <- t.deadline:
		default:
		}
	}
}

type stubTimer struct {
	clk      *stubClock
	ch       chan time.Time
	deadline time.Time
	armed    bool
}

func (t *stubTimer) C() <-chan time.Time { return t.ch }

func (t *stubTimer) Reset(d time.Duration) bool {
	was := t.armed
	t.deadline, t.armed = t.clk.now.Add(d), true
	return was
}

func (t *stubTimer) Stop() bool {
	was := t.armed
	t.armed = false
	return was
}

// driver is a Coordinator whose event loop is the test goroutine.
type driver struct {
	c   *Coordinator
	clk *stubClock
	pub *gatedPublisher
}

// gatedPublisher records events and can PARK the goroutine that is publishing. The
// parking is what turns an ordering question into a deterministic one: with the
// publisher held inside a reaction, a test can look at the outside world and ask "has
// the acknowledgement already been given?" without racing anything.
type gatedPublisher struct {
	mu      sync.Mutex
	events  []Event
	entered chan struct{} // signalled on entry to a gated Publish
	release chan struct{} // gated Publish blocks until this is closed
}

func (p *gatedPublisher) Publish(ev Event) {
	p.mu.Lock()
	p.events = append(p.events, ev)
	entered, release := p.entered, p.release
	p.mu.Unlock()
	if entered == nil {
		return
	}
	select {
	case entered <- struct{}{}:
	default:
	}
	<-release
}

// arm makes the NEXT Publish park until release is closed.
func (p *gatedPublisher) arm() (entered, release chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entered, p.release = make(chan struct{}, 1), make(chan struct{})
	return p.entered, p.release
}

func (p *gatedPublisher) all() []Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Event(nil), p.events...)
}

func (p *gatedPublisher) countOf(k EventKind) int {
	n := 0
	for _, ev := range p.all() {
		if ev.Kind == k {
			n++
		}
	}
	return n
}

func newDriver(t *testing.T) *driver {
	t.Helper()
	clk := newStubClock()
	pub := &gatedPublisher{}
	c := New(slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		MaxDepth: 2, StreamKbps: 2000, Clock: clk,
		// Liveness out of the way: these tests are about ordering and guards, and a
		// fleet that never beats would otherwise be reaped by the backstop mid-test.
		DegradedAfter: time.Hour, GoneAfter: time.Hour,
	}, nil, pub)
	// Run would own these; here the test goroutine does.
	c.wake = clk.NewTimer(time.Hour)
	c.wake.Stop()
	c.wakeArmed = false
	return &driver{c: c, clk: clk, pub: pub}
}

func (d *driver) join(peerID, name string, upload int) {
	d.c.handle(event{kind: evJoin, roomID: "r", peerID: peerID, name: name})
	d.c.handle(event{kind: evReport, roomID: "r", peerID: peerID,
		report: metrics.Report{Name: name, UploadKbps: upload}})
}

func (d *driver) rev() uint64 {
	rs := d.c.rooms["r"]
	if rs == nil || rs.published == nil {
		return 0
	}
	return rs.published.Rev
}

// guard bounds a wait in REAL time so a wedged loop fails the test instead of hanging
// it with no stack. It never affects a decision — only whether a deadlock is reported.
const guard = 10 * time.Second

// TestSyncDrainsFiredDeadlinesBeforeAcking is the DELETION discriminator: remove the
// drainWake call from handle's evSync branch and this fails deterministically.
//
// It cannot catch REORDERING (acking before draining), because it drives handle
// synchronously and both statements complete before handle returns — the ordering is
// invisible from outside a single call. TestSyncDrainsBeforeItAcks below closes that,
// so between them the claim "this file is the discriminator" is finally true for both
// mutations.
func TestSyncDrainsFiredDeadlinesBeforeAcking(t *testing.T) {
	d := newDriver(t)
	d.join("p1", "a", 8000)
	d.join("p2", "b", 0)
	d.join("p3", "c", 0)
	built := d.rev()
	if built == 0 {
		t.Fatal("precondition: the meet must have built")
	}

	// A leave is not urgent, so it is suppressed and arms the cooldown deadline.
	d.c.handle(event{kind: evLeave, roomID: "r", peerID: "p3"})
	if got := d.rev(); got != built {
		t.Fatalf("precondition: the leave must be suppressed by the cooldown; rev %d -> %d", built, got)
	}
	if !d.c.wakeArmed {
		t.Fatal("precondition: the suppressed recompute must have armed a deadline")
	}

	// Fire it. Nothing consumes the delivery, so the wake channel is now hot — the
	// exact state a parked Run loop would be in when a sync arrives alongside a fire.
	d.clk.advance(RecomputeCooldown)
	if got := d.rev(); got != built {
		t.Fatalf("firing a timer must not by itself change state; rev %d -> %d", built, got)
	}

	ack := make(chan struct{})
	d.c.handle(event{kind: evSync, ack: ack})

	select {
	case <-ack:
	default:
		t.Fatal("Sync must ack")
	}
	if got := d.rev(); got != built+1 {
		t.Fatalf("Sync acked while a FIRED deadline was still pending: rev %d, want %d. "+
			"A barrier that reports quiescent before the reaction it exists to wait for "+
			"is a permanently-flaky-test generator, and it gets blamed on the harness.",
			got, built+1)
	}
}

// TestAdvanceIsIdempotentWhenNothingIsDue: the drain must be safe to run on a cold
// channel, and re-arming an unchanged deadline must not disturb the timer — a
// re-stamped tiebreak sequence would reorder same-instant deadlines under a virtual
// clock and make a trace unreplayable.
func TestAdvanceIsIdempotentWhenNothingIsDue(t *testing.T) {
	d := newDriver(t)
	d.join("p1", "a", 8000)
	d.join("p2", "b", 0)
	d.c.handle(event{kind: evJoin, roomID: "r", peerID: "p3", name: "c"}) // silent: arms the settle
	if !d.c.wakeArmed {
		t.Fatal("precondition: the settle must have armed a deadline")
	}
	at, rev := d.c.wakeAt, d.rev()

	for i := 0; i < 5; i++ {
		ack := make(chan struct{})
		d.c.handle(event{kind: evSync, ack: ack})
	}
	if !d.c.wakeArmed || !d.c.wakeAt.Equal(at) {
		t.Fatalf("a sync must not move an un-due deadline: armed=%v at %v, want %v", d.c.wakeArmed, d.c.wakeAt, at)
	}
	if got := d.rev(); got != rev {
		t.Fatalf("a sync must not build anything on its own; rev %d -> %d", rev, got)
	}
}

// TestDeadlinesFireInOrder: two deadlines that come due in the same advance must be
// serviced oldest-first, and the loop must converge — it may not leave one pending.
func TestDeadlinesFireInOrder(t *testing.T) {
	d := newDriver(t)
	d.join("p1", "a", 8000)
	d.join("p2", "b", 0)
	d.c.handle(event{kind: evJoin, roomID: "r", peerID: "p3", name: "c"}) // arms the settle
	rev := d.rev()

	// Jump far past BOTH the settle and any cooldown, so several deadlines are due
	// at once and only the loop's own ordering decides what happens.
	d.clk.advance(10 * RecomputeCooldown)
	ack := make(chan struct{})
	d.c.handle(event{kind: evSync, ack: ack})

	if got := d.rev(); got != rev+1 {
		t.Fatalf("the settle must have closed and produced exactly one build; rev %d -> %d", rev, got)
	}
	// Something is always armed (the health backstops), but NOTHING may still be
	// due: advance must service every due deadline before it returns, or a fired
	// timer would sit unhandled until some unrelated event happened to wake the loop.
	if d.c.wakeArmed && !d.c.wakeAt.After(d.clk.Now()) {
		t.Fatalf("a deadline is still due after advance: wakeAt=%v now=%v", d.c.wakeAt, d.clk.Now())
	}
	if topo := d.c.rooms["r"].published; topo.ParentOf("c") == "" {
		t.Fatalf("the silent joiner must be placed when the window closes: %+v", topo)
	}
}

// TestSyncDrainsBeforeItAcks is the REORDERING discriminator, and it is deterministic
// rather than probabilistic.
//
// The trick is to freeze the loop INSIDE the reaction the drain triggers: the publisher
// parks on its first event, so while it is parked the drained recompute has provably
// started and provably not finished. At that instant the acknowledgement must not yet
// have been given. Swap the two statements in handle's evSync branch — close(ev.ack)
// before c.drainWake() — and the ack is already closed when the publisher parks.
//
// This is the ordering question asked as a state question, which is the only way to ask
// it without a race: "is the ack already closed at a moment we control" rather than
// "did the ack happen after the drain", which nothing can observe directly.
func TestSyncDrainsBeforeItAcks(t *testing.T) {
	d := newDriver(t)
	d.join("p1", "a", 8000)
	d.join("p2", "b", 0)
	d.join("p3", "c", 0)
	built := d.rev()
	if built == 0 {
		t.Fatal("precondition: the meet must have built")
	}

	// A leave is not urgent, so it is suppressed and arms the cooldown deadline.
	d.c.handle(event{kind: evLeave, roomID: "r", peerID: "p3"})
	if !d.c.wakeArmed {
		t.Fatal("precondition: the suppressed recompute must have armed a deadline")
	}
	// Fire it, with nobody consuming the delivery: the wake channel is now hot.
	d.clk.advance(RecomputeCooldown)

	entered, release := d.pub.arm()
	ack := make(chan struct{})
	go d.c.handle(event{kind: evSync, ack: ack})

	ctx, cancel := context.WithTimeout(context.Background(), guard)
	defer cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("the fired deadline was never reacted to: the sync branch did not drain")
	}

	select {
	case <-ack:
		t.Fatal("Sync acknowledged BEFORE the reaction it exists to wait for had finished: " +
			"the drain must complete before the ack, not merely happen in the same call")
	default:
	}

	close(release)
	select {
	case <-ack:
	case <-ctx.Done():
		t.Fatal("Sync never acknowledged")
	}
	if got := d.rev(); got != built+1 {
		t.Fatalf("the drained deadline must have rebuilt; rev %d, want %d", got, built+1)
	}
}

// TestPublishIsGatedByTheIndependentOracle covers the LAST LINE OF DEFENCE between a
// buggy builder and the whole fleet: a computed tree is validated by an independent
// oracle before it is published, and a tree that fails is never sent.
//
// It is untestable through recompute, because overlay.BuildTree is correct — there are
// no inputs that make it emit a tree Validate rejects. So the gate is exercised at its
// own seam: commit() takes an already-computed tree, which is exactly the boundary
// where a bug in the layer below would arrive.
func TestPublishIsGatedByTheIndependentOracle(t *testing.T) {
	d := newDriver(t)
	d.join("p1", "a", 8000)
	d.join("p2", "b", 0)
	good := d.c.rooms["r"].published
	if good == nil {
		t.Fatal("precondition: the meet must have built")
	}
	nodes, _ := d.c.project(d.c.rooms["r"])
	cons := overlay.Constraints{
		Root: good.Root, MaxDepth: 2, StreamKbps: 2000,
		Epoch: good.Epoch, Rev: good.Rev + 1, StickinessMs: overlay.DefaultStickinessMs,
	}

	cases := []struct {
		name string
		tree *overlay.Topology
	}{
		{
			name: "an edge naming a node that is not in the fleet",
			tree: &overlay.Topology{Epoch: good.Epoch, Rev: good.Rev + 1, Root: "a",
				Edges: []overlay.Edge{{Parent: "a", Child: "ghost"}}},
		},
		{
			name: "a node left unattached",
			tree: &overlay.Topology{Epoch: good.Epoch, Rev: good.Rev + 1, Root: "a"},
		},
		{
			name: "an unstamped revision no peer would accept",
			tree: &overlay.Topology{Epoch: good.Epoch, Rev: 0, Root: "a",
				Edges: []overlay.Edge{{Parent: "a", Child: "b"}}},
		},
		{
			name: "a root disagreeing with the constraints",
			tree: &overlay.Topology{Epoch: good.Epoch, Rev: good.Rev + 1, Root: "b",
				Edges: []overlay.Edge{{Parent: "b", Child: "a"}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rs := d.c.rooms["r"]
			before := rs.published
			beforeUnbuildable := d.pub.countOf(EventUnbuildable)
			beforeTopology := d.pub.countOf(EventTopology)

			d.c.commit(rs, tc.tree, nodes, cons, OutcomeBuilt, "", "oracle test")

			if rs.published != before {
				t.Fatalf("a tree that fails its own validator must NEVER be published; "+
					"rev %d replaced rev %d", rs.published.Rev, before.Rev)
			}
			if d.pub.countOf(EventTopology) != beforeTopology {
				t.Fatal("no topology event may be emitted for a rejected tree")
			}
			if d.pub.countOf(EventUnbuildable) != beforeUnbuildable+1 {
				t.Fatal("a rejected tree must be reported, not silently dropped")
			}
		})
	}

	// And the gate is not simply refusing everything: a legal tree still goes out.
	rs := d.c.rooms["r"]
	valid, err := overlay.BuildTree(nodes, rs.working, cons)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	d.c.commit(rs, valid, nodes, cons, OutcomeBuilt, "", "oracle test")
	if rs.published != valid {
		t.Fatal("a tree that passes the oracle must be published")
	}
}

// TestOutboundQueueDropsRatherThanBlocks: a wedged Sender must cost one meet a missed
// push, never the control loop. The queue is bounded and the enqueue is non-blocking,
// so the degradation is a drop — counted, so it is visible rather than silent.
func TestOutboundQueueDropsRatherThanBlocks(t *testing.T) {
	d := newDriver(t)
	// A queue of one, with nothing draining it: the second push has nowhere to go.
	d.c.sendQ = make(chan sendOp, 1)

	if !d.c.enqueueSend(sendOp{roomID: "r", peerID: "p1"}) {
		t.Fatal("the first push must be accepted")
	}
	if d.c.enqueueSend(sendOp{roomID: "r", peerID: "p2"}) {
		t.Fatal("a full queue must DROP rather than block the Run goroutine")
	}
	if d.c.dropped != 1 {
		t.Fatalf("a drop must be counted so it is visible in a log; dropped = %d", d.c.dropped)
	}
}

// TestDeriveWorkingEmitsEveryEdgeEvenWhenItCannotOrderThem: the working copy is a HINT
// to the builder, not a tree, so a promotion that names a descendant — something the
// backup invariant forbids and only a misbehaving peer could report — must not cause
// edges to be silently dropped. Truncating the hint would move nodes the coordinator had
// no reason to move.
func TestDeriveWorkingEmitsEveryEdgeEvenWhenItCannotOrderThem(t *testing.T) {
	published := &overlay.Topology{
		Epoch: 1, Rev: 3, Root: "r",
		Edges: edges("r", "u", "r", "p"),
	}
	// u and p each claim the other as their new parent: a cycle with no placeable end.
	got := deriveWorking(published, map[string]bool{"r": true, "u": true, "p": true},
		map[string]string{"u": "p", "p": "u"})

	if got == nil {
		t.Fatal("deriveWorking returned nil for a non-nil published tree")
	}
	if len(got.Edges) != 2 {
		t.Fatalf("every surviving edge must reach the hint; got %v", got.Edges)
	}
	if got.ParentOf("u") != "p" || got.ParentOf("p") != "u" {
		t.Fatalf("the reported parents must survive into the hint; got %v", got.Edges)
	}
}
