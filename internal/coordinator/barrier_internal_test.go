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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/metrics"
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
}

func newDriver(t *testing.T) *driver {
	t.Helper()
	clk := newStubClock()
	c := New(slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		MaxDepth: 2, StreamKbps: 2000, Clock: clk,
		// Liveness out of the way: this test is about the barrier, and a fleet that
		// never beats would otherwise be reaped by the backstop mid-test.
		DegradedAfter: time.Hour, GoneAfter: time.Hour,
	}, nil, nil)
	// Run would own these; here the test goroutine does.
	c.wake = clk.NewTimer(time.Hour)
	c.wake.Stop()
	c.wakeArmed = false
	return &driver{c: c, clk: clk}
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

// TestSyncDrainsFiredDeadlinesBeforeAcking is the discriminator. Remove the
// drainWake call from handle's evSync branch and this test fails deterministically.
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
