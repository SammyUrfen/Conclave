package simnet

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
)

// vcEpoch is the fixed instant every clock test starts from. A literal, never
// time.Now(): a test whose expectations shift with the wall clock is not a test,
// and the whole point of VirtualClock is that nothing here touches real time.
var vcEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestVirtualClockAdvance(t *testing.T) {
	tests := []struct {
		name string
		run  func(c *VirtualClock)
		want time.Duration // offset from vcEpoch
	}{
		{"zero advance is a no-op", func(c *VirtualClock) { c.Advance(0) }, 0},
		{"advances accumulate", func(c *VirtualClock) { c.Advance(3 * time.Second); c.Advance(4 * time.Second) }, 7 * time.Second},
		{"AdvanceTo a future instant", func(c *VirtualClock) { c.AdvanceTo(vcEpoch.Add(90 * time.Second)) }, 90 * time.Second},
		{"AdvanceTo the current instant", func(c *VirtualClock) { c.AdvanceTo(vcEpoch) }, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewVirtualClock(vcEpoch)
			tc.run(c)
			if got := c.Now().Sub(vcEpoch); got != tc.want {
				t.Errorf("Now()-epoch = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestVirtualClockNeverRunsBackward: a test that rewinds the clock has a bug, and a
// silent no-op would hide it. Panicking is the loud failure the contract asks for.
func TestVirtualClockNeverRunsBackward(t *testing.T) {
	tests := []struct {
		name string
		run  func(c *VirtualClock)
	}{
		{"AdvanceTo the past", func(c *VirtualClock) { c.AdvanceTo(vcEpoch.Add(-time.Nanosecond)) }},
		{"Advance by a negative duration", func(c *VirtualClock) { c.Advance(-time.Second) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic; time must never run backward")
				}
			}()
			tc.run(NewVirtualClock(vcEpoch))
		})
	}
}

// TestVirtualClockSameInstantOrder pins the DETERMINISM RULE: timers due at the same
// virtual instant fire in creation (or last-Reset) sequence order, never in Go's
// randomised map order. The barrier is the observation point — exactly one timer
// fires per barrier call, so the recorded sequence IS the firing order.
func TestVirtualClockSameInstantOrder(t *testing.T) {
	tests := []struct {
		name  string
		setup func(c *VirtualClock, timers []clock.Timer)
		want  []int
	}{
		{
			name:  "creation order breaks the tie",
			setup: func(*VirtualClock, []clock.Timer) {},
			want:  []int{0, 1, 2, 3},
		},
		{
			name: "Reset re-stamps the sequence and moves a timer to the back",
			setup: func(_ *VirtualClock, timers []clock.Timer) {
				timers[0].Reset(10 * time.Millisecond)
			},
			want: []int{1, 2, 3, 0},
		},
		{
			name: "a stopped timer never fires",
			setup: func(_ *VirtualClock, timers []clock.Timer) {
				timers[2].Stop()
			},
			want: []int{0, 1, 3},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewVirtualClock(vcEpoch)
			timers := make([]clock.Timer, 4)
			for i := range timers {
				timers[i] = c.NewTimer(10 * time.Millisecond)
			}
			tc.setup(c, timers)

			var order []int
			c.SetBarrier(func() {
				for i, tm := range timers {
					select {
					case <-tm.C():
						order = append(order, i)
					default:
					}
				}
			})
			c.Advance(10 * time.Millisecond)

			if fmt.Sprint(order) != fmt.Sprint(tc.want) {
				t.Errorf("fire order = %v, want %v", order, tc.want)
			}
		})
	}
}

// TestVirtualClockRearmMidAdvance is the regression test for the defect that makes
// or breaks this harness (docs/PLAN.md §4.3 as written): an Advance spanning several
// deadlines must NOT fire them from one snapshot. A timer re-armed by its handler
// partway through must take its place in the virtual timeline, ahead of a later
// deadline that has not fired yet — and that is only possible if Advance fires one
// deadline at a time and waits for the reaction between fires.
//
// The discriminator: "a" re-arms every 10ms and "b" is a one-shot at 25ms. The only
// correct trace interleaves them by virtual time. A snapshot-based Advance produces
// a@10, b@25 and nothing else, because a's re-arms all land in the past.
func TestVirtualClockRearmMidAdvance(t *testing.T) {
	c := NewVirtualClock(vcEpoch)
	r := newReactor(c, vcEpoch, 10*time.Millisecond, 25*time.Millisecond, 4)
	defer r.stop()
	c.SetBarrier(r.sync)

	c.Advance(100 * time.Millisecond)

	want := []string{"a@10ms", "a@20ms", "b@25ms", "a@30ms", "a@40ms"}
	if got := r.trace(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("fire trace = %v, want %v", got, want)
	}
}

// TestVirtualClockTicker: a ticker re-arms itself for its period, so one Advance
// spanning several periods must produce one fire per period, not a single coalesced
// one (which is what a real time.Ticker does under load, and which would hide a
// missed heartbeat in a Phase 5 test).
func TestVirtualClockTicker(t *testing.T) {
	c := NewVirtualClock(vcEpoch)
	tk := c.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	var at []time.Duration
	c.SetBarrier(func() {
		select {
		case <-tk.C():
			at = append(at, c.Now().Sub(vcEpoch))
		default:
		}
	})
	c.Advance(35 * time.Millisecond)

	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond}
	if fmt.Sprint(at) != fmt.Sprint(want) {
		t.Errorf("ticks at %v, want %v", at, want)
	}
}

// TestVirtualClockPending guards the leak check a scenario ends with: a run that
// finishes holding unexpected armed deadlines usually means a control loop leaked a
// timer, which on a real clock is an invisible slow leak.
func TestVirtualClockPending(t *testing.T) {
	tests := []struct {
		name string
		run  func(c *VirtualClock)
		want int
	}{
		{"a fresh clock has nothing armed", func(*VirtualClock) {}, 0},
		{"an armed timer counts", func(c *VirtualClock) { c.NewTimer(time.Second) }, 1},
		{"a fired timer is retired", func(c *VirtualClock) { c.NewTimer(time.Second); c.Advance(time.Second) }, 0},
		{"a stopped timer is retired", func(c *VirtualClock) { c.NewTimer(time.Second).Stop() }, 0},
		{"a ticker stays armed across fires", func(c *VirtualClock) { c.NewTicker(time.Second); c.Advance(5 * time.Second) }, 1},
		{"a stopped ticker is retired", func(c *VirtualClock) { tk := c.NewTicker(time.Second); c.Advance(time.Second); tk.Stop() }, 0},
		{"After leaks an armed deadline until it fires", func(c *VirtualClock) { c.After(time.Second) }, 1},
		{"a reset timer is armed once, not twice", func(c *VirtualClock) { c.NewTimer(time.Second).Reset(2 * time.Second) }, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewVirtualClock(vcEpoch)
			tc.run(c)
			if got := c.Pending(); got != tc.want {
				t.Errorf("Pending() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestVirtualClockDeliveryIsNonBlocking pins the DELIVERY RULE: capacity-1 channels
// and a non-blocking send, so a test that never drains a timer cannot deadlock
// Advance. It misses the tick, exactly as a real time.Ticker's consumer does.
func TestVirtualClockDeliveryIsNonBlocking(t *testing.T) {
	c := NewVirtualClock(vcEpoch)
	tk := c.NewTicker(10 * time.Millisecond)

	c.Advance(time.Second) // 100 ticks into a capacity-1 channel, nobody draining

	if got := cap(tk.C()); got != 1 {
		t.Errorf("channel capacity = %d, want 1", got)
	}
	if got := len(tk.C()); got != 1 {
		t.Errorf("buffered ticks = %d, want 1 (coalesced, like a real ticker)", got)
	}
	if got := c.Now().Sub(vcEpoch); got != time.Second {
		t.Errorf("Advance did not complete: now = %v, want 1s", got)
	}
	tk.Stop()
}

// TestVirtualClockAfter: After is the handle-less convenience. It still delivers the
// fire instant, and (see Pending above) it is documented as un-retirable.
func TestVirtualClockAfter(t *testing.T) {
	c := NewVirtualClock(vcEpoch)
	ch := c.After(5 * time.Millisecond)
	c.Advance(5 * time.Millisecond)
	select {
	case at := <-ch:
		if !at.Equal(vcEpoch.Add(5 * time.Millisecond)) {
			t.Errorf("fired at %v, want %v", at, vcEpoch.Add(5*time.Millisecond))
		}
	default:
		t.Fatal("After channel did not fire")
	}
}

// TestVirtualClockResetStopReturns mirrors time.Timer's contract, because a control
// loop that branches on Reset's return value must behave identically on both clocks.
func TestVirtualClockResetStopReturns(t *testing.T) {
	tests := []struct {
		name string
		run  func(c *VirtualClock) bool
		want bool
	}{
		{"Stop on an armed timer reports active", func(c *VirtualClock) bool { return c.NewTimer(time.Second).Stop() }, true},
		{"Stop on a fired timer reports inactive", func(c *VirtualClock) bool {
			tm := c.NewTimer(time.Second)
			c.Advance(time.Second)
			return tm.Stop()
		}, false},
		{"Reset on an armed timer reports active", func(c *VirtualClock) bool { return c.NewTimer(time.Second).Reset(time.Second) }, true},
		{"Reset on a fired timer reports inactive", func(c *VirtualClock) bool {
			tm := c.NewTimer(time.Second)
			c.Advance(time.Second)
			return tm.Reset(time.Second)
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.run(NewVirtualClock(vcEpoch)); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// reactor is a minimal stand-in for a real control loop: one goroutine owning its
// timers, reacting to fires, and offering a sync round-trip as its quiescence
// barrier. It exists to test the ADVANCE LOOP, which is only deterministic if a
// timer re-armed by a handler mid-Advance is visible to the next iteration.
//
// The load-bearing detail is in run(): the sync branch DRAINS the timer channels
// before acking. Without it, a fire and a sync arriving at the same parked select
// are resolved by Go's uniform-random choice, and the barrier can ack before the
// reaction it is supposed to be waiting for. Every control loop that wants a Sync
// barrier to mean anything needs this discipline — including the real coordinator.
type reactor struct {
	clk   *VirtualClock
	start time.Time
	rearm time.Duration
	limit int

	t0, t1 clock.Timer
	syncCh chan chan struct{}
	stopCh chan struct{}

	mu     sync.Mutex
	fires  []string
	aFired int
}

func newReactor(c *VirtualClock, start time.Time, rearm, oneShot time.Duration, limit int) *reactor {
	r := &reactor{
		clk:    c,
		start:  start,
		rearm:  rearm,
		limit:  limit,
		t0:     c.NewTimer(rearm),
		t1:     c.NewTimer(oneShot),
		syncCh: make(chan chan struct{}),
		stopCh: make(chan struct{}),
	}
	go r.run()
	return r
}

func (r *reactor) run() {
	for {
		select {
		case <-r.t0.C():
			r.handle("a", r.t0)
		case <-r.t1.C():
			r.handle("b", r.t1)
		case ack := <-r.syncCh:
			r.drain()
			close(ack)
		case <-r.stopCh:
			return
		}
	}
}

func (r *reactor) drain() {
	for {
		select {
		case <-r.t0.C():
			r.handle("a", r.t0)
			continue
		default:
		}
		select {
		case <-r.t1.C():
			r.handle("b", r.t1)
			continue
		default:
		}
		return
	}
}

func (r *reactor) handle(label string, tm clock.Timer) {
	r.mu.Lock()
	r.fires = append(r.fires, fmt.Sprintf("%s@%v", label, r.clk.Now().Sub(r.start)))
	if label == "a" {
		r.aFired++
	}
	again := label == "a" && r.aFired < r.limit
	r.mu.Unlock()
	if again {
		tm.Reset(r.rearm)
	}
}

func (r *reactor) sync() {
	ack := make(chan struct{})
	select {
	case r.syncCh <- ack:
		<-ack
	case <-r.stopCh:
	}
}

func (r *reactor) trace() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.fires...)
}

func (r *reactor) stop() { close(r.stopCh) }
