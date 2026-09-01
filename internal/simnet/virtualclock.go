package simnet

import (
	"fmt"
	"sync"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
)

// maxFiresPerAdvance bounds the work one Advance may do. A control loop that
// re-arms a timer with a non-positive delay would otherwise spin the advance loop
// forever, and a hanging test is strictly worse than a failing one: it yields no
// stack, no seed, and no clue. 100_000 is far beyond any legitimate scenario — a
// ten-minute virtual meet at a one-second heartbeat is 600 fires — so tripping this
// always means a bug, never a big-but-honest run.
const maxFiresPerAdvance = 100_000

// VirtualClock is a deterministic clock.Clock: time advances only when Advance is
// called, never on its own. It is the mechanism that makes hysteresis, dwell, and
// heartbeat timeouts testable — all three are inherently temporal, and none can be
// asserted on reliably against a wall clock without sleeps that are either flaky or
// slow.
//
// It is safe for concurrent use: the control loop under test lives on its own
// goroutine and will call Now/Reset/Stop while the driving goroutine is inside
// Advance.
type VirtualClock struct {
	mu      sync.Mutex
	now     time.Time
	nextID  uint64
	nextSeq uint64
	// armed holds every un-fired timer and every live ticker, keyed by identity.
	// Ranging it is safe — and is the only map range in this package — because the
	// range SELECTS A UNIQUE MINIMUM under the total order (deadline, seq) rather
	// than building an order out of the iteration itself.
	armed   map[uint64]*virtualTimer
	barrier func()
}

// NewVirtualClock returns a clock stopped at start.
func NewVirtualClock(start time.Time) *VirtualClock {
	return &VirtualClock{now: start, armed: make(map[uint64]*virtualTimer)}
}

// SetBarrier registers a quiescence hook the advance loop runs after EVERY
// individual timer fire. It is not decoration — it is what makes Advance
// deterministic across more than one deadline.
//
// Advance fires one deadline at a time so that a timer re-armed by its own handler
// takes its correct place in the virtual timeline, ahead of any later deadline that
// has not fired yet. But the handler runs on ANOTHER goroutine (it receives from the
// timer's channel), so without a barrier the next iteration's view of the armed set
// is decided by the Go scheduler, and the trace stops being replayable. The barrier
// round-trips through the loop under test — in practice coordinator.Sync — so the
// reaction is complete before the next deadline is chosen.
//
// With no barrier set, Advance still fires in (deadline, seq) order, but a
// mid-Advance Reset may or may not be seen in time. That is fine for a clock nobody
// is reacting to, and wrong for anything else. Scenario wires this automatically.
//
// The hook must not itself call Advance.
func (c *VirtualClock) SetBarrier(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.barrier = fn
}

// Now reports the clock's current virtual instant.
func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer arms a one-shot timer for d.
func (c *VirtualClock) NewTimer(d time.Duration) clock.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.armLocked(d, 0)
}

// NewTicker arms a repeating ticker with period d. It panics on a non-positive
// period, exactly as time.NewTicker does — a zero-period ticker is a spin, and the
// advance loop would hit maxFiresPerAdvance instead of reporting the real mistake.
func (c *VirtualClock) NewTicker(d time.Duration) clock.Ticker {
	if d <= 0 {
		panic("simnet: NewTicker period must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return virtualTicker{c.armLocked(d, d)}
}

// After is the handle-less convenience. It arms a timer nobody can Stop, so the
// deadline stays armed (and counted by Pending) until it fires — see clock.Clock.
// Prefer NewTimer in anything that loops.
func (c *VirtualClock) After(d time.Duration) <-chan time.Time {
	return c.NewTimer(d).C()
}

// Pending reports how many timers and tickers are armed. A scenario that ends
// holding unexpected pending deadlines usually means a control loop leaked one —
// which on a real clock is an invisible slow leak and here is a one-line assertion.
func (c *VirtualClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.armed)
}

// Advance moves virtual time forward by d, firing every timer and ticker that
// becomes due. See AdvanceTo for the ordering and delivery rules.
func (c *VirtualClock) Advance(d time.Duration) {
	if d < 0 {
		panic(fmt.Sprintf("simnet: Advance(%v): time never runs backward; a test that wants that has a bug", d))
	}
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()
	c.AdvanceTo(target)
}

// AdvanceTo moves virtual time to t, firing what becomes due on the way. It panics
// if t is before Now: time never runs backward, and a silent no-op would hide the
// bug that asked for it.
//
// DETERMINISM RULE: deadlines fire strictly in (deadline, sequence) order. The
// sequence is a monotonic counter stamped at NewTimer/NewTicker time and RE-STAMPED
// on every Reset and on every ticker re-arm, so two timers due at the same virtual
// instant fire in the order they were most recently armed — never in map order,
// which Go randomises.
//
// One deadline is fired per iteration and the armed set is re-examined afterwards,
// so a timer a handler re-arms mid-advance is placed correctly in the timeline
// rather than being stranded in the past. That is only sound if the handler has
// finished; see SetBarrier.
//
// DELIVERY RULE: every channel has capacity 1 and the send is non-blocking. A test
// that has not drained a fired timer cannot deadlock Advance; it simply misses the
// tick, exactly like a real time.Ticker's consumer. The barrier that makes an
// assertion safe is Scenario.Settle, not Advance.
func (c *VirtualClock) AdvanceTo(t time.Time) {
	c.mu.Lock()
	if t.Before(c.now) {
		from := c.now
		c.mu.Unlock()
		panic(fmt.Sprintf("simnet: AdvanceTo(%v) would rewind the clock from %v; time never runs backward", t, from))
	}
	barrier := c.barrier
	c.mu.Unlock()

	for fired := 0; ; fired++ {
		if fired > maxFiresPerAdvance {
			panic(fmt.Sprintf("simnet: more than %d timer fires in one Advance; a handler is almost certainly re-arming with a non-positive delay", maxFiresPerAdvance))
		}
		c.mu.Lock()
		due := c.earliestDueLocked(t)
		if due == nil {
			c.now = t
			c.mu.Unlock()
			return
		}
		c.now = due.deadline
		c.fireLocked(due)
		c.mu.Unlock()

		if barrier != nil {
			barrier()
		}
	}
}

// earliestDueLocked returns the single armed timer with the smallest (deadline,
// seq) that is due at or before limit, or nil when nothing is.
func (c *VirtualClock) earliestDueLocked(limit time.Time) *virtualTimer {
	var best *virtualTimer
	for _, tm := range c.armed {
		if tm.deadline.After(limit) {
			continue
		}
		if best == nil || tm.deadline.Before(best.deadline) ||
			(tm.deadline.Equal(best.deadline) && tm.seq < best.seq) {
			best = tm
		}
	}
	return best
}

// fireLocked delivers tm's deadline and either retires it (a one-shot) or re-arms it
// for another period (a ticker), re-stamping the sequence so a re-armed ticker sorts
// behind anything armed since.
func (c *VirtualClock) fireLocked(tm *virtualTimer) {
	select {
	case tm.ch <- tm.deadline:
	default: // capacity 1, coalesced: see the DELIVERY RULE
	}
	if tm.period > 0 {
		tm.deadline = tm.deadline.Add(tm.period)
		tm.seq = c.nextSeq
		c.nextSeq++
		return
	}
	tm.armed = false
	delete(c.armed, tm.id)
}

// armLocked creates and registers a timer due in d, with period p (0 for a one-shot).
func (c *VirtualClock) armLocked(d, p time.Duration) *virtualTimer {
	tm := &virtualTimer{
		clk:      c,
		ch:       make(chan time.Time, 1),
		id:       c.nextID,
		seq:      c.nextSeq,
		deadline: c.now.Add(d),
		period:   p,
		armed:    true,
	}
	c.nextID++
	c.nextSeq++
	c.armed[tm.id] = tm
	return tm
}

// virtualTimer is the one-shot timer and the engine behind virtualTicker.
type virtualTimer struct {
	clk      *VirtualClock
	ch       chan time.Time
	id       uint64
	seq      uint64
	deadline time.Time
	period   time.Duration // 0 ⇒ one-shot
	armed    bool
}

// C is the delivery channel (a method, not a field, because interfaces cannot
// declare fields — see internal/clock).
func (t *virtualTimer) C() <-chan time.Time { return t.ch }

// Reset restarts the timer for d and reports whether it was still armed, matching
// time.Timer.Reset — including the caveat that it does NOT drain the channel. It
// re-stamps the tiebreak sequence, so a reset timer sorts behind every timer armed
// since: "creation order" means most-recent arming, which is the only rule that
// stays meaningful once timers are recycled in a loop.
func (t *virtualTimer) Reset(d time.Duration) bool {
	t.clk.mu.Lock()
	defer t.clk.mu.Unlock()
	was := t.armed
	t.deadline = t.clk.now.Add(d)
	t.seq = t.clk.nextSeq
	t.clk.nextSeq++
	t.armed = true
	t.clk.armed[t.id] = t
	return was
}

// Stop retires the timer and reports whether it had not already fired or been
// stopped. It does not close the channel, matching package time.
func (t *virtualTimer) Stop() bool {
	t.clk.mu.Lock()
	defer t.clk.mu.Unlock()
	was := t.armed
	t.armed = false
	delete(t.clk.armed, t.id)
	return was
}

// virtualTicker adapts virtualTimer to clock.Ticker. The wrapper exists for one
// reason: clock.Ticker.Stop returns NOTHING while clock.Timer.Stop returns a bool,
// and Go matches method signatures exactly — so a single type cannot satisfy both.
// Shadowing Stop here is the whole adapter.
type virtualTicker struct{ *virtualTimer }

// Stop halts the ticker. It does not close the channel.
func (t virtualTicker) Stop() { t.virtualTimer.Stop() }
