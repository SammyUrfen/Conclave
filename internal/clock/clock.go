package clock

import "time"

// Clock is the subset of package time this project depends on. Every component
// that measures or waits takes one at construction, so a test can hand it a
// virtual clock and drive time explicitly.
type Clock interface {
	// Now reports the clock's current instant.
	Now() time.Time
	// NewTimer arms a one-shot timer for d.
	NewTimer(d time.Duration) Timer
	// NewTicker arms a repeating ticker with period d.
	NewTicker(d time.Duration) Ticker
	// After is the convenience one-shot. Prefer NewTimer in a control loop: After
	// hands back a bare channel with no handle, so the caller cannot Stop it and a
	// virtual clock cannot retire it — an After in a loop that runs per event
	// accumulates armed deadlines for the lifetime of the process.
	After(d time.Duration) <-chan time.Time
}

// Timer mirrors *time.Timer.
//
// A virtual implementation is free to carry extra unexported state — notably the
// monotonic creation/Reset sequence a deterministic clock needs to break ties
// between timers due at the same instant. That state stays behind this interface
// on purpose: the discriminator is an implementation detail of whoever supplies
// the clock, so it can be added later without touching a single caller.
type Timer interface {
	// C is the delivery channel. It is a method, not a field, because interfaces
	// cannot declare fields.
	C() <-chan time.Time
	// Reset restarts the timer for d and reports whether it was still active, like
	// time.Timer.Reset — and with the same caveat: it does NOT drain the channel,
	// so a caller that has not received a previously fired value may still see it.
	// The only unambiguous usage pattern is a timer owned and reset by a single
	// goroutine, which is what the control loops here do.
	Reset(d time.Duration) bool
	// Stop prevents an un-fired timer from firing and reports whether it had not
	// already fired or been stopped.
	Stop() bool
}

// Ticker mirrors *time.Ticker.
type Ticker interface {
	// C is the delivery channel. See Timer.C for why it is a method.
	C() <-chan time.Time
	// Stop halts the ticker. It does not close the channel.
	Stop()
}

// System returns the real, wall-clock implementation backed by package time. It is
// what every binary wires at startup; nil Clock fields default to it.
//
// The returned value is stateless, so one instance may be shared by every component
// in a process.
func System() Clock { return systemClock{} }

// systemClock is the thin adapter from package time to the interfaces above.
type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) NewTimer(d time.Duration) Timer         { return systemTimer{t: time.NewTimer(d)} }
func (systemClock) NewTicker(d time.Duration) Ticker       { return systemTicker{t: time.NewTicker(d)} }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type systemTimer struct{ t *time.Timer }

func (s systemTimer) C() <-chan time.Time        { return s.t.C }
func (s systemTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }
func (s systemTimer) Stop() bool                 { return s.t.Stop() }

type systemTicker struct{ t *time.Ticker }

func (s systemTicker) C() <-chan time.Time { return s.t.C }
func (s systemTicker) Stop()               { s.t.Stop() }
