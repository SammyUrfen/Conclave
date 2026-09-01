package clock_test

import (
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
)

// tick is a short, real duration used by the System() tests. The clock package is
// the one place a wall-clock test is legitimate (it IS the wall-clock adapter), so
// keep the waits small and always assert on "did it fire", never on "how long it
// took" — the latter is what makes timing tests flaky on a loaded machine.
const tick = 20 * time.Millisecond

// wait is the upper bound on how long a fired timer may take to be observed. It is
// generously larger than tick so ordinary scheduler jitter cannot fail the test.
const wait = 2 * time.Second

func TestSystemClock(t *testing.T) {
	clk := clock.System()

	t.Run("Now returns wall time", func(t *testing.T) {
		before := time.Now()
		got := clk.Now()
		after := time.Now()
		if got.Before(before) || got.After(after) {
			t.Errorf("Now() = %v, want within [%v, %v]", got, before, after)
		}
	})

	t.Run("NewTimer fires once", func(t *testing.T) {
		tm := clk.NewTimer(tick)
		defer tm.Stop()
		select {
		case <-tm.C():
		case <-time.After(wait):
			t.Fatal("timer did not fire")
		}
	})

	t.Run("Stop prevents an un-fired timer", func(t *testing.T) {
		tm := clk.NewTimer(wait)
		if !tm.Stop() {
			t.Error("Stop() = false on an armed timer, want true")
		}
		select {
		case <-tm.C():
			t.Error("stopped timer fired")
		case <-time.After(tick):
		}
	})

	t.Run("Reset rearms a stopped timer", func(t *testing.T) {
		tm := clk.NewTimer(wait)
		tm.Stop()
		tm.Reset(tick)
		select {
		case <-tm.C():
		case <-time.After(wait):
			t.Fatal("reset timer did not fire")
		}
	})

	t.Run("NewTicker ticks repeatedly", func(t *testing.T) {
		tk := clk.NewTicker(tick)
		defer tk.Stop()
		for i := 0; i < 2; i++ {
			select {
			case <-tk.C():
			case <-time.After(wait):
				t.Fatalf("ticker stopped after %d ticks", i)
			}
		}
	})

	t.Run("After fires", func(t *testing.T) {
		select {
		case <-clk.After(tick):
		case <-time.After(wait):
			t.Fatal("After did not fire")
		}
	})
}

// TestInterfaceIsImplementableOutsidePackage is the §2.4 guarantee in test form:
// a consumer (simnet's VirtualClock, a per-package fake) must be able to satisfy
// clock.Clock without importing anything but this package. If Timer or Ticker ever
// grew a field-shaped requirement, or if the method set referenced an unexported
// type, this would stop compiling — which is exactly the regression to catch.
func TestInterfaceIsImplementableOutsidePackage(t *testing.T) {
	var c clock.Clock = fake{}
	if got := c.Now(); !got.IsZero() {
		t.Errorf("fake Now() = %v, want the zero time", got)
	}
	if c.NewTimer(time.Second) == nil || c.NewTicker(time.Second) == nil || c.After(time.Second) == nil {
		t.Error("fake clock returned a nil timer/ticker/channel")
	}
}

type fake struct{}

func (fake) Now() time.Time                       { return time.Time{} }
func (fake) NewTimer(time.Duration) clock.Timer   { return fakeTimer{} }
func (fake) NewTicker(time.Duration) clock.Ticker { return fakeTicker{} }
func (fake) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

type fakeTimer struct{}

func (fakeTimer) C() <-chan time.Time      { return make(chan time.Time) }
func (fakeTimer) Reset(time.Duration) bool { return false }
func (fakeTimer) Stop() bool               { return false }

type fakeTicker struct{}

func (fakeTicker) C() <-chan time.Time { return make(chan time.Time) }
func (fakeTicker) Stop()               {}
