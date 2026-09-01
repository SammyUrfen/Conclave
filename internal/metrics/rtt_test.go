package metrics

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

// TestRTTProbeReportsNothingBeforeItHasProbed pins the distinction the wire format
// cannot express on its own: Report.RTTServerMs is a float64 whose zero value means
// BOTH "not measured" and "a perfect 0 ms link", and arbiter.Score reads 0 ms as full
// marks on its largest term (weight 0.35). So the probe must hand its caller an
// explicit ok, and the caller must leave the field alone when it is false.
//
// The mutation this catches: returning (0, true) before the first probe, which would
// give every peer a perfect network score for the first few seconds of its life —
// exactly the window in which a bootstrap election runs.
func TestRTTProbeReportsNothingBeforeItHasProbed(t *testing.T) {
	p := NewRTTProbe(discardLogger(), 0, nil, func(context.Context) error { return nil })
	if ms, ok := p.LastMs(); ok {
		t.Fatalf("un-probed sampler reported %v ms; it must report nothing", ms)
	}
}

// TestRTTProbeMeasuresOnTheInjectedClock pins that the elapsed time comes from the
// clock.Clock seam and not from package time. The fake advances only inside the probe
// function, so a measurement taken any other way reads zero.
//
// The mutation this catches: using time.Since / time.Now, which the determinism gate
// would also catch in production code — but this pins the VALUE, so a future refactor
// that keeps the clock and measures the wrong interval still fails.
func TestRTTProbeMeasuresOnTheInjectedClock(t *testing.T) {
	clk := newFakeClock()
	p := NewRTTProbe(discardLogger(), 0, clk, func(context.Context) error {
		clk.advance(40 * time.Millisecond)
		return nil
	})
	p.probeOnce(context.Background())

	ms, ok := p.LastMs()
	if !ok {
		t.Fatal("a successful probe must report a value")
	}
	if math.Abs(ms-40) > 1e-9 {
		t.Errorf("got %v ms, want 40 ms", ms)
	}
}

// TestFailedProbeReportsTheUnreachableCeiling is the sharp one, and it pins a
// direction rather than a number.
//
// A probe that errors immediately — a closed socket returns instantly — has an
// elapsed time of ~0. Recording that would put 0 ms on the wire, which Score reads as
// a PERFECT control link. A peer whose connection to the arbiter has just died would
// therefore score its network term higher than a healthy peer on a 20 ms link. The
// sensor must fail toward "unusable", never toward "ideal".
//
// The mutation this catches: recording the measured elapsed time on the error path,
// which passes every other test in this file.
func TestFailedProbeReportsTheUnreachableCeiling(t *testing.T) {
	clk := newFakeClock()
	p := NewRTTProbe(discardLogger(), 0, clk, func(context.Context) error {
		return errors.New("socket closed") // returns instantly: elapsed ~0
	})
	p.probeOnce(context.Background())

	ms, ok := p.LastMs()
	if !ok {
		t.Fatal("a failed probe must still report a value: silence would leave a stale-good reading standing")
	}
	if ms != UnreachableRTTMs {
		t.Errorf("got %v ms, want the unreachable ceiling %v ms", ms, UnreachableRTTMs)
	}
}

// TestFailureOverwritesAGoodReading pins that the ceiling REPLACES a previous good
// measurement rather than being ignored while one exists. A peer that measured 12 ms
// a minute ago and cannot reach the arbiter now must not keep reporting 12 ms.
func TestFailureOverwritesAGoodReading(t *testing.T) {
	clk := newFakeClock()
	fail := false
	p := NewRTTProbe(discardLogger(), 0, clk, func(context.Context) error {
		clk.advance(12 * time.Millisecond)
		if fail {
			return errors.New("gone")
		}
		return nil
	})
	p.probeOnce(context.Background())
	if ms, _ := p.LastMs(); math.Abs(ms-12) > 1e-9 {
		t.Fatalf("setup: got %v ms, want 12 ms", ms)
	}
	fail = true
	p.probeOnce(context.Background())
	if ms, _ := p.LastMs(); ms != UnreachableRTTMs {
		t.Errorf("got %v ms after the link died, want %v ms", ms, UnreachableRTTMs)
	}
}

// TestUnreachableCeilingExceedsEveryThresholdItMustCross states, as arithmetic, why
// the constant is the value it is. metrics cannot import arbiter or coordinator (both
// import metrics), so the thresholds are restated here and cmd/server carries the
// cross-package test that the restatement is still true. This half catches "somebody
// lowered UnreachableRTTMs"; that half catches "somebody raised a threshold past it".
func TestUnreachableCeilingExceedsEveryThresholdItMustCross(t *testing.T) {
	// arbiter.MaxUsefulRTTMs — past this, Score's RTT term is saturated at zero.
	const maxUsefulRTTMs = 300.0
	// coordinator.DegradedRTTMs — past this, the degradation dwell arms.
	const degradedRTTMs = 400.0

	if UnreachableRTTMs <= maxUsefulRTTMs {
		t.Errorf("UnreachableRTTMs %v does not saturate Score's RTT term (%v)", UnreachableRTTMs, maxUsefulRTTMs)
	}
	if UnreachableRTTMs <= degradedRTTMs {
		t.Errorf("UnreachableRTTMs %v does not arm the degradation dwell (%v)", UnreachableRTTMs, degradedRTTMs)
	}
}

// TestRTTProbeRunsImmediatelyThenOnTheTicker pins the same immediate-then-ticker
// shape Reporter.Run has, and for the same reason: a peer that waits a full interval
// before its first probe reports RTTServerMs unset through the whole window in which
// the bootstrap election happens.
func TestRTTProbeRunsImmediatelyThenOnTheTicker(t *testing.T) {
	clk := newFakeClock()
	probes := make(chan struct{}, 4)
	p := NewRTTProbe(discardLogger(), time.Second, clk, func(context.Context) error {
		clk.advance(time.Millisecond)
		probes <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()

	select {
	case <-probes:
	case <-time.After(2 * time.Second):
		t.Fatal("no probe before the first tick: Run waits an interval it should not")
	}
	clk.tick()
	select {
	case <-probes:
	case <-time.After(2 * time.Second):
		t.Fatal("no probe on the ticker")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on context cancellation")
	}
}

// TestRTTProbeNilClockDefaults matches the convention every seam in this project
// follows: nil means "give me the real one".
func TestRTTProbeNilClockDefaults(t *testing.T) {
	p := NewRTTProbe(discardLogger(), 0, nil, func(context.Context) error { return nil })
	if p.clk == nil {
		t.Error("nil Clock was not defaulted to clock.System()")
	}
	if p.interval != DefaultInterval {
		t.Errorf("interval = %v, want default %v", p.interval, DefaultInterval)
	}
}
