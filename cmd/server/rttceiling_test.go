package main

import (
	"testing"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
)

// TestUnreachableRTTCrossesEveryThresholdThatActsOnIt is the other half of the
// agreement metrics.UnreachableRTTMs documents, and it lives here for the same reason
// TestArbiterIDMatchesSignalingServerID does: this is the only package that can see
// all three constants at once.
//
// metrics cannot name arbiter.MaxUsefulRTTMs or coordinator.DegradedRTTMs — both of
// those packages import metrics, so importing either back is a cycle — so its own
// test restates the numbers as literals. That catches somebody LOWERING the ceiling.
// It cannot catch somebody RAISING a threshold past the ceiling, because the literals
// would still agree with each other while no longer agreeing with the real constants.
// This test is that half.
//
// The consequence if the relationship breaks: a peer whose control-link probe never
// comes back reports UnreachableRTTMs, and the whole point of that value is that it
// scores zero on Score's RTT term and arms the degradation dwell. A ceiling that no
// longer clears both thresholds turns a dead control link into a merely mediocre one,
// and the peer stays electable.
func TestUnreachableRTTCrossesEveryThresholdThatActsOnIt(t *testing.T) {
	t.Run("saturates Score's RTT term", func(t *testing.T) {
		if metrics.UnreachableRTTMs <= arbiter.MaxUsefulRTTMs {
			t.Errorf("metrics.UnreachableRTTMs = %v, arbiter.MaxUsefulRTTMs = %v; "+
				"an unreachable peer no longer scores zero on the RTT term",
				metrics.UnreachableRTTMs, arbiter.MaxUsefulRTTMs)
		}
	})

	t.Run("arms the degradation dwell", func(t *testing.T) {
		if metrics.UnreachableRTTMs <= coordinator.DegradedRTTMs {
			t.Errorf("metrics.UnreachableRTTMs = %v, coordinator.DegradedRTTMs = %v; "+
				"an unreachable peer no longer arms the dwell",
				metrics.UnreachableRTTMs, coordinator.DegradedRTTMs)
		}
	})

	t.Run("an unreachable peer actually scores zero on the RTT term", func(t *testing.T) {
		// The relationship stated as its consequence rather than as arithmetic: a
		// peer that is perfect on every other axis but unreachable must lose exactly
		// the RTT weight. Asserting the delta rather than an absolute keeps this
		// robust to a reweighting, which is a legitimate change; a broken ceiling is
		// not.
		perfect := arbiter.Fitness{
			CPUFreePct: 100, RTTServerMs: 0, LossPct: 0, UptimeSec: 1e6,
			Live: true, Coordinatable: true,
		}
		unreachable := perfect
		unreachable.RTTServerMs = metrics.UnreachableRTTMs

		lost := arbiter.Score(perfect) - arbiter.Score(unreachable)
		if lost < 0.34 || lost > 0.36 {
			t.Errorf("an unreachable control link cost %.4f of Score; want the full RTT weight (~0.35)", lost)
		}
	})
}
