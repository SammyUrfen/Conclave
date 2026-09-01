package arbiter

import (
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestScoreFloorOfAFullyDegradedPeer computes what a live coordinator can actually
// reach, and it exists because the answer is a surprise with an operational
// consequence.
//
// Now that the sensors are real, the obvious expectation is "load the coordinator's
// CPU and watch it get demoted". It cannot happen, and the reason is arithmetic
// rather than a bug. Score's four terms are weighted 0.30 CPU + 0.35 RTT + 0.20 loss
// + 0.15 uptime, so a peer that is MAXIMALLY bad on CPU and RTT but clean on loss,
// and settled, still scores 0.20 + 0.15 = 0.35 — and the demotion test is
// `inc < demoteBelow` with DemoteBelowScore = 0.35, a STRICT comparison against
// exactly that number.
//
// So: CPU exhaustion alone removes 0.30 of a possible 1.0 and can never demote
// anybody. CPU plus an unreachable arbiter lands precisely ON the floor and still
// cannot. Voluntary demotion of a settled coordinator REQUIRES a loss signal, which
// in this system comes only from RTCP receiver reports on forwarded media — so it
// requires the coordinator to also be a relay with children that are actually losing
// packets.
//
// This test pins the numbers so the claim is checkable rather than asserted, and so
// that a future re-weighting has to confront it deliberately.
func TestScoreFloorOfAFullyDegradedPeer(t *testing.T) {
	settled := Fitness{
		CPUFreePct: 0, RTTServerMs: 10000, LossPct: 0,
		UptimeSec: StableUptimeSec * 2, // long settled: the uptime term is maxed
		NAT:       overlay.NATDirect, Live: true, Coordinatable: true,
	}

	t.Run("CPU and RTT exhausted still cannot cross the floor", func(t *testing.T) {
		got := Score(settled)
		if got != weightLoss+weightUptime {
			t.Fatalf("floor = %v, want %v (loss + uptime, the two terms nothing above can move)",
				got, weightLoss+weightUptime)
		}
		if got < DemoteBelowScore {
			t.Errorf("floor %v is below DemoteBelowScore %v; this test's premise no longer holds "+
				"and the live-demotion limitation in docs/DESIGN.md should be revisited", got, DemoteBelowScore)
		}
	})

	t.Run("CPU alone removes only its own weight", func(t *testing.T) {
		healthy := Fitness{
			CPUFreePct: 100, RTTServerMs: 0, LossPct: 0, UptimeSec: StableUptimeSec * 2,
			NAT: overlay.NATDirect, Live: true, Coordinatable: true,
		}
		loaded := healthy
		loaded.CPUFreePct = 0

		lost := Score(healthy) - Score(loaded)
		if lost < weightCPU-1e-9 || lost > weightCPU+1e-9 {
			t.Errorf("a fully loaded CPU cost %.4f, want exactly the CPU weight %.2f", lost, weightCPU)
		}
		if Score(loaded) < DemoteBelowScore {
			t.Errorf("a CPU-loaded peer scores %.4f, below the floor %.2f — the limitation is gone",
				Score(loaded), DemoteBelowScore)
		}
	})

	t.Run("adding loss is what actually crosses it", func(t *testing.T) {
		lossy := settled
		lossy.LossPct = MaxUsefulLossPct
		got := Score(lossy)
		if got >= DemoteBelowScore {
			t.Errorf("even with saturated loss the score is %.4f, still not below %.2f", got, DemoteBelowScore)
		}
	})
}
