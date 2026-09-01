package arbiter_test

import (
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
)

// TestDwellSurvivesChallengerJitter is a REGRESSION TEST FOR A BUG THE SENSORS
// INTRODUCED, and it is worth stating the whole causal chain because the failure has
// nothing to do with the code it breaks.
//
// Before RTTServerMs and CPUPct were measured, every eligible peer scored IDENTICALLY
// (docs/DESIGN.md §8.1(b): 0.85–1.0, with uptime the only varying term). Identical
// scores meant candidates() fell through to its name tiebreak, so "the best
// challenger" was a perfectly stable choice and the election dwell counted up
// undisturbed.
//
// With real sensors the top two candidates are no longer tied — they differ by
// microseconds of measured RTT, and which one is ahead FLIPS from one report to the
// next. The dwell resets whenever pendingTarget changes:
//
//	if ms.pendingReason != want || ms.pendingTarget != chal.id { ...restart the clock }
//
// so on a fleet with two comparable peers the clock restarts about once a second and
// the 20 s dwell is NEVER reached. No voluntary promotion or demotion can ever fire.
//
// This was observed live before it was written down: a three-peer meet run with
// -coordinate -elect sat on the arbiter for five minutes with two candidates at 0.835
// and 0.835 whose measured RTTs traded places every few seconds, and announced
// nothing beyond bootstrap.
//
// The dwell's own comment says it exists so that "a flapping metric" cannot move the
// role. The defect is that a flapping metric instead PINS the role, which is the same
// mechanism failing in the other direction.
func TestDwellSurvivesChallengerJitter(t *testing.T) {
	// Two peers a real deployment would call interchangeable: same class of machine,
	// same link, differing only by measurement noise.
	twin := func(name string, rtt float64) peerOpts {
		return peerOpts{name: name, uploadKbps: 5000, coordinatable: true, cpuPct: 5, rttMs: rtt}
	}

	h := newHarness(t, arbiter.Config{
		Elect: true, Coordinate: true, ArbiterID: arbiter.DefaultArbiterID,
	})
	h.join("m", "p1", twin("alice", 0.30))
	h.join("m", "p2", twin("bob", 0.26))

	// Beat for well past the dwell and the term floor, re-reporting each peer with
	// jittered RTT so that the leader changes hands repeatedly — exactly what the
	// live run showed. The jitter is tiny: hundredths of a millisecond on a link
	// whose useful range is 300 ms.
	jitter := []float64{0.30, 0.26, 0.51, 0.44, 0.34, 0.23, 0.58, 0.65}
	for i := range 120 {
		h.report("m", "p1", twin("alice", jitter[i%len(jitter)]))
		h.report("m", "p2", twin("bob", jitter[(i+1)%len(jitter)]))
		h.elapse("m", time.Second)
	}

	m := h.meet("m")
	if m.Coordinator == "" {
		t.Fatalf("after %d s the arbiter still holds the meet: the election dwell is being "+
			"reset by measurement noise and can never elapse", 120)
	}
	t.Logf("handed over to %q after jittered reporting", m.Coordinator)
}

// TestDwellStillRejectsARealTargetChange is the other half, and it is what stops the
// fix above from becoming "ignore the target entirely".
//
// The dwell must still restart when the challenger changes for a REASON — when a
// materially better candidate appears, the evidence for handing the role to the
// previous target is genuinely stale, and starting a term on it would be acting on a
// decision the arbiter no longer believes. Only noise-scale churn may be absorbed.
func TestDwellStillRejectsARealTargetChange(t *testing.T) {
	h := newHarness(t, arbiter.Config{
		Elect: true, Coordinate: true, ArbiterID: arbiter.DefaultArbiterID,
	})
	h.join("m", "p1", modest("alice"))
	h.join("m", "p2", modest("bob"))

	// Halfway through the dwell a genuinely much better peer arrives. It must win,
	// not inherit the pending target's accumulated dwell.
	h.elapse("m", 10*time.Second)
	h.join("m", "p3", strong("carol"))
	h.elapse("m", 90*time.Second)

	m := h.meet("m")
	if m.Coordinator != "carol" {
		t.Errorf("coordinator = %q, want carol: a materially fitter candidate must take the role", m.Coordinator)
	}
}
