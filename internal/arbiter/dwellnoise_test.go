package arbiter_test

import (
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/overlay"
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

// TestDwellSurvivesABucketBoundary covers the case quantized ranking does NOT fully
// erase, and it exists because instrumenting a live run turned up a mechanism that
// reasoning had missed.
//
// Two peers with a sub-quantum score gap normally share a bucket, where the name
// tiebreak makes the ordering stable. But the uptime term drives BOTH scores steadily
// upward over a meet's first two minutes, so the pair sweeps across boundary after
// boundary — and at each crossing the marginally better peer enters the higher bucket
// first. When that peer sorts LATER by name, the best challenger flips for the
// duration of the crossing and the dwell restarts. The boundary case is therefore not
// a rare accident; it recurs on a schedule, driven by a term that always moves.
//
// What this asserts is that the handover still COMPLETES. The crossings delay it —
// live instrumentation counted 8 challenger flips over 370 samples, against a flip on
// nearly every sample before quantization — but each stable interval between
// crossings is long enough for the dwell to elapse, and the drift stops entirely once
// uptime saturates. A pending-target stickiness rule was written to erase even this
// and then deleted: it bought nothing this test can detect, and absorbing a
// boundary-scale difference also absorbs an exact tie, at which point the pending
// target (whichever peer joined first) decides a tie that
// TestTiesBreakDeterministically requires the NAME order to decide.
//
// alpha is deliberately the WORSE peer and the alphabetically first one. With those
// aligned the flip cannot happen at all, and this test would pass against the bug.
func TestDwellSurvivesABucketBoundary(t *testing.T) {
	near := func(name string, rtt float64) peerOpts {
		return peerOpts{name: name, uploadKbps: 5000, coordinatable: true, cpuPct: 5, rttMs: rtt}
	}
	h := newHarness(t, arbiter.Config{
		Elect: true, Coordinate: true, ArbiterID: arbiter.DefaultArbiterID,
	})
	h.join("m", "p1", near("alpha", 5.0)) // name-first, marginally WORSE
	h.join("m", "p2", near("bravo", 0.7)) // name-later, marginally BETTER

	// A score gap of 0.35 x (4.3ms / 300ms) = 0.005 — half a quantum, i.e. below the
	// resolution this system claims to distinguish, and 1.4% of the useful RTT range.
	// Sized so the pair spends about half of every boundary period in different
	// buckets; a much smaller gap makes the crossing window so narrow that whether a
	// beat lands inside it is luck.
	h.elapse("m", 150*time.Second)

	if got := h.meet("m").Coordinator; got == "" {
		t.Fatal("the arbiter still holds the meet: the dwell never elapsed between boundary crossings")
	}
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

// TestScoreQuantumBracketsBothScales earns the constant's value from both directions.
// TestDwellSurvivesChallengerJitter already fails if the quantum is too SMALL to
// absorb sensor noise; on its own that is satisfied by making the quantum enormous,
// which would erase every real fitness difference and reduce the election to a name
// sort. This is the upper bound.
//
// The two scales it must sit between:
//   - below: measurement noise, ~0.0005 of score on a quiet link.
//   - above: the smallest term that carries meaning, the 0.15 uptime weight.
func TestScoreQuantumBracketsBothScales(t *testing.T) {
	const sensorNoise = 0.0005 // 0.35 weight x 0.3ms jitter / 300ms useful range

	if arbiter.ScoreQuantum <= sensorNoise {
		t.Errorf("ScoreQuantum %v does not absorb sensor noise %v; candidate order will follow jitter",
			arbiter.ScoreQuantum, sensorNoise)
	}
	// The uptime weight is the smallest term in Score. A quantum that large would put
	// a peer with two minutes of proven stability in the same bucket as one that
	// joined a second ago.
	if arbiter.ScoreQuantum >= 0.15 {
		t.Errorf("ScoreQuantum %v is at least the uptime weight 0.15; a real fitness difference "+
			"would be quantized away", arbiter.ScoreQuantum)
	}

	// Stated as its consequence: two peers differing by one full weight term must
	// still be ranked apart.
	base := arbiter.Fitness{
		CPUFreePct: 100, RTTServerMs: 0, LossPct: 0, UptimeSec: arbiter.StableUptimeSec,
		NAT: overlay.NATDirect, Live: true, Coordinatable: true,
	}
	worse := base
	worse.UptimeSec = 0 // costs exactly the 0.15 uptime weight
	if gap := arbiter.Score(base) - arbiter.Score(worse); gap <= arbiter.ScoreQuantum {
		t.Errorf("a full uptime term is worth %v, within one quantum %v", gap, arbiter.ScoreQuantum)
	}
}
