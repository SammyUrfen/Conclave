package arbiter

import (
	"math"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// Fitness is the CONTROL-PLANE evidence about one peer's suitability to coordinate.
//
// It deliberately does NOT contain UploadKbps. Upload is the relay (data-plane)
// resource; mixing it in here is the "supernode score" the architecture forbids, and
// in practice it would keep electing the fat-pipe desktop that is pegged at 100% CPU.
// The two roles are independent facts about a peer and are scored by two different
// functions in two different packages — this one, and overlay.BuildTree.
type Fitness struct {
	// CPUFreePct is 100 - metrics.Report.CPUPct: headroom, not load, so every term
	// in the formula points the same way (bigger is better).
	CPUFreePct float64
	// RTTServerMs is the peer's round trip to the arbiter — a proxy for how quickly
	// it can learn about the meet and how quickly its pushes land.
	RTTServerMs float64
	// LossPct is loss on the CONTROL link. Kept separate from RTT because a link can
	// be fast and lossy, and a lossy control link means topology pushes need retries.
	LossPct float64
	// UptimeSec is seconds since this peer joined this meet. An anti-flap prior: all
	// else equal, prefer the peer that has already proven it stays.
	UptimeSec float64
	// NAT is the peer's reachability class. NATRelayed is a hard disqualifier: a
	// TURN-bound peer's every control frame is someone else's relay hop.
	NAT overlay.NATType
	// Live is the ARBITER'S OWN liveness verdict, not the coordinator's.
	//
	// This is the crux of the failure detector. Typing it as a health value sourced
	// from the coordinator's health FSM is circular in the one case that matters: in
	// Phase 6 that FSM runs ON the peer being judged, so a frozen or partitioned
	// coordinator's detector is the thing that has stopped running and cannot notice
	// its own death. The arbiter therefore keeps its own last-seen map, fed by the
	// same frames the Hub already terminates. For every other peer this is a
	// redundant second opinion; for the sitting coordinator it is the only opinion
	// that can exist.
	Live bool
	// Coordinatable is the peer's own declaration that it is willing to be elected.
	// A hard disqualifier, and conservative by construction: the wire field is a
	// bare bool, so a peer that never reported — or one running a build that predates
	// the flag — is treated as unwilling rather than silently elected.
	Coordinatable bool
}

// The saturation points of the fitness formula. Each one answers "past what value
// does more of this stop telling us anything?", which is a different question from
// "what is acceptable" — these are where the term stops discriminating, not where a
// peer becomes unusable.
const (
	// MaxUsefulRTTMs: past 300 ms round-trip to the arbiter, a peer's control loop
	// is reacting to a picture of the meet that is already a third of a second old.
	// Everything worse is equally unusable, so the term saturates rather than
	// continuing to discriminate between bad and terrible.
	MaxUsefulRTTMs = 300.0

	// MaxUsefulLossPct: 10% loss on the CONTROL link means topology pushes need
	// retries; past that the distinction between bad and terrible does not matter.
	MaxUsefulLossPct = 10.0

	// StableUptimeSec: a peer that has been in the meet 2 minutes has demonstrated
	// it is not a drive-by joiner. Beyond that, more uptime is not more evidence,
	// so the term saturates.
	StableUptimeSec = 120.0
)

// The fitness weights. They sum to exactly 1 so Score lands in [0, 1] without a
// normalisation step, and so a reader can read each one as "this term's share".
//
// Network reliability (0.35) outweighs CPU (0.30) because the coordinator's work is
// tiny — a millisecond of BuildTree per event — while its REACH is everything: a fast
// machine that cannot deliver a push is useless. Loss (0.20) is separate from RTT
// because a link can be fast and lossy. Uptime (0.15) is the smallest term and exists
// purely as an anti-flap prior.
const (
	weightCPU    = 0.30
	weightRTT    = 0.35
	weightLoss   = 0.20
	weightUptime = 0.15
)

// ScoreQuantum is the granularity at which two peers' fitness counts as different.
//
// It exists because Score's inputs became MEASURED. While CPUPct, LossPct and
// RTTServerMs were structurally zero every eligible peer scored identically, so
// ranking candidates by raw score fell through to the name tiebreak and was perfectly
// stable. With live sensors two comparable machines differ by microseconds of RTT,
// their order flips about once a second, and anything that depends on a stable "best
// challenger" — the election dwell above all — is defeated by noise rather than by
// evidence.
//
// 0.01 is chosen to sit in the wide gap between the two scales. Sensor noise on a
// quiet link is worth ~0.0005 of score (0.35 weight x 0.3ms of jitter / 300ms of
// useful range), twenty times smaller. The smallest term that carries real meaning is
// uptime at 0.15, fifteen times larger. Anything in between is a distinction the
// inputs cannot actually support.
//
// It is deliberately NOT applied inside Score: the reported value stays exact, so the
// dashboard and the logs show what was measured. Only ORDERING and the dwell's notion
// of "the same target" are quantized, because those are the decisions a
// hundredth-of-a-point difference must not be allowed to make.
const ScoreQuantum = 0.01

// bucket maps a score onto the ScoreQuantum grid, so that "which of these two peers
// is fitter" is asked at a granularity the sensors can actually answer.
func bucket(score float64) int {
	return int(math.Round(score / ScoreQuantum))
}

// Score maps a Fitness to [0, 1]. Higher is fitter.
//
// It returns exactly 0 for a peer disqualified by ANY of: NAT == NATRelayed, !Live,
// !Coordinatable. A hard zero rather than a penalty, because these are not "worse",
// they are "ineligible", and no bonus elsewhere may buy an ineligible peer the job.
//
// Note that 0 is NOT the arbiter's eligibility test: a fully degraded but willing,
// live, directly-reachable peer also scores 0, and it is still a legal coordinator
// (a bad coordinator beats none). Eligibility is decided by the disqualifiers
// themselves; Score only ranks.
//
// Score is pure and total: same input, same output, no clock, no allocation, and no
// NaN or out-of-range output for any input — including the NaN a future sensor bug
// could put in a Report, which is clamped to the worst case rather than propagated
// into an ordering comparison where it would silently poison the sort.
func Score(f Fitness) float64 {
	if !f.Live || !f.Coordinatable || f.NAT == overlay.NATRelayed {
		return 0
	}
	return weightCPU*more(f.CPUFreePct/100) +
		weightRTT*less(f.RTTServerMs/MaxUsefulRTTMs) +
		weightLoss*less(f.LossPct/MaxUsefulLossPct) +
		weightUptime*more(f.UptimeSec/StableUptimeSec)
}

// more scores a ratio where bigger is better, confined to [0, 1].
//
// NaN maps to 0 — the WORST case, not the best. Both helpers resolve NaN
// pessimistically for their own direction, which is why there are two of them
// rather than one shared clamp: a single clamp would have to pick one NaN answer
// and would then silently hand a full-marks CPU term to a broken sensor. The NaN
// branch is explicit because the obvious min/max chain lets NaN fall through both
// comparisons and escape into the score.
func more(v float64) float64 {
	if math.IsNaN(v) || v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// less scores a ratio where smaller is better, confined to [0, 1]. NaN maps to 0,
// again the worst case for this direction.
func less(v float64) float64 {
	if math.IsNaN(v) || v > 1 {
		return 0
	}
	if v < 0 {
		return 1
	}
	return 1 - v
}

// eligible reports whether a peer may hold the coordinator role at all. It is the
// same three disqualifiers Score zeroes on, named separately because ranking and
// eligibility are different questions and conflating them would make a legitimately
// terrible peer unelectable even when it is the only one there is.
func eligible(f Fitness) bool {
	return f.Live && f.Coordinatable && f.NAT != overlay.NATRelayed
}
