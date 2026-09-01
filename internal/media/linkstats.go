package media

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/metrics"
)

// RTTMemory is how long a measured pairwise RTT stays usable after the
// PeerConnection that produced it has gone away.
//
// THE MEMORY IS THE FEATURE, not an optimisation. overlay.BuildTree's min-latency
// rank reads Node.RTT[candidate], and a peer only holds a PeerConnection to its
// CURRENT tree neighbours — so a sensor that forgot a peer the moment its edge closed
// would give the incumbent parent a real number and leave every challenger at
// unknownRTT (math.MaxFloat64). No challenger could ever win, and -stickiness-ms,
// whose entire job is to decide whether a challenger's advantage is worth an
// interruption, could never bind. Remembering turns a former neighbour into a
// comparable candidate, which is what makes "a closer relay wins a re-parent"
// reachable at all.
//
// Two minutes is the trade. Long enough that a peer which has been moved around by
// one churn event still remembers where it came from — the tree re-optimises on a
// scale of seconds, so anything shorter forgets before the comparison can be made.
// Short enough that the value is still evidence: a path between two hosts does not
// usually change character inside two minutes, and 120 s is the same horizon
// arbiter.StableUptimeSec already treats as "settled".
//
// WHAT THIS DOES NOT FIX, stated plainly because the limitation is structural: a peer
// it has NEVER been connected to stays absent, so first attachment is still
// RTT-blind. WebRTC offers no way to measure a path without forming it.
//
// A remembered value can be wrong — the link may have degraded since. The damage is
// bounded by the correction path: acting on it opens the edge, the fresh measurement
// immediately replaces the remembered one, and the next rebuild sees the truth.
const RTTMemory = 2 * time.Minute

// rttSample is one measurement and when it was taken.
type rttSample struct {
	ms float64
	at time.Time
}

// rttStore holds this peer's measured round-trips to other peers, live and
// remembered. It is not goroutine-safe on its own; Router guards it with mu.
type rttStore struct {
	byName map[string]rttSample
}

// record stores a measurement, replacing any earlier one for the same peer and
// restarting its TTL.
//
// It REJECTS anything that is not a finite, non-negative number. A NaN would reach
// overlay's bestParent, where it loses every numeric comparison silently — so a
// candidate would be neither chosen nor rejected on latency but on whichever branch
// NaN happened to fall through. A negative value would beat every real candidate and
// pin the peer to a bad parent. Both are cheaper to refuse here than to reason about
// three packages downstream.
func (s *rttStore) record(name string, ms float64, now time.Time) {
	if name == "" || math.IsNaN(ms) || math.IsInf(ms, 0) || ms < 0 {
		return
	}
	if s.byName == nil {
		s.byName = make(map[string]rttSample)
	}
	s.byName[name] = rttSample{ms: ms, at: now}
}

// snapshot returns every sample still inside RTTMemory, ordered by name, and drops
// the ones that have aged out.
//
// The ordering is not cosmetic. This value travels as metrics.Report.PeerRTT and
// becomes overlay.Node.RTT, which shapes the tree — and it is built by ranging a map,
// which Go randomises. Without the sort, two peers holding identical measurements
// would emit different frames and the first tree of an epoch would depend on a hash
// seed.
func (s *rttStore) snapshot(now time.Time) []metrics.PeerRTT {
	if len(s.byName) == 0 {
		return nil
	}
	out := make([]metrics.PeerRTT, 0, len(s.byName))
	for name, sample := range s.byName {
		if now.Sub(sample.at) > RTTMemory {
			delete(s.byName, name)
			continue
		}
		out = append(out, metrics.PeerRTT{Name: name, RTTMs: sample.ms})
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// selectedPairRTTMs pulls the round-trip of the NOMINATED, SUCCEEDED ICE candidate
// pair out of a pion stats report, in milliseconds.
//
// Only that pair describes the path media actually takes: a PeerConnection
// accumulates a pair per candidate combination during checking, and the failed and
// in-progress ones carry either nothing or the RTT of a path that was rejected.
//
// A zero RTT is reported as ABSENT rather than as zero. pion leaves the field at 0
// until it has timed a STUN response, and 0 ms is indistinguishable from a perfect
// link everywhere downstream — arbiter.Score would read it as full marks and
// overlay's bestParent would prefer it over every measured candidate.
func selectedPairRTTMs(report webrtc.StatsReport) (float64, bool) {
	for _, s := range report {
		pair, ok := s.(webrtc.ICECandidatePairStats)
		if !ok || !pair.Nominated || pair.State != webrtc.StatsICECandidatePairStateSucceeded {
			continue
		}
		ms := pair.CurrentRoundTripTime * 1000
		if ms <= 0 || math.IsNaN(ms) || math.IsInf(ms, 0) {
			continue
		}
		return ms, true
	}
	return 0, false
}

// fractionLost converts an RTCP reception report's FractionLost to a 0–1 fraction.
// The wire field is a uint8 in units of 1/256, not a percentage and not a fraction —
// reading it raw makes a single lost packet look like catastrophic loss.
func fractionLost(r rtcp.ReceptionReport) float64 {
	return float64(r.FractionLost) / 256.0
}

// lossTracker holds the most recent loss each downstream child reported about the
// media this node sends it, harvested from the RTCP the forwarder already drains.
//
// THE AGGREGATION IS THE WORST LEG, and the choice is deliberate. A node has one
// uplink and several downstream legs; loss on its own uplink appears on every leg at
// once, while one child's bad downlink appears on one. The maximum cannot tell those
// apart. Taking it anyway is the conservative direction: overlay.Node.LossPct derates
// the node's usable upload and can arm the impairment dwell, so over-reporting moves
// children off a suspect relay and under-reporting leaves them on it. Averaging would
// be the seductive alternative and is worse in exactly the case that matters — it
// dilutes one badly-suffering leg to nothing as the relay's fan-out grows.
//
// The residual imprecision is recorded rather than hidden: a relay whose single child
// has a bad downlink will report loss it did not cause, and will be derated for it.
type lossTracker struct {
	mu     sync.Mutex
	byPeer map[string]float64 // child name → fraction lost, 0–1
}

// observe records what one child reported. frac is a 0–1 fraction; anything outside
// that range is clamped, because rtcp.ReceptionReport.FractionLost is a uint8 in
// 1/256 units and a caller that forgets to convert would otherwise put a 25500% loss
// on the wire.
func (l *lossTracker) observe(child string, frac float64) {
	if child == "" || math.IsNaN(frac) {
		return
	}
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.byPeer == nil {
		l.byPeer = make(map[string]float64)
	}
	l.byPeer[child] = frac
}

// forget drops a departed child's last report. Without it, one child's bad final
// report would derate the relay permanently — long after the child that saw it left.
func (l *lossTracker) forget(child string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.byPeer, child)
}

// worstPct returns the highest loss any current downstream leg reports, as a
// percentage. A node with no legs — every leaf — reports 0, which is the honest
// answer: it receives no reception reports at all, and 0 is what every build before
// this sensor reported.
func (l *lossTracker) worstPct() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	worst := 0.0
	for _, frac := range l.byPeer {
		if frac > worst {
			worst = frac
		}
	}
	return worst * 100
}
