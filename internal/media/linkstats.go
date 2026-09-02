package media

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
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

// pctFor returns what ONE child last reported, as a percentage — the per-leg input
// to selectLayer, as against worstPct's node-wide aggregate.
//
// A child that has never reported reads as 0, i.e. clean. That is safe because the
// only production caller is reviewLayer, which runs FROM a reception report and so
// always has one; it is stated rather than guarded because a guard would have to
// invent a third state for a value that has no caller who can see it.
func (l *lossTracker) pctFor(child string) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.byPeer[child] * 100
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

// NATRelayedThreshold is the fraction of a peer's MEASURED media paths that must be
// relay-typed before the peer is classified overlay.NATRelayed.
//
// It is 1.0 — every path, no exceptions — and that is the hysteresis, not pedantry.
// The bit this feeds is brutal: overlay.BuildTree and Validate force a NATRelayed node
// to be a LEAF, and arbiter.Fitness disqualifies it as coordinator outright. So
// flipping on the FIRST relayed edge would tear a working subtree apart on the
// weakest possible evidence — one peer that happened to need a relay to reach this
// one says nothing about the paths to its other children, which are demonstrably
// direct and are carrying media right now. Requiring every path inverts the burden:
// the peer is only demoted once it has shown it has no direct path to ANYONE, which
// is the actual condition "TURN-bound" is meant to name.
//
// The cost of the choice, stated rather than hidden: a peer with one stubborn direct
// neighbour and four relayed ones stays eligible and may be given children it can
// only reach over TURN. That is the conservative direction here — the tree still
// works, it is merely more expensive — whereas the opposite error removes a healthy
// relay from the fleet and re-parents everyone behind it.
const NATRelayedThreshold = 1.0

// NATDirectMemory is how long a MEASURED direct verdict keeps a peer classified
// overlay.NATDirect after every edge it can still measure has become relay-typed.
//
// IT EXISTS BECAUSE THE VERDICT IS OTHERWISE A ONE-WAY LATCH, and the latch closes on
// its own. overlay.BuildTree and Validate deny a NATRelayed node children, so consider
// peer P, CGNAT-bound upstream but holding a child C on its own LAN:
//
//	edges {parent A: relayed, child C: host} → natClass(2,1) = NATDirect. Eligible.
//	a rebuild re-parents C away        → edges {A: relayed} → natClass(1,1) = NATRelayed.
//	P is now a forced leaf, so the coordinator never gives it a child again — and its
//	only remaining edge is the relayed uplink, so it re-measures NATRelayed forever.
//
// P can never regain the edge that PROVED it was direct. Nothing about its reachability
// changed; the tree's reaction to the verdict removed the only evidence that could
// clear it. RTTMemory's doc comment argues this exact structure for the RTT sensor —
// "no challenger could ever win" — and the cure is the same one: remember.
//
// ONLY THE PERMISSIVE VERDICT IS REMEMBERED. A relayed verdict is never cached, because
// the two directions are not symmetric: latching toward NATRelayed is the harm (forced
// leaf, disqualified as coordinator, no path back), while latching toward NATDirect
// merely DELAYS a demotion, and its correction path is immediate — the next measurement
// past the horizon reclassifies the peer with no further evidence needed. Nor does an
// UNMEASURED tick refresh the memory: natClass's permissive answer for a peer with no
// edges is an absence of data, not a direct path, and treating it as one would restore
// the latch by another route.
//
// Two minutes, the same horizon and the same justification as RTTMemory: long enough
// that a peer moved around by one churn event still remembers what it measured before
// the move (the tree re-optimises on a scale of seconds), short enough that the value
// is still evidence about a path that does not usually change character inside two
// minutes — and it is what arbiter.StableUptimeSec already treats as "settled".
const NATDirectMemory = 2 * time.Minute

// relayedPath reports whether the path media actually takes over ONE PeerConnection
// runs through a TURN relay, reading pion's stats report for that connection.
//
// THIS IS NOT NAT-TYPE DETECTION. It does not discover mapping or filtering behaviour,
// it does not implement RFC 5780, and it cannot tell symmetric NAT from a firewall
// that happens to block the direct path. It answers exactly one behavioural question —
// "is this peer's media going through a relay?" — which is the only thing conclave
// consumes NAT for: may this node be a parent.
//
// The evidence is the NOMINATED, SUCCEEDED candidate pair's LOCAL candidate. That pair
// is the path media takes; a local candidate of type relay means the packets leave via
// a TURN allocation. Three things this deliberately does NOT do:
//
//   - It does not scan the report for any relay-typed candidate. Every peer configured
//     with -turn GATHERS a relay candidate whether or not it ends up using one, so a
//     scan would classify an entire TURN-configured fleet as forced leaves.
//   - It does not look at the REMOTE candidate. The far end being relay-bound is the
//     far end's constraint; it says nothing about whether this node can serve children.
//   - It does not filter on RTT. selectedPairRTTMs skips a zero round-trip because 0 ms
//     reads as a perfect link downstream; which candidate the path leaves through is
//     known the moment the pair is nominated, long before the first STUN response is
//     timed.
//
// ok is false when this connection has nothing to say — no nominated succeeded pair
// yet, or one whose local candidate is missing from the report. An unclassified edge
// is not a verdict; natClass decides what an absence of verdicts means.
func relayedPath(report webrtc.StatsReport) (relayed, ok bool) {
	// Two passes, because a pair references its local candidate by id and map order
	// gives no guarantee the candidate was seen first. The report is a handful of
	// entries; the expensive part is PeerConnection.GetStats, which the caller already
	// paid for and shares with selectedPairRTTMs.
	// LOCAL candidates only. ICECandidateStats carries both sides and they are
	// distinguished by Type, not by the Go type — so indexing them together lets a
	// REMOTE entry answer a LocalCandidateID lookup and classify this peer from its
	// neighbour's candidate. pion's ids are agent-unique today, which makes this
	// hardening rather than a live bug; the id space is a library detail and the
	// failure would be silent (a healthy relay forced to a leaf, permanently).
	local := make(map[string]webrtc.ICECandidateType, len(report))
	for _, s := range report {
		if c, isCand := s.(webrtc.ICECandidateStats); isCand && c.Type == webrtc.StatsTypeLocalCandidate {
			local[c.ID] = c.CandidateType
		}
	}

	for _, s := range report {
		pair, isPair := s.(webrtc.ICECandidatePairStats)
		if !isPair || !pair.Nominated || pair.State != webrtc.StatsICECandidatePairStateSucceeded {
			continue
		}
		kind, found := local[pair.LocalCandidateID]
		if !found {
			continue
		}
		ok = true
		// One direct nominated pair is enough for this edge to count as direct, the
		// same all-paths rule NATRelayedThreshold applies across edges. An ICE restart
		// can leave two nominated pairs on one connection; if either is direct, this
		// peer demonstrably has a direct path to this neighbour.
		if kind != webrtc.ICECandidateTypeRelay {
			return false, true
		}
		relayed = true
	}
	return relayed, ok
}

// natClass folds the per-edge verdicts into the single overlay.NATType the control
// plane consumes. measured is how many edges yielded a verdict at all; relayed is how
// many of those were relay-typed.
//
// FAILING SAFE ON ABSENCE IS THE LOAD-BEARING RULE. The classification is only
// observable AFTER a PeerConnection exists, so a peer that has just joined has
// measured nothing — the same structural limitation RTTMemory records for pairwise RTT
// ("first attachment is RTT-blind"), for the same reason: WebRTC offers no way to
// characterise a path without forming it. A peer that has not measured anything must
// therefore report the PERMISSIVE class, not be silently demoted to a forced leaf on
// no evidence. NATDirect is exactly what such a peer reported when the class was a
// flag, so the unmeasured case is unchanged from the declared build.
func natClass(measured, relayed int) overlay.NATType {
	if measured == 0 {
		return overlay.NATDirect
	}
	if float64(relayed)/float64(measured) < NATRelayedThreshold {
		return overlay.NATDirect
	}
	return overlay.NATRelayed
}

// natMemory holds the last MEASURED direct verdict, so a peer that has demonstrated a
// direct path is not reclassified the instant the tree takes that edge away. It is not
// goroutine-safe on its own; Router guards it with mu, exactly as it guards rttStore.
type natMemory struct {
	lastDirect time.Time
	seen       bool
}

// classify folds one tick's per-edge counts into the class the telemetry frame carries,
// applying NATDirectMemory. See that constant for why the memory is one-sided.
func (m *natMemory) classify(measured, relayed int, now time.Time) overlay.NATType {
	class := natClass(measured, relayed)
	if class == overlay.NATDirect {
		// Only a verdict backed by an actual edge is evidence. measured == 0 reaches
		// here as natClass's fail-safe, and remembering that would let a peer with no
		// edges refresh its memory forever.
		if measured > 0 {
			m.lastDirect, m.seen = now, true
		}
		return overlay.NATDirect
	}
	if m.seen && now.Sub(m.lastDirect) <= NATDirectMemory {
		return overlay.NATDirect
	}
	// Past the horizon the memory is not merely ignored, it is dropped: leaving it set
	// would make every later comparison a subtraction against a timestamp that can only
	// get older, which is the same answer at more cost.
	m.seen = false
	return overlay.NATRelayed
}
