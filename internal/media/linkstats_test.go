package media

import (
	"math"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// --- rttStore: the memory that makes a challenger measurable -----------------
//
// The whole reason this type exists: BuildTree's min-latency rank reads
// overlay.Node.RTT[candidate], and a peer only has a PeerConnection to its CURRENT
// tree neighbours. Measuring only live edges gives the incumbent parent a number and
// leaves every challenger at unknownRTT (math.MaxFloat64), so no challenger can ever
// win and -stickiness-ms can never bind. Remembering a neighbour after the edge
// closes is what turns a former parent into a comparable candidate.

// TestRTTStoreRemembersAClosedEdge is the property the feature rests on. The mutation
// it catches: dropping an entry when its session goes away, which is the obvious
// implementation and the one that makes the whole sensor pointless.
func TestRTTStoreRemembersAClosedEdge(t *testing.T) {
	clk := newFakeClock()
	var s rttStore

	s.record("r1", 5, clk.Now())
	clk.advance(30 * time.Second)
	// r1's session is gone; only r2 is measurable now.
	s.record("r2", 100, clk.Now())

	got := s.snapshot(clk.Now())
	if len(got) != 2 {
		t.Fatalf("got %d entries %v, want both the live and the remembered neighbour", len(got), got)
	}
	byName := map[string]float64{}
	for _, e := range got {
		byName[e.Name] = e.RTTMs
	}
	if byName["r1"] != 5 {
		t.Errorf("remembered r1 = %v ms, want 5 ms", byName["r1"])
	}
	if byName["r2"] != 100 {
		t.Errorf("live r2 = %v ms, want 100 ms", byName["r2"])
	}
}

// TestRTTStoreExpiresStaleSamples pins the other half. A remembered measurement is
// real but ages: a path that was 5 ms an hour ago is not evidence about now, and
// acting on it would re-parent a peer onto a link that no longer exists as measured.
//
// The mutation this catches: no TTL at all, which passes every other test here.
func TestRTTStoreExpiresStaleSamples(t *testing.T) {
	clk := newFakeClock()
	var s rttStore
	s.record("old", 5, clk.Now())

	clk.advance(RTTMemory - time.Second)
	if got := s.snapshot(clk.Now()); len(got) != 1 {
		t.Fatalf("entry expired early at age %v: got %v", RTTMemory-time.Second, got)
	}
	clk.advance(2 * time.Second) // now just past the TTL
	if got := s.snapshot(clk.Now()); len(got) != 0 {
		t.Errorf("entry survived past RTTMemory (%v): got %v", RTTMemory, got)
	}
}

// TestRTTStoreFreshMeasurementReplacesARememberedOne pins the correction path, which
// is what bounds the damage a stale sample can do: if a peer acts on a remembered
// 5 ms and the link is now 300 ms, the very next measurement over the newly opened
// edge must overwrite it. Without this the peer could oscillate forever between a
// stale-good and a fresh-bad reading.
func TestRTTStoreFreshMeasurementReplacesARememberedOne(t *testing.T) {
	clk := newFakeClock()
	var s rttStore
	s.record("p", 5, clk.Now())
	clk.advance(10 * time.Second)
	s.record("p", 300, clk.Now())

	got := s.snapshot(clk.Now())
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if got[0].RTTMs != 300 {
		t.Errorf("got %v ms, want the fresh 300 ms — the remembered value was not replaced", got[0].RTTMs)
	}
}

// TestRTTStoreRefreshingResetsTheClock pins that re-measuring a peer restarts its
// TTL. The mutation: keeping the FIRST observation time, which would expire a
// continuously-measured live neighbour after RTTMemory — silently blinding rank 2 to
// the one edge it definitely has data for.
func TestRTTStoreRefreshingResetsTheClock(t *testing.T) {
	clk := newFakeClock()
	var s rttStore
	s.record("p", 5, clk.Now())
	clk.advance(RTTMemory - time.Second)
	s.record("p", 6, clk.Now())
	clk.advance(RTTMemory - time.Second)

	if got := s.snapshot(clk.Now()); len(got) != 1 {
		t.Errorf("a peer re-measured just before expiry was dropped anyway: got %v", got)
	}
}

// TestRTTStoreSnapshotIsOrdered pins the determinism requirement metrics.PeerRTT
// documents: this value becomes overlay.Node.RTT and shapes the tree, and it is built
// by ranging a map. Go randomises that, so without an explicit sort two peers with
// identical measurements would emit different frames.
func TestRTTStoreSnapshotIsOrdered(t *testing.T) {
	clk := newFakeClock()
	// Repeat, because an unsorted map range passes a single run by luck often enough
	// to look green.
	for range 20 {
		var s rttStore
		for _, n := range []string{"zulu", "alpha", "mike", "bravo"} {
			s.record(n, 1, clk.Now())
		}
		got := s.snapshot(clk.Now())
		want := []string{"alpha", "bravo", "mike", "zulu"}
		for i, w := range want {
			if got[i].Name != w {
				t.Fatalf("entry %d = %q, want %q (snapshot is not ordered)", i, got[i].Name, w)
			}
		}
	}
}

// TestRTTStoreIgnoresNonsense pins that a bad reading never enters the store. A
// negative or NaN RTT would reach overlay.Node.RTT, where bestParent compares it
// numerically — NaN loses every comparison silently, and a negative value beats every
// real candidate, so a single bad sample would pin a peer to the wrong parent.
func TestRTTStoreIgnoresNonsense(t *testing.T) {
	clk := newFakeClock()
	var s rttStore
	s.record("neg", -1, clk.Now())
	s.record("nan", math.NaN(), clk.Now())
	s.record("inf", math.Inf(1), clk.Now())
	s.record("ok", 7, clk.Now())

	got := s.snapshot(clk.Now())
	if len(got) != 1 || got[0].Name != "ok" {
		t.Errorf("got %v, want only the valid sample", got)
	}
}

// --- selectedPairRTTMs: reading pion's stats report -------------------------

// TestSelectedPairRTTMs covers how the nominated ICE candidate pair is picked out of
// a StatsReport. A PeerConnection accumulates several pairs during checking; only the
// nominated, succeeded one describes the path media actually takes.
//
// The mutations these catch:
//   - "picks the nominated succeeded pair": taking the first pair in map order reports
//     a failed or in-progress candidate's RTT, which is either zero or the RTT of a
//     path that was rejected.
//   - "ignores a zero RTT": pion reports 0 until the first STUN response is timed, and
//     0 ms is indistinguishable from a perfect link everywhere downstream.
//   - "no pairs at all": a session that has not connected must yield nothing.
func TestSelectedPairRTTMs(t *testing.T) {
	pair := func(state webrtc.StatsICECandidatePairState, nominated bool, rttSec float64) webrtc.ICECandidatePairStats {
		return webrtc.ICECandidatePairStats{
			Type: webrtc.StatsTypeCandidatePair, State: state,
			Nominated: nominated, CurrentRoundTripTime: rttSec,
		}
	}
	tests := []struct {
		name   string
		report webrtc.StatsReport
		wantMs float64
		wantOK bool
	}{
		{
			name: "picks the nominated succeeded pair",
			report: webrtc.StatsReport{
				"a": pair(webrtc.StatsICECandidatePairStateFailed, false, 0.900),
				"b": pair(webrtc.StatsICECandidatePairStateInProgress, false, 0.500),
				"c": pair(webrtc.StatsICECandidatePairStateSucceeded, true, 0.025),
			},
			wantMs: 25, wantOK: true,
		},
		{
			name: "succeeded but not nominated is not the selected pair",
			report: webrtc.StatsReport{
				"a": pair(webrtc.StatsICECandidatePairStateSucceeded, false, 0.500),
			},
			wantOK: false,
		},
		{
			name: "ignores a zero RTT that has not been measured yet",
			report: webrtc.StatsReport{
				"a": pair(webrtc.StatsICECandidatePairStateSucceeded, true, 0),
			},
			wantOK: false,
		},
		{
			name:   "no pairs at all",
			report: webrtc.StatsReport{},
			wantOK: false,
		},
		{
			name: "non-pair stats are skipped, not mis-cast",
			report: webrtc.StatsReport{
				"t": webrtc.TransportStats{Type: webrtc.StatsTypeTransport},
				"c": pair(webrtc.StatsICECandidatePairStateSucceeded, true, 0.010),
			},
			wantMs: 10, wantOK: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := selectedPairRTTMs(tt.report)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (got %v ms)", ok, tt.wantOK, got)
			}
			if ok && math.Abs(got-tt.wantMs) > 1e-9 {
				t.Errorf("got %v ms, want %v ms", got, tt.wantMs)
			}
		})
	}
}

// --- lossTracker: uplink loss from the RTCP the relay already drains ---------

// TestLossTrackerTakesTheWorstLeg pins the aggregation choice and, more importantly,
// its direction. A relay has one uplink and several downstream legs; each leg's child
// reports what IT lost. Loss on the relay's own uplink shows up on every leg at once,
// while one child's bad downlink shows up on one. The maximum conflates the two, and
// that is the deliberate choice: overlay.Node.LossPct derates capacity and can arm the
// impairment dwell, so over-reporting moves children off a suspect relay while
// under-reporting leaves them on it. Correctness before performance.
//
// The mutation this catches: averaging, which dilutes one badly-suffering leg into
// nothing as the relay's fan-out grows — precisely the case that matters.
func TestLossTrackerTakesTheWorstLeg(t *testing.T) {
	var l lossTracker
	l.observe("a", 0.01)
	l.observe("b", 0.25)
	l.observe("c", 0.00)
	if got := l.worstPct(); math.Abs(got-25) > 1e-9 {
		t.Errorf("got %v%%, want 25%% (the worst leg)", got)
	}
}

// TestLossTrackerReportsNothingWithoutLegs pins that a leaf — which forwards to
// nobody and therefore receives no reception reports — reports 0, not a stale or
// fabricated value. 0 is the honest answer here and matches the pre-sensor behaviour.
func TestLossTrackerReportsNothingWithoutLegs(t *testing.T) {
	var l lossTracker
	if got := l.worstPct(); got != 0 {
		t.Errorf("got %v%%, want 0%% from a node with no downstream legs", got)
	}
}

// TestLossTrackerForgetsADepartedLeg pins that a child's last bad report does not
// haunt the relay forever. The mutation: never deleting, which pins a relay at the
// worst loss any child ever reported — permanently derating a healthy machine.
func TestLossTrackerForgetsADepartedLeg(t *testing.T) {
	var l lossTracker
	l.observe("gone", 0.40)
	l.observe("here", 0.02)
	l.forget("gone")
	if got := l.worstPct(); math.Abs(got-2) > 1e-9 {
		t.Errorf("got %v%%, want 2%% after the lossy child left", got)
	}
}

// TestLossTrackerClampsFractionLost pins the range. rtcp.ReceptionReport.FractionLost
// is a uint8 in 1/256 units, so a caller that forgets to divide passes a value of 255
// as a fraction. Anything outside [0,1] must not become a 25500% loss on the wire.
func TestLossTrackerClampsFractionLost(t *testing.T) {
	var l lossTracker
	l.observe("bad", 42)
	if got := l.worstPct(); got < 0 || got > 100 {
		t.Errorf("got %v%%, which is not a percentage", got)
	}
}

// TestFractionLostFromReceptionReport pins the 1/256 conversion at the one place it
// happens. The mutation: dividing by 100, or not dividing at all — either of which
// makes every relay look catastrophically lossy the moment a single packet is missed.
func TestFractionLostFromReceptionReport(t *testing.T) {
	tests := []struct {
		name string
		raw  uint8
		want float64
	}{
		{name: "no loss", raw: 0, want: 0},
		{name: "half", raw: 128, want: 0.5},
		{name: "total", raw: 255, want: 255.0 / 256.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fractionLost(rtcp.ReceptionReport{FractionLost: tt.raw})
			if math.Abs(got-tt.want) > 1e-9 {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// --- relayedPath: is the path media actually takes a TURN relay? -------------
//
// The one bit conclave consumes from "NAT" is: MAY THIS NODE BE A PARENT? That bit is
// directly observable — the nominated ICE candidate pair names the local candidate
// media is leaving through, and a relay-typed local candidate means the allocation is
// on a TURN server. This is a BEHAVIOURAL classification of the paths this peer holds,
// not RFC 5780 NAT-type discovery, and the tests below are written against that claim.

// candidate builds one ICECandidateStats entry as pion reports it. Local and remote
// candidates share the type; only the stat type and the ID distinguish them.
func candidate(id string, kind webrtc.ICECandidateType, statsType webrtc.StatsType) webrtc.ICECandidateStats {
	return webrtc.ICECandidateStats{Type: statsType, ID: id, CandidateType: kind}
}

func localCand(id string, kind webrtc.ICECandidateType) webrtc.ICECandidateStats {
	return candidate(id, kind, webrtc.StatsTypeLocalCandidate)
}

func remoteCand(id string, kind webrtc.ICECandidateType) webrtc.ICECandidateStats {
	return candidate(id, kind, webrtc.StatsTypeRemoteCandidate)
}

// candPair builds one ICECandidatePairStats. rttSec is separate from the rest because
// one case exists purely to prove the RTT filter does not gate the classification.
func candPair(state webrtc.StatsICECandidatePairState, nominated bool, local, remote string, rttSec float64) webrtc.ICECandidatePairStats {
	return webrtc.ICECandidatePairStats{
		Type: webrtc.StatsTypeCandidatePair, State: state, Nominated: nominated,
		LocalCandidateID: local, RemoteCandidateID: remote, CurrentRoundTripTime: rttSec,
	}
}

func TestRelayedPath(t *testing.T) {
	tests := []struct {
		name        string
		report      webrtc.StatsReport
		wantRelayed bool
		wantOK      bool
	}{
		{
			// Mutation caught: ignoring CandidateType entirely and always reporting a
			// direct path — which is what "declared, not measured" already does, so the
			// sensor would look wired and change nothing.
			name: "a nominated pair on a relay local candidate is relayed",
			report: webrtc.StatsReport{
				"l": localCand("l", webrtc.ICECandidateTypeRelay),
				"r": remoteCand("r", webrtc.ICECandidateTypeHost),
				"p": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "l", "r", 0.02),
			},
			wantRelayed: true, wantOK: true,
		},
		{
			// Mutation caught: returning NATRelayed unconditionally, which would force
			// every peer in a meet to a leaf and make the tree unbuildable.
			name: "a nominated pair on a host local candidate is not relayed",
			report: webrtc.StatsReport{
				"l": localCand("l", webrtc.ICECandidateTypeHost),
				"p": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "l", "r", 0.02),
			},
			wantRelayed: false, wantOK: true,
		},
		{
			// Mutation caught: treating anything that is not a host candidate as
			// relayed. A server-reflexive candidate is a DIRECT path discovered via
			// STUN — the ordinary case for every peer behind a cone NAT, and the one
			// that must stay eligible to relay.
			name: "a server-reflexive local candidate is a direct path",
			report: webrtc.StatsReport{
				"l": localCand("l", webrtc.ICECandidateTypeSrflx),
				"p": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "l", "r", 0.02),
			},
			wantRelayed: false, wantOK: true,
		},
		{
			// Mutation caught: scanning the report for ANY relay-typed candidate
			// instead of following the nominated pair's LocalCandidateID. Every peer
			// configured with -turn gathers a relay candidate whether or not it uses
			// one, so that mutation classifies the entire fleet as forced leaves the
			// moment TURN is configured at all.
			name: "an unused gathered relay candidate does not classify the peer",
			report: webrtc.StatsReport{
				"host":  localCand("host", webrtc.ICECandidateTypeHost),
				"spare": localCand("spare", webrtc.ICECandidateTypeRelay),
				"p":     candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "host", "r", 0.02),
			},
			wantRelayed: false, wantOK: true,
		},
		{
			// Mutation caught: following RemoteCandidateID. The far end being TURN-bound
			// is the far end's problem; it says nothing about whether THIS node can
			// serve children, which is the only question the bit answers.
			name: "the remote end being relay-typed does not classify this peer",
			report: webrtc.StatsReport{
				"l": localCand("l", webrtc.ICECandidateTypeHost),
				"r": remoteCand("r", webrtc.ICECandidateTypeRelay),
				"p": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "l", "r", 0.02),
			},
			wantRelayed: false, wantOK: true,
		},
		{
			// Mutation caught: ignoring the Nominated flag. A PeerConnection accumulates
			// a pair per candidate combination during checking, including relay ones it
			// then rejects in favour of a direct path — reading those would classify a
			// perfectly direct peer as TURN-bound.
			name: "a succeeded but un-nominated relay pair is not the path media takes",
			report: webrtc.StatsReport{
				"l": localCand("l", webrtc.ICECandidateTypeRelay),
				"p": candPair(webrtc.StatsICECandidatePairStateSucceeded, false, "l", "r", 0.02),
			},
			wantOK: false,
		},
		{
			// Mutation caught: ignoring State, which admits pairs that were nominated
			// and then failed.
			name: "a nominated pair that failed is not consulted",
			report: webrtc.StatsReport{
				"l": localCand("l", webrtc.ICECandidateTypeRelay),
				"p": candPair(webrtc.StatsICECandidatePairStateFailed, true, "l", "r", 0.02),
			},
			wantOK: false,
		},
		{
			// Mutation caught: dropping the "nothing measured" signal, i.e. returning
			// ok=true with a fabricated verdict for a session that has not connected.
			name: "no pairs at all is unclassified, not a verdict",
			report: webrtc.StatsReport{
				"l": localCand("l", webrtc.ICECandidateTypeRelay),
			},
			wantOK: false,
		},
		{
			// Mutation caught: treating a dangling LocalCandidateID as relay-typed
			// (webrtc.ICECandidateTypeUnknown is the zero value, so a missing lookup
			// that is not checked yields SOME type and would silently become a verdict).
			name: "a nominated pair whose local candidate is absent is unclassified",
			report: webrtc.StatsReport{
				"p": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "gone", "r", 0.02),
			},
			wantOK: false,
		},
		{
			// Mutation caught: reusing selectedPairRTTMs's `ms <= 0 continue` guard.
			// That guard exists because 0 ms reads as a perfect link downstream; it has
			// nothing to do with which candidate the path leaves through. A relay pair
			// whose first STUN response has not been timed yet is still a relay pair,
			// and skipping it would leave a genuinely TURN-bound peer classified direct
			// for as long as pion has not filled the field in.
			name: "an unmeasured RTT does not block the classification",
			report: webrtc.StatsReport{
				"l": localCand("l", webrtc.ICECandidateTypeRelay),
				"p": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "l", "r", 0),
			},
			wantRelayed: true, wantOK: true,
		},
		{
			// Mutation caught: returning on the FIRST relayed pair found. Within one
			// PeerConnection an ICE restart can leave two nominated pairs; if either is
			// direct the peer demonstrably has a direct path over this edge, and
			// NATRelayedThreshold says that is enough to stay eligible.
			name: "one direct nominated pair beats a relayed one in the same report",
			report: webrtc.StatsReport{
				"lr": localCand("lr", webrtc.ICECandidateTypeRelay),
				"lh": localCand("lh", webrtc.ICECandidateTypeHost),
				"p1": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "lr", "r", 0.02),
				"p2": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "lh", "r", 0.02),
			},
			wantRelayed: false, wantOK: true,
		},
		{
			// Mutation caught: mis-casting non-pair stats, the same hazard
			// TestSelectedPairRTTMs pins for the RTT walk.
			name: "non-pair, non-candidate stats are skipped",
			report: webrtc.StatsReport{
				"t": webrtc.TransportStats{Type: webrtc.StatsTypeTransport},
				"l": localCand("l", webrtc.ICECandidateTypeRelay),
				"p": candPair(webrtc.StatsICECandidatePairStateSucceeded, true, "l", "r", 0.02),
			},
			wantRelayed: true, wantOK: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relayed, ok := relayedPath(tt.report)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (relayed = %v)", ok, tt.wantOK, relayed)
			}
			if ok && relayed != tt.wantRelayed {
				t.Errorf("relayed = %v, want %v", relayed, tt.wantRelayed)
			}
		})
	}
}

// TestNATClass pins the fold from per-edge verdicts to the one bit overlay.BuildTree
// and arbiter.Fitness consume. Two rules live here and both are load-bearing:
// the fail-safe on absence, and NATRelayedThreshold.
func TestNATClass(t *testing.T) {
	tests := []struct {
		name              string
		measured, relayed int
		want              overlay.NATType
	}{
		{
			// THE FAIL-SAFE, and the most important case in this file. A peer that has
			// not connected to anybody has measured nothing. Classifying it relayed
			// would demote a fresh peer to a forced leaf and disqualify it as
			// coordinator on no evidence at all — the permissive default is the only
			// honest answer for an unmeasured peer, and it is what the -nat flag
			// defaulted to before this sensor existed.
			//
			// Mutation caught: making NATRelayed the zero-evidence answer, or returning
			// the empty NATType (which travels as omitempty and decodes to "" at the
			// coordinator, not to "direct").
			name: "a peer that has measured nothing is direct",
			want: overlay.NATDirect,
		},
		{
			// Mutation caught: never returning NATRelayed at all, i.e. wiring a sensor
			// whose output is constant.
			name: "every path relayed is relayed", measured: 3, relayed: 3, want: overlay.NATRelayed,
		},
		{
			// THE HYSTERESIS RULE. One relayed edge is not evidence that this peer
			// cannot serve children — it demonstrably serves the ones it reaches
			// directly. Flipping the whole peer to a forced leaf on a single relayed
			// edge would tear down a working subtree on the worst possible evidence.
			//
			// Mutation caught: `relayed > 0` instead of the all-paths threshold.
			name: "one direct path among relayed ones keeps the peer eligible", measured: 3, relayed: 2, want: overlay.NATDirect,
		},
		{
			name: "all direct is direct", measured: 2, relayed: 0, want: overlay.NATDirect,
		},
		{
			// A leaf holds exactly one edge. If a single measured edge could not yield
			// a verdict, the classification would be unreachable for precisely the
			// peers it is meant to describe.
			name: "a single relayed edge is enough to classify", measured: 1, relayed: 1, want: overlay.NATRelayed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := natClass(tt.measured, tt.relayed); got != tt.want {
				t.Errorf("natClass(%d, %d) = %q, want %q", tt.measured, tt.relayed, got, tt.want)
			}
		})
	}
}

// TestLinkStatsNATFailsSafeWithNoEdges pins the fail-safe through the Router surface
// rather than only through the fold, because that is where a fresh peer actually hits
// it: cmd/peer samples telemetry on a timer from the moment it joins, and the first
// several reports are taken before any PeerConnection exists.
//
// The mutation this catches: a zero-value overlay.NATType leaking out of LinkStats.
// It is not NATRelayed, so the fold test would still pass, but it travels as an absent
// JSON field and reaches the coordinator as "" — which is neither class, and which
// overlay's `n.NAT == NATRelayed` checks happen to read as eligible today by accident
// rather than by decision.
func TestLinkStatsNATFailsSafeWithNoEdges(t *testing.T) {
	r := NewRouter(discardLog(), nil, RouterConfig{SelfName: "fresh", Managed: true})
	_, _, nat := r.LinkStats()
	if nat != overlay.NATDirect {
		t.Errorf("LinkStats nat = %q from a peer with no edges, want %q", nat, overlay.NATDirect)
	}
}
