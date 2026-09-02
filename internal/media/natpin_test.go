package media

import (
	"context"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestPionLinksCandidatePairsToTheirLocalCandidate PINS A LIBRARY BEHAVIOUR the whole
// NAT sensor rests on, and it is the companion to TestPionPopulatesSelectedPairRTT.
//
// WHY IT IS NEEDED. Every case in TestRelayedPath builds its own webrtc.StatsReport by
// hand, so between them they prove the FOLD is right and prove nothing about whether a
// real report can be folded at all. relayedPath depends on two things pion is under no
// obligation to keep doing: that ICECandidatePairStats.LocalCandidateID names an entry
// that is present in the SAME report as an ICECandidateStats, and that the entry's
// CandidateType is populated. If either stops holding — a renamed id space, candidate
// stats collected from a different transport, a report that carries pairs but not
// candidates — relayedPath returns ok=false for every edge, natClass sees measured==0,
// and the peer reports NATDirect.
//
// THAT FAILURE IS INVISIBLE, which is the entire reason this test exists. NATDirect is
// the fail-safe, the pre-sensor default, and the true answer for almost every peer on a
// developer's machine — so a totally dead sensor and a healthy fleet produce byte-
// identical telemetry. Nothing else in the suite would go red, and docs/verify-turn.md,
// the only end-to-end exercise of the relayed path, has not been run.
//
// The assertion is deliberately about ok, not about relayed: a loopback pair is host-
// typed and must classify DIRECT, which is the second half of what is checked here.
// Proving the relayed branch on a live connection needs a TURN server and a blocked
// direct path, which is what docs/verify-turn.md is for.
func TestPionLinksCandidatePairsToTheirLocalCandidate(t *testing.T) {
	bTr, aTr := newGatedPair("b", "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	offerer := true
	answerer, err := NewSession(SessionConfig{Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr})
	if err != nil {
		t.Fatalf("new answerer: %v", err)
	}
	defer answerer.Close()
	initiator, err := NewSession(SessionConfig{Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr, Offerer: &offerer})
	if err != nil {
		t.Fatalf("new offerer: %v", err)
	}
	defer initiator.Close()

	if err := answerer.AddRecvOnlyVideo(); err != nil {
		t.Fatalf("recv-only: %v", err)
	}
	// The offerer needs an m-line or no candidates are exchanged at all — the same
	// constraint TestPionPopulatesSelectedPairRTT documents. Nothing is ever written to
	// this track: the classification must be readable on a negotiated edge that is
	// carrying no RTP, because a candidate parent is classified before media is taken
	// from it.
	if _, err := initiator.AddVideoTrack("probe", "probe"); err != nil {
		t.Fatalf("add track: %v", err)
	}
	answerer.Start(ctx)
	initiator.Start(ctx)

	waitFor(t, "both sides connected", 20*time.Second, func() bool {
		return initiator.ConnectionState() == webrtc.PeerConnectionStateConnected &&
			answerer.ConnectionState() == webrtc.PeerConnectionStateConnected
	})

	var relayedOfferer, relayedAnswerer bool
	waitFor(t, "a NAT verdict from both ends", 20*time.Second, func() bool {
		var okA, okB bool
		relayedOfferer, okA = relayedPath(initiator.stats())
		relayedAnswerer, okB = relayedPath(answerer.stats())
		return okA && okB
	})

	// A loopback pair is host-typed on both ends. Anything else means the walk latched
	// onto the wrong candidate — the remote one, or a gathered-but-unused entry.
	for name, relayed := range map[string]bool{"offerer": relayedOfferer, "answerer": relayedAnswerer} {
		if relayed {
			t.Errorf("%s classified its loopback path as relayed", name)
		}
	}
}
