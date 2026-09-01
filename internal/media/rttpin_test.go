package media

import (
	"context"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestPionPopulatesSelectedPairRTT PINS A LIBRARY BEHAVIOUR the pairwise-RTT sensor
// is entirely built on, and it is here because the obvious source for this number
// does not work.
//
// WHAT DOES NOT WORK, and why it is worth writing down: the textbook place to read a
// peer's uplink loss and RTT is RemoteInboundRTPStreamStats — the RTCP Receiver
// Report the far end sends back about our outbound stream. pion v4.2.16 declares that
// type and never produces it. PeerConnection.GetStats calls collectStats on the ICE
// gatherer, the ICE transport, data channels, the SCTP transport, certificates, the
// media engine, and every RTPReceiver — there is no RTPSender collector at all
// (peerconnection.go:2732-2780). So GetStats yields inbound stream stats and no
// outbound ones, and anything reached for through RemoteInboundRTPStreamStats is a
// zero value that looks like a perfect link.
//
// WHAT DOES WORK is the ICE candidate pair's CurrentRoundTripTime, measured from STUN
// connectivity and consent checks. It is strictly better for this purpose anyway: it
// needs no media to be flowing, it is symmetric (both ends measure the same path),
// and it exists as soon as the pair is nominated rather than after the first RTCP
// report interval.
//
// If a future pion starts collecting sender stats, or stops populating this field,
// this test is where that shows up — and it must fail loudly, because the sensor
// silently reporting 0 ms would hand every peer full marks on arbiter.Score's largest
// term rather than erroring.
func TestPionPopulatesSelectedPairRTT(t *testing.T) {
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
	// The offerer needs an m-line to offer or the SDP carries no media section, no
	// candidates are exchanged, and ICE never starts — a genuinely empty
	// PeerConnection between two conclave Sessions does not connect at all. Nothing
	// is ever WRITTEN to this track, which is the point: the measurement below is
	// taken on an edge that has negotiated but is carrying no RTP.
	if _, err := initiator.AddVideoTrack("probe", "probe"); err != nil {
		t.Fatalf("add track: %v", err)
	}
	answerer.Start(ctx)
	initiator.Start(ctx)

	waitFor(t, "both sides connected", 20*time.Second, func() bool {
		return initiator.ConnectionState() == webrtc.PeerConnectionStateConnected &&
			answerer.ConnectionState() == webrtc.PeerConnectionStateConnected
	})

	// The pair is nominated at connect, but CurrentRoundTripTime is only non-zero
	// once a STUN response has been timed, which is a moment later.
	var gotOfferer, gotAnswerer float64
	waitFor(t, "a measured RTT on both ends", 20*time.Second, func() bool {
		var okA, okB bool
		gotOfferer, okA = selectedPairRTTMs(initiator.stats())
		gotAnswerer, okB = selectedPairRTTMs(answerer.stats())
		return okA && okB
	})

	// No RTP was ever written — that is deliberate, and it is half the point: the
	// sensor must work on an edge that is negotiated but carrying nothing, because
	// the RTT of a candidate parent matters before any media is taken from it, and
	// because a relay measures a child that has not started sending yet.
	for name, ms := range map[string]float64{"offerer": gotOfferer, "answerer": gotAnswerer} {
		if ms <= 0 || ms > 5000 {
			t.Errorf("%s measured %v ms, which is not a plausible loopback RTT", name, ms)
		}
		t.Logf("%s selected-pair RTT = %.3f ms", name, ms)
	}
}
