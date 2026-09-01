package media

import (
	"context"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestMediaPortRangeConfinesCandidates pins that MediaPortRange actually binds this
// peer's media to the ports it was given.
//
// It is a real deployment knob before it is anything else — an operator opening a
// firewall or forwarding ports through a NAT needs to know which ports a peer will
// use, and every WebRTC server exposes exactly this. It also happens to be what makes
// per-peer network fault injection possible without root: with each peer confined to
// a distinct range, an nftables rule inside an unprivileged network namespace can cut
// ONE peer's media while leaving its control socket and every other peer alone.
//
// The mutation this catches: building the API without the SettingEngine, which
// compiles, connects, and silently gathers on ephemeral ports — so the flag would be
// accepted, documented, and inert.
func TestMediaPortRangeConfinesCandidates(t *testing.T) {
	const lo, hi = 47000, 47019

	bTr, aTr := newGatedPair("b", "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	offerer := true
	answerer, err := NewSession(SessionConfig{
		Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr,
		MediaPortRange: [2]uint16{lo, hi},
	})
	if err != nil {
		t.Fatalf("new answerer: %v", err)
	}
	defer answerer.Close()
	initiator, err := NewSession(SessionConfig{
		Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr, Offerer: &offerer,
		MediaPortRange: [2]uint16{lo, hi},
	})
	if err != nil {
		t.Fatalf("new offerer: %v", err)
	}
	defer initiator.Close()

	if err := answerer.AddRecvOnlyVideo(); err != nil {
		t.Fatalf("recv-only: %v", err)
	}
	if _, err := initiator.AddVideoTrack("v", "s"); err != nil {
		t.Fatalf("add track: %v", err)
	}
	answerer.Start(ctx)
	initiator.Start(ctx)

	waitFor(t, "both sides connected", 20*time.Second, func() bool {
		return initiator.ConnectionState() == webrtc.PeerConnectionStateConnected &&
			answerer.ConnectionState() == webrtc.PeerConnectionStateConnected
	})

	// Read the ports off the negotiated candidate pair rather than off the SDP: the
	// SDP lists everything gathered, while this is the port media actually uses.
	for name, s := range map[string]*Session{"offerer": initiator, "answerer": answerer} {
		port, ok := selectedLocalPort(s.stats())
		if !ok {
			t.Fatalf("%s: no nominated candidate pair", name)
		}
		if port < lo || port > hi {
			t.Errorf("%s bound media to port %d, outside the configured range %d-%d",
				name, port, lo, hi)
		}
		t.Logf("%s media port = %d", name, port)
	}
}

// TestZeroMediaPortRangeLeavesPionAlone pins that the zero value means "ephemeral",
// not "port 0". A caller that does not set the field — every existing test, and any
// peer run without the flag — must get pion's default behaviour unchanged.
func TestZeroMediaPortRangeLeavesPionAlone(t *testing.T) {
	tr, _ := newGatedPair("a", "b")
	s, err := NewSession(SessionConfig{Log: discardLog(), SelfID: "a", PeerID: "b", Transport: tr})
	if err != nil {
		t.Fatalf("a session with no port range must construct: %v", err)
	}
	s.Close()
}

// selectedLocalPort digs the local port out of the nominated candidate pair.
func selectedLocalPort(report webrtc.StatsReport) (int, bool) {
	for _, s := range report {
		pair, ok := s.(webrtc.ICECandidatePairStats)
		if !ok || !pair.Nominated || pair.State != webrtc.StatsICECandidatePairStateSucceeded {
			continue
		}
		local, ok := report[pair.LocalCandidateID].(webrtc.ICECandidateStats)
		if !ok {
			continue
		}
		return int(local.Port), true
	}
	return 0, false
}
