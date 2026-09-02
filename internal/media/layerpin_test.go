package media

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestPionAnswererFillsEveryOfferedRecvonlyMLine is a LIBRARY-BEHAVIOUR PIN, in the
// sense of docs/testing.md §5: it asserts what pion does, using raw pion, so a
// version bump that changes it fails a test instead of shipping a silent regression.
//
// WHY IT EXISTS. §5.12 fixes ONE offerer per edge, and on a relay edge that offerer
// is the relay — only an offer can add the forwarded m-lines. The sender is
// therefore the ANSWERER, and an answerer cannot create m-lines: it can only fold
// its local tracks into m-lines the offer already carried. A multi-layer origin has
// N tracks, so the relay must offer N recvonly m-lines or N-1 of the layers have
// nowhere to go.
//
// That failure is silent in exactly the way this project keeps finding: negotiation
// succeeds, one layer flows, the relay has one rung to choose from, every log line
// is clean, and the whole feature is inert. It was found by running the real
// binaries, not by a unit test — which is why the pin is here now.
//
// The assertion is the number of tracks that ARRIVE, because that is the only
// question the relay cares about.
func TestPionAnswererFillsEveryOfferedRecvonlyMLine(t *testing.T) {
	const layers = 3

	bTr, aTr := newGatedPair("b", "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	arrived := map[string]bool{}

	// The ANSWERER is the origin: it publishes one track per layer.
	answerer, err := NewSession(SessionConfig{
		Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr,
	})
	if err != nil {
		t.Fatalf("new answerer: %v", err)
	}
	defer answerer.Close()

	// The OFFERER is the relay: it receives, and offers the m-lines to receive into.
	offerer, err := NewSession(SessionConfig{
		Log: discardLog(), SelfID: "b", PeerID: "a", Transport: bTr,
		OnRemoteTrack: func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			mu.Lock()
			arrived[tr.ID()] = true
			mu.Unlock()
			for {
				if _, _, err := tr.ReadRTP(); err != nil {
					return
				}
			}
		},
	})
	if err != nil {
		t.Fatalf("new offerer: %v", err)
	}
	defer offerer.Close()

	// Everything is added BEFORE Start, which is the production ordering.
	for i := 0; i < layers; i++ {
		if err := offerer.AddRecvOnlyVideo(); err != nil {
			t.Fatalf("add recvonly %d: %v", i, err)
		}
	}
	tracks := make([]*webrtc.TrackLocalStaticSample, 0, layers)
	for _, id := range []string{"video.q", "video.h", "video.f"} {
		tr, err := answerer.AddVideoTrack(id, "conclave")
		if err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
		tracks = append(tracks, tr)
	}

	answerer.Start(ctx)
	offerer.Start(ctx)

	waitFor(t, "the PeerConnection to connect", 60*time.Second, func() bool {
		return offerer.pc.ConnectionState() == webrtc.PeerConnectionStateConnected
	})

	// A track only fires OnTrack once media flows on it, so all three must be fed.
	// DEFER ORDER MATTERS: wg.Wait must run AFTER stop, so it is registered FIRST
	// (defers run LIFO). The other way round the test waits for pumps it has not
	// cancelled and hangs until the package timeout — which is how the first draft
	// of this test behaved.
	var wg sync.WaitGroup
	defer wg.Wait()
	pumpCtx, stop := context.WithCancel(ctx)
	defer stop()
	for _, tr := range tracks {
		wg.Add(1)
		go func(tr *webrtc.TrackLocalStaticSample) {
			defer wg.Done()
			_ = SendSynthetic(pumpCtx, tr, 30)
		}(tr)
	}

	waitFor(t, "every published layer to arrive", 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(arrived) >= layers
	})

	mu.Lock()
	defer mu.Unlock()
	for _, id := range []string{"video.q", "video.h", "video.f"} {
		if !arrived[id] {
			t.Errorf("layer track %q never arrived; got %v — an answerer can only fill m-lines "+
				"the OFFER carried, so a relay that offers one recvonly m-line silently "+
				"receives one layer and the whole ladder collapses to its first rung", id, arrived)
		}
	}
}

// TestRelayEdgeRecvSlots pins OUR side of the contract above, on BOTH sides of the
// edge — because the two sides want opposite things and getting either wrong is a
// silent black-video failure.
//
// MUTATION CAUGHT (offerer arm): reverting to a single AddRecvOnlyVideo, or adding
// the slots BEFORE the forwarded tracks so pc.AddTrack cannibalises them. The pion
// pin above stays green through both — it drives raw pion — while the production
// path silently receives one rung.
//
// MUTATION CAUGHT (answerer arm): giving the answerer layerLadder-many slots too.
// pion's satisfyTypeAndDirection prefers a recvonly transceiver over a sendrecv one
// for a remote sendrecv m-line, so the surplus hijacks the m-line the forwarded
// track needed and the relay answers "connected" while sending nothing. That is not
// hypothetical: it is what TestRouterPromotesBackupParent caught (DESIGN §7.5).
func TestRelayEdgeRecvSlots(t *testing.T) {
	// One forwarded leg, so the arm about AddTrack cannibalising a slot is live
	// rather than vacuous — with no legs there is nothing to cannibalise and both
	// orderings look identical.
	topo := &overlay.Topology{Edges: []overlay.Edge{
		{Parent: "relay", Child: "leaf-b"},
		{Parent: "relay", Child: "leaf-d"},
	}}

	// The answerer's expected recvonly count is ZERO, not one, and that is the point
	// rather than an accident: it adds exactly one slot and pc.AddTrack immediately
	// consumes it for the forwarded leg. NO LEFTOVER RECVONLY TRANSCEIVER is the
	// invariant — a spare one is what hijacks the m-line matching.
	cases := []struct {
		name        string
		offerer     bool
		wantRecv    int
		wantSending int
	}{
		{"offerer gets one slot per rung, unconsumed", true, len(layerLadder), 1},
		{"answerer is left with no spare slot at all", false, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, aTr := newGatedPair("b", "a")
			offerer := tc.offerer
			s, err := NewSession(SessionConfig{
				Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr, Offerer: &offerer,
			})
			if err != nil {
				t.Fatalf("new session: %v", err)
			}
			defer s.Close()

			r := &Router{
				log:      discardLog(),
				selfName: "relay",
				fwd:      newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock()),
			}
			r.setupRelayEdge(s, topo, "leaf-b")

			var recvonly, sending int
			for _, tr := range s.pc.GetTransceivers() {
				if tr.Kind() != webrtc.RTPCodecTypeVideo {
					continue
				}
				switch tr.Direction() {
				case webrtc.RTPTransceiverDirectionRecvonly:
					recvonly++
				case webrtc.RTPTransceiverDirectionSendrecv, webrtc.RTPTransceiverDirectionSendonly:
					sending++
				}
			}
			if recvonly != tc.wantRecv {
				t.Errorf("recvonly video m-lines = %d, want %d", recvonly, tc.wantRecv)
			}
			// The forwarded leg must still have a transceiver of its own. This is what
			// catches "the offerer's slots were added FIRST and pc.AddTrack ate one":
			// the recvonly count above would still read 3 — two fresh slots plus the
			// one the forwarded track did not need — while the forwarded leg quietly
			// lost its m-line and the child received nothing.
			if sending != tc.wantSending {
				t.Errorf("sending video m-lines = %d, want %d (the forwarded leg toward leaf-b)",
					sending, tc.wantSending)
			}
		})
	}
}
