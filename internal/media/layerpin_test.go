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
			// This count is what catches "the offerer's slots were added FIRST and
			// pc.AddTrack ate one": the first forwarded track cannibalises one of the
			// three, leaving 2 recvonly and one fewer m-line for the neighbour's rungs.
			// Verified by applying that mutation — it fails HERE, reading 2, want 3.
			if recvonly != tc.wantRecv {
				t.Errorf("recvonly video m-lines = %d, want %d", recvonly, tc.wantRecv)
			}
			// The forwarded leg must still have a transceiver of its own — a slot the
			// answerer arm relies on being consumed, and the offerer arm on NOT being.
			//
			// It does NOT discriminate the slots-added-first mutation, despite an
			// earlier comment here claiming it did: a cannibalised recvonly becomes
			// SENDRECV, so it is still counted as sending and this reads 1 either way.
			// The reason was wrong in exactly the way §6.5's Case C describes, which is
			// how the next person deletes the assertion that is actually load-bearing.
			if sending != tc.wantSending {
				t.Errorf("sending video m-lines = %d, want %d (the forwarded leg toward leaf-b)",
					sending, tc.wantSending)
			}
		})
	}
}

// TestRelayChildIsPinnedFromBothCallSites closes the gap between "the forwarder can
// pin a leg" and "the Router ever asks it to".
//
// TestPinnedLegNeverSelects exercises addOutPinned directly, so it stays green even
// when NOTHING calls it — which is the §7.5 shape exactly: a correct unit that was
// never wired up, invisible to the whole suite. There are two call sites, one per
// path a leg is born on, and each has to be asserted.
//
// The assertion is BEHAVIOURAL — sustained loss on the leg leaves it on the top rung
// — rather than on forwardOut.pinned, so it also catches a pin that is recorded and
// then ignored.
//
// MUTATION CAUGHT (setupRelayEdge arm): `childIsRelay := false`, i.e. the
// before-Start path never pins. MUTATION CAUGHT (addLegLive arm): dropping the
// topo.IsRelay branch and always calling addOutLive, i.e. the mid-call path never
// pins. Both survive the entire suite as it stands: a congested intermediate relay
// is walked down to `q` and its whole subtree — including peers on perfect links —
// is capped there, because it can only forward what it receives.
func TestRelayChildIsPinnedFromBothCallSites(t *testing.T) {
	// leaf-b originates; relay forwards it to sub-relay (a relay) and to leaf-d.
	topo := &overlay.Topology{Edges: []overlay.Edge{
		{Parent: "relay", Child: "leaf-b"},
		{Parent: "relay", Child: "sub-relay"},
		{Parent: "sub-relay", Child: "leaf-e"},
		{Parent: "relay", Child: "leaf-d"},
	}}
	if !topo.IsRelay("sub-relay") || topo.IsRelay("leaf-d") {
		t.Fatalf("fixture is wrong: IsRelay(sub-relay)=%v IsRelay(leaf-d)=%v",
			topo.IsRelay("sub-relay"), topo.IsRelay("leaf-d"))
	}

	newRouter := func(s *Session) *Router {
		r := &Router{
			log:      discardLog(),
			selfName: "relay",
			topo:     topo,
			fwd:      newForwarder(discardLog(), &uploadMeter{}, func(func()) {}, newFakeClock()),
			peers:    map[string]*peerLink{"id-sub": {session: s}, "id-d": {session: s}},
			idByName: map[string]string{"sub-relay": "id-sub", "leaf-d": "id-d"},
		}
		// A three-rung source, so "pinned" and "not pinned" have different answers.
		r.fwd.setUpstream("leaf-b", &captureRTCP{})
		for _, id := range layerLadder {
			r.fwd.learnSSRC("leaf-b", id, r.fwd.newUpstreamGen("leaf-b", id), ssrcQ)
		}
		return r
	}

	// A leg that ends up on `f` after sustained loss was pinned; one on `q` was not.
	drive := func(t *testing.T, f *forwarder, child string) string {
		t.Helper()
		f.loss.observe(child, 0.9)
		for i := 0; i < layerDownRounds*len(layerLadder)*2; i++ {
			f.reviewLayer("leaf-b", child)
		}
		return f.legLayer("leaf-b", child)
	}

	t.Run("setupRelayEdge, before Start", func(t *testing.T) {
		_, aTr := newGatedPair("b", "a")
		offerer := true
		s, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr, Offerer: &offerer,
		})
		if err != nil {
			t.Fatalf("new session: %v", err)
		}
		defer s.Close()
		r := newRouter(s)

		r.setupRelayEdge(s, topo, "sub-relay")
		r.setupRelayEdge(s, topo, "leaf-d")

		if got := drive(t, r.fwd, "sub-relay"); got != "f" {
			t.Errorf("the leg toward a child RELAY fell to %q under loss, want the top rung "+
				"%q — a relay can only forward what it receives, so walking it down caps "+
				"its whole subtree", got, "f")
		}
		if got := drive(t, r.fwd, "leaf-d"); got == "f" {
			t.Errorf("the leg toward a LEAF stayed on %q under sustained loss; the control arm "+
				"must move, or the test above proves nothing about pinning", got)
		}
	})

	t.Run("addLegLive, mid-call", func(t *testing.T) {
		_, aTr := newGatedPair("b", "a")
		offerer := true
		s, err := NewSession(SessionConfig{
			Log: discardLog(), SelfID: "a", PeerID: "b", Transport: aTr, Offerer: &offerer,
		})
		if err != nil {
			t.Fatalf("new session: %v", err)
		}
		defer s.Close()
		r := newRouter(s)

		if !r.addLegLive(leg{src: "leaf-b", child: "sub-relay"}) {
			t.Fatal("addLegLive toward sub-relay reported failure")
		}
		if !r.addLegLive(leg{src: "leaf-b", child: "leaf-d"}) {
			t.Fatal("addLegLive toward leaf-d reported failure")
		}

		if got := drive(t, r.fwd, "sub-relay"); got != "f" {
			t.Errorf("a MID-CALL leg toward a child RELAY fell to %q under loss, want %q — "+
				"the before-Start path pins and this one must too", got, "f")
		}
		if got := drive(t, r.fwd, "leaf-d"); got == "f" {
			t.Errorf("the mid-call leaf leg stayed on %q under sustained loss; the control "+
				"arm must move", got)
		}
	})
}
