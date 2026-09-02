package media

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// ladderOf reports the rungs the relay's forwarder currently holds for one source.
// Reading it under the forwarder's own lock is the point: the ladder is written on
// pion's OnTrack goroutines, one per rung, as each track arrives.
func ladderOf(f *forwarder, src string) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := f.sources[src]
	if s == nil {
		return nil
	}
	return s.ladder()
}

// TestOriginPublishesEveryLayerEndToEnd closes the OTHER half of the §7.5 wire, and
// it is the half nothing covered.
//
// docs/DESIGN.md §7.5 records a feature that was inert end to end while every unit
// test was green: the relay offered ONE recvonly m-line, so a three-rung sender put
// one rung on the wire, the relay learned a one-rung ladder, selectLayer correctly
// declined to choose, and every log line was clean. `TestRelayEdgeRecvSlots` and
// `TestPionAnswererFillsEveryOfferedRecvonlyMLine` closed the RECEIVE side of that.
// They say nothing about the SEND side: `startPeerOpt`'s publish loop can be cut to
// `break` after the first track and the entire suite stays green, producing
// byte-for-byte the same silent failure from the other end.
//
// MUTATION CAUGHT: a `break` (or a `[:1]`) in startPeerOpt's `for _, ml := range
// mediaLayersFor(...)` loop. The origin then publishes `video.q` only, so the relay's
// ladder holds one rung and the leaf is served the LOWEST quality a 720p sender can
// make, permanently and silently.
//
// It runs the real binaries' path — a real Hub, a real tree, real pion negotiation —
// because that is what found the original defect and a unit test is what missed it.
// Three synthetic layers rather than three IVF files: mediaLayersFor keys on the
// NUMBER of paths, so three empty paths publish three tracks fed by SendSynthetic,
// which is all the wire needs to prove.
func TestOriginPublishesEveryLayerEndToEnd(t *testing.T) {
	logger := discardLog()
	hub := signaling.NewHub(logger)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	topo := &overlay.Topology{Edges: []overlay.Edge{
		{Parent: "relay", Child: "leaf-b"},
		{Parent: "relay", Child: "leaf-d"},
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nodes := []struct {
		name string
		cfg  RouterConfig
	}{
		{"relay", RouterConfig{Topology: topo, SelfName: "relay"}},
		{"leaf-b", RouterConfig{
			Topology: topo, SelfName: "leaf-b", SendMedia: true,
			// THREE paths ⇒ three layers, q/h/f. Empty paths feed each track from
			// SendSynthetic; what is under test is how many tracks reach the wire.
			MediaPaths: []string{"", "", ""},
		}},
		{"leaf-d", RouterConfig{Topology: topo, SelfName: "leaf-d"}},
	}

	routers := make(map[string]*Router, len(nodes))
	var wg sync.WaitGroup
	for _, n := range nodes {
		client, err := signaling.Dial(ctx, logger, srv.URL, "layers", n.name)
		if err != nil {
			t.Fatalf("%s dial: %v", n.name, err)
		}
		defer client.Close()
		r := NewRouter(logger, client, n.cfg)
		routers[n.name] = r
		wg.Add(1)
		go func(r *Router) {
			defer wg.Done()
			_ = r.Run(ctx)
		}(r)
	}
	defer func() {
		cancel()
		wg.Wait()
	}()
	relay := routers["relay"]

	// A track fires OnTrack only once media flows on it, so this waits for all three
	// synthetic pumps to have produced a packet each — the real evidence that three
	// encodings are on the wire, not merely three m-lines in an SDP.
	waitFor(t, "the relay to learn every rung leaf-b publishes", 20*time.Second, func() bool {
		return len(ladderOf(relay.fwd, "leaf-b")) >= len(layerLadder)
	})

	got := ladderOf(relay.fwd, "leaf-b")
	if len(got) != len(layerLadder) {
		t.Fatalf("the relay learned ladder %v from a three-layer origin, want %v — an origin "+
			"that publishes one track caps the whole tree at its lowest rung, silently",
			got, layerLadder)
	}
	for i, id := range layerLadder {
		if got[i] != id {
			t.Fatalf("ladder = %v, want %v (lowest rung first)", got, layerLadder)
		}
	}

	// …and the ladder is USED: the leaf's leg resolves to the TOP rung, which is the
	// whole point of publishing more than one. A one-rung ladder would answer "q"
	// here and look just as healthy.
	if leg := relay.fwd.legLayer("leaf-b", "leaf-d"); leg != layerLadder[len(layerLadder)-1] {
		t.Errorf("the leg toward leaf-d carries rung %q, want the top rung %q",
			leg, layerLadder[len(layerLadder)-1])
	}
}
