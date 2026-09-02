package media

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtcp"

	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// captureRTCP is a fake rtcpWriter that records the packets written to it.
type captureRTCP struct {
	mu   sync.Mutex
	pkts []rtcp.Packet
}

func (c *captureRTCP) WriteRTCP(p []rtcp.Packet) error {
	c.mu.Lock()
	c.pkts = append(c.pkts, p...)
	c.mu.Unlock()
	return nil
}

func (c *captureRTCP) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pkts)
}

// TestForwarderTranslatesPLISSRC pins the #1 SFU footgun deterministically: an
// upstream keyframe request must carry the SOURCE's SSRC, never a downstream one,
// or the source ignores it and the picture stays black. It also checks the throttle.
func TestForwarderTranslatesPLISSRC(t *testing.T) {
	f := newForwarder(slog.New(slog.NewTextHandler(io.Discard, nil)), &uploadMeter{}, func(func()) {}, newFakeClock())
	up := &captureRTCP{}
	f.setUpstream("src", up)

	// Simulate the source's media having arrived with a known SSRC. A source with
	// one unnamed layer is what a single-track sender publishes.
	gen := f.newUpstreamGen("src", "")
	f.learnSSRC("src", "", gen, 0xABCDEF)

	f.requestUpstreamKeyframe("src", "")
	f.requestUpstreamKeyframe("src", "") // immediate second call: throttled away

	if up.count() != 1 {
		t.Fatalf("wrote %d upstream RTCP packets, want 1 (second throttled)", up.count())
	}
	pli, ok := up.pkts[0].(*rtcp.PictureLossIndication)
	if !ok {
		t.Fatalf("upstream packet is %T, want *rtcp.PictureLossIndication", up.pkts[0])
	}
	if pli.MediaSSRC != 0xABCDEF {
		t.Errorf("PLI.MediaSSRC = %#x, want the source SSRC %#x", pli.MediaSSRC, 0xABCDEF)
	}
	if pli.SenderSSRC != 0 {
		t.Errorf("PLI.SenderSSRC = %#x, want 0 (receivers key off MediaSSRC)", pli.SenderSSRC)
	}

	// A source whose media hasn't arrived (ssrc 0) must not emit a PLI.
	f.setUpstream("silent", &captureRTCP{})
	before := f.PLIForwarded()
	f.requestUpstreamKeyframe("silent", "")
	if f.PLIForwarded() != before {
		t.Error("requested a keyframe for a source with no media (ssrc 0)")
	}
}

// TestForwarderRemoveSource pins the churn fix: a departed peer's SOURCE role must
// be cleaned up, not just its CHILD role. removeChild (which handles legs INTO the
// peer) must leave a source entry alone, while removeSource drops it so f.sources
// cannot grow without bound as senders come and go.
func TestForwarderRemoveSource(t *testing.T) {
	f := newForwarder(slog.New(slog.NewTextHandler(io.Discard, nil)), &uploadMeter{}, func(func()) {}, newFakeClock())
	f.setUpstream("A", &captureRTCP{}) // creates forwardSource "A"
	// A leg carrying source A's media down to child B.
	f.mu.Lock()
	f.sources["A"].outs = append(f.sources["A"].outs, &forwardOut{child: "B"})
	f.mu.Unlock()

	// removeChild("A") must NOT drop source A — here A is a SOURCE, not a child.
	f.removeChild("A")
	f.mu.RLock()
	_, present := f.sources["A"]
	f.mu.RUnlock()
	if !present {
		t.Fatal("removeChild deleted the source entry; it should only trim child legs")
	}

	// removeSource("A") drops it, bounding the map across sender churn.
	f.removeSource("A")
	f.mu.RLock()
	n := len(f.sources)
	f.mu.RUnlock()
	if n != 0 {
		t.Fatalf("removeSource left %d sources; want 0", n)
	}
}

// TestRelayForwardsThroughTree is the Phase 3 acceptance test: a leaf receives
// another leaf's media FORWARDED BY THE RELAY, never having a direct connection to
// the source. Three real peers join one room through the real Hub with a hardcoded
// tree (relay → leaf-b, relay → leaf-d): leaf-b sends synthetic VP8, the relay
// forwards it to leaf-d. The proof is topological — leaf-d holds a media track
// while having NO session to leaf-b, so the bytes could only have transited the
// relay. It also asserts a keyframe request reached upstream and that shutdown
// joins every forwarder goroutine. Under -race.
func TestRelayForwardsThroughTree(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("MESH_DEBUG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

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
		{"relay", RouterConfig{Topology: topo, SelfName: "relay"}},                    // pure forwarder
		{"leaf-b", RouterConfig{Topology: topo, SelfName: "leaf-b", SendMedia: true}}, // sender (synthetic VP8)
		{"leaf-d", RouterConfig{Topology: topo, SelfName: "leaf-d"}},                  // receiver / counter
	}

	routers := make(map[string]*Router, len(nodes))
	var wg sync.WaitGroup
	for _, n := range nodes {
		client, err := signaling.Dial(ctx, logger, srv.URL, "tree", n.name)
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
	relay, leafB, leafD := routers["relay"], routers["leaf-b"], routers["leaf-d"]

	// Converge: relay connected to both leaves; each leaf to the relay only; and
	// leaf-d has actually received forwarded media.
	deadline := time.Now().Add(20 * time.Second)
	for {
		if allConnected(relay.Stats(), 2) &&
			allConnected(leafB.Stats(), 1) &&
			allConnected(leafD.Stats(), 1) &&
			receivedAny(leafD.Stats()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tree did not converge:\n relay=%+v\n leaf-b=%+v\n leaf-d=%+v",
				relay.Stats(), leafB.Stats(), leafD.Stats())
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Topological proof: leaf-d received media but has exactly ONE peer (the relay)
	// and no session to leaf-b — the only path for the bytes was through the relay.
	if got := len(leafD.Stats().Peers); got != 1 {
		t.Errorf("leaf-d has %d peers, want 1 (relay only): %+v", got, leafD.Stats().Peers)
	}
	if got := len(leafB.Stats().Peers); got != 1 {
		t.Errorf("leaf-b has %d peers, want 1 (relay only): %+v", got, leafB.Stats().Peers)
	}

	// The relay forwarded at least one keyframe request upstream (SSRC-translated).
	for i := 0; i < 50 && relay.fwd.PLIForwarded() == 0; i++ {
		time.Sleep(100 * time.Millisecond)
	}
	if relay.fwd.PLIForwarded() == 0 {
		t.Error("relay never forwarded a keyframe request upstream")
	}

	// Clean shutdown: cancelling the context must join every forwarder goroutine
	// (per-child RTCP drains) — a leak would hang this.
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("routers did not shut down cleanly — a forwarder goroutine leaked")
	}
}

// allConnected reports whether stats shows exactly want peers, all connected.
func allConnected(stats Stats, want int) bool {
	if len(stats.Peers) != want {
		return false
	}
	for _, st := range stats.Peers {
		if st.String() != "connected" {
			return false
		}
	}
	return true
}

// receivedAny reports whether a media track has arrived from any peer.
func receivedAny(stats Stats) bool {
	for _, ok := range stats.Received {
		if ok {
			return true
		}
	}
	return false
}
