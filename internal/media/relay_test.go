package media

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtcp"

	"github.com/SammyUrfen/conclave/internal/signaling"
)

// TestTopology covers the pure tree queries the Router drives every decision from.
func TestTopology(t *testing.T) {
	// A: root, R: relay (children B,C,D), leaves B,C,D.
	topo := &Topology{Edges: []Edge{
		{Parent: "A", Child: "R"},
		{Parent: "R", Child: "B"},
		{Parent: "R", Child: "C"},
		{Parent: "R", Child: "D"},
	}}

	if got := topo.ParentOf("R"); got != "A" {
		t.Errorf("ParentOf(R) = %q, want A", got)
	}
	if got := topo.ParentOf("A"); got != "" {
		t.Errorf("ParentOf(A) = %q, want \"\" (root)", got)
	}
	if got := topo.ChildrenOf("R"); len(got) != 3 {
		t.Errorf("ChildrenOf(R) = %v, want 3 children", got)
	}
	if got := topo.NeighborsOf("R"); len(got) != 4 { // B,C,D + parent A
		t.Errorf("NeighborsOf(R) = %v, want 4", got)
	}
	if got := topo.NeighborsOf("B"); len(got) != 1 || got[0] != "R" {
		t.Errorf("NeighborsOf(B) = %v, want [R]", got)
	}
	if !topo.IsRelay("R") || topo.IsRelay("B") {
		t.Errorf("IsRelay: R=%v B=%v, want true,false", topo.IsRelay("R"), topo.IsRelay("B"))
	}
	// A has child R, so it is structurally a relay too — a single-child root that
	// only receives (its forwarder would have no downstream legs). Harmless, and the
	// honest reading of "has children".
	if !topo.IsRelay("A") {
		t.Error("IsRelay(A) = false, want true (A has child R)")
	}

	// Offerer: the relay offers on every edge; the non-relay answers.
	if !topo.Offers("R", "B") {
		t.Error("Offers(R,B) = false, want true (relay offers)")
	}
	if topo.Offers("B", "R") {
		t.Error("Offers(B,R) = true, want false (leaf answers)")
	}
	if !topo.Offers("R", "A") {
		t.Error("Offers(R,A) = false, want true (relay offers toward its parent too)")
	}
}

// TestLoadTopology covers the loader's fail-loud validation, including the
// single-parent tree invariant (a re-parenting copy/paste slip must not slip
// through and become a silently half-formed tree at run time).
func TestLoadTopology(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr string // substring; "" means expect success
	}{
		{name: "valid star", json: `{"edges":[{"parent":"r","child":"b"},{"parent":"r","child":"d"}]}`},
		{name: "valid two-level", json: `{"edges":[{"parent":"a","child":"r"},{"parent":"r","child":"b"}]}`},
		{name: "no edges", json: `{"edges":[]}`, wantErr: "no edges"},
		{name: "missing child", json: `{"edges":[{"parent":"r","child":""}]}`, wantErr: "required"},
		{name: "self parent", json: `{"edges":[{"parent":"r","child":"r"}]}`, wantErr: "its own parent"},
		{name: "two parents", json: `{"edges":[{"parent":"p1","child":"c"},{"parent":"p2","child":"c"}]}`, wantErr: "two parents"},
		{name: "bad json", json: `{"edges":`, wantErr: "parse"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tree.json")
			if err := os.WriteFile(path, []byte(tt.json), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadTopology(path)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("LoadTopology(%s): unexpected error: %v", tt.name, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("LoadTopology(%s): got %v, want error containing %q", tt.name, err, tt.wantErr)
			}
		})
	}
}

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
	f := newForwarder(slog.New(slog.NewTextHandler(io.Discard, nil)), &uploadMeter{}, func(func()) {})
	up := &captureRTCP{}
	f.setUpstream("src", up)

	// Simulate the source's media having arrived with a known SSRC.
	f.mu.Lock()
	f.sources["src"].ssrc.Store(0xABCDEF)
	f.mu.Unlock()

	f.requestUpstreamKeyframe("src")
	f.requestUpstreamKeyframe("src") // immediate second call: throttled away

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
	f.requestUpstreamKeyframe("silent")
	if f.PLIForwarded() != before {
		t.Error("requested a keyframe for a source with no media (ssrc 0)")
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

	topo := &Topology{Edges: []Edge{
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
