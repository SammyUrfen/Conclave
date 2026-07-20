package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// fakeSender records the latest topology pushed to each peer and counts pushes, so
// a test can assert on what the coordinator decided without any signaling.
type fakeSender struct {
	mu    sync.Mutex
	topos map[string]*overlay.Topology
	total int
}

func newFakeSender() *fakeSender { return &fakeSender{topos: map[string]*overlay.Topology{}} }

func (f *fakeSender) SendTopology(roomID, peerID string, topo *overlay.Topology) error {
	f.mu.Lock()
	f.topos[peerID] = topo
	f.total++
	f.mu.Unlock()
	return nil
}

func (f *fakeSender) topo(peerID string) *overlay.Topology {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.topos[peerID]
}

func (f *fakeSender) pushes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.total
}

func mustJSON(t *testing.T, rep metrics.Report) []byte {
	t.Helper()
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// waitFor polls pred until it holds or the deadline passes, the standard way to
// assert on an asynchronous event loop without sleeping a fixed guess.
func waitFor(t *testing.T, d time.Duration, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func testCoordinator(t *testing.T, fs Sender) (*Coordinator, context.CancelFunc) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := New(log, Config{MaxDepth: 2, StreamKbps: 2000, DefaultUploadKbps: 2000}, fs)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Run(ctx) }()
	return c, cancel
}

// TestCoordinatorComputesTree is the core contract: three peers join a managed room
// and report telemetry; the coordinator elects the highest-upload node as the root
// relay and pushes every member a tree attaching the leaves to it.
func TestCoordinatorComputesTree(t *testing.T) {
	fs := newFakeSender()
	c, cancel := testCoordinator(t, fs)
	defer cancel()

	c.PeerJoined("room", "p1", "relay")
	c.Metrics("room", "p1", mustJSON(t, metrics.Report{Name: "relay", UploadKbps: 8000}))
	c.PeerJoined("room", "p2", "leaf-b")
	c.Metrics("room", "p2", mustJSON(t, metrics.Report{Name: "leaf-b", UploadKbps: 0}))
	c.PeerJoined("room", "p3", "leaf-d")
	c.Metrics("room", "p3", mustJSON(t, metrics.Report{Name: "leaf-d", UploadKbps: 0}))

	// Each leaf must be pushed a tree where the relay is its parent.
	waitFor(t, 2*time.Second, "leaf-d attached to relay", func() bool {
		topo := fs.topo("p3")
		return topo != nil && topo.ParentOf("leaf-d") == "relay"
	})
	waitFor(t, 2*time.Second, "leaf-b attached to relay", func() bool {
		topo := fs.topo("p2")
		return topo != nil && topo.ParentOf("leaf-b") == "relay"
	})

	// The relay is the root: no parent, and it forwards.
	topo := fs.topo("p1")
	if topo == nil || topo.ParentOf("relay") != "" || !topo.IsRelay("relay") {
		t.Fatalf("relay should be the root relay, got %+v", topo)
	}
}

// TestCoordinatorAntiThrash proves the Phase 4 hysteresis slice: a node's SECOND
// report does not by itself recompute the tree (no thrashing on metric wiggle), but
// the updated number IS used at the next membership change.
func TestCoordinatorAntiThrash(t *testing.T) {
	fs := newFakeSender()
	c, cancel := testCoordinator(t, fs)
	defer cancel()

	c.PeerJoined("room", "p1", "relay")
	c.Metrics("room", "p1", mustJSON(t, metrics.Report{Name: "relay", UploadKbps: 8000}))
	c.PeerJoined("room", "p2", "leaf-b")
	c.Metrics("room", "p2", mustJSON(t, metrics.Report{Name: "leaf-b", UploadKbps: 0}))
	waitFor(t, 2*time.Second, "relay is root", func() bool {
		topo := fs.topo("p2")
		return topo != nil && topo.ParentOf("leaf-b") == "relay"
	})

	// A dramatic SECOND report from leaf-b: it now claims a huge upload. If the
	// coordinator recomputed on every report it would re-root the tree onto leaf-b.
	// It must not — a subsequent report only refreshes the stored number.
	c.Metrics("room", "p2", mustJSON(t, metrics.Report{Name: "leaf-b", UploadKbps: 999999}))
	// Give the event loop time to (wrongly) act, then assert it did NOT re-root.
	time.Sleep(150 * time.Millisecond)
	if topo := fs.topo("p1"); topo == nil || topo.ParentOf("relay") != "" {
		t.Fatalf("subsequent report re-rooted the tree (thrash): %+v", topo)
	}

	// Now a membership change occurs — recompute runs, and NOW leaf-b's stored huge
	// upload makes it the root. This shows the number was retained, just not acted on
	// until a threshold event.
	c.PeerJoined("room", "p3", "leaf-e")
	waitFor(t, 2*time.Second, "leaf-b becomes root after a join", func() bool {
		topo := fs.topo("p1")
		return topo != nil && topo.ParentOf("leaf-b") == "" && topo.IsRelay("leaf-b")
	})
}

// TestCoordinatorLeaveRecomputes checks a departure triggers a fresh, smaller tree.
func TestCoordinatorLeaveRecomputes(t *testing.T) {
	fs := newFakeSender()
	c, cancel := testCoordinator(t, fs)
	defer cancel()

	c.PeerJoined("room", "p1", "relay")
	c.Metrics("room", "p1", mustJSON(t, metrics.Report{Name: "relay", UploadKbps: 8000}))
	c.PeerJoined("room", "p2", "leaf-b")
	c.PeerJoined("room", "p3", "leaf-d")
	waitFor(t, 2*time.Second, "3-node tree", func() bool {
		topo := fs.topo("p1")
		return topo != nil && len(topo.Edges) == 2
	})

	c.PeerLeft("room", "p3") // leaf-d leaves
	waitFor(t, 2*time.Second, "tree shrinks to 1 edge", func() bool {
		topo := fs.topo("p1")
		return topo != nil && len(topo.Edges) == 1 && topo.ParentOf("leaf-b") == "relay"
	})
}

// TestCoordinatorSkipsUnnamed confirms a peer that joined without a name is not
// pushed a topology (it cannot be placed in a name-keyed tree) and does not break
// the tree computed for the named members.
func TestCoordinatorSkipsUnnamed(t *testing.T) {
	fs := newFakeSender()
	c, cancel := testCoordinator(t, fs)
	defer cancel()

	c.PeerJoined("room", "p1", "relay")
	c.Metrics("room", "p1", mustJSON(t, metrics.Report{Name: "relay", UploadKbps: 8000}))
	c.PeerJoined("room", "p2", "") // anonymous peer
	c.PeerJoined("room", "p3", "leaf-d")
	c.Metrics("room", "p3", mustJSON(t, metrics.Report{Name: "leaf-d", UploadKbps: 0}))

	waitFor(t, 2*time.Second, "leaf-d attached", func() bool {
		topo := fs.topo("p3")
		return topo != nil && topo.ParentOf("leaf-d") == "relay"
	})
	if got := fs.topo("p2"); got != nil {
		t.Errorf("anonymous peer p2 was pushed a topology: %+v", got)
	}
}
