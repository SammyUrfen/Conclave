package simnet

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestBuildTreeProperty is deterministic simulation testing in miniature (the
// FoundationDB/TigerBeetle idea): across hundreds of seeded random fleets, the real
// BuildTree must EITHER refuse an over-constrained fleet with an error OR return a
// tree that passes the independent Validate oracle. There is no third outcome — a
// silently invalid tree — and a single one fails the test. Seeded rand makes any
// failure reproducible from its seed.
func TestBuildTreeProperty(t *testing.T) {
	cons := overlay.Constraints{MaxDepth: 2, StreamKbps: 1000}
	built, deepest := 0, 0
	for seed := int64(1); seed <= 300; seed++ {
		rng := rand.New(rand.NewSource(seed))
		net := Random(rng, 1+rng.Intn(10)) // 1..10 nodes
		topo, c, err := net.Build(nil, cons)
		if err != nil {
			continue // honestly over-constrained fleet — an allowed outcome
		}
		built++
		nodes := net.OverlayNodes()
		if verr := validateTopology(topo, nodes, c); verr != nil {
			t.Fatalf("seed %d: built tree fails Validate: %v\nnodes=%+v\nedges=%+v", seed, verr, nodes, topo.Edges)
		}
		if d := depth(topo, c.Root); d > deepest {
			deepest = d
		}
	}
	if built < 100 {
		t.Fatalf("only %d/300 seeds built a tree — generator is too harsh to be a meaningful property test", built)
	}
	if deepest < 2 {
		t.Errorf("no multi-level tree ever built (deepest=%d); capacity-forced depth is not being exercised", deepest)
	}
}

// TestChurnKeepsInvariants replays a long, seeded sequence of joins and leaves
// against a fleet kept buildable by a pinned strong seed, and asserts the tree is
// valid after EVERY step. This is the harness the hard phases lean on: it proves the
// build pipeline survives churn without ever emitting a broken graph.
func TestChurnKeepsInvariants(t *testing.T) {
	cons := overlay.Constraints{MaxDepth: 2, StreamKbps: 1000}
	rng := rand.New(rand.NewSource(7))

	net := New()
	net.Add(overlay.Node{Name: "seed", UploadKbps: 8000}) // pinned root, cap 8

	for step := 0; step < 200; step++ {
		// Keep the fleet in [1, 12] nodes; within that band it is always buildable
		// (root cap 8 plus relay overflow at depth 2), so a build error here is a
		// real regression, not an over-constraint.
		if net.Len() < 12 && (net.Len() < 3 || rng.Intn(3) != 0) {
			up := []int{2000, 4000}[rng.Intn(2)] // cap 2 or 4; every node can relay overflow
			net.Add(overlay.Node{Name: fmt.Sprintf("s%d", step), UploadKbps: up})
		} else {
			net.RemoveRandom(rng)
		}

		topo, c, err := net.Build(nil, cons)
		if err != nil {
			t.Fatalf("step %d (%d nodes): build failed unexpectedly: %v", step, net.Len(), err)
		}
		if verr := validateTopology(topo, net.OverlayNodes(), c); verr != nil {
			t.Fatalf("step %d: invariant broken after churn: %v\nedges=%+v", step, verr, topo.Edges)
		}
	}
}

// TestLatencyAttachment proves injected latencies actually steer attachment: with
// the root full, a leaf must pick the lower-RTT of the two remaining relays.
func TestLatencyAttachment(t *testing.T) {
	net := New()
	net.Add(overlay.Node{Name: "root", UploadKbps: 2000}) // cap 1 — fills after one child
	net.Add(overlay.Node{Name: "r1", UploadKbps: 4000})
	net.Add(overlay.Node{Name: "r2", UploadKbps: 4000})
	net.Add(overlay.Node{Name: "leaf", UploadKbps: 0})
	net.SetRTT("leaf", "r1", 80)
	net.SetRTT("leaf", "r2", 5)

	topo, c, err := net.Build(nil, overlay.Constraints{MaxDepth: 3, StreamKbps: 2000})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if verr := validateTopology(topo, net.OverlayNodes(), c); verr != nil {
		t.Fatalf("validate: %v", verr)
	}
	if p := topo.ParentOf("leaf"); p != "r2" {
		t.Errorf("leaf attached to %q, want r2 (RTT 5 < 80)", p)
	}
}

// TestBuildReplayIsDeterministic pins the harness's headline promise at the Network
// level: the same seed replays the exact same tree, so any bug found in simnet is
// reproducible. (Renamed from TestScenarioDeterministic when Scenario became a real
// type, to stop the name claiming coverage it does not have.)
func TestBuildReplayIsDeterministic(t *testing.T) {
	cons := overlay.Constraints{MaxDepth: 2, StreamKbps: 1000}
	run := func() []overlay.Edge {
		rng := rand.New(rand.NewSource(99))
		topo, _, err := Random(rng, 9).Build(nil, cons)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		return topo.Edges
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("non-deterministic edge count: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("non-deterministic scenario at edge %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

// depth returns the height of the tree rooted at root (0 for a lone root).
func depth(topo *overlay.Topology, root string) int {
	max := 0
	var walk func(name string, d int)
	walk = func(name string, d int) {
		if d > max {
			max = d
		}
		for _, ch := range topo.ChildrenOf(name) {
			walk(ch, d+1)
		}
	}
	walk(root, 0)
	return max
}
