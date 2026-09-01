package simnet

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// simChurnPool is the fixture fleet the churn property runs over: two strong
// relays, several mid-capacity nodes, one TURN-bound forced leaf, and some
// zero-upload leaves — a shape where capacity, depth, and the TURN rule all bind
// somewhere.
//
// NO measured RTT, deliberately. The churn oracle cannot see latency (it is a pure
// function of two trees), so over an RTT-rich fleet it would report a legitimate
// move onto a materially closer parent as gratuitous. That is also the production
// shape: pairwise RTT is not measured live.
func simChurnPool() []overlay.Node {
	return []overlay.Node{
		{Name: "n0", UploadKbps: 20000},
		{Name: "n1", UploadKbps: 12000},
		{Name: "n2", UploadKbps: 6000},
		{Name: "n3", UploadKbps: 6000},
		{Name: "n4", UploadKbps: 4000},
		{Name: "n5", UploadKbps: 2000},
		{Name: "n6", UploadKbps: 0},
		{Name: "n7", UploadKbps: 9000, NAT: overlay.NATRelayed}, // forced leaf
	}
}

// simStep is one recorded transition, kept so a whole run can be compared
// byte-for-byte between two identical replays.
type simStep struct {
	Event string            `json:"event"`
	Topo  *overlay.Topology `json:"topo,omitempty"`
	Delta int               `json:"delta"`
}

// runSimChurn drives a deterministic churn sequence through the FAILURE INJECTORS
// (Kill, Leave, Add) and asserts, after every rebuild, both oracles:
//
//	Validate            — is this tree legal?
//	ValidateLocalRepair — was this change BOUNDED by the churn that caused it?
//
// The second is the headline property. "The same fleet always converges to the same
// tree" is FALSE by construction — stickiness makes BuildTree path-dependent, and
// that is exactly the price paid for minimal-disruption rebuilds. What IS true, and
// is what the design actually buys, is that the edge-set delta between consecutive
// published trees is bounded by the churn: only nodes orphaned by a departure,
// nodes that self-promoted, and nodes whose old parent demonstrably had no room may
// move. ValidateLocalRepair is precisely that predicate.
//
// The three trees of docs/PLAN.md §5.6a are kept distinct here because conflating
// them makes the oracle assert against the very belief it is supposed to check:
//
//	published — the last tree the fleet realized; the ONLY sound baseline
//	working   — published patched with ratified self-promotions; BuildTree's prev
//	next      — this round's output
func runSimChurn(t *testing.T, seed int64, steps int) []simStep {
	t.Helper()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // determinism, not secrecy
	pool := simChurnPool()
	byName := map[string]overlay.Node{}
	for _, n := range pool {
		byName[n.Name] = n
	}

	net := New()
	active := []string{"n0", "n1", "n2"} // a meet always starts with somebody in it
	for _, n := range active {
		net.Add(byName[n])
	}
	inactive := []string{"n3", "n4", "n5", "n6", "n7"}

	var published, working *overlay.Topology
	var trace []simStep
	// Pending churn accumulates across rebuilds that were discarded, exactly as the
	// coordinator's does: the oracle compares against the last PUBLISHED tree.
	pendGone, pendJoined, pendPromoted := map[string]bool{}, map[string]bool{}, map[string]bool{}
	maxDelta := 0

	for step := 0; step < steps; step++ {
		event := "noop"
		switch {
		case len(inactive) > 0 && (len(active) < 3 || rng.Intn(100) < 45):
			i := rng.Intn(len(inactive))
			name := inactive[i]
			inactive = append(inactive[:i], inactive[i+1:]...)
			active = append(active, name)
			sort.Strings(active)
			net.Add(byName[name])
			pendJoined[name] = true
			delete(pendGone, name)
			event = "join " + name
		case len(active) > 2 && rng.Intn(100) < 70:
			i := rng.Intn(len(active))
			name := active[i]
			active = append(active[:i], active[i+1:]...)
			inactive = append(inactive, name)
			sort.Strings(inactive)
			// Kill and Leave are the same removal to the TREE; they differ only in
			// whether the control plane was told. Alternating exercises both.
			if rng.Intn(2) == 0 {
				net.Kill(name)
				event = "kill " + name
			} else {
				net.Leave(name)
				event = "leave " + name
			}
			pendGone[name] = true
			delete(pendJoined, name)
			delete(pendPromoted, name)
		default:
			// A peer whose media edge to a healthy parent failed promotes its warm
			// backup on its own and reports it. The coordinator RATIFIES: it patches
			// its WORKING copy so the peer's choice becomes the incumbent, then
			// rebuilds from the patched tree — while the oracle still measures
			// against the last PUBLISHED tree.
			if working == nil || len(working.Backups) == 0 {
				continue
			}
			b := working.Backups[rng.Intn(len(working.Backups))]
			if !containsName(active, b.Node) || !containsName(active, b.Parent) {
				continue
			}
			patched, err := repatch(working, b.Node, b.Parent)
			if err != nil {
				t.Fatalf("seed %d step %d: repatch: %v", seed, step, err)
			}
			pendPromoted[b.Node] = true
			working = patched
			event = fmt.Sprintf("promote %s→%s", b.Node, b.Parent)
		}

		cons := overlay.Constraints{MaxDepth: 2, StreamKbps: 2000, StickinessMs: overlay.DefaultStickinessMs}
		next, c, err := net.Build(working, cons)
		if err != nil {
			// Not buildable right now: hold the previous tree, as the coordinator
			// does, and carry the pending churn into the next attempt.
			trace = append(trace, simStep{Event: event})
			continue
		}
		nodes := net.OverlayNodes()
		if verr := overlay.Validate(next, nodes, c); verr != nil {
			t.Fatalf("seed %d step %d (%s): built tree fails Validate: %v\n%+v", seed, step, event, verr, next.Edges)
		}

		delta := 0
		if published != nil {
			delta = len(changedParents(published, next))
			gone, joined, promoted := sortedKeys(pendGone), sortedKeys(pendJoined), sortedKeys(pendPromoted)
			rerr := overlay.ValidateLocalRepair(published, next, gone, joined, promoted)
			switch {
			case next.Root != published.Root && !pendGone[published.Root]:
				// A VOLUNTARY re-root (a challenger cleared RootChangeMarginKbps) is
				// the one legitimately global reconfiguration, so minimality does not
				// apply — but the oracle must still SAY so rather than wave it
				// through, otherwise it would also miss an ACCIDENTAL re-root.
				if rerr == nil || !strings.Contains(rerr.Error(), "root changed") {
					t.Fatalf("seed %d step %d (%s): a voluntary re-root %q→%q was not reported by the oracle: %v",
						seed, step, event, published.Root, next.Root, rerr)
				}
			case rerr != nil:
				t.Fatalf("seed %d step %d (%s): rebuild churned more than the event justified: %v\n prev: %+v\n next: %+v",
					seed, step, event, rerr, published.Edges, next.Edges)
			default:
				if delta > maxDelta {
					maxDelta = delta
				}
			}
		}

		published, working = next, next
		pendGone, pendJoined, pendPromoted = map[string]bool{}, map[string]bool{}, map[string]bool{}
		trace = append(trace, simStep{Event: event, Topo: next, Delta: delta})
	}
	if published == nil {
		t.Fatalf("seed %d: the whole run never produced a tree", seed)
	}
	t.Logf("seed %d: %d steps, largest bounded edge delta %d", seed, steps, maxDelta)
	return trace
}

// TestChurnBoundedEdgeDelta is the headline property (b): over long seeded churn
// driven through the failure injectors, every published tree is legal and every
// transition is bounded by the churn that caused it.
func TestChurnBoundedEdgeDelta(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 1337, 20260901} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			trace := runSimChurn(t, seed, 200)
			built := 0
			for _, s := range trace {
				if s.Topo != nil {
					built++
				}
			}
			if built < 50 {
				t.Fatalf("only %d of %d steps produced a tree; the property is barely exercised", built, len(trace))
			}
		})
	}
}

// TestChurnReplayIsDeterministic is property (c) at the Network level: the same seed
// and the same event SEQUENCE yield a byte-identical trace.
func TestChurnReplayIsDeterministic(t *testing.T) {
	for _, seed := range []int64{3, 11} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			first, err := json.Marshal(runSimChurn(t, seed, 120))
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				again, err := json.Marshal(runSimChurn(t, seed, 120))
				if err != nil {
					t.Fatal(err)
				}
				if string(again) != string(first) {
					t.Fatalf("seed %d replay %d diverged:\n first: %s\n again: %s", seed, i, first, again)
				}
			}
		})
	}
}

// TestSingleDepartureMovesOnlyItsSubtree is the crisp, numeric statement of the
// bound the oracle encodes: when exactly ONE node departs and nothing else changes,
// the only peers allowed to re-parent are the ones that node was carrying. A leaf's
// departure must move nobody at all.
//
// Both departure primitives are exercised because they must be indistinguishable to
// the TREE — the Kill/Leave difference is a control-plane notice, not a topology
// fact, and a divergence here would mean the harness had leaked one into the other.
func TestSingleDepartureMovesOnlyItsSubtree(t *testing.T) {
	fleet := []overlay.Node{
		{Name: "R", UploadKbps: 8000}, // highest upload: the root
		{Name: "A", UploadKbps: 6000},
		{Name: "B", UploadKbps: 6000},
		{Name: "L1", UploadKbps: 0},
		{Name: "L2", UploadKbps: 0},
		{Name: "L3", UploadKbps: 0},
		{Name: "L4", UploadKbps: 0},
	}
	cons := overlay.Constraints{MaxDepth: 3, StreamKbps: 2000, StickinessMs: overlay.DefaultStickinessMs}

	build := func() (*Network, *overlay.Topology) {
		t.Helper()
		net := New()
		for _, n := range fleet {
			net.Add(n)
		}
		topo, c, err := net.Build(nil, cons)
		if err != nil {
			t.Fatalf("baseline build: %v", err)
		}
		if verr := overlay.Validate(topo, net.OverlayNodes(), c); verr != nil {
			t.Fatalf("baseline validate: %v", verr)
		}
		return net, topo
	}

	_, base := build()
	for _, victim := range base.Nodes() {
		if victim == base.Root {
			continue // a departed root is a forced re-root: no minimality to assert
		}
		for _, mode := range []string{"kill", "leave"} {
			t.Run(mode+" "+victim, func(t *testing.T) {
				net, prev := build()
				if mode == "kill" {
					net.Kill(victim)
				} else {
					net.Leave(victim)
				}
				next, c, err := net.Build(prev, cons)
				if err != nil {
					t.Fatalf("rebuild after %s %s: %v", mode, victim, err)
				}
				if verr := overlay.Validate(next, net.OverlayNodes(), c); verr != nil {
					t.Fatalf("rebuild fails Validate: %v", verr)
				}
				if rerr := overlay.ValidateLocalRepair(prev, next, []string{victim}, nil, nil); rerr != nil {
					t.Fatalf("rebuild churned more than one departure justifies: %v", rerr)
				}

				allowed := map[string]bool{}
				for _, n := range prev.Subtree(victim) {
					allowed[n] = true
				}
				moved := changedParents(prev, next)
				for _, m := range moved {
					if !allowed[m] {
						t.Errorf("%q re-parented (%q → %q) although it was not under the departed %q",
							m, prev.ParentOf(m), next.ParentOf(m), victim)
					}
				}
				if len(prev.ChildrenOf(victim)) == 0 && len(moved) != 0 {
					t.Errorf("a leaf departed but %v re-parented; a leaf carries nobody", moved)
				}
			})
		}
	}
}

// changedParents returns every node present in both trees whose parent differs, in
// prev's edge order so a failure is reported reproducibly.
func changedParents(prev, next *overlay.Topology) []string {
	inNext := map[string]bool{next.Root: true}
	for _, e := range next.Edges {
		inNext[e.Parent], inNext[e.Child] = true, true
	}
	var out []string
	for _, e := range prev.Edges {
		if !inNext[e.Child] {
			continue
		}
		if next.ParentOf(e.Child) != e.Parent {
			out = append(out, e.Child)
		}
	}
	return out
}

// repatch returns a copy of t with node re-parented onto newParent, edges rebuilt in
// BFS order so the topological-order invariant survives the patch. It models the
// coordinator's ratification step: the peer already moved, and the coordinator's
// WORKING copy has to agree before it rebuilds.
func repatch(t *overlay.Topology, node, newParent string) (*overlay.Topology, error) {
	parent := map[string]string{}
	for _, e := range t.Edges {
		parent[e.Child] = e.Parent
	}
	if _, ok := parent[node]; !ok {
		return nil, fmt.Errorf("repatch: %q is not an attached child", node)
	}
	parent[node] = newParent
	children := map[string][]string{}
	for _, e := range t.Edges { // slice order in, slice order out
		children[parent[e.Child]] = append(children[parent[e.Child]], e.Child)
	}
	var edges []overlay.Edge
	queue := []string{t.Root}
	seen := map[string]bool{t.Root: true}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, c := range children[cur] {
			if seen[c] {
				return nil, fmt.Errorf("repatch: %q reached twice (the patch made a cycle)", c)
			}
			seen[c] = true
			edges = append(edges, overlay.Edge{Parent: cur, Child: c})
			queue = append(queue, c)
		}
	}
	if len(edges) != len(t.Edges) {
		return nil, fmt.Errorf("repatch: %d of %d edges reachable (the patch detached a subtree)", len(edges), len(t.Edges))
	}
	return &overlay.Topology{Epoch: t.Epoch, Rev: t.Rev, Root: t.Root, Edges: edges}, nil
}

// sortedKeys returns a set's members in sorted order — never a bare map range, which
// Go randomises and which would make a failing seed unreproducible.
func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// containsName reports whether names holds name.
func containsName(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}
