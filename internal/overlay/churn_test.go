package overlay

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

// churnPool is the fixture fleet the churn property runs over: a couple of strong
// relays, several mid-capacity nodes, one TURN-bound forced leaf, and some
// zero-upload leaves — i.e. a shape where capacity, depth, and the TURN rule all
// bind somewhere.
func churnPool(withRTT bool) []Node {
	rtt := func(pairs ...any) map[string]float64 {
		if !withRTT {
			// Production shape: pairwise RTT is never measured live, so attachment
			// falls through to the structural ranks. See docs/PLAN.md §13.2.
			return nil
		}
		m := map[string]float64{}
		for i := 0; i+1 < len(pairs); i += 2 {
			m[pairs[i].(string)] = float64(pairs[i+1].(int))
		}
		return m
	}
	return []Node{
		{Name: "n0", UploadKbps: 20000, RTT: rtt("n1", 20, "n2", 40, "n3", 15)},
		{Name: "n1", UploadKbps: 12000, RTT: rtt("n0", 20, "n2", 10, "n4", 60)},
		{Name: "n2", UploadKbps: 6000, RTT: rtt("n0", 40, "n1", 10, "n5", 25)},
		{Name: "n3", UploadKbps: 6000, RTT: rtt("n0", 15, "n1", 55, "n2", 30)},
		{Name: "n4", UploadKbps: 4000, RTT: rtt("n0", 70, "n1", 60, "n3", 12)},
		{Name: "n5", UploadKbps: 2000, RTT: rtt("n0", 33, "n2", 25)},
		{Name: "n6", UploadKbps: 0, RTT: rtt("n0", 8, "n1", 90)},
		{Name: "n7", UploadKbps: 9000, NAT: NATRelayed, RTT: rtt("n0", 5)}, // forced leaf
	}
}

// churnStep is one recorded transition of the simulation, kept so the whole run can
// be compared byte-for-byte between two identical replays.
type churnStep struct {
	Event string    `json:"event"`
	Topo  *Topology `json:"topo"`
}

// runChurn drives a deterministic pseudo-random churn sequence and asserts, after
// EVERY rebuild, both oracles: Validate (is this tree legal?) and
// ValidateLocalRepair (was this change minimal?). It returns the recorded trace.
//
// This is the headline stability property. Stickiness makes BuildTree
// path-dependent by construction (see TestStragglerPathIsPathDependent), so
// "identical fleets converge to identical trees" is NOT true and must not be
// asserted. What IS true, and is what the design actually buys, is that the edge
// delta between consecutive trees is bounded by the churn that caused the rebuild —
// which is exactly the predicate ValidateLocalRepair encodes.
func runChurn(t *testing.T, seed int64, steps int, withRTT bool) []churnStep {
	t.Helper()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // determinism, not secrecy
	pool := churnPool(withRTT)
	byName := map[string]Node{}
	for _, n := range pool {
		byName[n.Name] = n
	}

	active := []string{"n0", "n1", "n2"} // a meet always starts with somebody in it
	inactive := []string{"n3", "n4", "n5", "n6", "n7"}
	// published is the tree the fleet realized and the only thing the churn oracle
	// may be measured against; working is what the builder rebuilds FROM, which the
	// ratification rule patches on a self-promotion. Conflating the two would have
	// the oracle assert against the very belief it is supposed to check.
	var published, working *Topology
	var trace []churnStep
	// Pending churn accumulates across builds that were discarded, exactly as the
	// coordinator's does: the oracle compares against the last PUBLISHED tree.
	pendingGone, pendingJoined, pendingPromoted := map[string]bool{}, map[string]bool{}, map[string]bool{}

	for step := 0; step < steps; step++ {
		event := "noop"
		switch {
		case len(inactive) > 0 && (len(active) < 3 || rng.Intn(100) < 45):
			i := rng.Intn(len(inactive))
			name := inactive[i]
			inactive = append(inactive[:i], inactive[i+1:]...)
			active = append(active, name)
			sort.Strings(active)
			pendingJoined[name] = true
			delete(pendingGone, name)
			event = "join " + name
		case len(active) > 2 && rng.Intn(100) < 70:
			i := rng.Intn(len(active))
			name := active[i]
			active = append(active[:i], active[i+1:]...)
			inactive = append(inactive, name)
			sort.Strings(inactive)
			pendingGone[name] = true
			delete(pendingJoined, name)
			delete(pendingPromoted, name)
			event = "leave " + name
		default:
			// A peer whose media edge to a healthy parent failed promotes its warm
			// backup on its own and reports it (§5.6). The coordinator RATIFIES:
			// it patches its working copy so the peer's choice becomes the
			// incumbent, then rebuilds from the patched tree — but the oracle still
			// compares against the last published tree, which is the one the fleet
			// actually realized.
			if working == nil || len(working.Backups) == 0 {
				continue
			}
			b := working.Backups[rng.Intn(len(working.Backups))]
			if !contains(active, b.Node) || !contains(active, b.Parent) {
				continue
			}
			patched, err := repatch(working, b.Node, b.Parent)
			if err != nil {
				t.Fatalf("seed %d step %d: repatch: %v", seed, step, err)
			}
			pendingPromoted[b.Node] = true
			working = patched
			event = fmt.Sprintf("promote %s→%s", b.Node, b.Parent)
		}

		nodes := make([]Node, 0, len(active))
		for _, name := range active { // slice order, never map order
			nodes = append(nodes, byName[name])
		}
		cons := Constraints{
			MaxDepth: 2, StreamKbps: 2000, Epoch: 1,
			StickinessMs: DefaultStickinessMs,
		}
		cons.Root = PickRoot(nodes, working)
		if cons.Root == "" {
			trace = append(trace, churnStep{Event: event})
			continue
		}
		cons.Rev = 1
		if working != nil {
			cons.Rev = working.Rev + 1
		}
		next, err := BuildTree(nodes, working, cons)
		if err != nil {
			// Over-constrained for now: keep the previous tree, as the coordinator
			// does, and carry the pending churn into the next attempt.
			trace = append(trace, churnStep{Event: event})
			continue
		}
		if err := Validate(next, nodes, cons); err != nil {
			t.Fatalf("seed %d step %d (%s): built tree fails Validate: %v\n%+v", seed, step, event, err, next)
		}
		if published != nil && !withRTT {
			// The churn oracle is only sound over a fleet with no measured RTT: it
			// cannot see latency, so it cannot tell a legitimate rank-2 move onto a
			// materially closer parent from gratuitous churn. That is the production
			// shape (§13.2), and the RTT-rich runs below assert Validate instead.
			prev := published
			gone, joined, promoted := keys(pendingGone), keys(pendingJoined), keys(pendingPromoted)
			err := ValidateLocalRepair(prev, next, gone, joined, promoted)
			switch {
			case next.Root != prev.Root && !pendingGone[prev.Root]:
				// A VOLUNTARY re-root (a challenger cleared RootChangeMarginKbps) is
				// the one legitimately global reconfiguration, so minimality does not
				// apply — but the oracle must still SAY so rather than wave it
				// through, otherwise it would also miss an accidental re-root.
				if err == nil || !strings.Contains(err.Error(), "root changed") {
					t.Fatalf("seed %d step %d (%s): a voluntary re-root %q→%q was not reported by the oracle: %v",
						seed, step, event, prev.Root, next.Root, err)
				}
			case err != nil:
				t.Fatalf("seed %d step %d (%s): rebuild churned more than the event justified: %v\n prev: %+v\n next: %+v",
					seed, step, event, err, prev.Edges, next.Edges)
			}
		}
		published, working = next, next
		pendingGone, pendingJoined, pendingPromoted = map[string]bool{}, map[string]bool{}, map[string]bool{}
		trace = append(trace, churnStep{Event: event, Topo: next})
	}
	if published == nil {
		t.Fatalf("seed %d: the whole run never produced a tree", seed)
	}
	return trace
}

// TestChurnKeepsBothOraclesGreen is the property run: every tree legal, every
// transition minimal, over long deterministic churn sequences of joins, departures
// and self-promotions.
func TestChurnKeepsBothOraclesGreen(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 1337} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			trace := runChurn(t, seed, 200, false)
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

// TestChurnWithMeasuredRTT runs the same churn over a fleet WITH pairwise latency,
// where minimum-RTT attachment and the stickiness margin are actually exercised. It
// asserts Validate only, for the reason stated in runChurn: the churn oracle is
// blind to latency and would report a legitimate move onto a materially closer
// parent as gratuitous.
func TestChurnWithMeasuredRTT(t *testing.T) {
	for _, seed := range []int64{2, 5, 99} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runChurn(t, seed, 200, true)
		})
	}
}

// TestChurnReplayIsDeterministic is property (c): the same EVENT SEQUENCE always
// yields the same trace. It is the weaker, true cousin of "the same fleet always
// yields the same tree" — which stickiness makes false on purpose.
func TestChurnReplayIsDeterministic(t *testing.T) {
	for _, seed := range []int64{3, 11} {
		first, err := json.Marshal(runChurn(t, seed, 120, true))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			again, err := json.Marshal(runChurn(t, seed, 120, true))
			if err != nil {
				t.Fatal(err)
			}
			if string(again) != string(first) {
				t.Fatalf("seed %d replay %d diverged:\n first: %s\n again: %s", seed, i, first, again)
			}
		}
	}
}

// repatch returns a copy of t with node re-parented onto newParent, edges rebuilt in
// BFS order so the topological-order invariant (§3.1) survives the patch. It models
// the coordinator's ratification step (§5.6).
func repatch(t *Topology, node, newParent string) (*Topology, error) {
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
		child := e.Child
		children[parent[child]] = append(children[parent[child]], child)
	}
	var edges []Edge
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
			edges = append(edges, Edge{Parent: cur, Child: c})
			queue = append(queue, c)
		}
	}
	if len(edges) != len(t.Edges) {
		return nil, fmt.Errorf("repatch: %d of %d edges reachable (the patch detached a subtree)", len(edges), len(t.Edges))
	}
	return &Topology{Epoch: t.Epoch, Rev: t.Rev, Root: t.Root, Edges: edges}, nil
}

// keys returns a set's members in sorted order — never a bare map range, which Go
// randomises and which would make a failing seed unreproducible.
func keys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
