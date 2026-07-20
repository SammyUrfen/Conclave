package overlay

import "fmt"

// Validate checks that a topology is a well-formed relay tree over exactly the
// given nodes and honours every constraint BuildTree is supposed to guarantee. It
// is the oracle the tests and the simnet harness assert against: rather than
// re-deriving the expected tree (which would just re-implement the heuristic and
// prove nothing), a test drives BuildTree on some fleet and asserts Validate
// passes. That separation — construct with one function, verify with an
// independent one — is what makes the property tests meaningful.
//
// The properties, each a way a forwarding tree could be silently broken:
//   - single root: exactly the constraint's Root has no parent;
//   - it IS a tree: every non-root has exactly one parent, and no cycles;
//   - connected: every node is reachable from the root (no orphan subtree);
//   - depth ≤ MaxDepth: latency per hop is bounded;
//   - no relay over-subscribed: children ≤ upload capacity (the degree bound);
//   - TURN-bound nodes are leaves: they never appear as a parent;
//   - closed world: every edge endpoint is a known node and every node appears.
func Validate(t *Topology, nodes []Node, c Constraints) error {
	byName := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		byName[n.Name] = n
	}
	if _, ok := byName[c.Root]; !ok {
		return fmt.Errorf("validate: root %q is not among the nodes", c.Root)
	}

	// Every edge endpoint must be a real node, and single-parent must hold.
	parent := make(map[string]string, len(nodes))
	childCount := make(map[string]int, len(nodes))
	for _, e := range t.Edges {
		if _, ok := byName[e.Parent]; !ok {
			return fmt.Errorf("validate: edge parent %q is not a known node", e.Parent)
		}
		if _, ok := byName[e.Child]; !ok {
			return fmt.Errorf("validate: edge child %q is not a known node", e.Child)
		}
		if prev, ok := parent[e.Child]; ok {
			return fmt.Errorf("validate: %q has two parents (%q and %q)", e.Child, prev, e.Parent)
		}
		parent[e.Child] = e.Parent
		childCount[e.Parent]++
	}

	// Exactly the root is parentless; every other node has a parent (⇒ connected,
	// since with n−1 edges and single-parent, "every non-root has a parent" already
	// forces a single connected tree — but we still walk from the root to reject a
	// stray cycle among a detached component, which single-parent alone permits).
	for _, n := range nodes {
		_, hasParent := parent[n.Name]
		if n.Name == c.Root && hasParent {
			return fmt.Errorf("validate: root %q has a parent", n.Name)
		}
		if n.Name != c.Root && !hasParent {
			return fmt.Errorf("validate: non-root %q has no parent (disconnected)", n.Name)
		}
	}

	// Walk from the root: every node must be reached exactly once, within depth, and
	// no relay may exceed its capacity. Reaching all n nodes from the root also rules
	// out a cycle (a cycle would strand its members from the root's reachable set).
	reached := map[string]int{c.Root: 0}
	queue := []string{c.Root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		d := reached[cur]
		if d > c.MaxDepth {
			return fmt.Errorf("validate: node %q at depth %d exceeds MaxDepth %d", cur, d, c.MaxDepth)
		}
		for _, e := range t.Edges {
			if e.Parent != cur {
				continue
			}
			if _, seen := reached[e.Child]; seen {
				return fmt.Errorf("validate: node %q reached twice (cycle or shared child)", e.Child)
			}
			reached[e.Child] = d + 1
			queue = append(queue, e.Child)
		}
	}
	if len(reached) != len(nodes) {
		return fmt.Errorf("validate: reached %d of %d nodes from root (disconnected component)", len(reached), len(nodes))
	}

	// Per-node role constraints: capacity and TURN-forced-leaf.
	for _, n := range nodes {
		if childCount[n.Name] > 0 && n.NAT == NATRelayed {
			return fmt.Errorf("validate: TURN-bound node %q has %d children (must be a leaf)", n.Name, childCount[n.Name])
		}
		if cap := capacityOf(n, c); childCount[n.Name] > cap {
			return fmt.Errorf("validate: node %q serves %d children over its capacity %d", n.Name, childCount[n.Name], cap)
		}
	}
	return nil
}
