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
//   - stamped: Epoch ≥ 1 and Rev ≥ 1, since a zero-stamped tree is unpublishable
//     and every peer would silently ignore it;
//   - Root agrees with Edges and with the constraints: it is exactly the unique
//     parentless node, and it is the root the caller asked for;
//   - single root and it IS a tree: every non-root has exactly one parent, no
//     cycles, and no node is unattached;
//   - connected: every node is reachable from the root (no orphan subtree);
//   - depth ≤ MaxDepth: latency per hop is bounded;
//   - edges are in topological (attach) order, which the stability-preserving
//     rebuild replays and therefore depends on;
//   - no relay over-subscribed: children ≤ upload capacity (the degree bound);
//   - TURN-bound nodes are leaves: they never appear as a parent;
//   - closed world: every edge endpoint is a known node and every node appears;
//   - backups are well-formed, legal, satisfy B ∉ Subtree(ParentOf(node)), keep the
//     promoted subtree within MaxDepth, and never commit a relay beyond its
//     capacity plus BackupOvershootAllowance.
func Validate(t *Topology, nodes []Node, c Constraints) error {
	byName := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		byName[n.Name] = n
	}
	if _, ok := byName[c.Root]; !ok {
		return fmt.Errorf("validate: root %q is not among the nodes", c.Root)
	}
	if t.Epoch < 1 {
		return fmt.Errorf("validate: epoch is 0; an unstamped tree is unpublishable")
	}
	if t.Rev < 1 {
		return fmt.Errorf("validate: rev is 0; an unstamped tree is unpublishable")
	}
	if t.Root == "" {
		return fmt.Errorf("validate: root is unset")
	}
	if t.Root != c.Root {
		return fmt.Errorf("validate: tree root %q disagrees with the constraints' root %q", t.Root, c.Root)
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
	// An unattached node is therefore NOT legal: Depth would answer -1 for it, and a
	// peer with no path to the root receives nothing.
	for _, n := range nodes {
		_, hasParent := parent[n.Name]
		if n.Name == t.Root && hasParent {
			return fmt.Errorf("validate: root %q has a parent", n.Name)
		}
		if n.Name != t.Root && !hasParent {
			return fmt.Errorf("validate: non-root %q has no parent (disconnected)", n.Name)
		}
	}

	// Walk from the root: every node must be reached exactly once, within depth, and
	// no relay may exceed its capacity. Reaching all n nodes from the root also rules
	// out a cycle (a cycle would strand its members from the root's reachable set).
	reached := map[string]int{t.Root: 0}
	queue := []string{t.Root}
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

	// Topological (attach) order: a parent must be introduced before it is used.
	// The stability-preserving rebuild replays this order to place parents before
	// their children, so a tree that violates it would silently scatter incumbents.
	introduced := map[string]bool{t.Root: true}
	for i, e := range t.Edges {
		if !introduced[e.Parent] {
			return fmt.Errorf("validate: edge %d (%q→%q) is out of topological order: %q has not been attached yet",
				i, e.Parent, e.Child, e.Parent)
		}
		introduced[e.Child] = true
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

	return validateBackups(t, byName, c, parent, childCount)
}

// validateBackups is the backup half of the oracle, split out only for readability;
// it is not independently useful. It re-derives everything from the tree rather than
// trusting any builder state, which is what makes it an oracle rather than a mirror.
func validateBackups(t *Topology, byName map[string]Node, c Constraints, parent map[string]string, childCount map[string]int) error {
	seen := make(map[string]bool, len(t.Backups))
	committed := make(map[string]int, len(t.Backups))
	for i, b := range t.Backups {
		if seen[b.Node] {
			return fmt.Errorf("validate: backup %d: node %q already has a backup; at most one is allowed", i, b.Node)
		}
		seen[b.Node] = true
		if _, ok := byName[b.Node]; !ok {
			return fmt.Errorf("validate: backup %d: node %q is not a known node", i, b.Node)
		}
		if _, ok := byName[b.Parent]; !ok {
			return fmt.Errorf("validate: backup %d: parent %q is not a known node", i, b.Parent)
		}
		if b.Node == t.Root {
			return fmt.Errorf("validate: backup %d: the root %q cannot have a backup parent", i, b.Node)
		}
		if b.Parent == b.Node {
			return fmt.Errorf("validate: backup %d: %q is its own backup parent", i, b.Node)
		}
		primary := parent[b.Node]
		if b.Parent == primary {
			return fmt.Errorf("validate: backup %d: %q's backup is its primary parent %q", i, b.Node, primary)
		}
		// The invariant: the backup must survive the failure it insures against, so
		// it may not lie anywhere inside the subtree the primary parent takes down.
		for _, n := range t.Subtree(primary) {
			if b.Parent == n {
				return fmt.Errorf("validate: backup %d: %q's backup %q is inside Subtree(%q) and dies with the same failure",
					i, b.Node, b.Parent, primary)
			}
		}
		if capacityOf(byName[b.Parent], c) == 0 {
			return fmt.Errorf("validate: backup %d: %q's backup %q cannot parent anyone (TURN-bound or too little upload)",
				i, b.Node, b.Parent)
		}
		// A promotion sinks the promoted node's WHOLE subtree, so bounding the
		// backup's own depth is not enough.
		if d := t.Depth(b.Parent) + 1 + backupHeight(t, b.Node); d > c.MaxDepth {
			return fmt.Errorf("validate: backup %d: promoting %q under %q would put its subtree at depth %d, past MaxDepth %d",
				i, b.Node, b.Parent, d, c.MaxDepth)
		}
		committed[b.Parent]++
	}
	// Fan-in: primary children plus every backup pointing at a node must stay within
	// its capacity plus the allowance, so a simultaneous failover oversubscribes a
	// relay by at most BackupOvershootAllowance children.
	for i, b := range t.Backups {
		limit := capacityOf(byName[b.Parent], c) + BackupOvershootAllowance
		if load := childCount[b.Parent] + committed[b.Parent]; load > limit {
			return fmt.Errorf("validate: backup %d: %q is committed to %d children (primary + backup), past its bound %d",
				i, b.Parent, load, limit)
		}
	}
	return nil
}

// ValidateLocalRepair asserts that next differs from prev only where it had to.
//
// gone is the set of node names that departed (or were declared gone) between the
// two builds; joined is the set that arrived; promoted is the set that re-parented
// THEMSELVES onto their precomputed backup after a parent or edge failure and told
// the coordinator so. The property: every node present in BOTH prev and next whose
// parent CHANGED must be justified — orphaned by something in gone, self-promoted,
// or its previous parent must have become unusable in next.
//
// prev is the LAST PUBLISHED tree, not the coordinator's patched working copy. That
// choice matters: the ratification rule has the coordinator patch a self-promotion
// into its working copy before rebuilding, and validating against the patched copy
// would make the patch itself unexaminable — the oracle would be asserting against
// the very belief it is supposed to check. The published tree is what the fleet
// actually realized, so it is what "did this transition move more peers than it had
// to?" must be measured from; `promoted` supplies the missing justification.
//
// This is deliberately a separate function from Validate: Validate answers "is this
// tree legal?", which is a property of one tree; this answers "was this change
// minimal?", which is a property of a TRANSITION. Conflating them would make
// Validate need a prev it does not otherwise want, and would make the churn
// assertion silently skippable.
//
// KNOWN LIMIT, stated because an oracle that hides its blind spot is worse than one
// that names it: without the fleet's telemetry (which the signature deliberately
// does not take, so the assertion stays a pure function of the two trees) this
// cannot see a node whose CAPACITY SHRANK between builds. It therefore assumes
// capacities are unchanged, which holds for the churn it exists to check — joins,
// departures, and promotions. A transition driven by a telemetry change is not a
// local repair and must not be asserted with this.
func ValidateLocalRepair(prev, next *Topology, gone, joined, promoted []string) error {
	if prev == nil {
		return fmt.Errorf("validate local repair: prev is nil; there is no transition to check (a first build has no minimality property)")
	}
	if next == nil {
		return fmt.Errorf("validate local repair: next is nil")
	}
	goneSet, joinedSet, promotedSet := setOf(gone), setOf(joined), setOf(promoted)
	prevNodes, nextNodes := nodeSetOf(prev), nodeSetOf(next)

	for _, g := range gone {
		if nextNodes[g] {
			return fmt.Errorf("validate local repair: departed node %q is still in the new tree", g)
		}
	}

	if next.Root != prev.Root {
		if prevNodes[prev.Root] && nextNodes[prev.Root] && !goneSet[prev.Root] {
			return fmt.Errorf("validate local repair: root changed from %q to %q although %q is still present; a re-root moves every peer and is not local repair",
				prev.Root, next.Root, prev.Root)
		}
		// A FORCED re-root (the old root departed) legitimately invalidates every
		// parent choice at once, so there is no minimality to assert below.
		return nil
	}

	// Orphans: everything the departed nodes took down with them.
	orphan := map[string]bool{}
	for _, g := range gone {
		for _, n := range prev.Subtree(g) {
			orphan[n] = true
		}
	}

	prevChildren := map[string]int{}
	for _, e := range prev.Edges {
		prevChildren[e.Parent]++
	}

	for _, e := range prev.Edges { // slice order: a failure is reported reproducibly
		u, was := e.Child, e.Parent
		if !nextNodes[u] {
			continue // u left; its absence is not a re-parent
		}
		now := next.ParentOf(u)
		if now == was {
			continue
		}
		switch {
		case orphan[u]:
			// Its parent (or an ancestor) is gone; it had to move.
		case promotedSet[u]:
			// It moved itself onto its warm backup and the coordinator ratified it.
		case !nextNodes[was]:
			// Its old parent is no longer in the fleet.
		case next.Depth(was) > prev.Depth(was):
			// Its old parent sank deeper, so keeping u there may breach MaxDepth.
		case incumbentOccupants(next, was, u, prevNodes, joinedSet)+1 > prevChildren[was]:
			// Its old parent has no demonstrated room left: the slots it holds in
			// next are all taken by nodes that were already in the fleet, so u
			// could not have stayed without exceeding what that parent ever served.
		default:
			return fmt.Errorf("validate local repair: %q moved from %q to %q, but %q survived with room for it (gratuitous re-parent)",
				u, was, now, was)
		}
	}
	return nil
}

// incumbentOccupants counts the children parent holds in next that were already in
// the fleet before this transition — i.e. everything except newcomers. A newcomer
// occupying the slot is precisely the displacement the processing order forbids, so
// it must NOT count as a reason the incumbent could not have stayed.
func incumbentOccupants(next *Topology, parent, exclude string, prevNodes, joined map[string]bool) int {
	n := 0
	for _, c := range next.ChildrenOf(parent) {
		if c == exclude || joined[c] || !prevNodes[c] {
			continue
		}
		n++
	}
	return n
}

// setOf builds a membership set. Only ever used for lookups — never ranged, which
// would reintroduce Go's randomised map order into a decision.
func setOf(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return set
}

// nodeSetOf is every name a topology mentions, including a childless root.
func nodeSetOf(t *Topology) map[string]bool {
	set := setOf(t.Nodes())
	if t.Root != "" {
		set[t.Root] = true
	}
	return set
}
