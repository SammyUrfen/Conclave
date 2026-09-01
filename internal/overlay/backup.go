package overlay

// assignBackups computes the warm secondary parents for a finished tree, as a
// second pass over Edges in order. order is the build's attach order, which gives
// candidate iteration a deterministic sequence that never depends on Go's
// randomised map iteration.
//
// THE INVARIANT (this is the whole point): for a node u with primary parent P, its
// backup B must satisfy B ∉ Subtree(P) — outside the subtree rooted at u's OWN
// PARENT, not merely outside u's own subtree.
//
// Why the stronger rule. The failure a backup insures against is "P is gone", and
// that single event orphans everything in Subtree(P), not just u. A backup that is
// a DESCENDANT of u would make a cycle on failover (u attaches to B while B's path
// to the root runs through u) and sever both. A backup that is a SIBLING of u
// passes the weaker "not in my own subtree" rule but is orphaned by the very same
// event, leaving two orphans hanging off each other. Since
// Subtree(P) ⊇ Subtree(u) ∪ siblings(u), the stated invariant rules out both.
//
// Consequence, and it is deliberate: if P is the root then Subtree(P) is the entire
// tree and no valid B exists, so THE ROOT'S DIRECT CHILDREN HAVE NO BACKUP. That is
// correct rather than a gap — losing the root is a whole-subnet event that re-roots
// the tree, and losing the coordinator is an election; both are handled by a full
// rebuild, not by a per-node warm standby.
//
// Selection among valid candidates, in this exact order:
//
//	rank 0  hard: B ∉ Subtree(P); B can parent at all; B's committed load leaves
//	        room within BackupOvershootAllowance; and promoting u under B keeps u's
//	        WHOLE SUBTREE within MaxDepth — u's children sink with it, so checking
//	        only B's own depth would let a promotion push a grandchild out of bounds;
//	rank 1  B has spare primary capacity — a failover into spare capacity is
//	        non-disruptive;
//	rank 2  B is no deeper than the parent it replaces — keeps u's subtree from
//	        sinking on failover;
//	rank 3  minimum RTT from u — the failover target should also be a good parent;
//	rank 4  fewest committed children, then name ascending — balance, then
//	        determinism.
func assignBackups(t *Topology, byName map[string]Node, c Constraints, order []string) []Backup {
	if len(t.Edges) == 0 {
		return nil
	}

	// depth and height, both computed off the topological edge order rather than by
	// repeated walks: parents precede children, so one forward pass fixes depth and
	// one backward pass fixes the height of each node's subtree.
	depth := map[string]int{t.Root: 0}
	committed := map[string]int{}
	for _, e := range t.Edges {
		depth[e.Child] = depth[e.Parent] + 1
		committed[e.Parent]++
	}
	height := map[string]int{}
	for i := len(t.Edges) - 1; i >= 0; i-- {
		e := t.Edges[i]
		if h := height[e.Child] + 1; h > height[e.Parent] {
			height[e.Parent] = h
		}
	}

	subtreeOf := map[string]map[string]bool{}
	inSubtree := func(parent string) map[string]bool {
		set, ok := subtreeOf[parent]
		if !ok {
			set = make(map[string]bool)
			for _, n := range t.Subtree(parent) {
				set[n] = true
			}
			subtreeOf[parent] = set
		}
		return set
	}

	var out []Backup
	for _, e := range t.Edges {
		u, p := e.Child, e.Parent
		if p == t.Root {
			continue // no legal backup exists; see the doc comment
		}
		excluded := inSubtree(p)
		var best backupCandidate
		for _, name := range order { // attach order: deterministic, never a map range
			if excluded[name] {
				continue // covers u itself, P, u's siblings and every descendant
			}
			capacity := capacityOf(byName[name], c)
			if capacity == 0 {
				continue // TURN-bound or too little upload to parent anyone
			}
			if committed[name] >= capacity+BackupOvershootAllowance {
				continue // already promised as much as the overshoot bound allows
			}
			if depth[name]+1+height[u] > c.MaxDepth {
				continue // promoting u here would sink its subtree past the bound
			}
			cand := backupCandidate{
				name:     name,
				spare:    committed[name] < capacity,
				noDeeper: depth[name] <= depth[p],
				rtt:      rttTo(byName[u], name),
				load:     committed[name],
			}
			if best.name == "" || cand.better(best) {
				best = cand
			}
		}
		if best.name != "" {
			out = append(out, Backup{Node: u, Parent: best.name})
			// Spend the budget: the next node's search sees this promise, which is
			// what bounds fan-in. Without it, every child of a dying relay could
			// name the same backup and promote at once.
			committed[best.name]++
		}
	}
	return out
}

// backupCandidate is one scored option, so the four preference ranks read as one
// lexicographic comparison instead of a nested chain of ifs.
type backupCandidate struct {
	name     string
	spare    bool
	noDeeper bool
	rtt      float64
	load     int
}

// better reports whether a outranks b under the ordering documented on
// assignBackups. It is a total order: the name tiebreak means no two distinct
// candidates ever compare equal.
func (a backupCandidate) better(b backupCandidate) bool {
	if a.spare != b.spare {
		return a.spare
	}
	if a.noDeeper != b.noDeeper {
		return a.noDeeper
	}
	if a.rtt != b.rtt {
		return a.rtt < b.rtt
	}
	if a.load != b.load {
		return a.load < b.load
	}
	return a.name < b.name
}

// backupHeight is the number of hops from name down to the deepest node in its
// subtree (0 for a leaf). Exposed as a helper for Validate, which has no build
// state to reuse and must recompute it from the tree alone.
func backupHeight(t *Topology, name string) int {
	sub := t.Subtree(name)
	if len(sub) == 0 {
		return 0
	}
	base := t.Depth(name)
	h := 0
	for _, n := range sub {
		if d := t.Depth(n) - base; d > h {
			h = d
		}
	}
	return h
}
