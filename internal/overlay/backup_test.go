package overlay

import "testing"

// backupFleet is the shared five-node fixture: R roots, P and Q are its children,
// u hangs off P and v off Q. Every backup case below is a variation of it.
func backupFleet() ([]Node, *Topology, Constraints) {
	nodes := []Node{
		{Name: "R", UploadKbps: 8000}, // cap 4, root
		{Name: "P", UploadKbps: 4000},
		{Name: "Q", UploadKbps: 4000},
		// u sits under P; Q is closer than R, so Q must be preferred as its backup.
		{Name: "u", RTT: map[string]float64{"P": 10, "R": 50, "Q": 5}},
		{Name: "v"},
	}
	prev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "P"},
		{Parent: "R", Child: "Q"},
		{Parent: "P", Child: "u"},
		{Parent: "Q", Child: "v"},
	}}
	cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}
	return nodes, prev, cons
}

// TestBackupInvariant covers §3.5: B ∉ Subtree(P) — stronger than "not in its own
// subtree", because a SIBLING is orphaned by the very failure the backup insures
// against.
func TestBackupInvariant(t *testing.T) {
	nodes, prev, cons := backupFleet()
	topo := mustBuild(t, nodes, prev, cons)
	if err := Validate(topo, nodes, cons); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	assigned := 0
	for _, b := range topo.Backups {
		assigned++
		p := topo.ParentOf(b.Node)
		for _, in := range topo.Subtree(p) {
			if b.Parent == in {
				t.Errorf("backup %q for %q lies inside Subtree(%q) — it dies with the same failure",
					b.Parent, b.Node, p)
			}
		}
	}
	if assigned == 0 {
		t.Fatal("no backups assigned at all; the assertion above is vacuous")
	}
	if got := topo.BackupOf("u"); got != "Q" {
		t.Errorf("BackupOf(u) = %q, want Q (outside Subtree(P), spare capacity, no deeper, closest)", got)
	}
}

// TestBackupRootChildrenHaveNone pins the deliberate gap in §3.5: Subtree(Root) is
// the whole tree, so a root child has no legal backup. Losing the root is a
// re-root, not a warm failover, and the UI says so rather than showing a blank.
func TestBackupRootChildrenHaveNone(t *testing.T) {
	nodes, prev, cons := backupFleet()
	topo := mustBuild(t, nodes, prev, cons)
	for _, child := range topo.ChildrenOf(topo.Root) {
		if b := topo.BackupOf(child); b != "" {
			t.Errorf("root child %q has backup %q; §3.5 says it must have none", child, b)
		}
	}
}

// TestBackupFanInIsBounded is the fix for the unbounded-fan-in defect: nothing in
// the original §3.5 stopped every child of a dying relay from naming the SAME
// backup, so the overshoot was |Subtree(P)|−1, not the one child §13.3 claims.
// Backups are now allocated against remaining budget, so a relay's committed load
// (primary children + backups pointing at it) never exceeds capacity by more than
// BackupOvershootAllowance.
func TestBackupFanInIsBounded(t *testing.T) {
	// R roots; P relays u1..u3; B is the only node outside Subtree(P), with room
	// for exactly one child.
	nodes := []Node{
		{Name: "R", UploadKbps: 8000}, // cap 4
		{Name: "P", UploadKbps: 8000}, // cap 4
		{Name: "B", UploadKbps: 2000}, // cap 1 — the tempting shared backup
		{Name: "u1", UploadKbps: 0}, {Name: "u2", UploadKbps: 0}, {Name: "u3", UploadKbps: 0},
	}
	prev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "P"},
		{Parent: "R", Child: "B"},
		{Parent: "P", Child: "u1"},
		{Parent: "P", Child: "u2"},
		{Parent: "P", Child: "u3"},
	}}
	cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}
	topo := mustBuild(t, nodes, prev, cons)

	if err := Validate(topo, nodes, cons); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	byName := map[string]Node{}
	for _, n := range nodes {
		byName[n.Name] = n
	}
	load := map[string]int{}
	for _, e := range topo.Edges {
		load[e.Parent]++
	}
	for _, b := range topo.Backups {
		load[b.Parent]++
	}
	for _, n := range nodes { // range the slice, never the map: determinism
		limit := capacityOf(byName[n.Name], cons) + BackupOvershootAllowance
		if load[n.Name] > limit {
			t.Errorf("node %q is committed to %d children (primary + backup), over its bound %d",
				n.Name, load[n.Name], limit)
		}
	}
}

// TestBackupRespectsPromotedSubtreeHeight covers the depth defect: Depth(B) <
// MaxDepth is not enough, because on promotion the whole subtree of the promoted
// node sinks with it. Here Q is the closest legal-looking backup but promoting u
// under it would push u's child w past MaxDepth.
func TestBackupRespectsPromotedSubtreeHeight(t *testing.T) {
	nodes := []Node{
		{Name: "R", UploadKbps: 8000},
		{Name: "P", UploadKbps: 4000},
		{Name: "S", UploadKbps: 4000},
		// Q is at depth 2 and is by far the closest — it must still be rejected.
		{Name: "Q", UploadKbps: 4000},
		{Name: "u", UploadKbps: 4000, RTT: map[string]float64{"P": 5, "R": 50, "S": 20, "Q": 1}},
		{Name: "w", UploadKbps: 0},
	}
	prev := &Topology{Epoch: 1, Rev: 1, Root: "R", Edges: []Edge{
		{Parent: "R", Child: "P"}, // depth 1
		{Parent: "R", Child: "S"}, // depth 1
		{Parent: "S", Child: "Q"}, // depth 2
		{Parent: "P", Child: "u"}, // depth 2
		{Parent: "u", Child: "w"}, // depth 3
	}}
	cons := Constraints{Root: "R", MaxDepth: 3, StreamKbps: 2000, Epoch: 1, Rev: 2, StickinessMs: DefaultStickinessMs}
	topo := mustBuild(t, nodes, prev, cons)

	if err := Validate(topo, nodes, cons); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if topo.Depth("w") != 3 {
		t.Fatalf("fixture drifted: w is at depth %d, want 3", topo.Depth("w"))
	}
	if got := topo.BackupOf("u"); got == "Q" {
		t.Errorf("BackupOf(u) = Q: promoting u under a depth-2 parent puts its child w at depth 4 > MaxDepth 3")
	}
	if got := topo.BackupOf("u"); got != "S" {
		t.Errorf("BackupOf(u) = %q, want S (the closest backup whose promotion keeps w within MaxDepth)", got)
	}
}
