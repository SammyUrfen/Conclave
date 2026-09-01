package overlay

import (
	"sort"
	"testing"
)

// deltaCons is the shared shape for the bounded-delta rows: a 6-child root, two
// mid-capacity relays, three leaves, depth 2.
func deltaCons(rev uint64) Constraints {
	return Constraints{Root: "R", MaxDepth: 2, StreamKbps: 2000, Epoch: 1, Rev: rev, StickinessMs: DefaultStickinessMs}
}

// deltaFleet is the base telemetry. Cases copy it and mutate one thing.
func deltaFleet() []Node {
	return []Node{
		{Name: "R", UploadKbps: 12000}, // cap 6
		{Name: "A", UploadKbps: 8000},  // cap 4
		{Name: "B", UploadKbps: 8000},  // cap 4
		{Name: "x"}, {Name: "y"}, {Name: "z"},
	}
}

// without returns the fleet minus name, preserving order.
func without(nodes []Node, name string) []Node {
	out := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n.Name != name {
			out = append(out, n)
		}
	}
	return out
}

// changedParents returns every node in next whose parent differs from its parent in
// prev, sorted. A node absent from prev counts as changed (its parent went from none to
// something) — that is what makes the "one join ⇒ exactly 1" row assert the joiner.
//
// This is the metric the §12.3(b) bounds are stated in, and it is the one a user
// actually feels: each changed parent is one stream interruption.
func changedParents(prev, next *Topology) []string {
	var out []string
	for _, n := range append([]string{next.Root}, next.Nodes()...) {
		if n == next.Root {
			continue // the root has no parent to change; a re-root is its own row
		}
		if next.ParentOf(n) != prev.ParentOf(n) {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return dedupe(out)
}

func dedupe(in []string) []string {
	out := in[:0]
	var last string
	for i, s := range in {
		if i == 0 || s != last {
			out = append(out, s)
		}
		last = s
	}
	return out
}

// TestBoundedEdgeDelta is the headline stability property (§12.3(b)). Path-independence
// is false by construction — stickiness IS history-dependence — so the claim worth
// asserting is not "the same fleet gives the same tree" but "the delta between
// consecutive trees is bounded by the churn that caused the rebuild". Each row pins one
// concrete numeric bound.
func TestBoundedEdgeDelta(t *testing.T) {
	// The shared starting tree, asserted so a fixture drift cannot silently loosen every
	// bound below.
	base := deltaFleet()
	prev := mustBuild(t, base, nil, deltaCons(1))
	if err := Validate(prev, base, deltaCons(1)); err != nil {
		t.Fatalf("base tree: %v", err)
	}
	wantShape := map[string]string{"A": "R", "B": "A", "x": "A", "y": "R", "z": "A"}
	for node, parent := range map[string]string{"A": "R", "B": "A", "x": "A", "y": "R", "z": "A"} {
		if got := prev.ParentOf(node); got != parent {
			t.Fatalf("fixture drifted: %s parent = %q, want %q (whole tree: %+v)", node, got, parent, prev.Edges)
		}
	}
	_ = wantShape

	t.Run("one join changes exactly 1", func(t *testing.T) {
		nodes := append(deltaFleet(), Node{Name: "w"})
		next := mustBuild(t, nodes, prev, deltaCons(2))
		if err := Validate(next, nodes, deltaCons(2)); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		got := changedParents(prev, next)
		if len(got) != 1 || got[0] != "w" {
			t.Errorf("changed parents = %v, want exactly [w] (the joiner)", got)
		}
		if err := ValidateLocalRepair(prev, next, nodes, deltaCons(2), Churn{Joined: []string{"w"}}); err != nil {
			t.Errorf("ValidateLocalRepair: %v", err)
		}
	})

	t.Run("one leaf departs changes 0", func(t *testing.T) {
		nodes := without(deltaFleet(), "z") // z is a leaf of A
		if prev.IsRelay("z") {
			t.Fatal("fixture: z must be a leaf for this row")
		}
		next := mustBuild(t, nodes, prev, deltaCons(2))
		if err := Validate(next, nodes, deltaCons(2)); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if got := changedParents(prev, next); len(got) != 0 {
			t.Errorf("changed parents = %v, want none: losing a leaf moves nobody", got)
		}
		if err := ValidateLocalRepair(prev, next, nodes, deltaCons(2), Churn{Gone: []string{"z"}}); err != nil {
			t.Errorf("ValidateLocalRepair: %v", err)
		}
	})

	t.Run("one relay departs changes at most its child count", func(t *testing.T) {
		bound := len(prev.ChildrenOf("A"))
		if bound == 0 {
			t.Fatal("fixture: A must be a relay for this row")
		}
		nodes := without(deltaFleet(), "A")
		next := mustBuild(t, nodes, prev, deltaCons(2))
		if err := Validate(next, nodes, deltaCons(2)); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		got := changedParents(prev, next)
		if len(got) > bound {
			t.Errorf("changed parents = %v (%d), want at most %d (A's former children)", got, len(got), bound)
		}
		if err := ValidateLocalRepair(prev, next, nodes, deltaCons(2), Churn{Gone: []string{"A"}}); err != nil {
			t.Errorf("ValidateLocalRepair: %v", err)
		}
	})

	t.Run("one self-promotion changes exactly 1", func(t *testing.T) {
		// The bound is stated against the PUBLISHED tree, because that is the tree the
		// oracle takes and the tree the peers were actually running: against it, a
		// promotion IS a real, visible, checkable parent change. (An earlier draft
		// stated 0, measured against the patched working copy — wrong in the direction
		// that makes a CORRECT implementation look like a bug.)
		promoted := "x"
		backup := prev.BackupOf(promoted)
		if backup == "" {
			t.Fatalf("fixture: %s must have a backup for this row (backups: %+v)", promoted, prev.Backups)
		}
		working, err := repatch(prev, promoted, backup)
		if err != nil {
			t.Fatal(err)
		}
		nodes := deltaFleet()
		next := mustBuild(t, nodes, working, deltaCons(2))
		if err := Validate(next, nodes, deltaCons(2)); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		got := changedParents(prev, next)
		if len(got) != 1 || got[0] != promoted {
			t.Errorf("changed parents vs the published tree = %v, want exactly [%s]", got, promoted)
		}
		// The strict form: a promoted node must land on the backup it was ASSIGNED, not
		// on whatever node it liked. The published tree is the only artifact that still
		// carries that assignment, which is precisely why the oracle takes it.
		if got := next.ParentOf(promoted); got != prev.BackupOf(promoted) {
			t.Errorf("%s promoted onto %q, want its assigned backup %q", promoted, got, prev.BackupOf(promoted))
		}
		// Secondary, and the reason three trees are named rather than two: against the
		// coordinator's patched WORKING copy the same promotion is invisible, because
		// the ratified edge is already in it. Same transition, two different questions.
		if got := changedParents(working, next); len(got) != 0 {
			t.Errorf("changed parents vs the working copy = %v, want none", got)
		}
		ch := Churn{Promoted: []string{promoted}}
		if err := ValidateLocalRepair(prev, next, nodes, deltaCons(2), ch); err != nil {
			t.Errorf("ValidateLocalRepair against the published tree: %v", err)
		}
	})

	t.Run("one impairment changes at most that node's child count", func(t *testing.T) {
		bound := len(prev.ChildrenOf("A"))
		nodes := deltaFleet()
		for i := range nodes {
			if nodes[i].Name == "A" {
				nodes[i].Impaired = true
			}
		}
		next := mustBuild(t, nodes, prev, deltaCons(2))
		if err := Validate(next, nodes, deltaCons(2)); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		got := changedParents(prev, next)
		if len(got) > bound {
			t.Errorf("changed parents = %v (%d), want at most %d (A's children lose incumbency, nobody else does)",
				got, len(got), bound)
		}
		if len(got) == 0 {
			t.Error("impairing a relay moved nobody; rank 1 is still protecting it and the dwell is inert")
		}
		if err := ValidateLocalRepair(prev, next, nodes, deltaCons(2), Churn{Impaired: []string{"A"}}); err != nil {
			t.Errorf("ValidateLocalRepair: %v", err)
		}
	})

	t.Run("an RTT improvement past the margin changes at most 1", func(t *testing.T) {
		// z sits under A at 100 ms and can reach R in 10 ms — a 90 ms win, far past
		// DefaultStickinessMs, so rank 1 steps aside for exactly this one node.
		nodes := deltaFleet()
		for i := range nodes {
			if nodes[i].Name == "z" {
				nodes[i].RTT = map[string]float64{"A": 100, "R": 10}
			}
		}
		next := mustBuild(t, nodes, prev, deltaCons(2))
		if err := Validate(next, nodes, deltaCons(2)); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		got := changedParents(prev, next)
		if len(got) != 1 || got[0] != "z" {
			t.Errorf("changed parents = %v, want exactly [z]", got)
		}
		if got := next.ParentOf("z"); got != "R" {
			t.Errorf("z parent = %q, want R (the materially closer relay)", got)
		}
		// This row is only assertable because the oracle can now see RTT: justification
		// 4 is exactly this move.
		if err := ValidateLocalRepair(prev, next, nodes, deltaCons(2), Churn{}); err != nil {
			t.Errorf("ValidateLocalRepair could not justify an RTT win past the margin: %v", err)
		}
	})

	t.Run("the root departing is unbounded but must re-root", func(t *testing.T) {
		nodes := without(deltaFleet(), "R")
		cons := deltaCons(2)
		cons.Root = PickRoot(nodes, prev, cons.StreamKbps)
		if cons.Root == "" || cons.Root == "R" {
			t.Fatalf("PickRoot = %q, want a surviving node", cons.Root)
		}
		next, err := BuildTree(nodes, prev, cons)
		if err != nil {
			t.Fatalf("BuildTree: %v", err)
		}
		if err := Validate(next, nodes, cons); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if next.Root == prev.Root {
			t.Fatal("the tree did not re-root after its root departed")
		}
		// No bound applies — but the oracle must SAY the transition was global rather
		// than wave it through. A design that cannot always be local must at minimum
		// always be legible about when it was not.
		err = ValidateLocalRepair(prev, next, nodes, cons, Churn{Gone: []string{"R"}})
		if err != nil {
			t.Errorf("a forced re-root is legitimate churn: %v", err)
		}
	})
}
