package coordinator_test

// §5.5 / §5.6 / §5.6a: local subtree repair, the re-root case, and the
// backup-ratification rule (the three trees kept distinct).

import (
	"testing"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// twoRelayMeet builds the fleet every test in this file starts from:
//
//	a (8000, root) ── x (8000) ── b, d
//	                └─ c
//
// It is the smallest shape with a relay that is neither the root nor a leaf, which
// is what makes "repair moves only the orphans" a non-trivial claim.
func twoRelayMeet(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "x", 8000)
	h.member("room", "p3", "b", 0)
	h.member("room", "p4", "c", 0)
	h.member("room", "p5", "d", 0)
	topo := h.published("room")
	if topo == nil || topo.Root != "a" || !topo.IsRelay("x") {
		t.Fatalf("fixture broken: want a rooted at a with x relaying, got %+v", topo)
	}
	return h
}

// consFor rebuilds the Constraints the coordinator used, so the independent oracles
// can be run over its output. Root/Epoch/Rev come from the tree itself.
func consFor(topo *overlay.Topology) overlay.Constraints {
	return overlay.Constraints{
		Root: topo.Root, MaxDepth: 2, StreamKbps: 2000,
		Epoch: topo.Epoch, Rev: topo.Rev, StickinessMs: overlay.DefaultStickinessMs,
	}
}

// TestLocalRepairMovesOnlyTheOrphans asserts §5.5 through the independent oracle:
// after a mid-tree relay departs, ValidateLocalRepair must accept the transition
// against the LAST PUBLISHED tree (§5.6a / §15.7), and nobody outside the departed
// node's subtree may have moved.
func TestLocalRepairMovesOnlyTheOrphans(t *testing.T) {
	h := twoRelayMeet(t)
	before := h.published("room")
	orphansWanted := before.ChildrenOf("x")
	if len(orphansWanted) == 0 {
		t.Fatalf("fixture broken: x must have children, got %+v", before)
	}

	h.advance(coordinator.RecomputeCooldown) // a leave is not urgent
	h.c.PeerLeft("room", "p2")
	h.sync()

	after := h.published("room")
	if after.Rev <= before.Rev {
		t.Fatalf("a departure must rebuild; rev %d -> %d", before.Rev, after.Rev)
	}
	nodes := nodesFor(h.snapshot("room"), nil)
	cons := consFor(after)
	if err := overlay.Validate(after, nodes, cons); err != nil {
		t.Fatalf("published tree fails its own oracle: %v", err)
	}
	if err := overlay.ValidateLocalRepair(before, after, nodes, cons,
		overlay.Churn{Gone: []string{"x"}}); err != nil {
		t.Fatalf("repair was not local: %v", err)
	}

	// The concrete bound from §12.3: only x's former children changed parent.
	moved := changedParents(before, after)
	for _, name := range moved {
		if !contains(orphansWanted, name) {
			t.Fatalf("node %q moved but was not orphaned by x (orphans=%v, moved=%v)", name, orphansWanted, moved)
		}
	}

	fo, ok := h.fp.lastOf(coordinator.EventFailover)
	if !ok || fo.Node != "x" || fo.Reroot {
		t.Fatalf("want a non-reroot failover event for x, got %+v (ok=%v)", fo, ok)
	}
	if len(fo.Orphans) != len(orphansWanted) {
		t.Fatalf("failover must name the subtree that had to move; want %v got %v", orphansWanted, fo.Orphans)
	}
	for i := 1; i < len(fo.Orphans); i++ {
		if fo.Orphans[i-1] > fo.Orphans[i] {
			t.Fatalf("Orphans must be ordered for a replayable dashboard, got %v", fo.Orphans)
		}
	}
}

// TestRootLossIsFlaggedAsAReroot: losing the root is the one legitimately GLOBAL
// repair, and §12.3 asks only that the coordinator SAY SO rather than pretend it
// was local.
func TestRootLossIsFlaggedAsAReroot(t *testing.T) {
	h := twoRelayMeet(t)
	before := h.published("room")

	h.advance(coordinator.RecomputeCooldown)
	h.c.PeerLeft("room", "p1") // a, the root
	h.sync()

	fo, ok := h.fp.lastOf(coordinator.EventFailover)
	if !ok || fo.Node != "a" {
		t.Fatalf("want a failover event for the departed root, got %+v (ok=%v)", fo, ok)
	}
	if !fo.Reroot {
		t.Fatal("losing the root must be flagged Reroot: a design that cannot always be local must at least be legible")
	}
	after := h.published("room")
	if after.Root == before.Root {
		t.Fatalf("the root must change, got %q", after.Root)
	}
	if err := overlay.Validate(after, nodesFor(h.snapshot("room"), nil), consFor(after)); err != nil {
		t.Fatalf("re-rooted tree fails Validate: %v", err)
	}
}

// TestRatificationPatchesTheWorkingCopy is §5.6's frozen rule and §5.6a's whole
// point. The coordinator must build from `working` — published patched with the
// promotion the peer already performed — so stickiness DEFENDS the peer's choice.
// Building from `published` instead would see the pre-failure parent as the
// incumbent and move the peer a second time.
func TestRatificationPatchesTheWorkingCopy(t *testing.T) {
	h := twoRelayMeet(t)
	before := h.published("room")
	// The peer promotes the backup it was ASSIGNED, which is what makes the strict
	// form of ValidateLocalRepair's justification 2 checkable below.
	promoted := before.ChildrenOf("x")[0]
	backup := before.BackupOf(promoted)
	if backup == "" || backup == "x" {
		t.Fatalf("fixture broken: %q needs a backup other than its parent, got %q", promoted, backup)
	}

	h.reparented("room", peerIDOf(h, "room", promoted), metrics.Reparented{
		Name: promoted, From: "x", To: backup, OK: true,
		Epoch: before.Epoch, Rev: before.Rev,
	})
	ev, ok := h.fp.lastOf(coordinator.EventReparent)
	if !ok || ev.Node != promoted || ev.Parent != backup || ev.PrevParent != "x" {
		t.Fatalf("want a reparent event %s: x -> %s, got %+v (ok=%v)", promoted, backup, ev, ok)
	}

	// A successful promotion is a threshold event but NOT urgent, so it is
	// coalesced into the cooldown window.
	h.advance(coordinator.RecomputeCooldown)
	after := h.published("room")
	if after.Rev <= before.Rev {
		t.Fatalf("the ratifying rebuild did not run; rev %d -> %d", before.Rev, after.Rev)
	}
	if got := after.ParentOf(promoted); got != backup {
		t.Fatalf("the coordinator must RATIFY the peer's own choice, not fight it: %q's parent is %q, want %q", promoted, got, backup)
	}
	if err := overlay.ValidateLocalRepair(before, after, nodesFor(h.snapshot("room"), nil), consFor(after),
		overlay.Churn{Promoted: []string{promoted}}); err != nil {
		t.Fatalf("the ratified transition is not minimal against the PUBLISHED tree: %v", err)
	}
}

// TestStrandedPeerBypassesTheCooldown: an OK:false reparent means a peer is
// receiving nothing, and the anti-thrash cooldown is the wrong trade there. This is
// one of the only two frozen bypasses.
func TestStrandedPeerBypassesTheCooldown(t *testing.T) {
	h := twoRelayMeet(t)

	// A departure first, deep inside the cooldown, so a rebuild is PENDING and
	// suppressed. Without this the stranded recompute would arrive back at the tree
	// the fleet already holds, which is suppressed for a different reason — and a test
	// that cannot tell "deferred by the cooldown" from "nothing to say" would pass
	// against a coordinator that had lost the bypass entirely.
	h.c.PeerLeft("room", peerIDOf(h, "room", "c"))
	h.sync()
	before := h.published("room")
	if before.Depth("c") < 0 {
		t.Fatal("precondition: the leave must have been coalesced by the cooldown")
	}
	stranded := before.ChildrenOf("x")[0]

	// Still no clock advance: we are deep inside RecomputeCooldown.
	h.reparented("room", peerIDOf(h, "room", stranded), metrics.Reparented{
		Name: stranded, From: "x", OK: false,
		Epoch: before.Epoch, Rev: before.Rev, Reason: "no backup",
	})

	after := h.published("room")
	if after.Rev != before.Rev+1 {
		t.Fatalf("a stranded peer must trigger an urgent rebuild inside the cooldown; rev %d -> %d", before.Rev, after.Rev)
	}
	if after.Depth("c") >= 0 {
		t.Fatalf("the urgent rebuild must be the one the cooldown was holding: %+v", after.Edges)
	}
	ev, ok := h.fp.lastOf(coordinator.EventReparent)
	if !ok || ev.Parent != "" || ev.Node != stranded {
		t.Fatalf("want a stranded reparent event for %q, got %+v (ok=%v)", stranded, ev, ok)
	}
}

// TestStaleReparentIsNotRatified: a promotion reported against a tree the
// coordinator has already replaced tells it nothing about the CURRENT tree, so it
// must not be patched into the working copy (§5.2's reason for carrying epoch/rev).
func TestStaleReparentIsNotRatified(t *testing.T) {
	h := twoRelayMeet(t)
	before := h.published("room")
	promoted := before.ChildrenOf("x")[0]

	h.reparented("room", peerIDOf(h, "room", promoted), metrics.Reparented{
		Name: promoted, From: "x", To: before.BackupOf(promoted), OK: true,
		Epoch: before.Epoch, Rev: before.Rev - 1, // one tree behind
	})
	h.advance(coordinator.RecomputeCooldown)

	after := h.published("room")
	if got := after.ParentOf(promoted); got != "x" {
		t.Fatalf("a stale promotion must not be ratified; %q's parent is %q, want the incumbent x", promoted, got)
	}
}

// changedParents lists every node present in both trees whose parent differs,
// ascending — the delta §12.3 bounds.
func changedParents(prev, next *overlay.Topology) []string {
	var out []string
	for _, n := range next.Nodes() {
		if prev.Depth(n) < 0 {
			continue
		}
		if prev.ParentOf(n) != next.ParentOf(n) {
			out = append(out, n)
		}
	}
	return out
}

func contains(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// peerIDOf resolves a member name back to its server-stamped id through the
// snapshot surface, because the Observer methods are keyed by id.
func peerIDOf(h *harness, roomID, name string) string {
	h.t.Helper()
	for _, m := range h.snapshot(roomID).Members {
		if m.Name == name {
			return m.ID
		}
	}
	h.t.Fatalf("no member named %q in %q", name, roomID)
	return ""
}
