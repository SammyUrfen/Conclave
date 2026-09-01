package coordinator_test

// Guards a mutation audit found undefended: each of these deletes cleanly from the
// implementation without any other test noticing.
//
// They are grouped here because they share a shape — a check whose whole job is to stop
// something that does not happen in the happy path, and which therefore has to be driven
// into the state it defends against on purpose.

import (
	"strings"
	"testing"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestSnapshotServesThePublishedTreeNotTheWorkingCopy enforces §5.6a's three-tree split
// at the READ surface, where it was previously enforced nowhere.
//
// The two trees only diverge while a build has failed after the working copy was
// derived, so the fleet below is driven into exactly that state: the root departs, its
// edges are stripped from the working copy, and the survivors cannot be rooted at all —
// so nothing is published and the two trees disagree about every edge in the meet.
//
// The snapshot must serve `published`: what the peers were last INSTRUCTED to run. The
// working copy is the coordinator's private scratch, and serving it would report a tree
// that no peer has ever been told about — which for a dashboard whose whole job is
// answering "is this meeting working" is worse than reporting a stale one.
func TestSnapshotServesThePublishedTreeNotTheWorkingCopy(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000) // the only relay-capable member
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	good := h.published("room")
	if good == nil || len(good.Edges) != 2 {
		t.Fatalf("precondition: want a 2-edge tree, got %+v", good)
	}

	h.advance(coordinator.RecomputeCooldown) // a leave is not urgent
	h.c.PeerLeft("room", "p1")
	h.sync()

	if n := h.fp.countOf(coordinator.EventUnbuildable); n == 0 {
		t.Fatalf("precondition: losing the only relay must leave the fleet unrootable; events=%+v", h.fp.all())
	}
	snap := h.snapshot("room")
	if snap.Topo == nil {
		t.Fatal("the last published tree must be retained and served")
	}
	if len(snap.Topo.Edges) != len(good.Edges) {
		t.Fatalf("Snapshot must serve `published` (%d edges), not the working copy "+
			"(which has had the departed root's edges stripped); got %d: %+v",
			len(good.Edges), len(snap.Topo.Edges), snap.Topo.Edges)
	}
	if snap.Topo.ParentOf("b") != "a" || snap.Topo.ParentOf("c") != "a" {
		t.Fatalf("the served tree must be what peers were last told to run, got %+v", snap.Topo.Edges)
	}
}

// TestSnapshotReportsRealizedChildren: MemberSnapshot.Children is the REALIZED
// downstream edge set, from the peer's last heartbeat — the other half of §9.4b's
// realized-vs-intended pair, and the only way a dashboard can show that a peer has not
// actually connected what it was told to.
func TestSnapshotReportsRealizedChildren(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)

	h.beat("room", "p1", metrics.Heartbeat{Name: "a", Seq: 1, Children: []metrics.ChildLink{
		{Name: "c", State: "connected"}, {Name: "b", State: "connected"},
	}})
	h.beat("room", "p2", metrics.Heartbeat{Name: "b", Seq: 1, Parent: "a", ParentState: "connected"})

	byName := map[string]coordinator.MemberSnapshot{}
	for _, m := range h.snapshot("room").Members {
		byName[m.Name] = m
	}
	got := byName["a"].Children
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("a's realized children must be reported, ascending; got %v", got)
	}
	if byName["b"].Parent != "a" {
		t.Fatalf("b's realized parent must be reported; got %q", byName["b"].Parent)
	}
	if len(byName["c"].Children) != 0 {
		t.Fatalf("a peer that has not beaten reports no realized children; got %v", byName["c"].Children)
	}
}

// TestRebuildTreatsParentAsAuthorityAndIgnoresChildren pins a decision that is correct
// and was completely untested, so changing it would have gone unnoticed.
//
// A parent link and a child link are the SAME edge seen from two ends, so consulting
// both needs a conflict rule for evidence that adds nothing. Parent is single-valued and
// therefore cannot contradict itself; a child list can, and here x claims a child that
// names someone else as its parent. The reconstruction must follow the parent.
//
// The fixture is chosen so the three possible implementations disagree: `b` sits under
// `a` if parents win, under `x` if children win, and under `x` again if the baseline is
// ignored entirely and the builder runs memoryless.
func TestRebuildTreatsParentAsAuthorityAndIgnoresChildren(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()

	h.beat("room", "p1", realizedBeat("a", "", "b", "c", "x"))
	h.beat("room", "p2", realizedBeat("x", "a", "b", "d")) // x wrongly claims b
	h.beat("room", "p3", realizedBeat("b", "a"))           // b says otherwise, and b is right
	h.beat("room", "p4", realizedBeat("c", "a"))
	h.beat("room", "p5", realizedBeat("d", "x"))

	topo := h.published("room")
	if topo == nil {
		t.Fatalf("no tree published; events=%+v", h.fp.all())
	}
	if got := topo.ParentOf("b"); got != "a" {
		t.Fatalf("the reconstruction must follow the reported PARENT, not a contradicting "+
			"child list: b's parent is %q, want a (tree %+v)", got, topo.Edges)
	}
	if err := overlay.Validate(topo, nodesFor(h.snapshot("room"), nil), consFor(topo)); err != nil {
		t.Fatalf("published tree fails Validate: %v", err)
	}
}

// TestRebuildReportsAnUnheardRelayAsTorn covers the branch a lost heartbeat from a RELAY
// reaches, which the leaf case cannot.
//
// Losing a leaf's heartbeat is benign — it is simply absent from the baseline. Losing a
// RELAY's is not: every child that named it becomes parentless in the reduced set, so
// there are several root candidates and no reconstruction exists at all. That is a
// different fault with a different operator signal, and it had no coverage because the
// only test in the neighbourhood silenced a leaf.
func TestRebuildReportsAnUnheardRelayAsTorn(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()
	h.fp.reset()

	// x, the relay, never beats. Its children still name it.
	h.beat("room", "p1", realizedBeat("a", "", "c", "x"))
	h.beat("room", "p3", realizedBeat("b", "x"))
	h.beat("room", "p4", realizedBeat("c", "a"))
	h.beat("room", "p5", realizedBeat("d", "x"))
	h.advance(metrics.RebuildWindow)

	ev, ok := h.fp.lastOf(coordinator.EventTopology)
	if !ok {
		t.Fatalf("a tree must still be published; events=%+v", h.fp.all())
	}
	if !strings.HasPrefix(ev.Reason, coordinator.ReasonRealizedStateInvalid) {
		t.Fatalf("an unreconstructable realized state must be reported as such, got Reason=%q", ev.Reason)
	}
	// Asserting on this package's OWN message, to pin the branch rather than the text:
	// "no unique root" is the only thing that distinguishes it from a reconstruction
	// that was built and then failed the validator.
	if !strings.Contains(ev.Reason, "unique root") {
		t.Fatalf("the reason must name the unique-root failure, not the validator's; got %q", ev.Reason)
	}
	if err := overlay.Validate(h.published("room"), nodesFor(h.snapshot("room"), nil), consFor(h.published("room"))); err != nil {
		t.Fatalf("published tree fails Validate: %v", err)
	}
}

// TestDuplicateMemberNamesAreSkipped: the tree is keyed by NAME, so two peers claiming
// one name cannot both be placed. The builder rejects a duplicate outright, so without
// the skip a single name collision makes the whole meet unbuildable — one peer's choice
// of label taking the meeting down.
func TestDuplicateMemberNamesAreSkipped(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "a", 8000) // same name, different socket
	h.member("room", "p3", "b", 0)

	topo := h.published("room")
	if topo == nil {
		t.Fatalf("a name collision must not take the meet down; events=%+v", h.fp.all())
	}
	if n := len(topo.Nodes()); n != 2 {
		t.Fatalf("the collided name may appear once; tree has %d nodes: %+v", n, topo.Edges)
	}
	if topo.ParentOf("b") != "a" {
		t.Fatalf("the surviving members must still be placed, got %+v", topo.Edges)
	}
}

// TestTurnBoundPeersAreForcedToBeLeaves: NAT is a hard constraint on the ROLE a peer may
// play, and it has to survive the projection. A TURN-bound peer's path is already
// indirect, so it must never be a parent however much upload it advertises — and the
// peer below advertises more than anyone.
func TestTurnBoundPeersAreForcedToBeLeaves(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.c.PeerJoined("room", "p1", "fat")
	h.c.Metrics("room", "p1", mustJSON(t, metrics.Report{
		Name: "fat", UploadKbps: 100000, NAT: overlay.NATRelayed,
	}))
	h.member("room", "p2", "a", 4000)
	h.member("room", "p3", "b", 0)

	topo := h.published("room")
	if topo == nil {
		t.Fatalf("no tree published; events=%+v", h.fp.all())
	}
	if topo.Root == "fat" {
		t.Fatalf("a TURN-bound peer must never root the tree whatever its upload: %+v", topo)
	}
	if topo.IsRelay("fat") {
		t.Fatalf("a TURN-bound peer must never be a parent: %+v", topo.Edges)
	}
	if topo.Root != "a" {
		t.Fatalf("the reachable relay must root the tree, got %q", topo.Root)
	}
}
