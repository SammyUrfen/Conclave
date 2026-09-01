package coordinator_test

// A confirmation that structurally cannot arrive, second variant.
//
// §5.6 step 4 ratifies a re-parent on an ARRIVING TRACK. That is the right evidence for
// a peer that RECEIVES, and the reason it is right is load-bearing: without it a peer
// that merely reaches its backup would be ratified, and stickiness would then defend an
// edge carrying nothing.
//
// The rule's premise is that every peer has a downstream to observe. A PURE SOURCE does
// not: its upstream leg exists to send, no track will ever arrive on it, and the
// evidence the rule asks for is not merely unmet but undefined. Measured live: such a
// peer tore down a leg it was successfully sending on, three times, and was abandoned —
// root relay forwarding 209 kbit/s -> 0 for 27 seconds.
//
// The coordinator cannot know a peer subscribes to nothing; only the peer knows that,
// which is why the rule itself is amended in `media` (see the WI-3 report). What the
// coordinator CAN do is hold better evidence than the peer has about itself: the
// PARENT's own heartbeat says whether that child is connected to it. That is
// third-party, direction-agnostic, and already on the wire.
//
// The clause that keeps the receiving case intact is that the corroborating parent's OWN
// upstream must be intact. An orphaned backup would also report its new child as
// connected — so corroboration alone would re-admit exactly the case §5.6 rejects.

import (
	"testing"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
)

// beatOrphanedRelay reports a relay whose own upstream has failed but whose children are
// still attached to it — the correlated-failure shape §5.6 step 4 exists to reject.
func beatOrphanedRelay(name, parent string, children ...string) metrics.Heartbeat {
	hb := metrics.Heartbeat{Name: name, Seq: 1, Parent: parent, ParentState: "failed"}
	for _, ch := range children {
		hb.Children = append(hb.Children, metrics.ChildLink{Name: ch, State: "connected"})
	}
	hb.Normalize()
	return hb
}

// TestPureSourceIsNotAbandonedWhenItsParentConfirmsTheEdge is the defect.
//
// b can never confirm its re-parent, so it reports failure forever. Its parent x reports
// b as a connected child and reports its OWN upstream to a as connected, so the path is
// intact end to end and the coordinator has direct evidence of it. Spending repairs on
// that edge — and then abandoning the peer — destroys a working stream to satisfy an
// observation the peer is structurally unable to make.
func TestPureSourceIsNotAbandonedWhenItsParentConfirmsTheEdge(t *testing.T) {
	h := twoRelayMeet(t)
	before := h.published("room")
	if before.ParentOf("b") != "x" {
		t.Fatalf("fixture: want b under x, got %+v", before.Edges)
	}
	h.fp.reset()

	const rounds = 12
	for i := 0; i < rounds; i++ {
		// The parent's 1 Hz beat: b is connected to me, and I am connected upstream.
		h.beat("room", peerIDOf(h, "room", "x"), realizedBeat("x", "a", "b", "d"))
		strand(h, "room", "b")
	}

	if n := h.fp.countOf(coordinator.EventUnbuildable); n != 0 {
		ev, _ := h.fp.lastOf(coordinator.EventUnbuildable)
		t.Fatalf("a peer whose parent confirms the edge must never be abandoned; got %d "+
			"unbuildable event(s), reason %q", n, ev.Reason)
	}
	after := h.published("room")
	if after.ParentOf("b") != "x" {
		t.Fatalf("the working edge must be left alone; b's parent is now %q (tree %+v)",
			after.ParentOf("b"), after.Edges)
	}
	if after.Rev != before.Rev {
		t.Fatalf("nothing is broken, so nothing may be republished; rev %d -> %d", before.Rev, after.Rev)
	}
	// It is still reported — silence would be its own defect — but as a corroborated
	// edge rather than as a stranding.
	ev, ok := h.fp.lastOf(coordinator.EventReparent)
	if !ok {
		t.Fatal("the peer's report must still be visible on the event stream")
	}
	if ev.Reason != coordinator.ReasonEdgeCorroborated {
		t.Fatalf("want the corroborated-edge reason, got %q", ev.Reason)
	}
}

// TestOrphanedParentIsStillAStranding is the property that must NOT weaken, asserted
// against a fleet that differs from the one above in exactly one fact: the corroborating
// parent's own upstream.
//
// x still reports b as a connected child — a peer orphaned by a correlated failure
// always does — but x's own link to a has failed, so nothing reaches b through it. That
// is a real stranding and must still be repaired and, if repair cannot help, reported.
func TestOrphanedParentIsStillAStranding(t *testing.T) {
	h := twoRelayMeet(t)
	h.fp.reset()

	const rounds = 12
	for i := 0; i < rounds; i++ {
		h.beat("room", peerIDOf(h, "room", "x"), beatOrphanedRelay("x", "a", "b", "d"))
		strand(h, "room", "b")
	}

	evs := h.fp.of(coordinator.EventUnbuildable)
	if len(evs) != 1 {
		t.Fatalf("a child of an orphaned relay IS stranded: it must be repaired and then "+
			"reported exactly once; got %d unbuildable events (%v)", len(evs), kindsOf(h))
	}
	if evs[0].Reason != coordinator.ReasonUnratifiable {
		t.Fatalf("want the un-ratifiable reason, got %q", evs[0].Reason)
	}
}

// TestRootCorroboratesItsOwnChildren: the root has no upstream, and that is not a fault.
// A pure source parented directly to the root is the commonest shape of this defect, so
// the corroboration rule must treat "is the root" as an intact upstream rather than as a
// missing one.
func TestRootCorroboratesItsOwnChildren(t *testing.T) {
	h := twoRelayMeet(t)
	before := h.published("room")
	if before.ParentOf("c") != "a" || before.Root != "a" {
		t.Fatalf("fixture: want c under the root a, got %+v", before.Edges)
	}
	h.fp.reset()

	for i := 0; i < 12; i++ {
		// The root beats with no parent at all, which is correct and not orphaned.
		h.beat("room", peerIDOf(h, "room", "a"), realizedBeat("a", "", "c", "x"))
		strand(h, "room", "c")
	}

	if n := h.fp.countOf(coordinator.EventUnbuildable); n != 0 {
		t.Fatalf("the root's own children must be corroboratable; got %d unbuildable events", n)
	}
	if got := h.published("room").ParentOf("c"); got != "a" {
		t.Fatalf("the working edge must be left alone; c's parent is now %q", got)
	}
}

// TestCorroborationNeedsTheEdgeToBeConnected: a parent that lists a child in a
// non-connected state corroborates nothing. Listing a child is the parent's INTENT;
// only the state makes it evidence.
func TestCorroborationNeedsTheEdgeToBeConnected(t *testing.T) {
	h := twoRelayMeet(t)
	h.fp.reset()

	hb := metrics.Heartbeat{Name: "x", Seq: 1, Parent: "a", ParentState: "connected",
		Children: []metrics.ChildLink{{Name: "b", State: "failed"}, {Name: "d", State: "connected"}}}
	hb.Normalize()
	for i := 0; i < 12; i++ {
		h.beat("room", peerIDOf(h, "room", "x"), hb)
		strand(h, "room", "b")
	}

	if n := h.fp.countOf(coordinator.EventUnbuildable); n != 1 {
		t.Fatalf("a failed child edge is not corroboration; want the stranding reported once, got %d", n)
	}
}
