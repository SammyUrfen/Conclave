package coordinator_test

// The repair loop must terminate.
//
// Found live, not here: a meet whose SOURCE is orphaned produces peers that can never
// confirm media arrived, so every ratification times out, the coordinator republishes,
// and the cycle repeats — rev churn, a `peer is stranded` warning per round, and a
// renegotiation storm underneath it. simnet cannot see this by construction: it is
// media-free, so the precondition ratification depends on is satisfied trivially there.
//
// From the coordinator's side the media-dependence is not actually needed to reproduce
// it. A stranded peer is just a `Reparented{OK:false}` frame, which a test can send as
// many times as it likes — so the honest place to cover a media-dependent rule in a
// media-free control plane is here, by MODELLING the confirmation as a signal that can
// be withheld rather than as something media implies.

import (
	"testing"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
)

// strand reports that name could not attach anywhere, under whatever tree is current.
func strand(h *harness, roomID, name string) {
	h.t.Helper()
	cur := h.published(roomID)
	var epoch, rev uint64
	if cur != nil {
		epoch, rev = cur.Epoch, cur.Rev
	}
	h.reparented(roomID, peerIDOf(h, roomID, name), metrics.Reparented{
		Name: name, From: cur.ParentOf(name), OK: false,
		Epoch: epoch, Rev: rev, Reason: "no media arrived from the new parent",
	})
}

// TestStrandedRepairLoopTerminates is the livelock itself, driven from the one input
// the coordinator actually sees.
//
// The peer below can never attach — because upstream of it nothing is flowing, which is
// a fact no amount of recomputing can change. The coordinator must notice that its
// answer is not helping and STOP, rather than spend a recompute, a republish and a
// renegotiation on every repetition forever.
func TestStrandedRepairLoopTerminates(t *testing.T) {
	h := twoRelayMeet(t)
	before := h.published("room")
	pushesBefore := h.fs.count()
	h.fp.reset()

	const rounds = 20
	for i := 0; i < rounds; i++ {
		strand(h, "room", "b")
	}

	after := h.published("room")
	if got := after.Rev - before.Rev; got > uint64(coordinator.MaxStrandedRepairs) {
		t.Fatalf("the repair loop republished %d times for %d strandings; it must stop after %d "+
			"unproductive attempts", got, rounds, coordinator.MaxStrandedRepairs)
	}
	if got := h.fs.count() - pushesBefore; got > coordinator.MaxStrandedRepairs*len(after.Nodes()) {
		t.Fatalf("the fan-out is unbounded: %d pushes for %d strandings", got, rounds)
	}

	evs := h.fp.of(coordinator.EventUnbuildable)
	if len(evs) != 1 {
		t.Fatalf("a repair loop that cannot make progress must SAY SO, exactly once; got %d: %v",
			len(evs), kindsOf(h))
	}
	if evs[0].Reason != coordinator.ReasonUnratifiable {
		t.Fatalf("want the un-ratifiable reason, got %q", evs[0].Reason)
	}
	if evs[0].Node != "b" {
		t.Fatalf("the event must name the peer that cannot be served, got %q", evs[0].Node)
	}
	// And it keeps serving everyone else: the tree is retained, not torn down.
	if h.published("room") == nil {
		t.Fatal("giving up on one peer must not discard the meet's tree")
	}
}

// TestIdenticalTreeIsNotRepublished is the other half, and the one that removes the
// churn at its source. A recompute that arrives at the tree the fleet is already running
// has nothing to tell anyone: bumping the revision would cost every peer a fence check
// and every changed edge a renegotiation, in exchange for a tree byte-identical to the
// one they hold.
func TestIdenticalTreeIsNotRepublished(t *testing.T) {
	h := twoRelayMeet(t)
	before := h.published("room")
	pushes := h.fs.count()
	h.fp.reset()

	// A stranding is urgent, so a recompute definitely runs — and with nothing changed
	// in the fleet it can only arrive back at the same tree.
	strand(h, "room", "b")

	after := h.published("room")
	if after.Rev != before.Rev {
		t.Fatalf("a structurally identical tree must not be republished; rev %d -> %d",
			before.Rev, after.Rev)
	}
	if got := h.fs.count(); got != pushes {
		t.Fatalf("nothing changed, so nothing may be pushed; got %d new pushes", got-pushes)
	}
	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("no topology event may be published for a tree nobody needs to hear about, got %d", n)
	}
}

// TestSuppressedPublishCatchesUpAMemberThatMissedTheTree guards the bug that
// suppression would otherwise introduce, which is worse than the churn it removes.
//
// A peer that reconnects arrives on a NEW socket with the SAME name. The tree over those
// names is unchanged, so the recompute is suppressed — and the returning peer would
// never be told anything, sitting dark forever in a meet the coordinator considers
// perfectly healthy. Suppressing a PUBLISH must not suppress a peer's copy of it.
func TestSuppressedPublishCatchesUpAMemberThatMissedTheTree(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	first := h.published("room")
	if first == nil {
		t.Fatal("precondition: no tree")
	}
	h.advance(coordinator.RecomputeCooldown) // clear the anti-thrash window

	// b's socket drops and it comes straight back on a new one, same name.
	h.c.PeerLeft("room", "p2")
	h.c.PeerJoined("room", "p2b", "b")
	h.c.Metrics("room", "p2b", mustJSON(t, metrics.Report{Name: "b"}))
	h.sync()

	after := h.published("room")
	if after.Rev != first.Rev {
		t.Fatalf("the tree over these names has not changed; rev %d -> %d", first.Rev, after.Rev)
	}
	got := h.fs.last("p2b")
	if got == nil {
		t.Fatal("the returning peer was never sent a topology: suppressing a PUBLISH must not " +
			"suppress a peer's copy of the tree everyone else already has")
	}
	if got.Rev != after.Rev {
		t.Fatalf("the returning peer must receive the CURRENT tree, got rev %d want %d", got.Rev, after.Rev)
	}
}

// TestStrandedBudgetResetsWhenThePeerAttaches: giving up is per-episode, not permanent.
// A peer that gets attached — reported either by a successful promotion or by a
// heartbeat naming a connected parent — starts again with a full budget, because the
// next failure is new evidence rather than a repetition of the old.
func TestStrandedBudgetResetsWhenThePeerAttaches(t *testing.T) {
	t.Run("a connected heartbeat", func(t *testing.T) {
		h := twoRelayMeet(t)
		h.fp.reset()
		for i := 0; i < coordinator.MaxStrandedRepairs-1; i++ {
			strand(h, "room", "b")
		}
		h.beat("room", peerIDOf(h, "room", "b"), metrics.Heartbeat{
			Name: "b", Seq: 1, Parent: "x", ParentState: "connected",
		})
		for i := 0; i < coordinator.MaxStrandedRepairs-1; i++ {
			strand(h, "room", "b")
		}
		if n := h.fp.countOf(coordinator.EventUnbuildable); n != 0 {
			t.Fatalf("attaching resets the budget, so %d+%d strandings around it must not exhaust it; got %d",
				coordinator.MaxStrandedRepairs-1, coordinator.MaxStrandedRepairs-1, n)
		}
	})

	t.Run("a successful promotion", func(t *testing.T) {
		h := twoRelayMeet(t)
		h.fp.reset()
		for i := 0; i < coordinator.MaxStrandedRepairs-1; i++ {
			strand(h, "room", "b")
		}
		cur := h.published("room")
		h.reparented("room", peerIDOf(h, "room", "b"), metrics.Reparented{
			Name: "b", From: "x", To: cur.BackupOf("b"), OK: true,
			Epoch: cur.Epoch, Rev: cur.Rev,
		})
		for i := 0; i < coordinator.MaxStrandedRepairs-1; i++ {
			strand(h, "room", "b")
		}
		if n := h.fp.countOf(coordinator.EventUnbuildable); n != 0 {
			t.Fatalf("a successful attach resets the budget; got %d unbuildable events", n)
		}
	})
}

// TestExhaustedStrandingNoLongerBypassesTheCooldown: once the coordinator has said it
// cannot help, further strandings are ordinary threshold events. The bypass exists so a
// peer receiving NOTHING is served immediately; it is not a licence to recompute without
// limit on a peer the coordinator has already failed to place.
func TestExhaustedStrandingNoLongerBypassesTheCooldown(t *testing.T) {
	h := twoRelayMeet(t)
	for i := 0; i < coordinator.MaxStrandedRepairs; i++ {
		strand(h, "room", "b")
	}
	h.fp.reset()

	// Change the fleet so a recompute WOULD have something new to publish, then strand
	// again inside the cooldown. Without the bypass the rebuild waits for the window.
	h.c.PeerLeft("room", peerIDOf(h, "room", "d"))
	h.sync()
	rev := h.published("room").Rev
	strand(h, "room", "b")

	if got := h.published("room").Rev; got != rev {
		t.Fatalf("an exhausted peer must not bypass the cooldown; rev %d -> %d", rev, got)
	}
	h.advance(coordinator.RecomputeCooldown)
	if got := h.published("room").Rev; got <= rev {
		t.Fatalf("the suppressed recompute must still run when the window closes; rev %d -> %d", rev, got)
	}
}
