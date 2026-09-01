package coordinator_test

// Phase 6 (§6.5-§6.7): epoch adoption, Yield, the rebuild window, rebuild-from-peers,
// and the coordinator's half of the ambiguity window.
//
// Everything here runs in virtual time and asserts through the same surfaces Phase 5
// uses — the Publisher event stream, the Sender, and Snapshot — plus overlay.Fence,
// which is the peer-side predicate the safety argument actually rests on. Using the
// real Fence rather than restating its rules is deliberate: a hand-rolled copy of the
// acceptance test in a test would prove only that the test agrees with itself.

import (
	"strings"
	"testing"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// realizedBeat is one peer's heartbeat carrying the topology it has ACTUALLY realized.
// It is the only frame that carries realized edges, and therefore the only input
// rebuild-from-peers can be built on.
func realizedBeat(name, parent string, children ...string) metrics.Heartbeat {
	hb := metrics.Heartbeat{Name: name, Seq: 1, Parent: parent}
	if parent != "" {
		hb.ParentState = "connected"
	}
	for _, ch := range children {
		hb.Children = append(hb.Children, metrics.ChildLink{Name: ch, State: "connected"})
	}
	hb.Normalize()
	return hb
}

// fiveNodeMeet is the Phase 6 fixture: two relay-capable peers and three leaves, which
// is the smallest fleet where a reconstructed baseline and a from-scratch build can
// disagree — and disagreeing is the only thing that makes the reconstruction testable.
func fiveNodeMeet(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "x", 8000)
	h.member("room", "p3", "b", 0)
	h.member("room", "p4", "c", 0)
	h.member("room", "p5", "d", 0)
	if topo := h.published("room"); topo == nil || topo.Root != "a" {
		t.Fatalf("fixture broken: want a tree rooted at a, got %+v", topo)
	}
	return h
}

// ---------------------------------------------------------------- Yield

// TestYieldStopsPublishing is Yield's whole contract: after it, this node is no longer
// the coordinator for that meet and must publish nothing further — not a tree, not a
// settling verdict, not an unbuildable one — however many threshold events arrive.
func TestYieldStopsPublishing(t *testing.T) {
	h := fiveNodeMeet(t)
	before := h.published("room")
	pushes := h.fs.count()
	h.fp.reset()

	h.c.Yield("room", 2)
	h.sync()

	// A burst of threshold events, one of every urgency class.
	h.member("room", "p6", "e", 0) // a join: normally bypasses the cooldown
	h.c.PeerLeft("room", "p5")     // a leave
	h.sync()
	h.advance(coordinator.RecomputeCooldown + coordinator.JoinSettle)

	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("a yielded coordinator must publish no tree, got %d", n)
	}
	if n := h.fp.countOf(coordinator.EventSettling) + h.fp.countOf(coordinator.EventUnbuildable); n != 0 {
		t.Fatalf("a yielded coordinator must publish no build verdict at all, got %d", n)
	}
	if got := h.fs.count(); got != pushes {
		t.Fatalf("a yielded coordinator must push nothing, got %d new pushes", got-pushes)
	}
	if got := h.published("room"); got != nil {
		t.Fatalf("Yield must drop the tree it is no longer authoritative for, got rev %d (was %d)",
			got.Rev, before.Rev)
	}
}

// TestYieldIsIdempotent: the contract says so, and the wiring will call it more than
// once (an announcement naming someone else can be re-broadcast on every membership
// change, §6.2's re-announce row).
func TestYieldIsIdempotent(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.Yield("room", 2)
	h.c.Yield("room", 2)
	h.c.Yield("room", 3)
	h.sync()
	h.c.Yield("nosuchmeet", 9) // and an unknown meet must not panic
	h.sync()

	if snap := h.snapshot("room"); len(snap.Members) != 5 {
		t.Fatalf("Yield must not disturb membership, got %d members", len(snap.Members))
	}
}

// TestYieldKeepsObservationsAndDropsBeliefs is the principle the epoch boundary is cut
// along: a handover invalidates what this node DECIDED (the tree), never what it
// OBSERVED (telemetry and liveness, which came off the wire and are still true). Losing
// the observations would also make a re-election cost a full settle for no reason.
func TestYieldKeepsObservationsAndDropsBeliefs(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.Yield("room", 2)
	h.sync()

	snap := h.snapshot("room")
	if snap.Topo != nil {
		t.Fatalf("the tree is a belief and must be dropped, got %+v", snap.Topo)
	}
	for _, m := range snap.Members {
		if !m.Reported {
			t.Fatalf("member %q lost its telemetry across a yield", m.Name)
		}
		if m.Health != coordinator.HealthHealthy {
			t.Fatalf("member %q lost its liveness across a yield: %q", m.Name, m.Health)
		}
	}
	if snap.Members[0].Report.UploadKbps == 0 && snap.Members[0].Name == "a" {
		t.Fatal("member a's reported capacity was discarded")
	}
}

// TestYieldedCoordinatorStillTracksLiveness: it is not the coordinator, but it is still
// a process watching a meet, and the dashboard still reads its snapshot. Health events
// keep flowing; only the tree stops.
func TestYieldedCoordinatorStillTracksLiveness(t *testing.T) {
	h := newHarness(t, livenessConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	h.c.Yield("room", 2)
	h.sync()
	h.fp.reset() // the fixture built a tree before the yield; only what follows counts

	alive := map[string]string{"p1": "a", "p3": "c"}
	for i := uint64(1); i <= metrics.GoneBeats; i++ {
		h.advance(metrics.HeartbeatInterval)
		beatAll(h, "room", i, alive)
	}
	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthGone {
		t.Fatalf("the health FSM must keep running after a yield, got %q", got)
	}
	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("but no tree may be published, got %d", n)
	}
}

// TestSetEpochResumesAfterYield: a re-election at a higher epoch puts this node back in
// charge, and its first tree of the new term is Rev 1 (§5.7 rule 2).
func TestSetEpochResumesAfterYield(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.Yield("room", 2)
	h.sync()

	h.c.SetEpoch("room", 3)
	h.sync()
	// Rebuild-from-peers: nothing is published until the window closes.
	h.advance(metrics.RebuildWindow)

	topo := h.published("room")
	if topo == nil {
		t.Fatalf("a re-elected coordinator must publish again; events=%d", len(h.fp.all()))
	}
	if topo.Epoch != 3 || topo.Rev != 1 {
		t.Fatalf("the first tree of a new term is rev 1 under the new epoch, got epoch %d rev %d",
			topo.Epoch, topo.Rev)
	}
}

// TestStaleSetEpochAfterYieldDoesNotResume: authority only ever moves forward. An
// announcement this node already superseded must not put it back in charge, or two
// coordinators could hold overlapping terms — the one thing §6.4's single-writer
// argument exists to make impossible.
func TestStaleSetEpochAfterYieldDoesNotResume(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.Yield("room", 5)
	h.sync()
	h.fp.reset()

	h.c.SetEpoch("room", 4) // an announcement from before the one that demoted us
	h.sync()
	h.c.SetEpoch("room", 5) // and the very one that demoted us
	h.sync()
	h.advance(metrics.RebuildWindow + coordinator.JoinSettle)

	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("a stale or equal epoch must not resume coordination, got %d topology events", n)
	}
	if got := h.published("room"); got != nil {
		t.Fatalf("still yielded, so still no tree; got %+v", got)
	}
}

// ------------------------------------------------- the rebuild window

// TestRebuildWindowPublishesNothing: §6.6 step 2. A freshly promoted coordinator
// accumulates reports and publishes nothing, so it cannot push a tree computed over a
// fleet it has not heard from.
func TestRebuildWindowPublishesNothing(t *testing.T) {
	h := fiveNodeMeet(t)
	h.fp.reset()
	pushes := h.fs.count()

	h.c.SetEpoch("room", 2)
	h.sync()

	if got := h.published("room"); got != nil {
		t.Fatalf("adopting a new epoch must drop the previous term's tree, got %+v", got)
	}
	// Only some of the fleet has been heard from.
	h.beat("room", "p1", realizedBeat("a", "", "x", "c"))
	h.beat("room", "p2", realizedBeat("x", "a", "b", "d"))
	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("nothing may be published inside the rebuild window, got %d", n)
	}
	if got := h.fs.count(); got != pushes {
		t.Fatalf("and nothing may be pushed, got %d new pushes", got-pushes)
	}
	ev, ok := h.fp.lastOf(coordinator.EventSettling)
	if !ok || ev.Outcome != coordinator.OutcomeSettling {
		t.Fatalf("the window is a 'not ready yet' state and must say so, got %+v (ok=%v)", ev, ok)
	}
	if len(ev.Waiting) == 0 {
		t.Fatalf("the settling event must name who has not been heard from, got %v", ev.Waiting)
	}
}

// TestRebuildWindowExitsEarlyWhenAllHeard: §6.6 step 3's fast path. Once every member
// the roster says is present has beaten, there is nothing left to wait for — asserted
// with the clock STOPPED, so only the early exit can explain a tree.
func TestRebuildWindowExitsEarlyWhenAllHeard(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()

	h.beat("room", "p1", realizedBeat("a", "", "x", "c"))
	h.beat("room", "p2", realizedBeat("x", "a", "b", "d"))
	h.beat("room", "p3", realizedBeat("b", "x"))
	h.beat("room", "p4", realizedBeat("c", "a"))
	if h.published("room") != nil {
		t.Fatal("the window must hold while one member is still unheard")
	}
	h.beat("room", "p5", realizedBeat("d", "x"))

	topo := h.published("room")
	if topo == nil {
		t.Fatal("with every member heard from, the window must close with no time elapsed")
	}
	if topo.Epoch != 2 || topo.Rev != 1 {
		t.Fatalf("want epoch 2 rev 1, got epoch %d rev %d", topo.Epoch, topo.Rev)
	}
}

// TestRebuildWindowExitsOnDeadline: the bound the fast path lacks. One silent member
// must not be able to hold the meet dark forever.
func TestRebuildWindowExitsOnDeadline(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()
	h.beat("room", "p1", realizedBeat("a", "", "x", "c"))

	h.advance(metrics.RebuildWindow)

	topo := h.published("room")
	if topo == nil {
		t.Fatalf("the window must close on its deadline whatever the fleet said; events=%+v", h.fp.all())
	}
	if topo.Epoch != 2 || topo.Rev != 1 {
		t.Fatalf("want epoch 2 rev 1, got epoch %d rev %d", topo.Epoch, topo.Rev)
	}
}

// TestRebuildReconstructsTheStickinessBaseline is the heart of §6.6, and the reason
// rebuild-from-peers is not simply "start from nothing".
//
// The realized tree the peers report below is deliberately NOT the tree a from-scratch
// build produces for this fleet: b is realized under a, while a greedy build puts it
// under x (x has fewer children at the moment b is placed). So a coordinator that
// ignored the reconstruction would re-parent b — a stream interruption caused by a
// handover rather than by anything that happened to b.
func TestRebuildReconstructsTheStickinessBaseline(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()

	// Ground truth from the wire: a roots, x relays only d, and b sits under a.
	h.beat("room", "p1", realizedBeat("a", "", "b", "c", "x"))
	h.beat("room", "p2", realizedBeat("x", "a", "d"))
	h.beat("room", "p3", realizedBeat("b", "a"))
	h.beat("room", "p4", realizedBeat("c", "a"))
	h.beat("room", "p5", realizedBeat("d", "x"))

	topo := h.published("room")
	if topo == nil {
		t.Fatal("no tree published")
	}
	want := map[string]string{"a": "", "x": "a", "b": "a", "c": "a", "d": "x"}
	for name, parent := range want {
		if got := topo.ParentOf(name); got != parent {
			t.Fatalf("the realized topology must be preserved across the handover: %q's parent is %q, want %q (tree %+v)",
				name, got, parent, topo.Edges)
		}
	}
	if err := overlay.Validate(topo, nodesFor(h.snapshot("room"), nil), consFor(topo)); err != nil {
		t.Fatalf("the first tree of the new term fails Validate: %v", err)
	}
}

// TestRebuildIgnoresAnUnheardMember is the M5 correction, stated as a property: the
// baseline is reconstructed over the REDUCED set actually heard from, so a member whose
// heartbeat was lost is merely ABSENT from prev — a newcomer — and moves alone. v1
// validated against the full roster, so one dropped frame re-parented the whole meet.
func TestRebuildIgnoresAnUnheardMember(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()

	// d never beats. Everyone else reports the same realized tree as above.
	h.beat("room", "p1", realizedBeat("a", "", "b", "c", "x"))
	h.beat("room", "p2", realizedBeat("x", "a", "d"))
	h.beat("room", "p3", realizedBeat("b", "a"))
	h.beat("room", "p4", realizedBeat("c", "a"))
	h.advance(metrics.RebuildWindow)

	topo := h.published("room")
	if topo == nil {
		t.Fatal("no tree published")
	}
	for name, parent := range map[string]string{"x": "a", "b": "a", "c": "a"} {
		if got := topo.ParentOf(name); got != parent {
			t.Fatalf("one lost heartbeat must not move a member that WAS heard from: %q's parent is %q, want %q",
				name, got, parent)
		}
	}
	if topo.ParentOf("d") == "" {
		t.Fatalf("the unheard member must still be placed, as a newcomer: %+v", topo.Edges)
	}
	if err := overlay.Validate(topo, nodesFor(h.snapshot("room"), nil), consFor(topo)); err != nil {
		t.Fatalf("published tree fails Validate: %v", err)
	}
}

// TestRebuildFallsBackWhenTheReconstructionIsTorn: the residual case §6.6 admits is
// real — a mid-flight re-parent captured half-applied. The coordinator must notice that
// what the peers described is not a legal tree and drop to prev = nil rather than seed
// the builder with it.
//
// The two subtests are deliberately different in what they can prove, and the
// difference is worth stating rather than hiding:
//
//   - "a cycle" asserts the OUTCOME is still correct, but it does NOT discriminate the
//     Validate gate. Remove the gate and it still passes, because BuildTree treats prev
//     as a pure PREFERENCE and re-checks every hard constraint itself — so a baseline
//     naming edges it cannot honour simply loses, and the torn edges never reach the
//     published tree either way.
//   - "deeper than MaxDepth" is the discriminator. There the illegal baseline IS
//     honourable up to its last edge, so stickiness pins four incumbents, the fifth
//     node has nowhere legal left, the sticky attempt fails, and the meet takes the
//     relaxed retry — re-parenting nearly everyone because of one bad heartbeat. The
//     gate is what turns that into an ordinary build.
func TestRebuildFallsBackWhenTheReconstructionIsTorn(t *testing.T) {
	t.Run("a cycle", func(t *testing.T) {
		h := fiveNodeMeet(t)
		h.c.SetEpoch("room", 2)
		h.sync()

		// b and c each name the other as parent: neither has a path to a.
		h.beat("room", "p1", realizedBeat("a", "", "x"))
		h.beat("room", "p2", realizedBeat("x", "a"))
		h.beat("room", "p3", realizedBeat("b", "c"))
		h.beat("room", "p4", realizedBeat("c", "b"))
		h.beat("room", "p5", realizedBeat("d", "x"))

		topo := h.published("room")
		if topo == nil {
			t.Fatal("a torn reconstruction must still produce a tree, from scratch")
		}
		if topo.ParentOf("b") == "c" || topo.ParentOf("c") == "b" {
			t.Fatalf("the torn edges must not survive into the published tree: %+v", topo.Edges)
		}
		if err := overlay.Validate(topo, nodesFor(h.snapshot("room"), nil), consFor(topo)); err != nil {
			t.Fatalf("published tree fails Validate: %v", err)
		}
	})

	t.Run("deeper than MaxDepth", func(t *testing.T) {
		// a can serve 3, x exactly 1, and h is the only other relay-capable peer. The
		// realized state below puts c at depth 3, one past the bound.
		h := newHarness(t, baseConfig())
		h.member("room", "p1", "a", 6000)
		h.member("room", "p2", "x", 2000)
		h.member("room", "p3", "h", 7000)
		h.member("room", "p4", "b", 0)
		h.member("room", "p5", "d", 0)
		h.member("room", "p6", "c", 0)
		h.c.SetEpoch("room", 2)
		h.sync()
		h.fp.reset()

		h.beat("room", "p1", realizedBeat("a", "", "b", "d", "x"))
		h.beat("room", "p2", realizedBeat("x", "a", "h"))
		h.beat("room", "p3", realizedBeat("h", "x", "c"))
		h.beat("room", "p4", realizedBeat("b", "a"))
		h.beat("room", "p5", realizedBeat("d", "a"))
		h.beat("room", "p6", realizedBeat("c", "h")) // depth 3, with MaxDepth 2

		ev, ok := h.fp.lastOf(coordinator.EventTopology)
		if !ok {
			t.Fatalf("no tree published; events=%+v", h.fp.all())
		}
		if ev.Outcome != coordinator.OutcomeBuilt {
			t.Fatalf("an illegal baseline must be REJECTED, not honoured: honouring it pins four "+
				"incumbents, strands the fifth, and forces the relaxed retry — outcome %q, reason %q",
				ev.Outcome, ev.Reason)
		}
		topo := h.published("room")
		if got := topo.Depth("c"); got < 0 || got > 2 {
			t.Fatalf("c must be placed within MaxDepth, got depth %d in %+v", got, topo.Edges)
		}
		if err := overlay.Validate(topo, nodesFor(h.snapshot("room"), nil), consFor(topo)); err != nil {
			t.Fatalf("published tree fails Validate: %v", err)
		}
	})
}

// TestBootstrapEpochDoesNotStallAnEmptyMeet: a bootstrap election arrives before anyone
// has joined, so "heard from every member the roster says is present" is vacuously
// true and the window closes at once. Waiting RebuildWindow there would cost the very
// first tree of every meet 3 seconds for nothing to rebuild from.
func TestBootstrapEpochDoesNotStallAnEmptyMeet(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.c.SetEpoch("fresh", 4)
	h.sync()

	h.member("fresh", "p1", "a", 8000)
	h.member("fresh", "p2", "b", 0)

	topo := h.published("fresh")
	if topo == nil {
		t.Fatalf("a bootstrap term must not wait for a fleet that did not exist yet; events=%+v", h.fp.all())
	}
	if topo.Epoch != 4 || topo.Rev != 1 {
		t.Fatalf("want epoch 4 rev 1, got epoch %d rev %d", topo.Epoch, topo.Rev)
	}
}

// ------------------------------------------------ the ambiguity window

// TestAmbiguityWindowStalePushIsNeitherSentNorAccepted drives BOTH halves of §6.7 with
// two real Coordinators over one meet, and a real overlay.Fence standing in for a peer.
//
// The interleaving is made deterministic rather than hoped for: the outgoing
// coordinator's sender is stopped INSIDE its first push (the fake signals entry), so at
// the moment Yield is called there is provably one push in flight and two queued behind
// it. That is exactly the residual window the contract reasons about.
//
// What must hold:
//   - the two QUEUED pushes are dropped — a term that has ended stops emitting;
//   - the ONE in-flight push is delivered, because it is already inside the network
//     call and no design can recall it — and §6.7 says that is safe;
//   - it is safe: a peer that has processed the E+1 announcement REJECTS it, and the
//     same peer accepts the new coordinator's E+1 tree.
func TestAmbiguityWindowStalePushIsNeitherSentNorAccepted(t *testing.T) {
	old := fiveNodeMeet(t)
	first := old.published("room")

	// A peer under the old term accepts the old coordinator's trees.
	var peer overlay.Fence
	if !peer.AdoptAnnouncement(1, "old-coord") {
		t.Fatal("peer failed to adopt the bootstrap announcement")
	}
	if ok, reason := peer.Accept("old-coord", first); !ok {
		t.Fatalf("precondition: the peer must accept the incumbent's tree, got %q", reason)
	}
	peer.Applied(first)

	// Wedge the outgoing coordinator's sender inside its next push.
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	old.fs.mu.Lock()
	old.fs.block, old.fs.entered = block, entered
	old.fs.mu.Unlock()

	old.c.PeerLeft("room", "p5") // a threshold event -> a new tree for the old term
	old.advanceNoSync(coordinator.RecomputeCooldown)
	<-entered // the sender is now provably inside push #1 of the stale tree

	old.c.Yield("room", 2)
	// Snapshot, not Sync, is the barrier here — and this is precisely why the two
	// differ. Snapshot rides only the Run goroutine, so it proves the yield has been
	// APPLIED; Sync would additionally ride the outbound queue, which is wedged behind
	// the very push this test is holding open, and would block until the release.
	_ = old.snapshot("room")
	close(block)
	old.sync()

	stale := old.fs.sinceRev(first.Rev)
	if len(stale) != 1 {
		t.Fatalf("exactly the one push already inside the network call may land; %d did", len(stale))
	}
	if stale[0].topo.Epoch != first.Epoch {
		t.Fatalf("the in-flight push should carry the OLD term, got epoch %d", stale[0].topo.Epoch)
	}

	// The successor publishes under E+1.
	next := newHarness(t, baseConfig())
	next.c.SetEpoch("room", 2)
	next.sync()
	next.c.SetRoster("room", []coordinator.Member{
		{ID: "p1", Name: "a"}, {ID: "p2", Name: "x"}, {ID: "p3", Name: "b"}, {ID: "p4", Name: "c"},
	})
	next.sync()
	for id, hb := range map[string]metrics.Heartbeat{
		"p1": realizedBeat("a", "", "c", "x"),
		"p2": realizedBeat("x", "a", "b"),
		"p3": realizedBeat("b", "x"),
		"p4": realizedBeat("c", "a"),
	} {
		next.c.Heartbeat("room", id, mustJSON(t, hb))
		next.c.Metrics("room", id, mustJSON(t, metrics.Report{Name: hb.Name, UploadKbps: uploadOf(hb.Name)}))
	}
	next.sync()
	fresh := next.published("room")
	if fresh == nil || fresh.Epoch != 2 || fresh.Rev != 1 {
		t.Fatalf("the successor must publish epoch 2 rev 1, got %+v", fresh)
	}

	// Now the peer processes the arbiter's E+1 announcement. From that instant the
	// stale push is refused — whatever its arrival order.
	if !peer.AdoptAnnouncement(2, "new-coord") {
		t.Fatal("peer failed to adopt the handover announcement")
	}
	if ok, reason := peer.Accept("old-coord", stale[0].topo); ok {
		t.Fatalf("a fenced peer must refuse the stale term's push, but accepted it (%q)", reason)
	}
	if ok, reason := peer.Accept("new-coord", fresh); !ok {
		t.Fatalf("the peer must accept the new coordinator's first tree, got %q", reason)
	}
}

// uploadOf mirrors fiveNodeMeet's capacities for the successor's telemetry.
func uploadOf(name string) int {
	if name == "a" || name == "x" {
		return 8000
	}
	return 0
}

// TestRebuildWithNoRealizedStateSaysSo is the case a coordinator must never diagnose as
// a healthy handover: every peer beat, but none of them named a parent, so there was
// nothing to reconstruct and the term's first tree is a full rebuild.
//
// It is a real wire state, not a hypothetical — a peer whose media layer cannot name its
// own edges beats with Parent empty — and it is indistinguishable from a healthy
// handover in the published TREE, because a from-scratch build is perfectly valid. The
// only place the difference can live is the reason attached to it. Without that, an
// operator watching every handover re-parent the whole meet has no way to tell missing
// telemetry from genuine churn.
func TestRebuildWithNoRealizedStateSaysSo(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()
	h.fp.reset()

	for id, name := range map[string]string{"p1": "a", "p2": "x", "p3": "b", "p4": "c", "p5": "d"} {
		h.c.Heartbeat("room", id, mustJSON(t, realizedBeat(name, "")))
	}
	h.sync()

	ev, ok := h.fp.lastOf(coordinator.EventTopology)
	if !ok {
		t.Fatalf("a tree must still be published; events=%+v", h.fp.all())
	}
	if ev.Reason != coordinator.ReasonNoRealizedState {
		t.Fatalf("a handover with no realized state to rebuild from must SAY so, got Reason=%q", ev.Reason)
	}
	if h.published("room") == nil {
		t.Fatal("the meet must still get a tree — degraded, not broken")
	}
}

// TestRebuildWithTornRealizedStateSaysSo: the other degradation, and it must be
// distinguishable from the one above. Same visible symptom (everyone re-parents),
// different cause (churn, not missing telemetry), different fix.
func TestRebuildWithTornRealizedStateSaysSo(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()
	h.fp.reset()

	h.beat("room", "p1", realizedBeat("a", "", "x"))
	h.beat("room", "p2", realizedBeat("x", "a"))
	h.beat("room", "p3", realizedBeat("b", "c"))
	h.beat("room", "p4", realizedBeat("c", "b"))
	h.beat("room", "p5", realizedBeat("d", "x"))

	ev, ok := h.fp.lastOf(coordinator.EventTopology)
	if !ok {
		t.Fatalf("a tree must still be published; events=%+v", h.fp.all())
	}
	if !strings.HasPrefix(ev.Reason, coordinator.ReasonRealizedStateInvalid) {
		t.Fatalf("a torn reconstruction must be reported as such, got Reason=%q", ev.Reason)
	}
	if ev.Reason == coordinator.ReasonNoRealizedState {
		t.Fatal("churn and missing telemetry must not collapse into one reason")
	}
}

// TestHealthyHandoverCarriesNoDegradationReason is the other half of the pair: a
// handover that actually reconstructed must say NOTHING, or the signal is noise. A
// reason that fires on every healthy handover teaches operators to ignore reasons.
func TestHealthyHandoverCarriesNoDegradationReason(t *testing.T) {
	h := fiveNodeMeet(t)
	h.c.SetEpoch("room", 2)
	h.sync()
	h.fp.reset()

	h.beat("room", "p1", realizedBeat("a", "", "b", "c", "x"))
	h.beat("room", "p2", realizedBeat("x", "a", "d"))
	h.beat("room", "p3", realizedBeat("b", "a"))
	h.beat("room", "p4", realizedBeat("c", "a"))
	h.beat("room", "p5", realizedBeat("d", "x"))

	ev, _ := h.fp.lastOf(coordinator.EventTopology)
	if ev.Reason != "" {
		t.Fatalf("a successful rebuild-from-peers must carry no degradation reason, got %q", ev.Reason)
	}
}

// TestBootstrapCarriesNoDegradationReason: a term over a meet nobody has joined yet has
// nothing to rebuild from and nothing is wrong with that. Reporting degraded telemetry
// there would fire on the very first tree of every meet.
func TestBootstrapCarriesNoDegradationReason(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.c.SetEpoch("fresh", 4)
	h.sync()
	h.member("fresh", "p1", "a", 8000)
	h.member("fresh", "p2", "b", 0)

	ev, ok := h.fp.lastOf(coordinator.EventTopology)
	if !ok {
		t.Fatal("no tree published")
	}
	if ev.Reason != "" {
		t.Fatalf("a bootstrap term has no realized state BY CONSTRUCTION and must not be flagged, got %q", ev.Reason)
	}
}
