package coordinator_test

// §5.8's surface: Sync as a barrier, Snapshot, SetEpoch's per-meet scope,
// SetRoster, and the requirement that a stalled Sender cannot stall the loop.

import (
	"context"
	"testing"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// syncBarrierTrials is how many independent fire-then-sync races the barrier test
// runs. A loop that acks WITHOUT first draining its fired timers resolves the race
// by Go's uniform-random select, so it survives one trial about half the time and
// this many trials with probability ~2^-200. One trial would be a coin flip
// masquerading as a test; this is the number that makes the assertion mean
// something.
const syncBarrierTrials = 200

// TestSyncIsASoundQuiescenceBarrier is the property simnet.AddBarrier documents and
// that every deterministic test in this repo rests on: when Sync returns, a timer
// that has already FIRED must have been fully reacted to — including the pushes that
// reaction produced.
//
// The shape is deliberate on two counts. The cooldown deadline is armed and then the
// virtual clock is advanced past it WITHOUT settling, so the wake channel holds a
// fired value at the exact moment Sync is enqueued: both are ready at one parked
// select, the precise situation Go resolves at random. And the assertion is made on
// what the SENDER has received, not on what the coordinator believes — because Sync
// rides the event queue and then the outbound queue in order, a loop that acked
// without draining would put its barrier marker AHEAD of the pushes the fired
// deadline is about to produce, and the marker would come back first.
func TestSyncIsASoundQuiescenceBarrier(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)

	for i := 0; i < syncBarrierTrials; i++ {
		// A transient member: joining is urgent (an unplaced joiner) and builds now.
		h.member("room", "pt", "t", 0)
		armed := h.published("room").Rev

		// Leaving is NOT urgent, so it is suppressed by the cooldown and leaves a
		// dirty recompute armed at lastBuild+RecomputeCooldown.
		h.c.PeerLeft("room", "pt")
		h.sync()
		if got := h.published("room").Rev; got != armed {
			t.Fatalf("trial %d: the leave should have been suppressed; rev %d -> %d", i, armed, got)
		}

		// Fire the deadline, then race a Sync against the loop's reaction.
		h.clk.Advance(coordinator.RecomputeCooldown)
		ctx, cancel := context.WithTimeout(context.Background(), guard)
		err := h.c.Sync(ctx)
		cancel()
		if err != nil {
			t.Fatalf("trial %d: Sync: %v", i, err)
		}

		if got := h.published("room").Rev; got != armed+1 {
			t.Fatalf("trial %d: Sync returned before the fired deadline was reacted to; rev %d, want %d",
				i, got, armed+1)
		}
		if got := h.fs.last("p1"); got == nil || got.Rev != armed+1 {
			t.Fatalf("trial %d: Sync returned before the fired deadline's push reached the Sender; got %+v, want rev %d",
				i, got, armed+1)
		}
	}
}

// TestSenderStallCannotStallTheLoop: §5.8 freezes SendTopology as "must not block
// and must not do network I/O on the calling goroutine", but the loop must not
// DEPEND on every future adapter honouring that — one peer with a full TCP window
// would otherwise stall every meet hosted by the process, and, because Sync rides
// the same loop, would surface as an unreproducible test flake rather than as the
// availability bug it is. So sends are handed to a separate goroutine.
//
// Snapshot is the probe here rather than Sync, and the difference is the design:
// Snapshot round-trips the Run goroutine ONLY, so it proves the control loop is
// still making progress; Sync additionally round-trips the outbound queue, which is
// what makes it a barrier strong enough to assert on pushes.
func TestSenderStallCannotStallTheLoop(t *testing.T) {
	h := newHarness(t, baseConfig())
	block := make(chan struct{})
	defer close(block) // released before the harness's cleanup stops the loop

	h.fs.mu.Lock()
	h.fs.block = block
	h.fs.mu.Unlock()

	h.c.PeerJoined("room", "p1", "a")
	h.c.Metrics("room", "p1", mustJSON(t, metrics.Report{Name: "a", UploadKbps: 8000}))
	h.c.PeerJoined("room", "p2", "b")
	h.c.Metrics("room", "p2", mustJSON(t, metrics.Report{Name: "b"}))
	// This publishes, and the very first send wedges forever.
	if snap := h.snapshot("room"); len(snap.Members) != 2 {
		t.Fatalf("want 2 members, got %d", len(snap.Members))
	}

	h.c.PeerJoined("room", "p3", "c")
	h.c.Metrics("room", "p3", mustJSON(t, metrics.Report{Name: "c"}))
	snap := h.snapshot("room")
	if len(snap.Members) != 3 {
		t.Fatalf("the control loop must keep running behind a wedged sender, got %d members", len(snap.Members))
	}
	if snap.Topo == nil {
		t.Fatal("trees must still be computed while the outbound queue is wedged")
	}
}

// TestSnapshotIsOrderedAndDeepCopied: a map in a snapshot breaks the dashboard's
// replayability, and a shared pointer lets a consumer corrupt coordinator state.
func TestSnapshotIsOrderedAndDeepCopied(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p3", "zeta", 0)
	h.member("room", "p1", "alpha", 8000)
	h.member("room", "p2", "mid", 0)

	snap := h.snapshot("room")
	if len(snap.Members) != 3 {
		t.Fatalf("want 3 members, got %d", len(snap.Members))
	}
	for i := 1; i < len(snap.Members); i++ {
		if snap.Members[i-1].Name > snap.Members[i].Name {
			t.Fatalf("Members must be ordered by name, got %+v", snap.Members)
		}
	}
	if snap.Topo == nil || snap.RoomID != "room" || snap.Epoch != 1 {
		t.Fatalf("unexpected snapshot header: %+v", snap)
	}
	if !snap.At.Equal(h.clk.Now()) {
		t.Fatalf("At must come from the injected clock, got %v want %v", snap.At, h.clk.Now())
	}

	// Corrupt everything the caller can reach.
	snap.Topo.Edges[0].Parent = "corrupted"
	snap.Topo.Root = "corrupted"
	if len(snap.Topo.Backups) > 0 {
		snap.Topo.Backups[0].Parent = "corrupted"
	}
	snap.Members[0].Name = "corrupted"

	again := h.snapshot("room")
	if again.Topo.Root == "corrupted" || again.Topo.Edges[0].Parent == "corrupted" {
		t.Fatalf("Snapshot must deep copy the topology, got %+v", again.Topo)
	}
	if again.Members[0].Name == "corrupted" {
		t.Fatal("Snapshot must copy the member slice")
	}
}

// TestSnapshotOfAnUnknownMeet returns a zero value and no error, per §5.8.
func TestSnapshotOfAnUnknownMeet(t *testing.T) {
	h := newHarness(t, baseConfig())
	ctx, cancel := context.WithTimeout(context.Background(), guard)
	defer cancel()
	snap, err := h.c.Snapshot(ctx, "nope")
	if err != nil {
		t.Fatalf("unknown meet must not be an error, got %v", err)
	}
	if snap.RoomID != "" || snap.Topo != nil || len(snap.Members) != 0 {
		t.Fatalf("want a zero RoomSnapshot, got %+v", snap)
	}
}

// TestSetEpochIsPerMeet pins the v2.2 correction: epoch is minted PER MEET, so a
// process-global setter would stamp both meets of a two-meet server with whichever
// epoch was set last and silently mis-fence every peer in the other one.
func TestSetEpochIsPerMeet(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.c.SetEpoch("alpha", 7)
	h.sync()

	h.member("alpha", "a1", "a", 8000)
	h.member("alpha", "a2", "b", 0)
	h.member("beta", "b1", "a", 8000)
	h.member("beta", "b2", "b", 0)

	if got := h.published("alpha"); got == nil || got.Epoch != 7 || got.Rev != 1 {
		t.Fatalf("alpha must be stamped with its own epoch, got %+v", got)
	}
	if got := h.published("beta"); got == nil || got.Epoch != 1 {
		t.Fatalf("beta must keep the default term, got %+v", got)
	}
}

// TestSetEpochResetsRev: a new term restarts the revision counter at 1 (§5.7 rule 2).
//
// PHASE 6 CHANGE: adopting a higher epoch now also enters the rebuild window (§6.6), so
// the clock advance below is not padding — without it the coordinator is still waiting
// for the fleet's realized state and has deliberately published nothing. The epoch/rev
// claim this test exists for is unchanged; phase6_test.go covers the window itself.
func TestSetEpochResetsRev(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	if got := h.published("room").Rev; got < 2 {
		t.Fatalf("precondition: want rev >= 2, got %d", got)
	}

	h.c.SetEpoch("room", 2)
	h.sync()
	h.member("room", "p4", "d", 0)
	if got := h.published("room"); got != nil {
		t.Fatalf("nothing may be published inside the rebuild window, got %+v", got)
	}
	h.advance(metrics.RebuildWindow)

	topo := h.published("room")
	if topo.Epoch != 2 || topo.Rev != 1 {
		t.Fatalf("a higher epoch resets Rev to 0 so the next tree is rev 1, got epoch %d rev %d", topo.Epoch, topo.Rev)
	}
}

// TestSetRosterReconcilesMembership: members the arbiter knows about but this
// coordinator does not are added; members it holds that the arbiter does not are
// treated as left. This is the seam that makes an elected-peer coordinator able to
// learn who is present at all.
func TestSetRosterReconcilesMembership(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)

	h.c.SetRoster("room", []coordinator.Member{
		{ID: "p1", Name: "a"},
		{ID: "p2", Name: "b"},
		{ID: "p9", Name: "z"}, // present per the arbiter, unknown here
	})
	h.sync()

	names := map[string]bool{}
	for _, m := range h.snapshot("room").Members {
		names[m.Name] = true
	}
	if !names["z"] {
		t.Fatalf("a roster member unknown here must be added, got %v", names)
	}
	if names["c"] {
		t.Fatalf("a member absent from the roster must be treated as left, got %v", names)
	}
	// z has never reported, so it is provisional and the settle governs its placement.
	h.advance(coordinator.JoinSettle + 1)
	if topo := h.published("room"); topo.ParentOf("z") == "" {
		t.Fatalf("the added member must be placed: %+v", topo)
	}
}

// TestUnnamedPeersAreNeverPushed: a peer that joined without a name cannot be
// placed in a name-keyed tree, and must not break the tree for everyone else.
func TestUnnamedPeersAreNeverPushed(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.join("room", "p2", "") // anonymous
	h.member("room", "p3", "c", 0)

	if topo := h.published("room"); topo == nil || topo.ParentOf("c") != "a" {
		t.Fatalf("the named members must still get a tree, got %+v", topo)
	}
	if got := h.fs.last("p2"); got != nil {
		t.Fatalf("an anonymous peer must never be pushed a topology, got %+v", got)
	}
}

// TestSendFailureIsTransient: a send error means the peer probably just left. It is
// logged and never fatal, and the tree stays published.
func TestSendFailureIsTransient(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.fs.mu.Lock()
	h.fs.failAll = true
	h.fs.mu.Unlock()

	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.sync()

	if topo := h.published("room"); topo == nil {
		t.Fatal("a failing sender must not prevent the tree from being computed and recorded")
	}
}

// TestEmptyMeetIsCollected: the last member leaving drops the meet's state rather
// than leaking a room record per meeting ever held.
func TestEmptyMeetIsCollected(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.c.PeerLeft("room", "p1")
	h.c.PeerLeft("room", "p2")
	h.sync()

	if snap := h.snapshot("room"); snap.RoomID != "" || len(snap.Members) != 0 {
		t.Fatalf("an emptied meet must be forgotten, got %+v", snap)
	}
}

// TestPushesEveryMemberOnPublish: every named member gets the tree, so nobody is
// left applying a stale one.
func TestPushesEveryMemberOnPublish(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	h.sync()

	topo := h.published("room")
	for _, id := range []string{"p1", "p2", "p3"} {
		got := h.fs.last(id)
		if got == nil || got.Rev != topo.Rev {
			t.Fatalf("peer %s was not pushed the current tree (rev %d), got %+v", id, topo.Rev, got)
		}
	}
}

// TestReportRefinesTheTree covers threshold row 4 end to end and asserts the
// published tree is legal under the independent oracle at every step.
func TestReportRefinesTheTree(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.join("room", "p1", "weak")
	h.join("room", "p2", "strong")
	h.report("room", "p1", metrics.Report{Name: "weak", UploadKbps: 2000})
	h.report("room", "p2", metrics.Report{Name: "strong", UploadKbps: 9000})

	topo := h.published("room")
	if topo == nil || topo.Root != "strong" {
		t.Fatalf("the reported capacities must shape the tree, got %+v", topo)
	}
	if err := overlay.Validate(topo, nodesFor(h.snapshot("room"), nil), overlay.Constraints{
		Root: topo.Root, MaxDepth: 2, StreamKbps: 2000, Epoch: topo.Epoch, Rev: topo.Rev,
	}); err != nil {
		t.Fatalf("published tree fails Validate: %v", err)
	}
}

// TestNamelessFrameFromAnUnknownPeerIsDropped: the resurrection rule admits an
// unknown peer from the NAME in its frame body. A frame with no name carries nothing
// that could be placed in a name-keyed tree, so admitting it would publish a
// membership event for the empty string and leave an unplaceable record behind.
func TestNamelessFrameFromAnUnknownPeerIsDropped(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	rev := h.published("room").Rev
	h.fp.reset()

	h.beat("room", "p9", metrics.Heartbeat{Seq: 1}) // no Name
	h.report("room", "p8", metrics.Report{})        // no Name

	if n := h.fp.countOf(coordinator.EventMember); n != 0 {
		t.Fatalf("a nameless frame must not admit a member, got %d member events", n)
	}
	if len(h.snapshot("room").Members) != 2 {
		t.Fatalf("membership must be unchanged, got %+v", h.snapshot("room").Members)
	}
	if got := h.published("room").Rev; got != rev {
		t.Fatalf("nothing to place means nothing to rebuild; rev %d -> %d", rev, got)
	}
	// And it must not leave a record behind for a meet nobody is in.
	if snap := h.snapshot("other"); snap.RoomID != "" {
		t.Fatalf("a stray frame must not create a meet, got %+v", snap)
	}
}
