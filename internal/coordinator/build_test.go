package coordinator_test

// §5.9 / §5.9a: the join settle rule, the cooldown, and the three build outcomes.

import (
	"testing"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestBuildsAndStampsEpochRev is the core contract: a managed meet whose members
// have all reported builds immediately (the §5.9 rule 3(a) early exit), stamps
// epoch 1 / rev 1, and pushes the tree to every named member.
func TestBuildsAndStampsEpochRev(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "relay", 8000)
	h.member("room", "p2", "leaf-b", 0)

	topo := h.published("room")
	if topo == nil {
		t.Fatalf("no tree published; events=%+v", h.fp.all())
	}
	if topo.Epoch != 1 || topo.Rev != 1 {
		t.Fatalf("want epoch 1 rev 1, got epoch %d rev %d", topo.Epoch, topo.Rev)
	}
	if topo.Root != "relay" || topo.ParentOf("leaf-b") != "relay" {
		t.Fatalf("unexpected tree: %+v", topo)
	}
	if h.fs.last("p1") == nil || h.fs.last("p2") == nil {
		t.Fatalf("both members must be pushed the tree; pushes=%d", h.fs.count())
	}
	ev, ok := h.fp.lastOf(coordinator.EventTopology)
	if !ok || ev.Outcome != coordinator.OutcomeBuilt || ev.Rev != 1 {
		t.Fatalf("want a built topology event at rev 1, got %+v (ok=%v)", ev, ok)
	}

	// A third member is a threshold event: rev must advance by exactly 1.
	h.member("room", "p3", "leaf-c", 0)
	if got := h.published("room").Rev; got != 2 {
		t.Fatalf("rev must advance by exactly 1 per published tree, got %d", got)
	}
}

// TestMinBuildableMembersNeverBuilds pins §5.9 rule 2: a one-member meet publishes
// nothing even after the settle window has fully elapsed, because a 1-node tree
// would seed the stickiness baseline with a root chosen from a fleet of one.
func TestMinBuildableMembersNeverBuilds(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "solo", 8000)
	h.advance(10 * coordinator.JoinSettle)

	if topo := h.published("room"); topo != nil {
		t.Fatalf("a %d-member meet must not build; got %+v", 1, topo)
	}
	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("want no topology events, got %d", n)
	}
	if n := h.fp.countOf(coordinator.EventSettling); n != 1 {
		t.Fatalf("EventSettling is emitted once per transition into ineligibility, got %d", n)
	}
	if h.fs.count() != 0 {
		t.Fatalf("nothing may be pushed to a sub-minimum meet, got %d pushes", h.fs.count())
	}

	// The second member makes it buildable, and the settle is spent on THIS episode.
	h.member("room", "p2", "other", 0)
	if topo := h.published("room"); topo == nil {
		t.Fatal("a two-member meet whose members have all reported must build")
	}
}

// TestJoinBurstCoalescesIntoOneBuild pins §5.9 rule 1: every join re-arms the
// settle, so a burst of silent joiners produces exactly one tree when the window
// finally closes — not one per join.
func TestJoinBurstCoalescesIntoOneBuild(t *testing.T) {
	h := newHarness(t, baseConfig())
	// a reports (so the fleet is rootable); b and c stay silent, so rule 3(a) can
	// never fire and only the window can end the wait.
	h.member("room", "p1", "a", 8000)
	h.advance(coordinator.JoinSettle / 3)
	h.join("room", "p2", "b")
	h.advance(coordinator.JoinSettle / 3)
	h.join("room", "p3", "c")

	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("no tree may be built while the settle runs, got %d", n)
	}
	// Past the LAST join's window (each join pushed it out).
	h.advance(coordinator.JoinSettle + 1)
	if n := h.fp.countOf(coordinator.EventTopology); n != 1 {
		t.Fatalf("a join burst must coalesce into exactly one build, got %d: %+v", n, h.fp.of(coordinator.EventTopology))
	}
}

// TestSettleEarlyExitOnFullReports pins rule 3(a): once every known member has
// reported there is nothing left to wait for, so the meet builds without spending
// the window.
func TestSettleEarlyExitOnFullReports(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.join("room", "p1", "a")
	h.join("room", "p2", "b")
	if h.published("room") != nil {
		t.Fatal("must not build before anyone has reported")
	}
	h.report("room", "p1", metrics.Report{Name: "a", UploadKbps: 8000})
	if h.published("room") != nil {
		t.Fatal("must not build while a known member is still unheard from")
	}
	h.report("room", "p2", metrics.Report{Name: "b", UploadKbps: 0})
	// The virtual clock has NOT moved, so only the early exit can explain a tree.
	if h.published("room") == nil {
		t.Fatal("all members reported: the settle must exit early, with no time elapsed")
	}
}

// TestUnplacedJoinerBypassesCooldown is the §5.9 rule 4 / §5.9a bypass: a joiner
// waits at most JoinSettle for media, never RecomputeCooldown. Without the bypass
// the second build below cannot happen until 5s of virtual time has passed.
func TestUnplacedJoinerBypassesCooldown(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	if h.published("room") == nil {
		t.Fatal("first build did not happen")
	}
	before := h.published("room").Rev

	// Well inside RecomputeCooldown (5s): move only a fraction of it.
	h.advance(coordinator.RecomputeCooldown / 10)
	h.member("room", "p3", "c", 0)

	topo := h.published("room")
	if topo.Rev != before+1 {
		t.Fatalf("an unplaced joiner must bypass the cooldown; rev stuck at %d", topo.Rev)
	}
	if topo.ParentOf("c") == "" {
		t.Fatalf("joiner c was not placed: %+v", topo)
	}
}

// TestNonUrgentEventRespectsCooldown is the other half: a leave is NOT urgent, so
// it is coalesced into the cooldown window and runs exactly once when it closes.
func TestNonUrgentEventRespectsCooldown(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	h.member("room", "p4", "d", 0)
	start := h.published("room").Rev

	h.c.PeerLeft("room", "p3")
	h.sync()
	h.c.PeerLeft("room", "p4")
	h.sync()
	if got := h.published("room").Rev; got != start {
		t.Fatalf("a burst of leaves inside the cooldown must not rebuild; rev %d -> %d", start, got)
	}

	h.advance(coordinator.RecomputeCooldown)
	topo := h.published("room")
	if topo.Rev != start+1 {
		t.Fatalf("the suppressed recompute must run exactly once when the window closes; rev %d -> %d", start, topo.Rev)
	}
	if len(topo.Edges) != 1 || topo.ParentOf("b") != "a" {
		t.Fatalf("tree did not shrink to the survivors: %+v", topo)
	}
}

// TestSubsequentReportIsNotAThresholdEvent pins §5.4's exhaustive list: only a
// node's FIRST report rebuilds. A later, dramatically different report is stored
// and used at the next real threshold event.
func TestSubsequentReportIsNotAThresholdEvent(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	rev := h.published("room").Rev

	// Past the cooldown, so a rebuild is not merely being suppressed.
	h.advance(2 * coordinator.RecomputeCooldown)
	h.report("room", "p2", metrics.Report{Name: "b", UploadKbps: 999999})
	if got := h.published("room").Rev; got != rev {
		t.Fatalf("a subsequent report must not rebuild the tree; rev %d -> %d", rev, got)
	}
	// …but the number was retained: the next threshold event re-roots onto b.
	h.member("room", "p3", "c", 0)
	if root := h.published("room").Root; root != "b" {
		t.Fatalf("the stored report must be used at the next threshold event; root=%q", root)
	}
}

// TestRelaxedRetryPublishesWhenStickinessBlocks is §5.9a. The fleet below is
// SATISFIABLE from scratch and UNSATISFIABLE stickily, so without the mandatory
// second attempt the coordinator would declare a perfectly good fleet unbuildable.
//
// How the state is reached (every step is a real threshold event):
//
//	a(6000,cap3) + x(2000,cap1)   -> a->x
//	h(7000) joins                 -> a stays root (7000 < 6000+2000), and h attaches
//	                                 under x (rank 3: x has fewer children), landing
//	                                 h at depth 2 = MaxDepth, so it can parent nobody
//	b,d join                      -> both attach to a, which is now FULL (3 children)
//	c joins                       -> sticky: a full, x full, h too deep => FAIL
//	                                 relaxed: h is placed at depth 1 and takes c.
func TestRelaxedRetryPublishesWhenStickinessBlocks(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 6000)
	h.member("room", "p2", "x", 2000)
	h.member("room", "p3", "h", 7000)
	h.member("room", "p4", "b", 0)
	h.member("room", "p5", "d", 0)

	pre := h.published("room")
	if pre == nil || pre.Root != "a" || pre.ParentOf("h") != "x" {
		t.Fatalf("precondition not met (root %q, h's parent %q): %+v", pre.Root, pre.ParentOf("h"), pre)
	}
	h.fp.reset()

	h.member("room", "p6", "c", 0)

	if n := h.fp.countOf(coordinator.EventUnbuildable); n != 0 {
		t.Fatalf("a sticky-only failure must never be reported unbuildable, got %d: %+v",
			n, h.fp.of(coordinator.EventUnbuildable))
	}
	ev, ok := h.fp.lastOf(coordinator.EventTopology)
	if !ok || ev.Outcome != coordinator.OutcomeRelaxed {
		t.Fatalf("want a published tree with OutcomeRelaxed, got %+v (ok=%v)", ev, ok)
	}
	if ev.Reason == "" {
		t.Fatal("OutcomeRelaxed must carry the FIRST attempt's error as its Reason; it is the only explanation of why everyone moved")
	}
	post := h.published("room")
	if post.ParentOf("c") == "" {
		t.Fatalf("the relaxed tree must place c: %+v", post)
	}
	if err := overlay.Validate(post, nodesFor(h.snapshot("room"), nil), overlay.Constraints{
		Root: post.Root, MaxDepth: 2, StreamKbps: 2000, Epoch: post.Epoch, Rev: post.Rev,
	}); err != nil {
		t.Fatalf("a relaxed tree is still gated by Validate: %v", err)
	}
}

// TestUnbuildableKeepsPreviousTree pins the fourth outcome: a genuinely
// over-constrained fleet (no build succeeds even from scratch) reports
// unbuildable, retains the last good tree, and pushes nothing new.
func TestUnbuildableKeepsPreviousTree(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 2000) // capacity for exactly one child
	h.member("room", "p2", "b", 0)
	good := h.published("room")
	if good == nil || len(good.Edges) != 1 {
		t.Fatalf("precondition: want a 1-edge tree, got %+v", good)
	}
	h.fp.reset()

	h.member("room", "p3", "c", 0) // one child too many, at any stickiness

	ev, ok := h.fp.lastOf(coordinator.EventUnbuildable)
	if !ok {
		t.Fatalf("want EventUnbuildable, got %+v", h.fp.all())
	}
	if ev.Reason == "" {
		t.Fatal("EventUnbuildable must carry the second attempt's error text")
	}
	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("nothing may be published when both attempts fail, got %d", n)
	}
	if got := h.published("room"); got == nil || got.Rev != good.Rev {
		t.Fatalf("the previous tree must be retained, got %+v", got)
	}
}

// TestUnrootableFleetShortCircuits pins §5.9a's last rule: PickRoot returning ""
// is unbuildable immediately, with no pointless relaxed attempt — dropping the
// stability preference does not create upload capacity.
func TestUnrootableFleetShortCircuits(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 0)
	h.member("room", "p2", "b", 0)

	ev, ok := h.fp.lastOf(coordinator.EventUnbuildable)
	if !ok {
		t.Fatalf("a fleet with no relay-capable member is unbuildable, got %+v", h.fp.all())
	}
	if ev.Outcome != coordinator.OutcomeUnbuildable {
		t.Fatalf("want OutcomeUnbuildable, got %q", ev.Outcome)
	}
	if ev.Reason != coordinator.ReasonNoEligibleRoot {
		t.Fatalf("an unrootable fleet must say so (and skip the retry); Reason=%q", ev.Reason)
	}
	if n := h.fp.countOf(coordinator.EventTopology); n != 0 {
		t.Fatalf("nothing may be published, got %d", n)
	}
}

// TestUnbuildableOnlyAfterSettle: while the meet is ineligible the coordinator says
// "settling", never "unbuildable". A warning that fires on every healthy startup
// teaches operators to ignore warnings (§5.9).
func TestUnbuildableOnlyAfterSettle(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.join("room", "p1", "a") // silent: cannot be placed, but we have not waited yet
	h.join("room", "p2", "b")
	h.join("room", "p3", "c")

	if n := h.fp.countOf(coordinator.EventUnbuildable); n != 0 {
		t.Fatalf("no unbuildable verdict may be reached before the settle closes, got %d", n)
	}
	if n := h.fp.countOf(coordinator.EventSettling); n != 1 {
		t.Fatalf("want exactly one settling event, got %d", n)
	}
	ev, _ := h.fp.lastOf(coordinator.EventSettling)
	if len(ev.Waiting) != 3 {
		t.Fatalf("EventSettling must name who is being waited on, got %v", ev.Waiting)
	}
	for i := 1; i < len(ev.Waiting); i++ {
		if ev.Waiting[i-1] > ev.Waiting[i] {
			t.Fatalf("Waiting must be ordered so the dashboard is replayable, got %v", ev.Waiting)
		}
	}

	h.advance(coordinator.JoinSettle + 1)
	if n := h.fp.countOf(coordinator.EventUnbuildable); n != 1 {
		t.Fatalf("post-settle, a 0-upload fleet IS unbuildable; got %d", n)
	}
}
