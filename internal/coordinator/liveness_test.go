package coordinator_test

// §5.3 / §5.4: the health FSM, the resurrection rule, the socket-detection floor,
// and the degradation dwell. All in virtual time.

import (
	"sort"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
)

// livenessConfig leaves DegradedAfter/GoneAfter at zero so the thresholds are
// derived per node from the cadence the peer DECLARED, which is the shipped design
// (§5.3) and the thing worth testing.
func livenessConfig() coordinator.Config {
	return coordinator.Config{MaxDepth: 2, StreamKbps: 2000}
}

// beatAll delivers one heartbeat for each (peerID, name) pair at the default
// cadence, in sorted id order — never a map range, so the recorded event stream is
// replayable (the same rule simnet's Network follows).
func beatAll(h *harness, room string, seq uint64, ids map[string]string) {
	h.t.Helper()
	keys := make([]string, 0, len(ids))
	for id := range ids {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		h.c.Heartbeat(room, id, mustJSON(h.t, metrics.Heartbeat{Name: ids[id], Seq: seq}))
	}
	h.sync()
}

// TestHealthFSMDegradedThenGone walks the three states in virtual time: three
// missed beats demote to degraded (advisory only — nothing re-parents), eight
// declare gone, and only the second is a threshold event.
func TestHealthFSMDegradedThenGone(t *testing.T) {
	h := newHarness(t, livenessConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	first := h.published("room")
	if first == nil {
		t.Fatal("precondition: no tree")
	}
	alive := map[string]string{"p1": "a", "p3": "c"} // b falls silent

	for i := uint64(1); i <= metrics.DegradedBeats; i++ {
		h.advance(metrics.HeartbeatInterval)
		beatAll(h, "room", i, alive)
	}
	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthDegraded {
		t.Fatalf("after %d missed beats b must be degraded, got %q", metrics.DegradedBeats, got)
	}
	if got, _ := h.healthOf("room", "a"); got != coordinator.HealthHealthy {
		t.Fatalf("a is beating and must stay healthy, got %q", got)
	}
	if rev := h.published("room").Rev; rev != first.Rev {
		t.Fatalf("degraded is ADVISORY: it must never rebuild the tree; rev %d -> %d", first.Rev, rev)
	}

	for i := uint64(metrics.DegradedBeats + 1); i <= metrics.GoneBeats; i++ {
		h.advance(metrics.HeartbeatInterval)
		beatAll(h, "room", i, alive)
	}
	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthGone {
		t.Fatalf("after %d missed beats b must be gone, got %q", metrics.GoneBeats, got)
	}
	// Gone IS a threshold event: repair ran and b is out of the tree.
	fo, ok := h.fp.lastOf(coordinator.EventFailover)
	if !ok || fo.Node != "b" {
		t.Fatalf("want a failover event for b, got %+v (ok=%v)", fo, ok)
	}
	after := h.published("room")
	if after.Rev <= first.Rev {
		t.Fatalf("declaring a node gone must rebuild; rev %d -> %d", first.Rev, after.Rev)
	}
	if after.ParentOf("b") != "" {
		t.Fatalf("a gone node must be excluded from the tree: %+v", after)
	}
	// …but its RECORD survives, which is what makes resurrection cheap (§5.3).
	if _, present := h.healthOf("room", "b"); !present {
		t.Fatal("gone must not delete the record; only PeerLeft deletes")
	}
}

// TestResurrectionOfAGoneNode is the coordinator's half of the two-party contract
// documented on signaling.Observer.Heartbeat. The Hub keeps no health state, so a
// beat on a live socket is PROOF the peer is back; dropping it is the permanent
// ejection bug a 9-second Wi-Fi roam otherwise produces.
func TestResurrectionOfAGoneNode(t *testing.T) {
	h := newHarness(t, livenessConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	alive := map[string]string{"p1": "a", "p3": "c"}

	for i := uint64(1); i <= metrics.GoneBeats; i++ {
		h.advance(metrics.HeartbeatInterval)
		beatAll(h, "room", i, alive)
	}
	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthGone {
		t.Fatalf("precondition: b must be gone, got %q", got)
	}
	goneRev := h.published("room").Rev
	h.fp.reset()

	// b's Wi-Fi came back. Its socket never closed, so there is no TypeJoined —
	// this beat is the only re-entry point.
	h.beat("room", "p2", metrics.Heartbeat{Name: "b", Seq: 1})

	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthHealthy {
		t.Fatalf("a heartbeat from a gone peer must resurrect it, got %q", got)
	}
	if n := h.fp.countOf(coordinator.EventMember); n != 1 {
		t.Fatalf("a resurrection is a membership-growth episode and must publish one member event, got %d", n)
	}
	if ev, _ := h.fp.lastOf(coordinator.EventMember); !ev.Present || ev.Node != "b" {
		t.Fatalf("want EventMember{Node:b, Present:true}, got %+v", ev)
	}
	topo := h.published("room")
	if topo.Rev <= goneRev {
		t.Fatalf("resurrection is a join threshold event and must rebuild; rev %d -> %d", goneRev, topo.Rev)
	}
	if topo.ParentOf("b") == "" {
		t.Fatalf("the resurrected peer must be back in the tree: %+v", topo)
	}
}

// TestResurrectionOfAnUnknownPeer covers the second row of §5.3's obligation table:
// a frame from a peer with NO record at all (deleted by PeerLeft, or this
// coordinator was just elected) creates the record rather than dropping it. That
// `if ns == nil { return }` early-out is the C4 blackhole.
func TestResurrectionOfAnUnknownPeer(t *testing.T) {
	h := newHarness(t, livenessConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	rev := h.published("room").Rev

	// p9 never joined through the Hub as far as this coordinator knows.
	h.beat("room", "p9", metrics.Heartbeat{Name: "z", Seq: 1})

	if got, ok := h.healthOf("room", "z"); !ok || got != coordinator.HealthHealthy {
		t.Fatalf("an unknown peer's heartbeat must create a healthy record, got %q (present=%v)", got, ok)
	}
	// A created member has never reported, so this counts as a membership-growth
	// episode and arms the settle — it is placed when the window closes.
	h.advance(coordinator.JoinSettle + 1)
	topo := h.published("room")
	if topo.Rev <= rev || topo.ParentOf("z") == "" {
		t.Fatalf("the created member must be placed by a rebuild: rev %d -> %d, tree %+v", rev, topo.Rev, topo)
	}
}

// TestMetricsAlsoResurrect: §5.3's table lists metrics and reparented frames
// alongside heartbeats. All three are proof of a live socket.
func TestMetricsAlsoResurrect(t *testing.T) {
	h := newHarness(t, livenessConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	alive := map[string]string{"p1": "a", "p3": "c"}
	for i := uint64(1); i <= metrics.GoneBeats; i++ {
		h.advance(metrics.HeartbeatInterval)
		beatAll(h, "room", i, alive)
	}
	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthGone {
		t.Fatalf("precondition: b must be gone, got %q", got)
	}

	h.report("room", "p2", metrics.Report{Name: "b", UploadKbps: 0})
	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthHealthy {
		t.Fatalf("a metrics frame is equally proof of life, got %q", got)
	}
}

// TestSocketDetectionFloor pins the v2.2 addition. A peer declaring a 500ms cadence
// derives GoneAfter = 4s, which is BELOW the 7s window in which the Hub may still
// consider it present — reopening the permanent-ejection bug from a place no
// startup flag check can reach. The floor closes it per node.
func TestSocketDetectionFloor(t *testing.T) {
	const fast = 500 * time.Millisecond
	cfg := livenessConfig()
	cfg.SocketDetection = 7 * time.Second // signaling.Hub.LivenessBudget()

	h := newHarness(t, cfg)
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	// b declares the fast cadence once, then falls silent. a and c keep beating.
	h.beat("room", "p2", metrics.Heartbeat{Name: "b", Seq: 1, IntervalMs: uint64(fast / time.Millisecond)})

	unfloored := metrics.GoneAfter(fast) // 4s
	if unfloored >= cfg.SocketDetection {
		t.Fatalf("test premise broken: %v must be below the %v socket window", unfloored, cfg.SocketDetection)
	}
	h.advance(unfloored + time.Second)
	if got, _ := h.healthOf("room", "b"); got == coordinator.HealthGone {
		t.Fatalf("a fast-beating peer must not shrink its own gone threshold below the socket-detection window (%v)", cfg.SocketDetection)
	}

	h.advance(cfg.SocketDetection)
	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthGone {
		t.Fatalf("past the floor the peer must still be reaped, got %q", got)
	}
}

// TestSocketDetectionZeroDisablesTheFloor: zero is what simnet wants, because it
// models no socket layer at all. The same fleet then goes gone at the derived 4s.
func TestSocketDetectionZeroDisablesTheFloor(t *testing.T) {
	const fast = 500 * time.Millisecond
	h := newHarness(t, livenessConfig()) // SocketDetection == 0
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	h.beat("room", "p2", metrics.Heartbeat{Name: "b", Seq: 1, IntervalMs: uint64(fast / time.Millisecond)})

	h.advance(metrics.GoneAfter(fast) + time.Millisecond)
	if got, _ := h.healthOf("room", "b"); got != coordinator.HealthGone {
		t.Fatalf("with the floor disabled the declared cadence governs; want gone, got %q", got)
	}
}

// TestDwellFiresOnSustainedDegradation: the dwell is armed by the first bad sample
// and fires only after an UNBROKEN run of them, at which point sustained
// degradation is a threshold event (§5.4 row 5).
func TestDwellFiresOnSustainedDegradation(t *testing.T) {
	cfg := baseConfig() // liveness out of the way; this test is about the dwell
	cfg.Dwell = 10 * metrics.DefaultInterval
	h := newHarness(t, cfg)
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 4000)
	h.member("room", "p3", "c", 0)
	rev := h.published("room").Rev

	bad := metrics.Report{Name: "b", UploadKbps: 4000, LossPct: coordinator.DegradedLossPct + 1}
	for i := 0; i < 9; i++ {
		h.advance(metrics.DefaultInterval)
		h.report("room", "p2", bad)
	}
	if got := h.published("room").Rev; got != rev {
		t.Fatalf("an unfired dwell must not rebuild; rev %d -> %d", rev, got)
	}
	h.advance(cfg.Dwell)
	if got := h.published("room").Rev; got <= rev {
		t.Fatalf("a fired dwell IS a threshold event; rev %d -> %d", rev, got)
	}
}

// TestDwellResetsOnAGoodSample: a single bad RTT resets nothing, and a good sample
// resets everything. The node below is bad for most of the window and then
// recovers, so the dwell must never fire.
func TestDwellResetsOnAGoodSample(t *testing.T) {
	cfg := baseConfig()
	cfg.Dwell = 10 * metrics.DefaultInterval
	h := newHarness(t, cfg)
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 4000)
	h.member("room", "p3", "c", 0)
	rev := h.published("room").Rev

	bad := metrics.Report{Name: "b", UploadKbps: 4000, LossPct: coordinator.DegradedLossPct + 1}
	good := metrics.Report{Name: "b", UploadKbps: 4000}
	for i := 0; i < 9; i++ {
		h.advance(metrics.DefaultInterval)
		h.report("room", "p2", bad)
	}
	h.advance(metrics.DefaultInterval)
	h.report("room", "p2", good) // recovery: the run is broken
	h.advance(2 * cfg.Dwell)

	if got := h.published("room").Rev; got != rev {
		t.Fatalf("a recovered node must not fire its dwell; rev %d -> %d", rev, got)
	}
}
