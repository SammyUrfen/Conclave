package coordinator_test

// v2.6's five plumbing fields, and the EventStale emission that closes the
// stale-rejection chain.
//
// Every one of these exists because a CONSUMER was reconstructing something the
// coordinator already knew. That is the class of bug this build has hit repeatedly, so
// the tests here are written to fail if a value is merely plausible rather than real.

import (
	"encoding/json"
	"testing"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
)

// beatWithStale renders a heartbeat carrying the frozen `stale_rejected` wire key.
//
// It splices the key in rather than setting a struct field because metrics.Heartbeat
// does not carry it yet — see TestStaleRejectedShimIsStillNeeded, which fails the
// moment it does, so this scaffold cannot outlive its reason.
func beatWithStale(t *testing.T, hb metrics.Heartbeat, stale uint64) []byte {
	t.Helper()
	raw, err := json.Marshal(hb)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m["stale_rejected"] = stale
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestStaleRejectedShimIsStillNeeded is a SELF-REMOVING SCAFFOLD. The coordinator reads
// `stale_rejected` off the raw heartbeat payload because metrics.Heartbeat has no field
// for it yet. The instant WI-0 adds one, this test fails and names the two lines to
// delete — so the duplicate reader cannot quietly become a second source of truth that
// drifts from the first.
func TestStaleRejectedShimIsStillNeeded(t *testing.T) {
	raw, err := json.Marshal(metrics.Heartbeat{Name: "a", Seq: 1})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["stale_rejected"]; ok {
		t.Fatal("metrics.Heartbeat now carries stale_rejected: delete coordinator's staleShim " +
			"and read hb.StaleRejected directly, then delete this test")
	}
}

// TestMemberEventsCarryTheServerStampedID: §9.4 mandates an `id` on member_joined and
// member_left. The coordinator holds it, so it must emit it — the dashboard joining a
// delta on a peer-SUPPLIED name instead would key membership on the one field a peer
// controls.
func TestMemberEventsCarryTheServerStampedID(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.c.PeerLeft("room", "p2")
	h.sync()
	// A peer admitted from a frame alone (the resurrection path) must carry it too.
	h.beat("room", "p7", metrics.Heartbeat{Name: "z", Seq: 1})
	h.c.SetRoster("room", []coordinator.Member{{ID: "p1", Name: "a"}})
	h.sync()

	want := map[string]string{"a": "p1", "b": "p2", "z": "p7"}
	seen := 0
	for _, ev := range h.fp.of(coordinator.EventMember) {
		id, known := want[ev.Node]
		if !known {
			t.Fatalf("unexpected member event for %q", ev.Node)
		}
		if ev.NodeID != id {
			t.Fatalf("member event for %q (present=%v) carries NodeID %q, want %q",
				ev.Node, ev.Present, ev.NodeID, id)
		}
		seen++
	}
	if seen < 5 { // 2 joins, 1 leave, 1 admitted-from-frame, 1 roster removal
		t.Fatalf("expected member events for every arrival and departure, got %d", seen)
	}
}

// TestHealthEventsCarryTheTransition: a health event names what it transitioned FROM.
// The dashboard remembering it in-process was wrong across a restart and wrong for a
// fresh subscriber, whose very first transition reported "".
func TestHealthEventsCarryTheTransition(t *testing.T) {
	h := newHarness(t, livenessConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)
	h.fp.reset()

	alive := map[string]string{"p1": "a", "p3": "c"}
	for i := uint64(1); i <= metrics.GoneBeats; i++ {
		h.advance(metrics.HeartbeatInterval)
		beatAll(h, "room", i, alive)
	}
	h.beat("room", "p2", metrics.Heartbeat{Name: "b", Seq: 1}) // resurrect

	var got []string
	for _, ev := range h.fp.of(coordinator.EventHealth) {
		if ev.Node != "b" {
			continue
		}
		if ev.NodeID != "p2" {
			t.Fatalf("health event for b carries NodeID %q, want p2", ev.NodeID)
		}
		if ev.PrevHealth == "" {
			t.Fatalf("health event %q -> %q has no PrevHealth", ev.PrevHealth, ev.Health)
		}
		got = append(got, string(ev.PrevHealth)+"->"+string(ev.Health))
	}
	want := []string{"healthy->degraded", "degraded->gone", "gone->healthy"}
	if len(got) != len(want) {
		t.Fatalf("want transitions %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transition %d = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
}

// TestImpairmentIsNotALivenessTransition pins the one rule that keeps EventHealth
// honest. Sustained degradation and its recovery have no event kind of their own, so
// they ride EventHealth — but they are NOT liveness transitions, and they say so by
// carrying PrevHealth == Health. A consumer can therefore tell the two apart with a
// comparison instead of by parsing Reason.
func TestImpairmentIsNotALivenessTransition(t *testing.T) {
	cfg := baseConfig()
	cfg.Dwell = 4 * metrics.DefaultInterval
	h := newHarness(t, cfg)
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 4000)
	h.member("room", "p3", "c", 0)
	h.fp.reset()

	bad := metrics.Report{Name: "b", UploadKbps: 4000, LossPct: coordinator.DegradedLossPct + 1}
	h.report("room", "p2", bad)
	h.advance(cfg.Dwell)
	h.report("room", "p2", metrics.Report{Name: "b", UploadKbps: 4000}) // recovered

	evs := h.fp.of(coordinator.EventHealth)
	if len(evs) < 2 {
		t.Fatalf("want an impairment and a recovery event, got %d: %+v", len(evs), evs)
	}
	for _, ev := range evs {
		if ev.PrevHealth != ev.Health {
			t.Fatalf("an impairment event must not claim a liveness transition, got %q -> %q (reason %q)",
				ev.PrevHealth, ev.Health, ev.Reason)
		}
		if ev.Health != coordinator.HealthHealthy {
			t.Fatalf("b never missed a beat, so it must stay healthy; got %q", ev.Health)
		}
	}
}

// TestStaleRejectedEmitsOnIncreaseOnly is the transition-not-sample discipline. A peer
// beats at 1 Hz; an event per beat would drown the dashboard's log at exactly the moment
// an operator is reading it during a handover.
func TestStaleRejectedEmitsOnIncreaseOnly(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.fp.reset()

	for _, n := range []uint64{0, 0, 3, 3, 3, 7} {
		h.c.Heartbeat("room", "p2", beatWithStale(t, metrics.Heartbeat{Name: "b", Seq: 1, Epoch: 2, Rev: 5}, n))
		h.sync()
	}

	evs := h.fp.of(coordinator.EventStale)
	if len(evs) != 2 {
		t.Fatalf("want one event per INCREASE (2), got %d: %+v", len(evs), evs)
	}
	if evs[0].Count != 3 || evs[1].Count != 7 {
		t.Fatalf("Count must carry the running total, got %d then %d", evs[0].Count, evs[1].Count)
	}
	for _, ev := range evs {
		if ev.Node != "b" || ev.NodeID != "p2" {
			t.Fatalf("stale event must identify the peer, got node %q id %q", ev.Node, ev.NodeID)
		}
	}
}

// TestStaleRejectedCarriesThePeersOwnFence: the numbers that explain the refusal are the
// PEER's epoch and rev, not the meet's. A peer stuck at epoch 0 — one that has never
// adopted an announcement, which is the most interesting case there is — must report 0,
// so the meet's epoch may not be substituted for it.
func TestStaleRejectedCarriesThePeersOwnFence(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.c.SetEpoch("room", 9)
	h.sync()
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.fp.reset()

	h.c.Heartbeat("room", "p2", beatWithStale(t, metrics.Heartbeat{Name: "b", Seq: 1}, 4))
	h.sync()

	evs := h.fp.of(coordinator.EventStale)
	if len(evs) != 1 {
		t.Fatalf("want one stale event, got %d", len(evs))
	}
	if evs[0].Epoch != 0 || evs[0].Rev != 0 {
		t.Fatalf("a peer that has adopted nothing reports epoch 0 rev 0; got epoch %d rev %d "+
			"(the meet is at epoch 9, and substituting it would hide the very mismatch this event exists to show)",
			evs[0].Epoch, evs[0].Rev)
	}
}

// TestStaleRejectedResetOnRejoinIsNotAnIncrease is the one that has to be right.
//
// The peer's fence resets on TypeJoined, so its counter resets too — a DECREASE is
// legitimate wire traffic, not corruption. Two ways to get this wrong: treat it as an
// increase (a spurious event and a total that goes backwards), or compute
// `new - old` on a uint64 and report a delta near 2^64.
func TestStaleRejectedResetOnRejoinIsNotAnIncrease(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)

	h.c.Heartbeat("room", "p2", beatWithStale(t, metrics.Heartbeat{Name: "b", Seq: 9}, 7))
	h.sync()
	h.fp.reset()

	// Same socket, fence reset: the peer rejoined and starts counting again.
	h.c.Heartbeat("room", "p2", beatWithStale(t, metrics.Heartbeat{Name: "b", Seq: 1}, 2))
	h.sync()

	if n := h.fp.countOf(coordinator.EventStale); n != 0 {
		evs := h.fp.of(coordinator.EventStale)
		t.Fatalf("a reset is not an increase; got %d event(s), first Count=%d", n, evs[0].Count)
	}
	if got := staleOf(h, "room", "b"); got != 2 {
		t.Fatalf("the coordinator must ADOPT the peer's new count, got %d, want 2 "+
			"(7 means the reset was ignored; a huge number means a uint64 delta underflowed)", got)
	}

	// And counting resumes from the new base, without replaying the old total.
	h.c.Heartbeat("room", "p2", beatWithStale(t, metrics.Heartbeat{Name: "b", Seq: 2}, 5))
	h.sync()
	evs := h.fp.of(coordinator.EventStale)
	if len(evs) != 1 || evs[0].Count != 5 {
		t.Fatalf("want one event carrying the new total 5, got %+v", evs)
	}
}

// TestStaleRejectedInSnapshots: a fresh subscriber's first frame must be correct without
// replaying the event log, which is the whole reason the counters are in the snapshot.
func TestStaleRejectedInSnapshots(t *testing.T) {
	h := newHarness(t, baseConfig())
	h.member("room", "p1", "a", 8000)
	h.member("room", "p2", "b", 0)
	h.member("room", "p3", "c", 0)

	h.c.Heartbeat("room", "p2", beatWithStale(t, metrics.Heartbeat{Name: "b", Seq: 1}, 4))
	h.c.Heartbeat("room", "p3", beatWithStale(t, metrics.Heartbeat{Name: "c", Seq: 1}, 6))
	h.sync()

	snap := h.snapshot("room")
	if snap.StaleRejected != 10 {
		t.Fatalf("the meet total is the sum across members; got %d, want 10", snap.StaleRejected)
	}
	want := map[string]uint64{"a": 0, "b": 4, "c": 6}
	for _, m := range snap.Members {
		if m.StaleRejected != want[m.Name] {
			t.Fatalf("member %q reports %d refusals, want %d", m.Name, m.StaleRejected, want[m.Name])
		}
	}
}

// staleOf reads one member's refusal count out of a snapshot.
func staleOf(h *harness, roomID, name string) uint64 {
	h.t.Helper()
	for _, m := range h.snapshot(roomID).Members {
		if m.Name == name {
			return m.StaleRejected
		}
	}
	h.t.Fatalf("no member named %q", name)
	return 0
}
