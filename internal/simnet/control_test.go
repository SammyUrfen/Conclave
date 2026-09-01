package simnet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// These are the scenarios that drive the REAL internal/coordinator rather than the
// model in harness_test.go. They exist because a harness that only ever exercises a
// model reports green about behaviour the shipped loop may not have: an adversarial
// mutation audit found that no simnet scenario could discriminate any coordinator
// mutation, because every scenario drove the model or overlay directly.
//
// simnet is TEST-ONLY and production never imports it, so `simnet -> coordinator` is
// the edge docs/PLAN.md §2.3 already grants ("simnet | overlay, coordinator, metrics,
// clock") and adds no production dependency. The reverse direction is what would be
// a problem, and it is why internal/coordinator's clock-driven tests live in the
// EXTERNAL package coordinator_test: coordinator_test -> simnet -> coordinator is
// legal, an internal white-box file importing simnet would not be.

const testRoom = "meet-1"

// controlFleet is the production shape: no measured pairwise RTT, because the
// coordinator's projection carries none (it has no source for it). Using an
// RTT-bearing fleet here would compare the model against the real loop on an input
// the real loop cannot receive.
func controlFleet() []overlay.Node {
	return []overlay.Node{
		{Name: "r", UploadKbps: 8000},
		{Name: "s", UploadKbps: 4000},
		{Name: "a", UploadKbps: 0},
		{Name: "b", UploadKbps: 0},
		{Name: "c", UploadKbps: 0},
	}
}

// controlConstraints are shared by both the model and the real loop so a divergence
// between them cannot be an artifact of asking for two different trees.
func controlConstraints() overlay.Constraints {
	return overlay.Constraints{MaxDepth: 2, StreamKbps: 2000, StickinessMs: overlay.DefaultStickinessMs}
}

// controlConfig mirrors controlConstraints into the coordinator's knobs.
//
// Liveness is pushed out of reach (an hour) because these scenarios advance virtual
// time past the settle, and a fleet that never heartbeats would otherwise be reaped
// mid-scenario by the backstop that is correct in production — turning an assertion
// about the tree into an accidental assertion about the health FSM. SocketDetection
// is 0 because simnet models no socket layer, which is exactly what that zero value
// documents.
func controlConfig(clk *VirtualClock) coordinator.Config {
	c := controlConstraints()
	return coordinator.Config{
		MaxDepth:        c.MaxDepth,
		StreamKbps:      c.StreamKbps,
		StickinessMs:    c.StickinessMs,
		DegradedAfter:   time.Hour,
		GoneAfter:       time.Hour,
		SocketDetection: 0,
		Clock:           clk,
	}
}

// realLoop is one live coordinator.Coordinator wired to a Scenario's clock and
// barrier, with simnet's Recorder and Capture standing in for the dashboard and the
// signaling hub.
type realLoop struct {
	c   *coordinator.Coordinator
	rec *Recorder
	cap *Capture
}

func startCoordinator(t *testing.T, sc *Scenario, name string) *realLoop {
	t.Helper()
	rec, cap := NewRecorder(), NewCapture()
	c := coordinator.New(slog.New(slog.NewTextHandler(io.Discard, nil)), controlConfig(sc.Clock()), cap, rec)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	sc.AddBarrier(name, c.Sync)
	return &realLoop{c: c, rec: rec, cap: cap}
}

// peerID maps a member name to a server-assigned peer id whose SORT order matches the
// Network's insertion order. It matters: the coordinator projects members in sorted
// peer-id order, and that order feeds PickRoot's and BuildTree's tie-breaks. Without
// the alignment a model/real disagreement could be an artifact of two different
// input orders rather than a real behavioural difference.
func peerID(net *Network, name string) string {
	for i, n := range net.order {
		if n == name {
			return fmt.Sprintf("p%02d", i)
		}
	}
	return "p??-" + name
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// joinAndReport delivers a member's join and its first telemetry frame, which is the
// production sequence: metrics.Reporter emits immediately on Run.
func joinAndReport(t *testing.T, loop *realLoop, net *Network, name string) {
	t.Helper()
	var node overlay.Node
	for _, n := range net.OverlayNodes() {
		if n.Name == name {
			node = n
		}
	}
	id := peerID(net, name)
	loop.c.PeerJoined(testRoom, id, name)
	loop.c.Metrics(testRoom, id, mustJSON(t, ReportOf(node)))
}

// TestScenarioDrivesTheRealCoordinator is the base integration scenario: a whole
// first build, in virtual time, through the shipped control loop. It asserts on the
// loop's two observable surfaces — what it PUBLISHED (Recorder) and what it PUSHED
// (Capture) — rather than on any internal state.
func TestScenarioDrivesTheRealCoordinator(t *testing.T) {
	sc := NewScenario(ScenarioConfig{Constraints: controlConstraints()})
	for _, n := range controlFleet() {
		sc.Net().Add(n)
	}
	loop := startCoordinator(t, sc, "coordinator")

	sc.At(0, func() {
		for _, name := range sc.Reporters() {
			joinAndReport(t, loop, sc.Net(), name)
		}
	})
	// Past the join settle, so the first build's eligibility window has closed.
	sc.At(coordinator.JoinSettle+100*time.Millisecond, func() {})

	if err := sc.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	ev, ok := loop.rec.Last(coordinator.EventTopology)
	if !ok {
		t.Fatalf("the coordinator published no tree; events = %v", loop.rec.Kinds())
	}
	if ev.Topo == nil {
		t.Fatal("EventTopology carried no topology")
	}
	if ev.Epoch != 1 || ev.Rev != 1 {
		t.Errorf("first tree stamped (epoch %d, rev %d), want (1, 1)", ev.Epoch, ev.Rev)
	}

	cons := controlConstraints()
	cons.Root, cons.Epoch, cons.Rev = ev.Topo.Root, ev.Topo.Epoch, ev.Topo.Rev
	nodes := projectionOf(sc.Net())
	if verr := validateTopology(ev.Topo, nodes, cons); verr != nil {
		t.Fatalf("the shipped loop published a tree that fails Validate: %v", verr)
	}
	if ev.Topo.Root != "r" {
		t.Errorf("root = %q, want r (the only member with a real upload budget)", ev.Topo.Root)
	}

	var want []string
	for _, n := range controlFleet() {
		want = append(want, peerID(sc.Net(), n.Name))
	}
	sort.Strings(want)
	if got := loop.cap.Recipients(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("pushed to %v, want every member %v", got, want)
	}
}

// projectionOf mirrors the coordinator's projection (no RTT, direct NAT) so Validate
// is asked about the same fleet the loop built from.
func projectionOf(net *Network) []overlay.Node {
	var out []overlay.Node
	for _, n := range net.OverlayNodes() {
		out = append(out, overlay.Node{Name: n.Name, UploadKbps: n.UploadKbps, NAT: overlay.NATDirect})
	}
	return out
}

// TestFencingHandover makes docs/PLAN.md §12.2's "Fencing | simnet + coordinator |
// Scenario" row true. It was previously the only row in that table describing
// something that did not exist: the scenario ran against a model of the coordinator,
// so no fencing behaviour of the shipped loop was covered from here at all.
//
// Two REAL coordinators over one meet, mid-handover.
func TestFencingHandover(t *testing.T) {
	sc := NewScenario(ScenarioConfig{Constraints: controlConstraints()})
	for _, n := range controlFleet() {
		sc.Net().Add(n)
	}
	c1 := startCoordinator(t, sc, "c1")
	c2 := startCoordinator(t, sc, "c2")

	// Both processes watch the same meet. Only one of them serves it at a time; that
	// is the whole thing being tested.
	sc.At(0, func() {
		for _, name := range sc.Reporters() {
			joinAndReport(t, c1, sc.Net(), name)
			joinAndReport(t, c2, sc.Net(), name)
		}
	})
	settled := coordinator.JoinSettle + 100*time.Millisecond
	sc.At(settled, func() {})

	// The arbiter mints epoch 2 and moves coordination from c1 to c2.
	handover := settled + time.Second
	sc.At(handover, func() {
		c1.c.Yield(testRoom, 2)
		c2.c.SetEpoch(testRoom, 2)
	})
	// c2's rebuild window closes on its deadline.
	afterRebuild := handover + metrics.RebuildWindow + 100*time.Millisecond
	sc.At(afterRebuild, func() {})

	if err := sc.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	t.Run("the demoted coordinator published under its own term first", func(t *testing.T) {
		evs := c1.rec.OfKind(coordinator.EventTopology)
		if len(evs) == 0 {
			t.Fatalf("c1 never published; kinds = %v", c1.rec.Kinds())
		}
		if got := evs[0].Epoch; got != 1 {
			t.Errorf("c1's first tree carried epoch %d, want 1", got)
		}
	})

	t.Run("the demoted coordinator publishes nothing after yielding", func(t *testing.T) {
		for _, ev := range c1.rec.OfKind(coordinator.EventTopology) {
			if ev.Epoch >= 2 {
				t.Errorf("c1 published under epoch %d after yielding at 2", ev.Epoch)
			}
		}
		for _, p := range c1.cap.Pushes() {
			if p.Topo != nil && p.Topo.Epoch >= 2 {
				t.Errorf("a push from the yielded c1 carried epoch %d; the outbound gate did not close", p.Topo.Epoch)
			}
		}
	})

	t.Run("the promoted coordinator publishes under the new term", func(t *testing.T) {
		var got *overlay.Topology
		for _, ev := range c2.rec.OfKind(coordinator.EventTopology) {
			if ev.Topo != nil && ev.Topo.Epoch == 2 {
				got = ev.Topo
			}
		}
		if got == nil {
			t.Fatalf("c2 published nothing under epoch 2; kinds = %v", c2.rec.Kinds())
		}
		if got.Rev != 1 {
			t.Errorf("the new term's first tree is rev %d, want 1 (rev resets with the epoch)", got.Rev)
		}
		cons := controlConstraints()
		cons.Root, cons.Epoch, cons.Rev = got.Root, got.Epoch, got.Rev
		if verr := validateTopology(got, projectionOf(sc.Net()), cons); verr != nil {
			t.Fatalf("the new term's tree fails Validate: %v", verr)
		}
	})
}

// TestStaleEpochDoesNotResumeAYieldedCoordinator: authority only ever moves forward.
// A re-broadcast of the announcement that DEMOTED a node, or of any earlier one, must
// not put it back in charge — only a strictly higher term does.
func TestStaleEpochDoesNotResumeAYieldedCoordinator(t *testing.T) {
	tests := []struct {
		name       string
		setEpoch   uint64
		wantResume bool
	}{
		{"the demoting announcement itself", 2, false},
		{"an earlier announcement", 1, false},
		{"a strictly higher term", 3, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sc := NewScenario(ScenarioConfig{Constraints: controlConstraints()})
			for _, n := range controlFleet() {
				sc.Net().Add(n)
			}
			loop := startCoordinator(t, sc, "c")

			sc.At(0, func() {
				for _, name := range sc.Reporters() {
					joinAndReport(t, loop, sc.Net(), name)
				}
			})
			settled := coordinator.JoinSettle + 100*time.Millisecond
			sc.At(settled, func() { loop.c.Yield(testRoom, 2) })
			sc.At(settled+time.Second, func() {
				loop.rec.Reset()
				loop.c.SetEpoch(testRoom, tc.setEpoch)
			})
			sc.At(settled+time.Second+metrics.RebuildWindow+100*time.Millisecond, func() {})

			if err := sc.Run(); err != nil {
				t.Fatalf("Run: %v", err)
			}
			resumed := len(loop.rec.OfKind(coordinator.EventTopology)) > 0
			if resumed != tc.wantResume {
				t.Errorf("after SetEpoch(%d) on a coordinator yielded at 2: resumed = %v, want %v (kinds %v)",
					tc.setEpoch, resumed, tc.wantResume, loop.rec.Kinds())
			}
		})
	}
}

// TestPeerFenceRefusalsAreCounted covers the other half of the fencing row: the
// PEER's fence. A peer refuses a stale-epoch instruction locally and reports its
// cumulative refusal count on its heartbeat; the coordinator must surface each RISE
// as one EventStale carrying that count and the peer's own (epoch, rev) — and must
// absorb a counter RESET, which is what a rejoining peer produces, without inventing
// a refusal that did not happen.
func TestPeerFenceRefusalsAreCounted(t *testing.T) {
	sc := NewScenario(ScenarioConfig{Constraints: controlConstraints()})
	for _, n := range controlFleet() {
		sc.Net().Add(n)
	}
	loop := startCoordinator(t, sc, "c")
	id := peerID(sc.Net(), "a")

	beat := func(seq, stale uint64) func() {
		return func() {
			loop.c.Heartbeat(testRoom, id, mustJSON(t, metrics.Heartbeat{
				Name: "a", Seq: seq, Epoch: 1, Rev: 1, StaleRejected: stale,
			}))
		}
	}
	sc.At(0, func() {
		for _, name := range sc.Reporters() {
			joinAndReport(t, loop, sc.Net(), name)
		}
	})
	settled := coordinator.JoinSettle + 100*time.Millisecond
	sc.At(settled, func() { loop.rec.Reset() })
	sc.At(settled+1*time.Second, beat(1, 3)) // first refusals observed
	sc.At(settled+2*time.Second, beat(2, 3)) // unchanged: a sample, not a transition
	sc.At(settled+3*time.Second, beat(3, 5)) // two more refusals
	sc.At(settled+4*time.Second, beat(4, 1)) // the peer rejoined; its fence restarted
	sc.At(settled+5*time.Second, beat(5, 2)) // and counts up again from the new base

	if err := sc.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var counts []uint64
	for _, ev := range loop.rec.OfKind(coordinator.EventStale) {
		if ev.Node != "a" {
			t.Errorf("EventStale named %q, want a", ev.Node)
		}
		if ev.NodeID != id {
			t.Errorf("EventStale carried NodeID %q, want %q", ev.NodeID, id)
		}
		if ev.Epoch != 1 {
			t.Errorf("EventStale carried epoch %d, want the PEER's own epoch 1", ev.Epoch)
		}
		counts = append(counts, ev.Count)
	}
	want := []uint64{3, 5, 2}
	if fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Errorf("stale-refusal counts = %v, want %v — one event per RISE, none for a repeat, none for a reset",
			counts, want)
	}
}

// TestModelAgreesWithTheRealCoordinator is the differential check the model owes for
// continuing to exist. Both loops are driven through the SAME scenario script on the
// settled path and must converge on the same tree; a model whose divergence from the
// shipped loop is untested is a liability, not a shortcut.
//
// It runs over every arrival permutation, so the agreement is not an accident of one
// ordering.
func TestModelAgreesWithTheRealCoordinator(t *testing.T) {
	names := make([]string, 0, len(controlFleet()))
	for _, n := range controlFleet() {
		names = append(names, n.Name)
	}
	for _, order := range permutations(names) {
		real := convergeReal(t, order)
		model := convergeModel(t, order)
		if real != model {
			t.Fatalf("order %v: the model and the shipped coordinator disagree\n  real: %s\n model: %s", order, real, model)
		}
	}
}

// convergeReal runs one arrival order through the shipped coordinator and returns its
// converged edge set.
func convergeReal(t *testing.T, order []string) string {
	t.Helper()
	sc := NewScenario(ScenarioConfig{Constraints: controlConstraints()})
	for _, n := range controlFleet() {
		sc.Net().Add(n)
	}
	sc.ReportOrder(order...)
	loop := startCoordinator(t, sc, "c")

	sc.At(0, func() {
		for _, name := range sc.Reporters() {
			joinAndReport(t, loop, sc.Net(), name)
		}
	})
	sc.At(coordinator.JoinSettle+100*time.Millisecond, func() {})
	if err := sc.Run(); err != nil {
		t.Fatalf("order %v: Run: %v", order, err)
	}
	ev, ok := loop.rec.Last(coordinator.EventTopology)
	if !ok || ev.Topo == nil {
		t.Fatalf("order %v: the shipped coordinator published no tree", order)
	}
	return string(mustJSON(t, ev.Topo.Edges))
}

// convergeModel runs the same arrival order through the model, configured to the same
// window and constraints, and returns its converged edge set.
func convergeModel(t *testing.T, order []string) string {
	t.Helper()
	sc := NewScenario(ScenarioConfig{Constraints: controlConstraints()})
	for _, n := range controlFleet() {
		sc.Net().Add(n)
	}
	sc.ReportOrder(order...)
	mc := newModelCoordinator(sc.Net(), sc.Clock(), sc.Clock().Now(), sc.Constraints(), coordinator.JoinSettle)
	defer mc.Stop()
	sc.AddBarrier("model", mc.Sync)

	sc.At(0, func() {
		for _, name := range sc.Reporters() {
			mc.Report(name)
		}
	})
	sc.At(coordinator.JoinSettle+100*time.Millisecond, func() {})
	if err := sc.Run(); err != nil {
		t.Fatalf("order %v: Run: %v", order, err)
	}
	if err := mc.Err(); err != nil {
		t.Fatalf("order %v: %v", order, err)
	}
	topo := mc.Published()
	if topo == nil {
		t.Fatalf("order %v: the model published no tree", order)
	}
	return string(mustJSON(t, topo.Edges))
}
