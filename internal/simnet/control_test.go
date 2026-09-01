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
		MaxDepth:   c.MaxDepth,
		StreamKbps: c.StreamKbps,
		// Stickiness() because coordinator.Config.StickinessMs is a *float64: nil is
		// the struct's zero value and resolves to the stability-preserving default,
		// so an explicit value — including 0 — must be constructed. Passing the
		// constraint's value keeps this harness and overlay.Constraints in agreement.
		StickinessMs:    coordinator.Stickiness(c.StickinessMs),
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

// joinAndReport delivers a member's join and its first telemetry frame together,
// which is what a peer that connects and immediately reports produces.
func joinAndReport(t *testing.T, loop *realLoop, net *Network, name string) {
	t.Helper()
	id := peerID(net, name)
	loop.c.PeerJoined(testRoom, id, name)
	loop.c.Metrics(testRoom, id, mustJSON(t, ReportOf(nodeNamed(net, name))))
}

// joinAll announces every member without any telemetry, and reportAll then delivers
// the first reports in the scenario's arrival order.
//
// The split is the whole reason these scenarios are meaningful, and it is a
// distinction the first version of this file got wrong. JOIN order and REPORT order
// are two different variables: the loop rebuilds on every membership threshold, so a
// meet that grows one peer at a time legitimately gets an incremental, sticky build
// history, and its final tree is a function of that history. Report-order invariance
// (docs/PLAN.md §12.3) is a claim about the SETTLED path — a known roster whose
// telemetry arrives in some order — and only this split can pose that question.
func joinAll(t *testing.T, loop *realLoop, net *Network) {
	t.Helper()
	for _, name := range net.order {
		loop.c.PeerJoined(testRoom, peerID(net, name), name)
	}
}

func reportAll(t *testing.T, loop *realLoop, net *Network, order []string) {
	t.Helper()
	for _, name := range order {
		loop.c.Metrics(testRoom, peerID(net, name), mustJSON(t, ReportOf(nodeNamed(net, name))))
	}
}

func nodeNamed(net *Network, name string) overlay.Node {
	for _, n := range net.OverlayNodes() {
		if n.Name == name {
			return n
		}
	}
	return overlay.Node{Name: name}
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

	sc.At(0, func() { joinAll(t, loop, sc.Net()) })
	sc.At(100*time.Millisecond, func() { reportAll(t, loop, sc.Net(), sc.Reporters()) })
	// Past the join settle, so the first build's eligibility window has closed even
	// if the early exit had not already fired.
	sc.At(coordinator.JoinSettle+200*time.Millisecond, func() {})

	if err := sc.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	built := loop.rec.OfKind(coordinator.EventTopology)
	if len(built) == 0 {
		t.Fatalf("the coordinator published no tree; events = %v", loop.rec.Kinds())
	}
	// Exactly one: no tree is published while the roster is still settling, and the
	// early exit then fires once, when the last report lands.
	if len(built) != 1 {
		t.Errorf("published %d trees on the settled path, want 1 (revs %v)", len(built), revsOf(built))
	}
	ev := built[0]
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
		joinAll(t, c1, sc.Net())
		joinAll(t, c2, sc.Net())
	})
	sc.At(100*time.Millisecond, func() {
		reportAll(t, c1, sc.Net(), sc.Reporters())
		reportAll(t, c2, sc.Net(), sc.Reporters())
	})
	settled := coordinator.JoinSettle + 200*time.Millisecond
	sc.At(settled, func() {})

	// The arbiter mints epoch 2 and moves coordination from c1 to c2.
	var beforeYield []coordinator.Event
	handover := settled + time.Second
	sc.At(handover, func() {
		beforeYield = c1.rec.OfKind(coordinator.EventTopology)
		c1.rec.Reset()
		c1.cap.Reset()
		c1.c.Yield(testRoom, 2)
		c2.c.SetEpoch(testRoom, 2)
	})

	// REAL CHURN after the handover. Without it this test proves nothing: a
	// coordinator with nothing to rebuild for is silent whether or not it yielded,
	// so "c1 published nothing" would be satisfied by a Yield that does not demote.
	// A departure is a threshold event both processes observe, and c2 — which DID
	// take the term — publishing for it is the control that proves the event was
	// live.
	sc.At(handover+200*time.Millisecond, func() {
		gone := peerID(sc.Net(), "c")
		c1.c.PeerLeft(testRoom, gone)
		c2.c.PeerLeft(testRoom, gone)
		sc.Net().Leave("c")
	})
	// Past c2's rebuild window AND past every recompute cooldown, so neither loop is
	// merely being throttled at the moment the assertions run.
	sc.At(handover+8*time.Second, func() {})

	if err := sc.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	t.Run("the demoted coordinator published under its own term first", func(t *testing.T) {
		if len(beforeYield) == 0 {
			t.Fatalf("c1 never published before the handover; kinds = %v", c1.rec.Kinds())
		}
		if got := beforeYield[0].Epoch; got != 1 {
			t.Errorf("c1's first tree carried epoch %d, want 1", got)
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
		if contains(got.Nodes(), "c") {
			t.Errorf("the departed member c is still in the new term's tree %v", got.Edges)
		}
		cons := controlConstraints()
		cons.Root, cons.Epoch, cons.Rev = got.Root, got.Epoch, got.Rev
		if verr := validateTopology(got, projectionOf(sc.Net()), cons); verr != nil {
			t.Fatalf("the new term's tree fails Validate: %v", verr)
		}
	})

	t.Run("the demoted coordinator publishes nothing, even on churn", func(t *testing.T) {
		if evs := c1.rec.OfKind(coordinator.EventTopology); len(evs) != 0 {
			t.Errorf("the yielded c1 published %d tree(s) (epochs %v); Yield must stop the tree",
				len(evs), epochsOf(evs))
		}
		if p := c1.cap.Pushes(); len(p) != 0 {
			t.Errorf("the yielded c1 pushed %d topolog(ies); the outbound gate did not close", len(p))
		}
	})

	t.Run("the demoted coordinator keeps observing", func(t *testing.T) {
		// Yield drops what this node BELIEVED (the tree), not what it OBSERVED. A
		// yielded coordinator that stopped tracking membership would make a
		// re-election pay a full settle for nothing.
		var sawDeparture bool
		for _, ev := range c1.rec.OfKind(coordinator.EventMember) {
			if ev.Node == "c" && !ev.Present {
				sawDeparture = true
			}
		}
		if !sawDeparture {
			t.Errorf("the yielded c1 stopped observing membership; kinds = %v", c1.rec.Kinds())
		}
	})
}

// epochsOf lists the epochs of a run of events, for a failure message that says what
// the loop actually did.
func epochsOf(evs []coordinator.Event) []uint64 {
	var out []uint64
	for _, ev := range evs {
		out = append(out, ev.Epoch)
	}
	return out
}

// contains reports whether names holds name.
func contains(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
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
		real, _ := convergeReal(t, order)
		model := convergeModel(t, order)
		if real != model {
			t.Fatalf("order %v: the model and the shipped coordinator disagree\n  real: %s\n model: %s", order, real, model)
		}
	}
}

// convergeReal runs one report-arrival order through the shipped coordinator on the
// SETTLED path — every member joins first, then telemetry arrives in `order` — and
// returns its converged edge set and how many trees it published getting there.
func convergeReal(t *testing.T, order []string) (string, int) {
	t.Helper()
	sc := NewScenario(ScenarioConfig{Constraints: controlConstraints()})
	for _, n := range controlFleet() {
		sc.Net().Add(n)
	}
	sc.ReportOrder(order...)
	loop := startCoordinator(t, sc, "c")

	sc.At(0, func() { joinAll(t, loop, sc.Net()) })
	sc.At(100*time.Millisecond, func() { reportAll(t, loop, sc.Net(), sc.Reporters()) })
	sc.At(coordinator.JoinSettle+200*time.Millisecond, func() {})
	if err := sc.Run(); err != nil {
		t.Fatalf("order %v: Run: %v", order, err)
	}
	built := loop.rec.OfKind(coordinator.EventTopology)
	if len(built) == 0 || built[len(built)-1].Topo == nil {
		t.Fatalf("order %v: the shipped coordinator published no tree", order)
	}
	return string(mustJSON(t, built[len(built)-1].Topo.Edges)), len(built)
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

// revsOf lists the revisions of a run of topology events, for a failure message that
// says what the loop actually did.
func revsOf(evs []coordinator.Event) []uint64 {
	var out []uint64
	for _, ev := range evs {
		out = append(out, ev.Rev)
	}
	return out
}

// TestRealCoordinatorReportOrderInvariance is docs/PLAN.md §12.3's mandatory property
// asserted against the SHIPPED loop rather than against a model of it: for a known
// roster, no permutation of first-telemetry arrival order may change the converged
// tree, nor the number of trees published on the way there.
//
// This is the direct encoding of the root-flap defect where the recompute was a pure
// function of which WebSocket frame arrived first. simnet already asserted it over
// the model; until now nothing asserted it over the code that ships.
func TestRealCoordinatorReportOrderInvariance(t *testing.T) {
	names := make([]string, 0, len(controlFleet()))
	for _, n := range controlFleet() {
		names = append(names, n.Name)
	}
	perms := permutations(names)
	if len(perms) != 120 {
		t.Fatalf("expected 120 permutations, got %d", len(perms))
	}
	want, wantBuilds := "", -1
	var wantOrder []string
	for _, order := range perms {
		got, builds := convergeReal(t, order)
		if want == "" {
			want, wantBuilds, wantOrder = got, builds, order
			continue
		}
		if got != want {
			t.Fatalf("report arrival order changed the shipped loop's converged tree\n order %v: %s\n order %v: %s",
				wantOrder, want, order, got)
		}
		if builds != wantBuilds {
			t.Fatalf("report arrival order changed how many trees were published: order %v got %d, order %v got %d",
				wantOrder, wantBuilds, order, builds)
		}
	}
	if wantBuilds != 1 {
		t.Errorf("the settled path published %d trees, want exactly 1", wantBuilds)
	}
}

// TestJoinOrderIsPathDependent is the CHARACTERIZATION that keeps the invariance
// claim above from being read too broadly. When members arrive one at a time WITH
// their telemetry, the loop rebuilds on every join — correctly, since each new member
// has to be placed — so the final tree is a function of the join history. That is the
// minimal-disruption property working as designed, not a defect, and it is the reason
// report order and join order must never be conflated in a scenario.
func TestJoinOrderIsPathDependent(t *testing.T) {
	names := make([]string, 0, len(controlFleet()))
	for _, n := range controlFleet() {
		names = append(names, n.Name)
	}
	seen := map[string]bool{}
	roots := map[string]bool{}
	for _, order := range permutations(names) {
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
		sc.At(coordinator.JoinSettle+200*time.Millisecond, func() {})
		if err := sc.Run(); err != nil {
			t.Fatalf("order %v: Run: %v", order, err)
		}
		ev, ok := loop.rec.Last(coordinator.EventTopology)
		if !ok || ev.Topo == nil {
			t.Fatalf("order %v: no tree", order)
		}
		cons := controlConstraints()
		cons.Root, cons.Epoch, cons.Rev = ev.Topo.Root, ev.Topo.Epoch, ev.Topo.Rev
		if verr := validateTopology(ev.Topo, projectionOf(sc.Net()), cons); verr != nil {
			t.Fatalf("order %v: incremental build fails Validate: %v", order, verr)
		}
		seen[string(mustJSON(t, ev.Topo.Edges))] = true
		roots[ev.Topo.Root] = true
	}
	if len(seen) < 2 {
		t.Fatalf("every join order converged to the same tree; the incremental build history no longer influences the result, so the stability trade-off has changed")
	}
	if len(roots) != 1 {
		t.Errorf("join order changed the converged ROOT (%d distinct); root stickiness is damped by RootChangeMarginKbps, not by history", len(roots))
	}
	t.Logf("join-order path dependence: %d distinct converged trees over 120 orders, one root", len(seen))
}

// goneAfter/degradedAfter are the liveness thresholds for the failover scenario. They
// are short in VIRTUAL time — the scenario steps past them deliberately — and the
// only reason they are stated at all is that the other scenarios push liveness out of
// reach so their assertions cannot be accidentally about the health FSM.
const (
	degradedAfter = 2 * time.Second
	goneAfter     = 4 * time.Second
)

// TestGoneRelayIsRepairedOutOfTheTree is the Phase 5 story end to end through the
// SHIPPED loop: a relay stops heartbeating, the health FSM declares it gone, and the
// coordinator repairs the tree by re-attaching exactly its orphans.
//
// It is the scenario that distinguishes a KILL from a LEAVE at the control plane. A
// departure is announced and removes the member immediately; nobody tells the
// coordinator about a machine that vanished, so only the liveness timers notice — and
// the mechanism that then removes it is the projection dropping a HealthGone node,
// which is what makes the general builder move its subtree and nobody else.
func TestGoneRelayIsRepairedOutOfTheTree(t *testing.T) {
	sc := NewScenario(ScenarioConfig{Constraints: controlConstraints()})
	for _, n := range controlFleet() {
		sc.Net().Add(n)
	}
	rec, cap := NewRecorder(), NewCapture()
	cfg := controlConfig(sc.Clock())
	cfg.DegradedAfter, cfg.GoneAfter = degradedAfter, goneAfter
	c := coordinator.New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, cap, rec)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	sc.AddBarrier("c", c.Sync)

	loop := &realLoop{c: c, rec: rec, cap: cap}
	sc.At(0, func() { joinAll(t, loop, sc.Net()) })
	sc.At(100*time.Millisecond, func() { reportAll(t, loop, sc.Net(), sc.Reporters()) })

	settled := coordinator.JoinSettle + 200*time.Millisecond
	var before *overlay.Topology
	sc.At(settled, func() {
		ev, ok := loop.rec.Last(coordinator.EventTopology)
		if !ok || ev.Topo == nil {
			t.Fatalf("no first tree; kinds = %v", loop.rec.Kinds())
		}
		before = ev.Topo
	})

	// Everyone keeps beating except "s", the relay. Beats run past goneAfter and past
	// the recompute cooldown so the repair is not merely being throttled.
	for i := 1; i <= 12; i++ {
		at := settled + time.Duration(i)*time.Second
		seq := uint64(i)
		sc.At(at, func() {
			for _, n := range controlFleet() {
				if n.Name == "s" {
					continue // the vanished machine
				}
				loop.c.Heartbeat(testRoom, peerID(sc.Net(), n.Name), mustJSON(t, metrics.Heartbeat{
					Name: n.Name, Seq: seq, Epoch: 1, Rev: 1,
				}))
			}
		})
	}

	if err := sc.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if before == nil || !before.IsRelay("s") {
		t.Fatalf("the fixture no longer makes s a relay; first tree = %v", before)
	}
	orphans := before.ChildrenOf("s")

	t.Run("the vanished relay is declared gone", func(t *testing.T) {
		var got string
		for _, ev := range loop.rec.OfKind(coordinator.EventHealth) {
			if ev.Node == "s" && ev.Health == coordinator.HealthGone {
				got = string(ev.Health)
			}
		}
		if got == "" {
			t.Errorf("s never reached %q; only the liveness timers can notice a vanished machine", coordinator.HealthGone)
		}
	})

	t.Run("failover names exactly the orphans", func(t *testing.T) {
		ev, ok := loop.rec.Last(coordinator.EventFailover)
		if !ok {
			t.Fatalf("no failover event; kinds = %v", loop.rec.Kinds())
		}
		if ev.Node != "s" {
			t.Errorf("failover named %q, want s", ev.Node)
		}
		want := append([]string(nil), orphans...)
		sort.Strings(want)
		if fmt.Sprint(ev.Orphans) != fmt.Sprint(want) {
			t.Errorf("failover orphans = %v, want %v (exactly what s was carrying)", ev.Orphans, want)
		}
	})

	t.Run("the repaired tree drops it and moves only its subtree", func(t *testing.T) {
		ev, ok := loop.rec.Last(coordinator.EventTopology)
		if !ok || ev.Topo == nil {
			t.Fatal("no tree after the failover")
		}
		after := ev.Topo
		if contains(after.Nodes(), "s") || after.Root == "s" {
			t.Fatalf("the gone relay s is still in the tree %v; the projection did not drop it", after.Edges)
		}
		for _, o := range orphans {
			if after.Depth(o) < 0 {
				t.Errorf("orphan %q was not re-attached", o)
			}
		}
		var nodes []overlay.Node
		for _, n := range projectionOf(sc.Net()) {
			if n.Name != "s" {
				nodes = append(nodes, n)
			}
		}
		cons := controlConstraints()
		cons.Root, cons.Epoch, cons.Rev = after.Root, after.Epoch, after.Rev
		if verr := validateTopology(after, nodes, cons); verr != nil {
			t.Fatalf("the repaired tree fails Validate: %v", verr)
		}
		if rerr := validateRepair(before, after, nodes, cons, overlay.Churn{Gone: []string{"s"}}); rerr != nil {
			t.Errorf("the repair moved more than one departure justifies: %v", rerr)
		}
	})
}
