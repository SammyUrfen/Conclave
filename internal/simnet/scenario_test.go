package simnet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestScenarioStepOrdering: steps run in VIRTUAL-TIME order regardless of the order
// they were registered in, and ties are broken by registration order — never by map
// or goroutine scheduling.
func TestScenarioStepOrdering(t *testing.T) {
	tests := []struct {
		name  string
		times []time.Duration
		want  string
	}{
		{"already in order", []time.Duration{0, 10, 20}, "s0 s1 s2"},
		{"registered out of order", []time.Duration{30, 10, 20}, "s1 s2 s0"},
		{"same instant keeps registration order", []time.Duration{10, 10, 10}, "s0 s1 s2"},
		{"mixed", []time.Duration{20, 5, 20, 1}, "s3 s1 s0 s2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sc := NewScenario(ScenarioConfig{})
			var got []string
			for i, d := range tc.times {
				i := i
				sc.At(d*time.Millisecond, func() { got = append(got, fmt.Sprintf("s%d", i)) })
			}
			if err := sc.Run(); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if strings.Join(got, " ") != tc.want {
				t.Errorf("step order = %q, want %q", strings.Join(got, " "), tc.want)
			}
			last := tc.times[0]
			for _, d := range tc.times {
				if d > last {
					last = d
				}
			}
			if adv := sc.Clock().Now().Sub(DefaultStart); adv != last*time.Millisecond {
				t.Errorf("clock advanced to %v, want %v", adv, last*time.Millisecond)
			}
		})
	}
}

// TestScenarioStartDefaultsToAFixedEpoch: a scenario seeded from the wall clock
// would not replay, so the zero ScenarioConfig must pin a literal instant.
func TestScenarioStartDefaultsToAFixedEpoch(t *testing.T) {
	a := NewScenario(ScenarioConfig{})
	b := NewScenario(ScenarioConfig{})
	if !a.Clock().Now().Equal(b.Clock().Now()) {
		t.Fatalf("two fresh scenarios started at different instants: %v vs %v", a.Clock().Now(), b.Clock().Now())
	}
	if !a.Clock().Now().Equal(DefaultStart) {
		t.Errorf("start = %v, want DefaultStart %v", a.Clock().Now(), DefaultStart)
	}
	custom := time.Date(2030, 6, 1, 12, 0, 0, 0, time.UTC)
	if got := NewScenario(ScenarioConfig{Start: custom}).Clock().Now(); !got.Equal(custom) {
		t.Errorf("explicit Start ignored: got %v, want %v", got, custom)
	}
}

// TestScenarioObserve: the observer is how a test records a trace, so it must fire
// on every settle — which is after every step and after every mid-Advance timer
// fire — with the virtual offset of that moment.
func TestScenarioObserve(t *testing.T) {
	sc := NewScenario(ScenarioConfig{})
	var seen []time.Duration
	sc.Observe(func(at time.Duration) { seen = append(seen, at) })
	sc.At(10*time.Millisecond, func() {})
	sc.At(25*time.Millisecond, func() {})
	if err := sc.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(seen) < 2 {
		t.Fatalf("observer fired %d times, want at least one per step", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] < seen[i-1] {
			t.Fatalf("observed offsets went backward: %v", seen)
		}
	}
	if seen[len(seen)-1] != 25*time.Millisecond {
		t.Errorf("last observation at %v, want 25ms", seen[len(seen)-1])
	}
}

// TestScenarioReportOrder pins the resolution rule: listed names first in the listed
// order, then everyone else in Network insertion order. Unknown names are dropped
// rather than silently inventing a member.
func TestScenarioReportOrder(t *testing.T) {
	tests := []struct {
		name  string
		order []string
		want  string
	}{
		{"unset falls back to insertion order", nil, "a b c d"},
		{"full permutation is honoured", []string{"d", "c", "b", "a"}, "d c b a"},
		{"partial list, rest in insertion order", []string{"c"}, "c a b d"},
		{"unknown names are ignored", []string{"zz", "b"}, "b a c d"},
		{"a repeat is listed once", []string{"b", "b", "d"}, "b d a c"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sc := NewScenario(ScenarioConfig{})
			for _, n := range []string{"a", "b", "c", "d"} {
				sc.Net().Add(overlay.Node{Name: n, UploadKbps: 4000})
			}
			sc.ReportOrder(tc.order...)
			if got := strings.Join(sc.Reporters(), " "); got != tc.want {
				t.Errorf("Reporters() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestScenarioRunReturnsFirstFailure: a scenario that keeps running after a failed
// step produces cascading noise, so Run stops at the first reported error.
func TestScenarioRunReturnsFirstFailure(t *testing.T) {
	sc := NewScenario(ScenarioConfig{})
	ran := 0
	sc.At(10*time.Millisecond, func() { ran++; sc.Fail(errors.New("boom")) })
	sc.At(20*time.Millisecond, func() { ran++ })
	err := sc.Run()
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Run err = %v, want the step's error", err)
	}
	if ran != 1 {
		t.Errorf("%d steps ran, want 1 (Run must stop at the first failure)", ran)
	}
}

// TestScenarioSettleTimesOut: SettleTimeout is a DEADLOCK GUARD. A barrier that
// never returns must fail the run rather than hang it forever, and the timeout must
// be overridable so the guard itself is testable in well under a second.
func TestScenarioSettleTimesOut(t *testing.T) {
	sc := NewScenario(ScenarioConfig{}).SettleTimeout(20 * time.Millisecond)
	sc.AddBarrier("wedged", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	sc.At(time.Millisecond, func() {})
	err := sc.Run()
	if err == nil {
		t.Fatal("Run succeeded; a wedged barrier must fail the scenario")
	}
	if !strings.Contains(err.Error(), "wedged") {
		t.Errorf("err = %v, want it to name the barrier that wedged", err)
	}
}

// TestScenarioSettleTimeoutDefault: the const is the frozen default (docs/PLAN.md
// §4.5) — far beyond any legitimate in-memory round trip, so exceeding it means a
// deadlock rather than slowness.
func TestScenarioSettleTimeoutDefault(t *testing.T) {
	if SettleTimeout != 5*time.Second {
		t.Errorf("SettleTimeout = %v, want 5s", SettleTimeout)
	}
}

// convergeFleet is WI-1's counterexample fleet: one strong node and four identical
// weak ones, so the root is unambiguous but parent assignment is contested.
func convergeFleet() []overlay.Node {
	return []overlay.Node{
		{Name: "A", UploadKbps: 4000},
		{Name: "B", UploadKbps: 4000},
		{Name: "C", UploadKbps: 12000},
		{Name: "D", UploadKbps: 4000},
		{Name: "E", UploadKbps: 4000},
	}
}

// runReportScenario drives one full first-build round: `inside` members report
// within the settle window, the rest straggle in one at a time AFTER it, each
// triggering a stability-preserving rebuild. It returns the converged tree, the
// number of published trees, and the trace.
func runReportScenario(t *testing.T, order []string, stragglers int) (*overlay.Topology, int, []modelStep) {
	t.Helper()
	const settleFor = 200 * time.Millisecond

	sc := NewScenario(ScenarioConfig{
		Seed:        5,
		Constraints: overlay.Constraints{MaxDepth: 2, StreamKbps: 2000, StickinessMs: overlay.DefaultStickinessMs},
	})
	for _, n := range convergeFleet() {
		sc.Net().Add(n)
	}
	sc.ReportOrder(order...)

	mc := newModelCoordinator(sc.Net(), sc.Clock(), sc.Clock().Now(), sc.Constraints(), settleFor)
	defer mc.Stop()
	sc.AddBarrier("coordinator", mc.Sync)

	names := sc.Reporters()
	split := len(names) - stragglers
	sc.At(10*time.Millisecond, func() {
		for _, n := range names[:split] {
			mc.Report(n)
		}
	})
	for i, n := range names[split:] {
		n := n
		sc.At(settleFor+time.Duration(i+1)*50*time.Millisecond, func() { mc.Report(n) })
	}

	if err := sc.Run(); err != nil {
		t.Fatalf("order %v: Run: %v", order, err)
	}
	if err := mc.Err(); err != nil {
		t.Fatalf("order %v: %v", order, err)
	}
	topo := mc.Published()
	if topo == nil {
		t.Fatalf("order %v: the run never converged on a tree", order)
	}
	return topo, mc.Builds(), mc.Trace()
}

// TestReportOrderInvarianceSettledPath is §12.3's mandatory property, driven through
// the harness rather than through BuildTree directly: when every member reports
// INSIDE the settle window, no permutation of arrival order may change the converged
// tree — nor even the number of trees published on the way there.
//
// It is the direct encoding of the root-flap defect in §3.8, where the discarded
// recompute was a pure function of which WebSocket frame landed first.
func TestReportOrderInvarianceSettledPath(t *testing.T) {
	perms := permutations(fleetNames())
	if len(perms) != 120 {
		t.Fatalf("expected 120 permutations, got %d", len(perms))
	}
	var want string
	var wantOrder []string
	wantBuilds := -1
	for _, order := range perms {
		topo, builds, _ := runReportScenario(t, order, 0)
		blob, err := json.Marshal(topo.Edges)
		if err != nil {
			t.Fatal(err)
		}
		if want == "" {
			want, wantOrder, wantBuilds = string(blob), order, builds
			continue
		}
		if string(blob) != want {
			t.Fatalf("arrival order changed the converged tree\n order %v: %s\n order %v: %s", wantOrder, want, order, blob)
		}
		if builds != wantBuilds {
			t.Fatalf("arrival order changed the number of published trees: order %v got %d, order %v got %d",
				wantOrder, wantBuilds, order, builds)
		}
	}
}

// TestStragglerPathIsPathDependent is a CHARACTERIZATION test, not a wish. Once a
// member reports AFTER the settle window, stickiness makes the converged tree a
// function of HISTORY — that is exactly what was traded for minimal-disruption
// rebuilds, and the two cannot both hold (WI-1 proved it: 120 permutations, 9
// distinct valid trees, one root).
//
// So this asserts what IS true: every converged tree is Validate-clean, holds every
// member, and agrees on the ROOT (root stickiness is damped by RootChangeMarginKbps,
// not by history) — and it deliberately asserts that at least two permutations
// DIFFER, so that if the divergence ever disappears the stability trade-off has
// silently changed and docs/PLAN.md §3.4 must be revisited.
func TestStragglerPathIsPathDependent(t *testing.T) {
	perms := permutations(fleetNames())
	seen := map[string][]string{}
	root := ""
	for _, order := range perms {
		topo, _, _ := runReportScenario(t, order, 2)
		if got := len(topo.Nodes()); got != len(convergeFleet()) {
			t.Fatalf("order %v: converged tree holds %d of %d members", order, got, len(convergeFleet()))
		}
		if root == "" {
			root = topo.Root
		} else if topo.Root != root {
			t.Fatalf("order %v: converged root %q, want the stable %q", order, topo.Root, root)
		}
		blob, err := json.Marshal(topo.Edges)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := seen[string(blob)]; !ok {
			seen[string(blob)] = order
		}
	}
	if len(seen) < 2 {
		t.Fatalf("every permutation converged to the same tree; the documented path-dependence of the straggler path is gone — docs/PLAN.md §3.4 needs revisiting")
	}
	t.Logf("straggler path: %d distinct converged trees over %d permutations, root %q", len(seen), len(perms), root)
}

// TestScenarioReplayIsDeterministic is property (c): the same seed and the same
// event SEQUENCE yield a byte-identical trace. It is the weaker, TRUE cousin of
// "the same fleet always yields the same tree", which stickiness makes false.
func TestScenarioReplayIsDeterministic(t *testing.T) {
	order := []string{"E", "A", "C", "B", "D"}
	run := func() string {
		_, _, trace := runReportScenario(t, order, 2)
		blob, err := json.Marshal(trace)
		if err != nil {
			t.Fatal(err)
		}
		return string(blob)
	}
	first := run()
	for i := 0; i < 4; i++ {
		if again := run(); again != first {
			t.Fatalf("replay %d diverged:\n first: %s\n again: %s", i, first, again)
		}
	}
}

func fleetNames() []string {
	out := make([]string, 0, len(convergeFleet()))
	for _, n := range convergeFleet() {
		out = append(out, n.Name)
	}
	return out
}

// permutations returns every ordering of names, in a deterministic sequence (never a
// map range) so a failure names a reproducible order.
func permutations(names []string) [][]string {
	if len(names) <= 1 {
		return [][]string{append([]string(nil), names...)}
	}
	var out [][]string
	for i := range names {
		rest := make([]string, 0, len(names)-1)
		rest = append(rest, names[:i]...)
		rest = append(rest, names[i+1:]...)
		for _, p := range permutations(rest) {
			out = append(out, append([]string{names[i]}, p...))
		}
	}
	return out
}
