package coordinator

// White-box unit tests for the pieces that have no clock and no goroutine: the
// working-copy derivation of §5.6a and the per-node threshold arithmetic of §5.3.
//
// This file deliberately imports NOTHING that could ever import coordinator back —
// in particular not simnet, which docs/PLAN.md §2.3 permits to depend on this
// package. The clock-driven tests live in package coordinator_test for exactly
// that reason.

import (
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

func edges(pairs ...string) []overlay.Edge {
	if len(pairs)%2 != 0 {
		panic("edges: want parent/child pairs")
	}
	out := make([]overlay.Edge, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, overlay.Edge{Parent: pairs[i], Child: pairs[i+1]})
	}
	return out
}

func edgeString(t *overlay.Topology) string {
	s := ""
	for _, e := range t.Edges {
		s += e.Parent + ">" + e.Child + " "
	}
	return s
}

// TestDeriveWorking pins §5.6a's middle tree: published patched with everything the
// coordinator already knows changed — departures removed, ratified promotions
// applied — and nothing else.
func TestDeriveWorking(t *testing.T) {
	published := &overlay.Topology{
		Epoch: 1, Rev: 4, Root: "a",
		Edges:   edges("a", "x", "x", "b", "a", "c", "x", "d"),
		Backups: []overlay.Backup{{Node: "b", Parent: "a"}},
	}

	cases := []struct {
		name       string
		live       []string
		promotions map[string]string
		want       string
	}{
		{
			name: "nothing changed is a faithful copy",
			live: []string{"a", "x", "b", "c", "d"},
			want: "a>x x>b a>c x>d ",
		},
		{
			name: "a departed relay takes only its own edges",
			live: []string{"a", "b", "c", "d"},
			want: "a>c ",
		},
		{
			name: "a departed leaf takes only its own edge",
			live: []string{"a", "x", "b", "c"},
			want: "a>x x>b a>c ",
		},
		{
			name:       "a ratified promotion replaces the parent in place",
			live:       []string{"a", "x", "b", "c", "d"},
			promotions: map[string]string{"b": "a"},
			want:       "a>x a>b a>c x>d ",
		},
		{
			name:       "a promotion onto a node that is itself gone is ignored",
			live:       []string{"a", "x", "b", "c"},
			promotions: map[string]string{"b": "d"},
			want:       "a>x x>b a>c ",
		},
		{
			// b and d were orphaned by x and are re-admitted at their published
			// positions AFTER the edges that survived intact — deterministic, and
			// still parents-first, which is all BuildTree replays it for.
			name:       "departure and promotion compose",
			live:       []string{"a", "b", "c", "d"},
			promotions: map[string]string{"b": "a", "d": "c"},
			want:       "a>c a>b c>d ",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			live := map[string]bool{}
			for _, n := range tc.live {
				live[n] = true
			}
			got := deriveWorking(published, live, tc.promotions)
			if got == nil {
				t.Fatal("deriveWorking returned nil for a non-nil published tree")
			}
			if s := edgeString(got); s != tc.want {
				t.Fatalf("edges = %q, want %q", s, tc.want)
			}
			if got.Root != published.Root || got.Epoch != published.Epoch || got.Rev != published.Rev {
				t.Fatalf("Root/Epoch/Rev must be carried over, got %+v", got)
			}
			// The working copy is never sent, but it must never alias published
			// either: a rebuild that mutated it would corrupt the oracle's `prev`.
			if len(got.Edges) > 0 && len(published.Edges) > 0 && &got.Edges[0] == &published.Edges[0] {
				t.Fatal("deriveWorking must not alias published's edge slice")
			}
		})
	}
}

// TestDeriveWorkingIsTopological: BuildTree replays prev.Edges to process parents
// before children, so a promotion that moves a node under a parent listed LATER
// must be re-ordered, or the builder attaches the child to a parent it has not
// placed yet and the ratification is silently undone.
func TestDeriveWorkingIsTopological(t *testing.T) {
	published := &overlay.Topology{
		Epoch: 1, Rev: 2, Root: "r",
		Edges: edges("r", "u", "r", "p"), // u is listed before p
	}
	got := deriveWorking(published, map[string]bool{"r": true, "u": true, "p": true},
		map[string]string{"u": "p"})

	placed := map[string]bool{"r": true}
	for i, e := range got.Edges {
		if !placed[e.Parent] {
			t.Fatalf("edge %d (%s>%s) names a parent that has not been placed: %v", i, e.Parent, e.Child, got.Edges)
		}
		placed[e.Child] = true
	}
	if got.ParentOf("u") != "p" {
		t.Fatalf("the promotion was lost: %v", got.Edges)
	}
}

// TestGoneThreshold pins the per-node arithmetic of §5.3, including the v2.2 floor.
func TestGoneThreshold(t *testing.T) {
	const socket = 7 * time.Second
	cases := []struct {
		name     string
		cfg      Config
		interval time.Duration
		want     time.Duration
	}{
		{
			name:     "derived from the declared cadence",
			interval: metrics.HeartbeatInterval,
			want:     metrics.GoneAfter(metrics.HeartbeatInterval),
		},
		{
			name:     "a slow peer gets a proportionally longer threshold",
			interval: 5 * time.Second,
			want:     metrics.GoneAfter(5 * time.Second),
		},
		{
			name:     "an undeclared cadence falls back to the default",
			interval: 0,
			want:     metrics.GoneAfter(metrics.HeartbeatInterval),
		},
		{
			name:     "a fast peer is FLOORED at the socket-detection window",
			cfg:      Config{SocketDetection: socket},
			interval: 500 * time.Millisecond,
			want:     socket,
		},
		{
			name:     "the floor never SHORTENS a slower peer's threshold",
			cfg:      Config{SocketDetection: socket},
			interval: 5 * time.Second,
			want:     metrics.GoneAfter(5 * time.Second),
		},
		{
			name:     "an explicit override still respects the floor",
			cfg:      Config{GoneAfter: time.Second, SocketDetection: socket},
			interval: metrics.HeartbeatInterval,
			want:     socket,
		},
		{
			name:     "zero socket detection disables the floor, which is what simnet wants",
			cfg:      Config{SocketDetection: 0},
			interval: 500 * time.Millisecond,
			want:     metrics.GoneAfter(500 * time.Millisecond),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Coordinator{cfg: normalize(tc.cfg)}
			if got := c.goneThreshold(&nodeState{interval: tc.interval}); got != tc.want {
				t.Fatalf("goneThreshold = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDegradedThreshold: the same cadence rule, without the floor — degraded is
// advisory and nothing acts on it, so it cannot eject anyone.
func TestDegradedThreshold(t *testing.T) {
	c := &Coordinator{cfg: normalize(Config{SocketDetection: time.Hour})}
	got := c.degradedThreshold(&nodeState{interval: 500 * time.Millisecond})
	if want := metrics.DegradedAfter(500 * time.Millisecond); got != want {
		t.Fatalf("degradedThreshold = %v, want %v (the floor is a GONE-threshold rule only)", got, want)
	}
}

// TestNormalizeFillsDefaults: every zero-valued knob resolves to its documented
// constant, so a caller may pass only what it wants to change.
func TestNormalizeFillsDefaults(t *testing.T) {
	cfg := normalize(Config{})
	if cfg.Dwell != DegradationDwell {
		t.Errorf("Dwell = %v, want %v", cfg.Dwell, DegradationDwell)
	}
	if cfg.RecomputeCooldown != RecomputeCooldown {
		t.Errorf("RecomputeCooldown = %v, want %v", cfg.RecomputeCooldown, RecomputeCooldown)
	}
	if cfg.JoinSettle != JoinSettle {
		t.Errorf("JoinSettle = %v, want %v", cfg.JoinSettle, JoinSettle)
	}
	if cfg.StickinessMs != overlay.DefaultStickinessMs {
		t.Errorf("StickinessMs = %v, want %v", cfg.StickinessMs, overlay.DefaultStickinessMs)
	}
	if cfg.Clock == nil {
		t.Error("Clock must default to the system clock")
	}
	if cfg.DefaultUploadKbps != 0 {
		t.Errorf("DefaultUploadKbps must stay 0: an unproven peer is a leaf, never a relay; got %d", cfg.DefaultUploadKbps)
	}
}

// TestIsDegradedSample enumerates the three metric thresholds that ARM the dwell.
func TestIsDegradedSample(t *testing.T) {
	cases := []struct {
		name string
		rep  metrics.Report
		want bool
	}{
		{"clean", metrics.Report{UploadKbps: 4000}, false},
		{"loss at the threshold", metrics.Report{LossPct: DegradedLossPct}, true},
		{"loss under the threshold", metrics.Report{LossPct: DegradedLossPct - 0.1}, false},
		{"rtt at the threshold", metrics.Report{RTTServerMs: DegradedRTTMs}, true},
		{"rtt under the threshold", metrics.Report{RTTServerMs: DegradedRTTMs - 1}, false},
		{"cpu at the threshold", metrics.Report{CPUPct: DegradedCPUPct}, true},
		{"cpu under the threshold", metrics.Report{CPUPct: DegradedCPUPct - 1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDegradedSample(tc.rep); got != tc.want {
				t.Fatalf("isDegradedSample(%+v) = %v, want %v", tc.rep, got, tc.want)
			}
		})
	}
}

// TestReconstructObserved pins §6.6 step 4's reconstruction: the stickiness baseline for
// a new term is built from what the peers say they have REALIZED, never from any
// topology, and never from the previous coordinator's beliefs.
func TestReconstructObserved(t *testing.T) {
	cases := []struct {
		name    string
		parents map[string]string
		want    string // "" ⇒ unreconstructable
		root    string
	}{
		{
			name:    "a star",
			parents: map[string]string{"a": "", "b": "a", "c": "a"},
			root:    "a",
			want:    "a>b a>c ",
		},
		{
			// Emission is BFS from the root with children sorted by name, so the same
			// realized fleet always reconstructs to byte-identical edges however the
			// heartbeats were ordered on the wire.
			name:    "two levels, emitted parents-first and name-ordered",
			parents: map[string]string{"a": "", "x": "a", "d": "x", "b": "a", "c": "a"},
			root:    "a",
			want:    "a>b a>c a>x x>d ",
		},
		{
			name:    "a member whose parent was not heard from becomes a second root",
			parents: map[string]string{"a": "", "b": "gone-relay"},
			want:    "",
		},
		{
			name:    "no root at all is unreconstructable",
			parents: map[string]string{"b": "c", "c": "b"},
			want:    "",
		},
		{
			// The torn case §6.6 admits: a is a clean root, but b and c form a cycle
			// off to the side. The reconstruction emits what it can reach; Validate is
			// the gate that then rejects it, which is where the fallback lives.
			name:    "a cycle beside a clean root is emitted only as far as it reaches",
			parents: map[string]string{"a": "", "x": "a", "b": "c", "c": "b"},
			root:    "a",
			want:    "a>x ",
		},
		{
			name:    "a lone root reconstructs to an edgeless tree",
			parents: map[string]string{"a": ""},
			root:    "a",
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reconstructObserved(tc.parents, 7)
			if tc.root == "" {
				if got != nil {
					t.Fatalf("want nil (unreconstructable), got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("want a reconstruction rooted at %q, got nil", tc.root)
			}
			if got.Root != tc.root {
				t.Fatalf("root = %q, want %q", got.Root, tc.root)
			}
			if s := edgeString(got); s != tc.want {
				t.Fatalf("edges = %q, want %q", s, tc.want)
			}
			// Every emitted edge's parent is already placed. This pins the property
			// that keeps WI-3's finding (1) — deriveWorking leaving an orphan out of
			// processingOrder's incumbent list — unreachable through the Phase 6
			// handover path: a reconstruction is a single connected BFS from one root
			// or it is nothing, so it can never produce the disconnected-fragment
			// shape that finding is about.
			placed := map[string]bool{got.Root: true}
			for i, e := range got.Edges {
				if !placed[e.Parent] {
					t.Fatalf("edge %d (%s>%s) names an unplaced parent: %v", i, e.Parent, e.Child, got.Edges)
				}
				placed[e.Child] = true
			}
			if got.Epoch != 7 {
				t.Fatalf("the baseline must carry the new term, got epoch %d", got.Epoch)
			}
			// Rev 0 is load-bearing: BuildTree refuses a prev whose Rev does not
			// advance, and the first tree of a new term is Rev 1.
			if got.Rev != 0 {
				t.Fatalf("the baseline must be stamped Rev 0, got %d", got.Rev)
			}
		})
	}
}

// TestReconstructObservedIsDeterministic: the same realized fleet must reconstruct
// identically every time. Go randomises map iteration, so a reconstruction that leaked
// it would make the first tree of every new term depend on nothing but the hash seed —
// silently destroying the replayability the whole test strategy rests on.
func TestReconstructObservedIsDeterministic(t *testing.T) {
	parents := map[string]string{"a": "", "x": "a", "y": "a", "b": "x", "c": "x", "d": "y", "e": "y"}
	first := edgeString(reconstructObserved(parents, 1))
	for i := 0; i < 50; i++ {
		if got := edgeString(reconstructObserved(parents, 1)); got != first {
			t.Fatalf("reconstruction %d differs: %q vs %q", i, got, first)
		}
	}
}
