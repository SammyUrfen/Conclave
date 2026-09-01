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
			want:       "a>x a>c ",
		},
		{
			name:       "departure and promotion compose",
			live:       []string{"a", "b", "c", "d"},
			promotions: map[string]string{"b": "a", "d": "c"},
			want:       "a>b a>c c>d ",
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
