package arbiter_test

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// eps is the tolerance for a float comparison against a hand-computed expectation.
// The formula is four multiply-adds, so anything beyond 1e-9 is a real disagreement
// and not accumulated rounding.
const eps = 1e-9

// healthy is a fitness that passes every disqualifier, so a case can vary exactly
// one term and attribute the whole delta to it.
func healthy() arbiter.Fitness {
	return arbiter.Fitness{
		CPUFreePct:    100,
		RTTServerMs:   0,
		LossPct:       0,
		UptimeSec:     arbiter.StableUptimeSec,
		NAT:           overlay.NATDirect,
		Live:          true,
		Coordinatable: true,
	}
}

func TestScore(t *testing.T) {
	tests := []struct {
		name string
		f    arbiter.Fitness
		want float64
	}{
		{
			name: "perfect fitness scores 1",
			f:    healthy(),
			want: 1.0,
		},
		{
			name: "worst eligible fitness scores 0",
			f: arbiter.Fitness{
				CPUFreePct: 0, RTTServerMs: arbiter.MaxUsefulRTTMs, LossPct: arbiter.MaxUsefulLossPct,
				UptimeSec: 0, NAT: overlay.NATDirect, Live: true, Coordinatable: true,
			},
			want: 0.0,
		},
		{
			name: "every term at half weight",
			f: arbiter.Fitness{
				CPUFreePct: 50, RTTServerMs: 150, LossPct: 5, UptimeSec: 60,
				NAT: overlay.NATDirect, Live: true, Coordinatable: true,
			},
			want: 0.5,
		},
		{
			name: "cpu term carries 0.30",
			f:    func() arbiter.Fitness { f := healthy(); f.CPUFreePct = 0; return f }(),
			want: 0.70,
		},
		{
			name: "rtt term carries 0.35",
			f:    func() arbiter.Fitness { f := healthy(); f.RTTServerMs = arbiter.MaxUsefulRTTMs; return f }(),
			want: 0.65,
		},
		{
			name: "loss term carries 0.20",
			f:    func() arbiter.Fitness { f := healthy(); f.LossPct = arbiter.MaxUsefulLossPct; return f }(),
			want: 0.80,
		},
		{
			name: "uptime term carries 0.15",
			f:    func() arbiter.Fitness { f := healthy(); f.UptimeSec = 0; return f }(),
			want: 0.85,
		},
		{
			name: "rtt saturates past MaxUsefulRTTMs",
			f:    func() arbiter.Fitness { f := healthy(); f.RTTServerMs = 10 * arbiter.MaxUsefulRTTMs; return f }(),
			want: 0.65,
		},
		{
			name: "loss saturates past MaxUsefulLossPct",
			f:    func() arbiter.Fitness { f := healthy(); f.LossPct = 100; return f }(),
			want: 0.80,
		},
		{
			name: "uptime saturates past StableUptimeSec",
			f:    func() arbiter.Fitness { f := healthy(); f.UptimeSec = 100 * arbiter.StableUptimeSec; return f }(),
			want: 1.0,
		},
		{
			name: "negative inputs clamp rather than score above 1",
			f: func() arbiter.Fitness {
				f := healthy()
				f.RTTServerMs, f.LossPct, f.UptimeSec = -50, -5, -10
				return f
			}(),
			want: 0.85,
		},
		{
			name: "cpu above 100 clamps",
			f:    func() arbiter.Fitness { f := healthy(); f.CPUFreePct = 400; return f }(),
			want: 1.0,
		},
		{
			name: "NaN inputs are treated as worst case, never propagated",
			f: func() arbiter.Fitness {
				f := healthy()
				f.RTTServerMs = math.NaN()
				return f
			}(),
			want: 0.65,
		},
		{
			name: "TURN-bound peer is disqualified",
			f:    func() arbiter.Fitness { f := healthy(); f.NAT = overlay.NATRelayed; return f }(),
			want: 0.0,
		},
		{
			name: "peer the arbiter has not heard from is disqualified",
			f:    func() arbiter.Fitness { f := healthy(); f.Live = false; return f }(),
			want: 0.0,
		},
		{
			name: "peer that declined the role is disqualified",
			f:    func() arbiter.Fitness { f := healthy(); f.Coordinatable = false; return f }(),
			want: 0.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := arbiter.Score(tc.f)
			if math.IsNaN(got) {
				t.Fatalf("Score() = NaN, want %v", tc.want)
			}
			if math.Abs(got-tc.want) > eps {
				t.Errorf("Score() = %v, want %v", got, tc.want)
			}
			if got < 0 || got > 1 {
				t.Errorf("Score() = %v, outside [0,1]", got)
			}
		})
	}
}

// TestScoreExcludesUploadStructurally is the compile-time-ish half of PLAN §1.2:
// upload is a DATA-plane resource and must not appear in the control-plane fitness
// at all. Asserting "a large UploadKbps changes nothing" is impossible to express
// as a value test precisely because the field must not exist, so the assertion is
// made against the type instead.
func TestScoreExcludesUploadStructurally(t *testing.T) {
	ft := reflect.TypeOf(arbiter.Fitness{})
	for i := 0; i < ft.NumField(); i++ {
		if n := ft.Field(i).Name; strings.Contains(strings.ToLower(n), "upload") ||
			strings.Contains(strings.ToLower(n), "bandwidth") {
			t.Errorf("Fitness.%s: upload is a data-plane input and must not reach the fitness score", n)
		}
	}
}

// TestScoreIsDeterministic pins the property the whole election rests on: the same
// evidence always yields the same number, so a replayed scenario replays.
func TestScoreIsDeterministic(t *testing.T) {
	f := arbiter.Fitness{
		CPUFreePct: 37.5, RTTServerMs: 91.25, LossPct: 1.75, UptimeSec: 43.125,
		NAT: overlay.NATDirect, Live: true, Coordinatable: true,
	}
	first := arbiter.Score(f)
	for i := 0; i < 1000; i++ {
		if got := arbiter.Score(f); got != first {
			t.Fatalf("Score() = %v on iteration %d, want %v on every call", got, i, first)
		}
	}
}

// TestScoreConstants pins the frozen values from PLAN §6.1/§6.2. They are load-
// bearing across packages and a silent edit would change election behaviour with no
// other test noticing.
func TestScoreConstants(t *testing.T) {
	tests := []struct {
		name string
		got  float64
		want float64
	}{
		{"MaxUsefulRTTMs", arbiter.MaxUsefulRTTMs, 300.0},
		{"MaxUsefulLossPct", arbiter.MaxUsefulLossPct, 10.0},
		{"StableUptimeSec", arbiter.StableUptimeSec, 120.0},
		{"PromoteMarginScore", arbiter.PromoteMarginScore, 0.20},
		{"DemoteBelowScore", arbiter.DemoteBelowScore, 0.35},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
			}
		})
	}
	t.Run("durations", func(t *testing.T) {
		if arbiter.ElectionDwell.String() != "20s" {
			t.Errorf("ElectionDwell = %v, want 20s", arbiter.ElectionDwell)
		}
		if arbiter.MinTermDuration.String() != "1m0s" {
			t.Errorf("MinTermDuration = %v, want 1m0s", arbiter.MinTermDuration)
		}
		// RebuildWindow is deliberately NOT asserted here: it moved out of this
		// package to metrics, whose copy is the one that matters. The repair path's
		// use of that grace is covered behaviourally in repair_test.go instead.
		if arbiter.MeetTTL.String() != "5m0s" {
			t.Errorf("MeetTTL = %v, want 5m0s", arbiter.MeetTTL)
		}
		if arbiter.MaxEndedMeets != 20 {
			t.Errorf("MaxEndedMeets = %d, want 20", arbiter.MaxEndedMeets)
		}
		if arbiter.MaxMeets != 100 {
			t.Errorf("MaxMeets = %d, want 100", arbiter.MaxMeets)
		}
	})
}
