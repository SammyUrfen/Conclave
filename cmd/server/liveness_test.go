package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/signaling"
	"github.com/SammyUrfen/conclave/internal/simnet"
)

// healthTimeline runs a REAL coordinator on a virtual clock, feeds it one peer
// beating at the declared cadence, and samples that peer's health at each offset.
//
// It drives the real loop rather than reimplementing the threshold arithmetic on
// purpose: the resolution this file is about (derive vs override, and the
// socket-detection floor on top) lives in unexported methods on unexported state, so
// the only honest place to observe it is the health it produces. Re-deriving the
// expected threshold here would be a second copy of the logic under test, which is
// exactly the shape of test that agrees with a bug.
//
// simnet is TEST-ONLY and production never imports it (docs/PLAN.md §2.3), so reaching
// for its clock from a _test.go file adds no production edge.
func healthTimeline(t *testing.T, cfg coordinator.Config, intervalMs uint64, offsets ...time.Duration) []coordinator.Health {
	t.Helper()

	start := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	clk := simnet.NewVirtualClock(start)
	cfg.Clock = clk

	coord := coordinator.New(testLogger(), cfg, simnet.NewCapture(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = coord.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	// Every Advance round-trips through the loop, so a deadline the loop re-arms
	// mid-advance is seen before the next one is chosen. Without it the clock and the
	// loop race and the result is a flaky test blamed on the harness.
	clk.SetBarrier(func() { _ = coord.Sync(ctx) })

	const room, peerID, name = "standup", "p1", "alice"
	coord.PeerJoined(room, peerID, name)
	beat, err := json.Marshal(metrics.Heartbeat{Name: name, Seq: 1, IntervalMs: intervalMs})
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	coord.Heartbeat(room, peerID, beat)
	if err := coord.Sync(ctx); err != nil {
		t.Fatalf("sync after the first beat: %v", err)
	}

	out := make([]coordinator.Health, 0, len(offsets))
	var elapsed time.Duration
	for _, off := range offsets {
		if off < elapsed {
			t.Fatalf("offsets must ascend; %v follows %v", off, elapsed)
		}
		clk.Advance(off - elapsed)
		elapsed = off
		if err := coord.Sync(ctx); err != nil {
			t.Fatalf("sync at %v: %v", off, err)
		}
		snap, err := coord.Snapshot(ctx, room)
		if err != nil {
			t.Fatalf("snapshot at %v: %v", off, err)
		}
		got := coordinator.Health("")
		for _, m := range snap.Members {
			if m.Name == name {
				got = m.Health
			}
		}
		if got == "" {
			t.Fatalf("peer %q vanished from the snapshot at %v", name, off)
		}
		out = append(out, got)
	}
	return out
}

// TestDefaultThresholdsDeriveFromTheDeclaredCadence is the point of defaulting
// -degraded-after and -gone-after to 0.
//
// coordinator.Config documents 0 as "derive the threshold per node from the cadence
// the peer DECLARED on the wire". A non-zero default would mean cmd/server always
// passes a fixed value, so the derivation — the shipped §5.3 design — would never
// execute in production and a peer running -heartbeat 5s would be declared gone at 8s
// while beating perfectly.
//
// The assertions are stated in terms of what an operator would observe, and each one
// FAILS against a fixed 8s threshold. That is the discrimination: this test cannot
// pass unless the derivation is actually reachable.
func TestDefaultThresholdsDeriveFromTheDeclaredCadence(t *testing.T) {
	defaults, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}

	t.Run("the default config hands the coordinator the derive sentinel", func(t *testing.T) {
		cfg := defaults.coordinatorConfig(signaling.WSLivenessBudget)
		if cfg.DegradedAfter != 0 {
			t.Fatalf("DegradedAfter = %v, want 0 (derive per node); a fixed default makes "+
				"coordinator.Config's documented derivation dead code", cfg.DegradedAfter)
		}
		if cfg.GoneAfter != 0 {
			t.Fatalf("GoneAfter = %v, want 0 (derive per node)", cfg.GoneAfter)
		}
	})

	t.Run("a peer beating every 5s survives past the old fixed default", func(t *testing.T) {
		cfg := defaults.coordinatorConfig(signaling.WSLivenessBudget)
		// Derived from the DECLARED 5s cadence: degraded at 3 beats (15s), gone at 8
		// (40s). The old fixed defaults would have said degraded at 3s and gone at 8s,
		// so every sample below distinguishes the two.
		got := healthTimeline(t, cfg, 5000, 4*time.Second, 9*time.Second, 16*time.Second, 41*time.Second)
		want := []coordinator.Health{
			coordinator.HealthHealthy,  // 4s: a fixed 3s -degraded-after would say degraded
			coordinator.HealthHealthy,  // 9s: a fixed 8s -gone-after would say GONE
			coordinator.HealthDegraded, // 16s: 3 * 5s
			coordinator.HealthGone,     // 41s: 8 * 5s
		}
		assertHealth(t, got, want)
	})

	t.Run("an explicit flag still overrides the derivation", func(t *testing.T) {
		f, err := parseFlags([]string{"-degraded-after", "4s", "-gone-after", "9s"}, io.Discard)
		if err != nil {
			t.Fatalf("parseFlags: %v", err)
		}
		if _, err := f.resolve(); err != nil {
			t.Fatalf("resolve: %v", err)
		}
		cfg := f.coordinatorConfig(signaling.WSLivenessBudget)
		if cfg.DegradedAfter != 4*time.Second || cfg.GoneAfter != 9*time.Second {
			t.Fatalf("explicit flags did not reach the config: %+v", cfg)
		}
		// The same 5s-cadence peer, which the derivation would have kept healthy until
		// 15s, is now degraded at 5s and gone at 10s — the override doing its job.
		got := healthTimeline(t, cfg, 5000, 5*time.Second, 10*time.Second)
		assertHealth(t, got, []coordinator.Health{coordinator.HealthDegraded, coordinator.HealthGone})
	})

	t.Run("the socket-detection floor survives the derived path", func(t *testing.T) {
		cfg := defaults.coordinatorConfig(signaling.WSLivenessBudget)
		// A peer beating every 500ms derives a 4s gone threshold — FASTER than the
		// Hub's 7s socket-death window. The floor raises it to 7s, and that floor is
		// what closes the permanent-ejection bug: a peer declared gone while the Hub
		// still holds its socket never rejoins, because the join handshake only
		// happens on a NEW connection.
		//
		// 7.5s is the sample that proves the floor and the derivation are BOTH live:
		// an unfloored derivation says gone at 4s, and the old fixed 8s default says
		// still degraded at 7.5s. Only derive-then-floor produces this row.
		got := healthTimeline(t, cfg, 500, 3*time.Second, 5*time.Second, 7500*time.Millisecond)
		want := []coordinator.Health{
			coordinator.HealthDegraded, // 3s: 3 * 500ms = 1.5s, degraded is NOT floored
			coordinator.HealthDegraded, // 5s: past the unfloored 4s, still inside the floor
			coordinator.HealthGone,     // 7.5s: past the 7s floor
		}
		assertHealth(t, got, want)
	})
}

func assertHealth(t *testing.T, got, want []coordinator.Health) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("sampled %d healths, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("health sample %d = %q, want %q (full timeline: %v, want %v)",
				i, got[i], want[i], got, want)
		}
	}
}

// TestLivenessBudgetStillGatesTheDerivedDefault pins that switching the defaults to 0
// did not disarm the startup check.
//
// The value being validated changes SHAPE with a zero -gone-after: there is no longer
// a fixed threshold to compare the socket window against, so the check falls back to
// the threshold the DEFAULT cadence derives. It must still reject a socket window that
// outlives the gone threshold, and it must still name a flag the operator can edit —
// metrics' own message names -heartbeat, which is a PEER flag and useless to whoever
// is starting this server.
func TestLivenessBudgetStillGatesTheDerivedDefault(t *testing.T) {
	f, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.goneAfter != 0 {
		t.Fatalf("-gone-after default = %v, want 0; this test is about the derived path", f.goneAfter)
	}

	t.Run("shipped defaults pass", func(t *testing.T) {
		if err := validateLivenessBudget(f.goneAfter, signaling.WSLivenessBudget); err != nil {
			t.Fatalf("the shipped defaults do not satisfy the liveness budget: %v", err)
		}
	})

	t.Run("a socket window past the derived threshold is rejected, naming -gone-after", func(t *testing.T) {
		tooSlow := metrics.GoneAfter(metrics.HeartbeatInterval) + time.Second
		err := validateLivenessBudget(f.goneAfter, tooSlow)
		if err == nil {
			t.Fatalf("a %v socket window against a derived %v threshold was accepted",
				tooSlow, metrics.GoneAfter(metrics.HeartbeatInterval))
		}
		msg := err.Error()
		for _, want := range []string{"-gone-after", tooSlow.String()} {
			if !strings.Contains(msg, want) {
				t.Fatalf("error %q does not name %q; an operator cannot act on it", msg, want)
			}
		}
	})

	t.Run("a non-positive socket window is still rejected", func(t *testing.T) {
		if err := validateLivenessBudget(f.goneAfter, 0); err == nil {
			t.Fatal("a zero socket window was accepted")
		}
	})
}
