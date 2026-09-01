package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/logging"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// TestFlagDefaults pins docs/PLAN.md §10's frozen default column. A default that
// drifts is not a compile error anywhere, so it is only ever caught here — and the
// defaults are load-bearing: -demo and -elect being false is the security posture,
// and -allowed-origins is the one allow-list two surfaces share.
func TestFlagDefaults(t *testing.T) {
	f, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags(nil): %v", err)
	}
	tests := []struct {
		flag string
		got  any
		want any
	}{
		{"-addr", f.addr, ":9000"},
		{"-log-level", f.logLevel, "info"},
		{"-log-format", f.logFormat, "text"},
		{"-coordinate", f.coordinate, false},
		{"-max-depth", f.maxDepth, 2},
		{"-stream-kbps", f.streamKbps, 2000},
		{"-default-upload-kbps", f.defaultUploadKbps, 0},
		{"-stickiness-ms", f.stickinessMs, overlay.DefaultStickinessMs},
		{"-root-change-margin-kbps", f.rootChangeMarginKbps, overlay.RootChangeMarginKbps},
		{"-join-settle", f.joinSettle, 1500 * time.Millisecond},
		{"-dwell", f.dwell, 10 * time.Second},
		{"-recompute-cooldown", f.recomputeCooldown, 5 * time.Second},
		// 0 is not "unset": coordinator.Config documents it as "derive this
		// threshold per node from the cadence the peer DECLARED on the wire". A
		// non-zero default here would make that derivation dead code in production.
		{"-degraded-after", f.degradedAfter, time.Duration(0)},
		{"-gone-after", f.goneAfter, time.Duration(0)},
		{"-elect", f.elect, false},
		{"-election-dwell", f.electionDwell, 20 * time.Second},
		{"-min-term", f.minTerm, 60 * time.Second},
		{"-dashboard", f.dashboard, true},
		{"-allowed-origins", f.allowedOrigins, defaultAllowedOrigins},
		{"-public-url", f.publicURL, ""},
		{"-demo", f.demo, false},
	}
	for _, tt := range tests {
		t.Run(tt.flag, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s default = %v, want %v", tt.flag, tt.got, tt.want)
			}
		})
	}
}

// TestValidateCoordinatorFlags pins the fail-loud config check: constraints BuildTree
// cannot satisfy are rejected at startup, but only when the coordinator is on (the
// flags are inert otherwise).
func TestValidateCoordinatorFlags(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*serverFlags)
		wantErr string // substring; "" means expect success
	}{
		{name: "disabled ignores bad values", mutate: func(f *serverFlags) {
			f.coordinate, f.streamKbps, f.maxDepth = false, 0, 0
		}},
		{name: "valid enabled", mutate: func(f *serverFlags) { f.coordinate = true }},
		{name: "zero stream cost", mutate: func(f *serverFlags) {
			f.coordinate, f.streamKbps = true, 0
		}, wantErr: "-stream-kbps"},
		{name: "negative stream cost", mutate: func(f *serverFlags) {
			f.coordinate, f.streamKbps = true, -1
		}, wantErr: "-stream-kbps"},
		{name: "zero depth", mutate: func(f *serverFlags) {
			f.coordinate, f.maxDepth = true, 0
		}, wantErr: "-max-depth"},
		{name: "negative stickiness", mutate: func(f *serverFlags) {
			f.coordinate, f.stickinessMs = true, -1
		}, wantErr: "-stickiness-ms"},
		{name: "negative root margin", mutate: func(f *serverFlags) {
			f.coordinate, f.rootChangeMarginKbps = true, -1
		}, wantErr: "-root-change-margin-kbps"},
		// The margin has no plumbing: overlay froze it as a compile-time constant
		// with no Constraints field, so any other value would be silently ignored.
		// Silently ignored is the one outcome this project does not ship.
		{name: "unhonourable root margin", mutate: func(f *serverFlags) {
			f.coordinate, f.rootChangeMarginKbps = true, 4000
		}, wantErr: "-root-change-margin-kbps"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := parseFlags(nil, io.Discard)
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			tt.mutate(f)
			err = validateCoordinatorFlags(f)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

// TestResolveRejectsContradictoryConfig is the fail-loud gate as a whole: a config
// that contradicts itself must not start. Each case is one operator typo that would
// otherwise surface hours later as a mysteriously silent surface.
func TestResolveRejectsContradictoryConfig(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "defaults are self-consistent", args: nil},
		{name: "bad log level", args: []string{"-log-level", "verbose"}, wantErr: "unknown level"},
		// logging.New silently falls back to JSON on an unknown format, so without an
		// explicit check a typo would quietly change the log format instead of failing.
		{name: "bad log format", args: []string{"-log-format", "yaml"}, wantErr: "-log-format"},
		{name: "good log format json", args: []string{"-log-format", "json"}},
		{name: "malformed origin", args: []string{"-allowed-origins", "notaurl"}, wantErr: "-allowed-origins"},
		{name: "wildcard host origin", args: []string{"-allowed-origins", "http://*.example.com"}, wantErr: "-allowed-origins"},
		{name: "bare wildcard origin", args: []string{"-allowed-origins", "*"}, wantErr: "-allowed-origins"},
		{name: "empty origins with dashboard", args: []string{"-allowed-origins", ""}, wantErr: "-allowed-origins"},
		{name: "empty origins without dashboard", args: []string{"-allowed-origins", "", "-dashboard=false"}},
		{name: "zero dwell", args: []string{"-dwell", "0"}, wantErr: "-dwell"},
		{name: "negative join settle", args: []string{"-join-settle", "-1s"}, wantErr: "-join-settle"},
		{name: "zero min term", args: []string{"-min-term", "0"}, wantErr: "-min-term"},
		// The two health thresholds are the exception to "every duration > 0": 0 is
		// their documented "derive per node" value and the default. Negative has no
		// meaning at all and must still be refused.
		{name: "zero gone after is the derive sentinel", args: []string{"-gone-after", "0"}},
		{name: "zero degraded after is the derive sentinel", args: []string{"-degraded-after", "0"}},
		{name: "negative gone after", args: []string{"-gone-after", "-1s"}, wantErr: "-gone-after"},
		{name: "negative degraded after", args: []string{"-degraded-after", "-1s"}, wantErr: "-degraded-after"},
		// Ordering is only checkable when BOTH are explicit. With one derived, the
		// comparison is between a fixed value and a per-peer one that startup cannot
		// see — so the check applies to the pair, not to either alone.
		{name: "both explicit and ordered", args: []string{"-degraded-after", "4s", "-gone-after", "9s"}},
		{name: "both explicit, degraded past gone", args: []string{"-degraded-after", "9s", "-gone-after", "8s"}, wantErr: "-degraded-after"},
		{name: "both explicit and equal", args: []string{"-degraded-after", "8s", "-gone-after", "8s"}, wantErr: "-degraded-after"},
		// One explicit, one derived: no ordering claim is possible, so neither may be
		// rejected on ordering grounds.
		{name: "only degraded explicit", args: []string{"-degraded-after", "30s"}},
		{name: "only gone explicit", args: []string{"-gone-after", "9s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := parseFlags(tt.args, io.Discard)
			if err != nil {
				if tt.wantErr != "" && strings.Contains(err.Error(), tt.wantErr) {
					return
				}
				t.Fatalf("parseFlags(%v): %v", tt.args, err)
			}
			_, err = f.resolve()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

// TestParseFormat pins the fail-loud -log-format check.
//
// logging.New's default branch turns ANY unrecognised format into JSON, so without
// this parse step "-log-format tex" would start a server logging JSON and say
// nothing. cmd/peer already fixed exactly this; the server carried the bug.
func TestParseFormat(t *testing.T) {
	tests := []struct {
		in      string
		want    logging.Format
		wantErr bool
	}{
		{in: "text", want: logging.FormatText},
		{in: "json", want: logging.FormatJSON},
		{in: "TEXT", want: logging.FormatText},
		{in: " json ", want: logging.FormatJSON},
		{in: "yaml", wantErr: true},
		{in: "", wantErr: true},
		{in: "tex", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseFormat(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseFormat(%q) = %q, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFormat(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("parseFormat(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestValidateLivenessBudget pins the cross-package liveness relationship §10 makes
// cmd/server responsible for: the Hub's socket-death detection window must not be
// SLOWER than the control plane's gone threshold, or a peer can be dropped from the
// tree while its socket is still registered and never re-announce itself.
//
// It is reported against -gone-after because that is the flag an operator can act on;
// the WS ping family is deliberately compile-time only (§10).
func TestValidateLivenessBudget(t *testing.T) {
	tests := []struct {
		name      string
		goneAfter time.Duration
		budget    time.Duration
		wantErr   string
	}{
		{name: "shipped default derives", goneAfter: 0, budget: signaling.WSLivenessBudget},
		{name: "explicit override agrees", goneAfter: 8 * time.Second, budget: signaling.WSLivenessBudget},
		{name: "gone shorter than socket detection", goneAfter: 5 * time.Second,
			budget: signaling.WSLivenessBudget, wantErr: "-gone-after"},
		{name: "gone equal to socket detection", goneAfter: signaling.WSLivenessBudget,
			budget: signaling.WSLivenessBudget},
		{name: "zero socket budget", goneAfter: 8 * time.Second, budget: 0, wantErr: "must be positive"},
		// A gone-after of 0 means "derive per node from the peer's declared cadence",
		// so the check falls back to the default cadence rather than skipping.
		{name: "derived threshold with default cadence", goneAfter: 0, budget: signaling.WSLivenessBudget},
		{name: "derived threshold too slow a socket", goneAfter: 0,
			budget: metrics.GoneAfter(metrics.HeartbeatInterval) + time.Second, wantErr: "-gone-after"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLivenessBudget(tt.goneAfter, tt.budget)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

// TestCoordinatorConfigWiring asserts every flag §10 routes into coordinator.Config
// actually lands there, including SocketDetection — which comes from the Hub rather
// than from a flag and is the one field a reader would not think to check.
func TestCoordinatorConfigWiring(t *testing.T) {
	f, err := parseFlags([]string{
		"-max-depth", "3", "-stream-kbps", "1500", "-default-upload-kbps", "500",
		"-stickiness-ms", "40", "-dwell", "11s", "-recompute-cooldown", "6s",
		"-degraded-after", "4s", "-gone-after", "9s", "-join-settle", "2s",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	got := f.coordinatorConfig(7 * time.Second)

	// StickinessMs is a *float64, so it must be asserted BY VALUE and then cleared
	// before the struct comparison: == on a pointer field compiles and silently
	// compares addresses, which would make this test pass against any value at all.
	// That is the same class of quiet wrongness the pointer was introduced to remove,
	// so it is worth spelling out rather than reaching for reflect.DeepEqual.
	if got.StickinessMs == nil {
		t.Fatal("coordinatorConfig().StickinessMs is nil; the flag never reached the config")
	}
	if *got.StickinessMs != 40 {
		t.Fatalf("StickinessMs = %v, want 40", *got.StickinessMs)
	}
	got.StickinessMs = nil

	want := coordinator.Config{
		MaxDepth: 3, StreamKbps: 1500, DefaultUploadKbps: 500,
		Dwell: 11 * time.Second, RecomputeCooldown: 6 * time.Second,
		DegradedAfter: 4 * time.Second, GoneAfter: 9 * time.Second,
		JoinSettle: 2 * time.Second, SocketDetection: 7 * time.Second,
	}
	if got != want {
		t.Fatalf("coordinatorConfig() = %+v, want %+v", got, want)
	}
}

// TestArbiterConfigWiring asserts the election flags land in arbiter.Config — and,
// separately below, the one constant that is duplicated across a forbidden edge.
func TestArbiterConfigWiring(t *testing.T) {
	f, err := parseFlags([]string{"-elect", "-coordinate", "-election-dwell", "30s", "-min-term", "90s"}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	got := f.arbiterConfig(signaling.WSLivenessBudget)
	if !got.Elect || !got.Coordinate {
		t.Fatalf("arbiterConfig() Elect=%v Coordinate=%v, want both true", got.Elect, got.Coordinate)
	}
	if got.ElectionDwell != 30*time.Second || got.MinTerm != 90*time.Second {
		t.Fatalf("arbiterConfig() dwell=%v minTerm=%v, want 30s/90s", got.ElectionDwell, got.MinTerm)
	}
	if got.ValidMeetID == nil {
		t.Fatal("arbiterConfig().ValidMeetID is nil; the boundary predicate must be wired from policy")
	}
	if got.ValidMeetID("Not A Meet") {
		t.Fatal("arbiterConfig().ValidMeetID accepted an id policy.MeetIDPattern rejects")
	}
	_ = arbiter.DefaultArbiterID
}
