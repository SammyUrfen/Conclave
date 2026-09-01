package arbiter_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// tuning is a fully-populated coordinator configuration with every field set to a
// DISTINCT value, so a test that asserts a field arrived cannot pass by reading a
// neighbour's value or a coincidental default.
func tuning() arbiter.CoordinatorConfig {
	return arbiter.CoordinatorConfig{
		MaxDepth:            3,
		StreamKbps:          1500,
		DefaultUploadKbps:   250,
		StickinessMs:        37.5,
		DwellMs:             11_000,
		RecomputeCooldownMs: 6_000,
		DegradedAfterMs:     4_000,
		GoneAfterMs:         9_000,
		JoinSettleMs:        1_750,
		SocketDetectionMs:   7_000,
	}
}

// electingTuned is the standard Phase 6 posture with a usable coordinator config.
func electingTuned() arbiter.Config {
	cfg := electing()
	cfg.CoordinatorConfig = tuning()
	return cfg
}

// TestAnnouncementCarriesCoordinatorConfig is the fix for a live defect: an elected peer
// adopted the role and hosted a coordinator it had no way to configure, so it published
// nothing and the tree was never repaired after a handover. The tuning is a property of
// the MEET, not of whichever node happens to hold the role, so it travels on the one
// frame that already says who holds it.
func TestAnnouncementCarriesCoordinatorConfig(t *testing.T) {
	t.Run("a bootstrap announcement carries the configuration", func(t *testing.T) {
		h := newHarness(t, electingTuned())
		h.join("m", "p1", strong("alice"))

		if got := h.lastAnn("m").Config; got != tuning() {
			t.Errorf("Config = %+v, want %+v", got, tuning())
		}
	})

	t.Run("every announcement in a meet carries the same configuration", func(t *testing.T) {
		h := newHarness(t, electingTuned())
		h.join("m", "p1", mid("alice"))
		h.join("m", "p2", strong("bob"))
		h.elapseCurrent("m", 61*time.Second) // promotion
		h.leave("m", "p2")                   // failover, then whatever follows

		got := h.ann.forRoom("m")
		if len(got) < 4 {
			t.Fatalf("announcements = %d, want at least 4", len(got))
		}
		for i, a := range got {
			if a.Config != tuning() {
				t.Errorf("announcement %d (%s): Config = %+v, want %+v — a handover must "+
					"never retune the meet", i, a.Reason, a.Config, tuning())
			}
		}
	})

	t.Run("a vacancy carries it too", func(t *testing.T) {
		h := newHarness(t, electingTuned())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", unwilling("bob"))
		h.leave("m", "p1")

		last := h.lastAnn("m")
		if last.Reason != arbiter.ReasonVacated {
			t.Fatalf("reason = %q, want %q", last.Reason, arbiter.ReasonVacated)
		}
		if last.Config != tuning() {
			t.Errorf("Config = %+v, want %+v", last.Config, tuning())
		}
	})

	t.Run("a unicast repair replays the configuration verbatim", func(t *testing.T) {
		h := newHarness(t, electingTuned())
		h.join("m", "p1", strong("alice"))
		h.join("m", "p2", mid("bob"))
		h.clk.Advance(repairGrace)
		h.beat("m", "p2")

		got := h.ann.repairsTo("p2")
		if len(got) != 1 {
			t.Fatalf("repairs = %d, want 1", len(got))
		}
		if got[0].Config != tuning() {
			t.Errorf("repaired Config = %+v, want %+v", got[0].Config, tuning())
		}
	})
}

// TestAnnouncedConfigBuildsATree is the discrimination test the live symptom demands: a
// coordinator built from the announced configuration must actually be able to publish.
// BuildTree hard-errors on StreamKbps <= 0, so a zero-valued config produces a
// coordinator that adopts the role, runs, and emits nothing — which is the failure being
// fixed, relocated one layer down.
func TestAnnouncedConfigBuildsATree(t *testing.T) {
	nodes := []overlay.Node{
		{Name: "alice", UploadKbps: 6000, NAT: overlay.NATDirect},
		{Name: "bob", UploadKbps: 3000, NAT: overlay.NATDirect},
		{Name: "carol", UploadKbps: 3000, NAT: overlay.NATDirect},
	}

	// constraintsFrom is the translation an elected peer's coordinator performs. Epoch
	// and Rev are the coordinator's own to mint and are deliberately NOT announced.
	constraintsFrom := func(c arbiter.CoordinatorConfig) overlay.Constraints {
		return overlay.Constraints{
			Root:         "alice",
			MaxDepth:     c.MaxDepth,
			StreamKbps:   c.StreamKbps,
			StickinessMs: c.StickinessMs,
			Epoch:        1,
			Rev:          1,
		}
	}

	t.Run("the announced config builds a tree", func(t *testing.T) {
		h := newHarness(t, electingTuned())
		h.join("m", "p1", strong("alice"))

		cons := constraintsFrom(h.lastAnn("m").Config)
		topo, err := overlay.BuildTree(nodes, nil, cons)
		if err != nil {
			t.Fatalf("BuildTree from the announced config: %v", err)
		}
		if err := overlay.Validate(topo, nodes, cons); err != nil {
			t.Errorf("Validate: %v", err)
		}
	})

	t.Run("a zero config cannot build a tree", func(t *testing.T) {
		// The proof that the assertion above is not vacuous.
		if _, err := overlay.BuildTree(nodes, nil, constraintsFrom(arbiter.CoordinatorConfig{})); err == nil {
			t.Fatal("BuildTree with a zero config succeeded; the test above proves nothing")
		}
	})

	t.Run("an unconfigured arbiter announces an unusable config", func(t *testing.T) {
		// Naming the failure rather than papering over it: an arbiter whose operator
		// never supplied the tuning must not silently invent one, because a made-up
		// StreamKbps shapes every meet on the server.
		h := newHarness(t, electing())
		h.join("m", "p1", strong("alice"))

		if err := h.lastAnn("m").Config.Validate(); err == nil {
			t.Fatal("Validate accepted a zero config; it is exactly what BuildTree rejects")
		}
	})
}

// TestCoordinatorConfigValidate pins what "usable" means, so cmd/server can fail loud at
// startup instead of every meet failing quietly at run time.
func TestCoordinatorConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*arbiter.CoordinatorConfig)
		wantErr string
	}{
		{name: "fully populated", mutate: func(*arbiter.CoordinatorConfig) {}},
		{
			name:    "zero StreamKbps",
			mutate:  func(c *arbiter.CoordinatorConfig) { c.StreamKbps = 0 },
			wantErr: "stream_kbps",
		},
		{
			name:    "negative StreamKbps",
			mutate:  func(c *arbiter.CoordinatorConfig) { c.StreamKbps = -1 },
			wantErr: "stream_kbps",
		},
		{
			name:    "zero MaxDepth",
			mutate:  func(c *arbiter.CoordinatorConfig) { c.MaxDepth = 0 },
			wantErr: "max_depth",
		},
		{
			name:    "negative DefaultUploadKbps",
			mutate:  func(c *arbiter.CoordinatorConfig) { c.DefaultUploadKbps = -1 },
			wantErr: "default_upload_kbps",
		},
		{
			name:    "negative StickinessMs",
			mutate:  func(c *arbiter.CoordinatorConfig) { c.StickinessMs = -1 },
			wantErr: "stickiness_ms",
		},
		{
			name:    "negative duration",
			mutate:  func(c *arbiter.CoordinatorConfig) { c.DwellMs = -1 },
			wantErr: "dwell_ms",
		},
		{
			// 0 is MEANINGFUL for these: it means "derive the threshold per node from
			// the cadence that peer declared". Rejecting it would force every
			// deployment to hard-code a threshold and reopen the bug that deriving
			// exists to close.
			name:   "zero optional durations are legal",
			mutate: func(c *arbiter.CoordinatorConfig) { c.DwellMs, c.DegradedAfterMs, c.GoneAfterMs = 0, 0, 0 },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := tuning()
			tc.mutate(&c)
			err := c.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want an error naming %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate() = %q, want it to name the offending field %q", err, tc.wantErr)
			}
		})
	}
}

// TestCoordinatorConfigDurations: the millisecond fields exist because the wire uses
// them; the accessors exist so the conversion happens in exactly one place and no
// consumer multiplies by the wrong unit.
func TestCoordinatorConfigDurations(t *testing.T) {
	c := tuning()
	tests := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"Dwell", c.Dwell(), 11 * time.Second},
		{"RecomputeCooldown", c.RecomputeCooldown(), 6 * time.Second},
		{"DegradedAfter", c.DegradedAfter(), 4 * time.Second},
		{"GoneAfter", c.GoneAfter(), 9 * time.Second},
		{"JoinSettle", c.JoinSettle(), 1750 * time.Millisecond},
		{"SocketDetection", c.SocketDetection(), 7 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("%s() = %v, want %v", tc.name, tc.got, tc.want)
			}
		})
	}
	t.Run("a zero millisecond field stays a zero duration", func(t *testing.T) {
		// Zero must survive the round trip: it is how a caller says "derive it".
		var z arbiter.CoordinatorConfig
		if z.Dwell() != 0 || z.GoneAfter() != 0 || z.SocketDetection() != 0 {
			t.Error("a zero field produced a non-zero duration; 'derive per node' would be lost")
		}
	})
}

// TestCoordinatorConfigWireShape guards two things the frontend depends on.
//
// The dashboard parses `epoch`/`rev`/`seq` as BigInt by REWRITING those keys in the raw
// JSON text before JSON.parse ever runs (web/js/format.js). That rewrite is keyed on the
// field NAME wherever it occurs, so a config field named like one of them would be
// silently transformed. And any uint64 here could exceed 2^53 and be silently rounded by
// a JS reader. Neither failure produces an error anywhere — hence a structural test.
func TestCoordinatorConfigWireShape(t *testing.T) {
	bigintKeys := map[string]bool{
		"seq": true, "epoch": true, "rev": true,
		"last_beat_seq": true, "peer_epoch": true, "meet_epoch": true,
	}

	ct := reflect.TypeOf(arbiter.CoordinatorConfig{})
	for i := 0; i < ct.NumField(); i++ {
		f := ct.Field(i)
		key := strings.Split(f.Tag.Get("json"), ",")[0]
		t.Run(f.Name, func(t *testing.T) {
			if key == "" || key == "-" {
				t.Fatalf("field %s has no json key; every wire field must name itself", f.Name)
			}
			if bigintKeys[key] {
				t.Errorf("json key %q collides with the dashboard's BigInt rewrite set", key)
			}
			if f.Type.Kind() == reflect.Uint64 {
				t.Errorf("field %s is uint64; a JS reader would silently round it past 2^53", f.Name)
			}
		})
	}

	t.Run("round-trips through JSON unchanged", func(t *testing.T) {
		in := arbiter.Announcement{
			RoomID: "m", Epoch: 4, Coordinator: "alice", CoordinatorID: "p1",
			Reason: arbiter.ReasonBootstrap, Config: tuning(),
		}
		blob, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var out arbiter.Announcement
		if err := json.Unmarshal(blob, &out); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if out.Config != in.Config {
			t.Errorf("Config = %+v after a round trip, want %+v", out.Config, in.Config)
		}
	})

	t.Run("every field is emitted even at zero", func(t *testing.T) {
		// No omitempty: 0 is a MEANINGFUL value for several of these ("derive it"),
		// and a configuration record that hides its own fields is unreadable in a log
		// at exactly the moment someone is debugging why a meet is shaped wrongly.
		blob, err := json.Marshal(arbiter.CoordinatorConfig{})
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		for i := 0; i < ct.NumField(); i++ {
			key := strings.Split(ct.Field(i).Tag.Get("json"), ",")[0]
			if !strings.Contains(string(blob), `"`+key+`"`) {
				t.Errorf("json key %q is omitted at zero: %s", key, blob)
			}
		}
	})
}

// TestCoordinatorConfigExcludesPerNodeFields: the config describes the MEET, so anything
// that must differ per holder cannot live here. SelfName is the elected peer's own
// identity and Clock is a per-process capability; announcing either would either make
// every coordinator claim one name or try to ship a live object over a socket.
func TestCoordinatorConfigExcludesPerNodeFields(t *testing.T) {
	ct := reflect.TypeOf(arbiter.CoordinatorConfig{})
	for _, banned := range []string{"SelfName", "Clock", "Epoch", "Rev"} {
		if _, ok := ct.FieldByName(banned); ok {
			t.Errorf("CoordinatorConfig.%s: not a property of the meet", banned)
		}
	}
}
