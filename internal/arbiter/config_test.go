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
		Resolved:            true,
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
			// The quiet failure. An operator who tuned only the fields that fail
			// LOUDLY leaves the rest at Go's zero, which every consumer would read as
			// "use the default" — silently retuning the meet on handover. Resolved is
			// what makes "I chose these values" distinguishable from "I forgot".
			name:    "unresolved",
			mutate:  func(c *arbiter.CoordinatorConfig) { c.Resolved = false },
			wantErr: "resolved",
		},
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

// TestZeroIsAChoiceNotAnAbsence is the fix for a defect the announcement must not
// inherit.
//
// coordinator.Config uses 0 to mean "use the package default" for StickinessMs, Dwell,
// RecomputeCooldown and JoinSettle. But 0 is ALSO a meaningful value for every one of
// them — memoryless re-parenting, no hysteresis, no anti-thrash floor, build
// immediately — so an operator asking for one silently gets the other. (That is live on
// the server today: -stickiness-ms 0 is documented and validated as "memoryless" and
// yields 25.) It is the same shape as a boolean whose polarity is inverted: a value that
// carries meaning colliding with a convention that treats it as absent.
//
// The announcement must not bake that ambiguity into a wire format, where every future
// reader would have to know which of the two a 0 meant. The representation chosen is
// that ABSENCE IS NOT REPRESENTABLE PER FIELD: every value is the resolved effective
// one, and a single struct-level Resolved flag distinguishes a configuration someone
// filled in from one nobody did.
func TestZeroIsAChoiceNotAnAbsence(t *testing.T) {
	// memoryless is a deliberately-chosen configuration whose values are all the ones
	// a zero-means-default convention would silently overwrite.
	memoryless := func() arbiter.CoordinatorConfig {
		c := tuning()
		c.StickinessMs = 0
		c.DwellMs, c.RecomputeCooldownMs, c.JoinSettleMs = 0, 0, 0
		return c
	}

	t.Run("a deliberately zero config is valid", func(t *testing.T) {
		if err := memoryless().Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil — every one of these zeros is a real choice", err)
		}
	})

	t.Run("the arbiter announces the chosen zeros, not defaults", func(t *testing.T) {
		cfg := electing()
		cfg.CoordinatorConfig = memoryless()
		h := newHarness(t, cfg)
		h.join("m", "p1", strong("alice"))

		got := h.lastAnn("m").Config
		if got != memoryless() {
			t.Fatalf("Config = %+v, want %+v", got, memoryless())
		}
		// Named individually so a failure says which knob was overwritten.
		if got.StickinessMs != 0 {
			t.Errorf("StickinessMs = %v, want 0 (memoryless), not a substituted default", got.StickinessMs)
		}
		if got.DwellMs != 0 || got.RecomputeCooldownMs != 0 || got.JoinSettleMs != 0 {
			t.Errorf("durations = %d/%d/%d, want all 0 as chosen",
				got.DwellMs, got.RecomputeCooldownMs, got.JoinSettleMs)
		}
	})

	t.Run("a chosen zero survives the wire distinguishably from an unset config", func(t *testing.T) {
		chosen, err := json.Marshal(memoryless())
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		unset, err := json.Marshal(arbiter.CoordinatorConfig{})
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if string(chosen) == string(unset) {
			t.Fatal("a deliberately-zero config is byte-identical to an unset one; the " +
				"wire cannot express the difference")
		}
		var back arbiter.CoordinatorConfig
		if err := json.Unmarshal(chosen, &back); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !back.Resolved || back.StickinessMs != 0 {
			t.Errorf("round-tripped = %+v, want Resolved with a zero StickinessMs", back)
		}
	})

	t.Run("the loud fields alone are not enough", func(t *testing.T) {
		// Exactly the shipped hazard: an operator sets what BuildTree hard-errors on
		// and leaves everything else at Go's zero. It must not read as a valid
		// configuration, because every quiet field would then be defaulted by the
		// receiver and the meet retuned on handover.
		partial := arbiter.CoordinatorConfig{MaxDepth: 2, StreamKbps: 2000}
		if err := partial.Validate(); err == nil {
			t.Fatal("Validate accepted a config with only the loud fields set")
		}
	})

	t.Run("derive-per-node thresholds need no present flag", func(t *testing.T) {
		// DegradedAfterMs/GoneAfterMs are the genuine exception the ruling allows to
		// stay bare: there 0 ALREADY means "derive per node from the cadence each peer
		// declared", and an explicit zero threshold would be nonsense (every peer
		// instantly gone). So the two meanings do not collide and there is no
		// tri-state to express.
		c := tuning()
		c.DegradedAfterMs, c.GoneAfterMs = 0, 0
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil", err)
		}
		if c.DegradedAfter() != 0 || c.GoneAfter() != 0 {
			t.Error("a zero threshold became non-zero; 'derive per node' would be lost")
		}
	})
}

// TestResolvedIsRequiredToAnnounceUsefully ties the flag to the symptom: an arbiter
// handed an unresolved config announces one, and a peer building a coordinator from it
// would run with defaults nobody chose.
func TestResolvedIsRequiredToAnnounceUsefully(t *testing.T) {
	cfg := electing()
	cfg.CoordinatorConfig = arbiter.CoordinatorConfig{MaxDepth: 2, StreamKbps: 2000}
	h := newHarness(t, cfg)
	h.join("m", "p1", strong("alice"))

	got := h.lastAnn("m").Config
	if got.Resolved {
		t.Error("Resolved = true on a config the operator never filled in")
	}
	if err := got.Validate(); err == nil {
		t.Error("Validate accepted the announced config; startup had nothing to fail on")
	}
	// The arbiter still announces it rather than refusing: an arbiter that will not
	// start is worse than one that says why every meet is misconfigured.
	if h.meet("m").Coordinator != "alice" {
		t.Error("the election was blocked by a bad config; it should warn, not refuse")
	}
	if n := h.logs.count("coordinator configuration is unusable"); n != 1 {
		t.Errorf("warnings = %d, want exactly 1 at construction", n)
	}
}
