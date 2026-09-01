package meetconfig_test

import (
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/meetconfig"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// tuned is a CoordinatorConfig with every field moved off its zero value, so a
// translator that dropped a field — or read the wrong one — cannot coincide with the
// right answer by luck. The durations are all distinct and none is a round multiple of
// another, which is what makes a transposed pair visible.
func tuned() arbiter.CoordinatorConfig {
	return arbiter.CoordinatorConfig{
		Resolved:            true,
		MaxDepth:            3,
		StreamKbps:          1500,
		DefaultUploadKbps:   500,
		StickinessMs:        40,
		DwellMs:             11_000,
		RecomputeCooldownMs: 6_000,
		DegradedAfterMs:     4_000,
		GoneAfterMs:         9_000,
		JoinSettleMs:        2_000,
		SocketDetectionMs:   7_000,
	}
}

// TestCoordinatorTranslatesEveryField is the field-by-field guard on the translation.
//
// It is the whole reason this package exists: two nodes converting the same
// announcement must reach the same coordinator configuration, so there is ONE
// conversion and this is what pins it. A dropped field here is a meet that re-shapes
// the moment the coordinator role moves.
func TestCoordinatorTranslatesEveryField(t *testing.T) {
	cc := tuned()
	got := meetconfig.Coordinator(cc)

	for _, tc := range []struct {
		field string
		got   any
		want  any
	}{
		{"MaxDepth", got.MaxDepth, 3},
		{"StreamKbps", got.StreamKbps, 1500},
		{"DefaultUploadKbps", got.DefaultUploadKbps, 500},
		{"Dwell", got.Dwell, 11 * time.Second},
		{"RecomputeCooldown", got.RecomputeCooldown, 6 * time.Second},
		{"DegradedAfter", got.DegradedAfter, 4 * time.Second},
		{"GoneAfter", got.GoneAfter, 9 * time.Second},
		{"JoinSettle", got.JoinSettle, 2 * time.Second},
		{"SocketDetection", got.SocketDetection, 7 * time.Second},
	} {
		t.Run(tc.field, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("%s = %v, want %v", tc.field, tc.got, tc.want)
			}
		})
	}

	t.Run("StickinessMs", func(t *testing.T) {
		if got.StickinessMs == nil {
			t.Fatal("StickinessMs is nil: the announced margin was dropped, and nil resolves " +
				"to the package default rather than to what the meet asked for")
		}
		if *got.StickinessMs != 40 {
			t.Fatalf("StickinessMs = %v, want 40", *got.StickinessMs)
		}
	})
}

// TestMillisecondsAreNotMisscaled pins the unit conversion.
//
// Durations cross the wire as milliseconds and arrive as time.Duration, whose zero
// unit is the nanosecond. A translator that assigned the raw int64 would configure a
// coordinator a MILLION times too fast, and nothing downstream would catch it: every
// value would still be a valid duration and every tree would still build. The
// accessors exist so the conversion lives in one place; this asserts it happened.
func TestMillisecondsAreNotMisscaled(t *testing.T) {
	got := meetconfig.Coordinator(arbiter.CoordinatorConfig{Resolved: true, DwellMs: 1})
	if got.Dwell != time.Millisecond {
		t.Fatalf("DwellMs 1 became %v, want 1ms (raw assignment would give %v)",
			got.Dwell, time.Duration(1))
	}
}

// TestMemorylessStickinessIsNotUnset is the property the pointer exists for, and the
// one a translator is most likely to get wrong.
//
// On the wire 0 means MEMORYLESS — the Phase-4 build with no re-parent margin. In
// coordinator.Config a NIL StickinessMs means "use the stability-preserving default",
// which is 25. So a translator that passed the value through a "zero means unset"
// convention would turn an explicit request for memoryless into its opposite, silently,
// and the meet would be stickier than the operator asked for with nothing to show why.
func TestMemorylessStickinessIsNotUnset(t *testing.T) {
	got := meetconfig.Coordinator(arbiter.CoordinatorConfig{Resolved: true, StickinessMs: 0})
	if got.StickinessMs == nil {
		t.Fatalf("a memoryless meet translated to a nil StickinessMs, which resolves to %v — "+
			"the opposite of what was asked for", overlay.DefaultStickinessMs)
	}
	if *got.StickinessMs != 0 {
		t.Fatalf("StickinessMs = %v, want 0 (memoryless)", *got.StickinessMs)
	}
}

// TestDerivedThresholdsStayZero pins the other meaningful zero.
//
// 0 on the health thresholds means "derive per node from the cadence each peer
// declared" — the shipped design, and the only thing keeping a slow-beating peer from
// being reaped. A translator that substituted a default cadence here would hand the
// coordinator a hard-coded threshold and reopen exactly that bug.
func TestDerivedThresholdsStayZero(t *testing.T) {
	got := meetconfig.Coordinator(arbiter.CoordinatorConfig{Resolved: true})
	if got.DegradedAfter != 0 || got.GoneAfter != 0 {
		t.Fatalf("degraded/gone = %v/%v, want 0/0 so the coordinator derives them per peer",
			got.DegradedAfter, got.GoneAfter)
	}
}

// TestSelfNameDoesNotTravel pins the one field that must NOT come from the meet.
//
// SelfName is the HOLDER's identity, which differs per holder by definition — that is
// why arbiter.CoordinatorConfig has no field for it. The translator must leave it
// empty and every caller sets its own: the arbiter leaves it empty (its coordinator
// runs in-process), and an elected peer sets its own name. A translator that invented
// one would give two different holders the same identity.
func TestSelfNameDoesNotTravel(t *testing.T) {
	if got := meetconfig.Coordinator(tuned()); got.SelfName != "" {
		t.Fatalf("SelfName = %q, want empty: the holder's identity is not meet tuning", got.SelfName)
	}
}

// TestClockIsNotSet pins the other absence. Clock is a per-process capability, not a
// value, so it cannot ride an announcement; leaving it nil is what makes
// coordinator.New fall back to the system clock while a test can still inject one.
func TestClockIsNotSet(t *testing.T) {
	if got := meetconfig.Coordinator(tuned()); got.Clock != nil {
		t.Fatal("Clock was set from meet tuning; it is a per-process capability")
	}
}
