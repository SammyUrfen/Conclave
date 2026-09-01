package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// captureAnnouncer records what the arbiter announced, so a test can assert on the
// frame the wire would carry rather than on the config object it was built from.
type captureAnnouncer struct {
	mu   sync.Mutex
	anns []arbiter.Announcement
}

func (c *captureAnnouncer) AnnounceCoordinator(_ string, a arbiter.Announcement) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.anns = append(c.anns, a)
	return nil
}

func (c *captureAnnouncer) RepairCoordinator(_, _ string, _ arbiter.Announcement) error { return nil }

func (c *captureAnnouncer) first(t *testing.T) arbiter.Announcement {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.anns) == 0 {
		t.Fatal("the arbiter announced nothing; there is no configuration to inspect")
	}
	return c.anns[0]
}

// announceWith runs a real arbiter built from the given flags and returns the
// bootstrap announcement it makes when a peer joins.
//
// It drives the real arbiter rather than reading arbiterConfig() back, because the
// question this file answers is what an ELECTED PEER receives — and between the config
// and the peer sits the arbiter's own copy of it. Asserting on the struct alone would
// pass even if the announcement never carried it.
func announceWith(t *testing.T, args ...string) (arbiter.Announcement, *serverFlags, time.Duration) {
	t.Helper()
	f, err := parseFlags(args, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags(%v): %v", args, err)
	}
	if _, err := f.resolve(); err != nil {
		t.Fatalf("resolve(%v): %v", args, err)
	}
	sd := signaling.WSLivenessBudget

	capt := &captureAnnouncer{}
	arb := arbiter.New(testLogger(), f.arbiterConfig(sd), capt, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); _ = arb.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	arb.PeerJoined("standup", "p1", "alice")
	if err := arb.Sync(ctx); err != nil {
		t.Fatalf("arbiter sync: %v", err)
	}
	return capt.first(t), f, sd
}

// TestAnnouncementCarriesAResolvedMeetConfig is the bug this change fixes.
//
// The tuning is a property of the MEET, not of whichever node holds the role, so the
// arbiter ships it on every announcement. cmd/server is the only place that knows the
// flags AND the Hub's liveness budget, so if it does not fill the config in, the
// arbiter announces an unresolved one and an elected peer coordinates under whatever
// its own zero values happen to mean — silently retuning the subnet mid-meet.
//
// Asserting only "an announcement was sent" would pass against exactly that bug, which
// is why every assertion here is about the CONTENT being usable.
func TestAnnouncementCarriesAResolvedMeetConfig(t *testing.T) {
	ann, f, sd := announceWith(t, "-coordinate", "-elect")

	if !ann.Config.Resolved {
		t.Fatal("announced Config.Resolved is false: cmd/server never filled the meet " +
			"configuration in, so an elected peer would read every zero as \"use the default\"")
	}
	if err := ann.Config.Validate(); err != nil {
		t.Fatalf("announced config is unusable: %v", err)
	}
	// BuildTree divides an upload budget by StreamKbps and hard-errors on a
	// non-positive one, so a peer adopting this config would run and publish nothing.
	if ann.Config.StreamKbps <= 0 {
		t.Fatalf("announced StreamKbps = %d, want > 0 or BuildTree rejects every tree",
			ann.Config.StreamKbps)
	}
	if ann.Config.MaxDepth < 1 {
		t.Fatalf("announced MaxDepth = %d, want >= 1", ann.Config.MaxDepth)
	}
	// The one field a peer cannot possibly supply for itself: it describes the
	// SERVER's transport. Without it an elected peer leaves the permanent-ejection
	// window open.
	if got := ann.Config.SocketDetection(); got != sd {
		t.Fatalf("announced SocketDetection = %v, want the Hub's budget %v", got, sd)
	}
	if ann.Config != f.meetCoordinatorConfig(sd) {
		t.Fatalf("the announced config is not the one cmd/server built:\n got %+v\nwant %+v",
			ann.Config, f.meetCoordinatorConfig(sd))
	}
}

// TestLocalCoordinatorDerivesFromTheAnnouncedConfig is the equality obligation, and it
// guards a TRANSLATOR rather than two independent literals.
//
// The announced config and the config this server's own coordinator runs under must be
// the same tuning, or the meet silently re-shapes the moment the role moves off the
// arbiter. Two literals would drift — that is the DefaultArbiterID/ServerID lesson.
// Deriving one from the other makes disagreement unrepresentable, and this test is what
// pins the derivation itself.
func TestLocalCoordinatorDerivesFromTheAnnouncedConfig(t *testing.T) {
	// Every field moved off its default, so a translator that dropped one — or read
	// the wrong source — cannot coincide with the right answer by luck.
	args := []string{
		"-coordinate", "-elect",
		"-max-depth", "3", "-stream-kbps", "1500", "-default-upload-kbps", "500",
		"-stickiness-ms", "40", "-dwell", "11s", "-recompute-cooldown", "6s",
		"-degraded-after", "4s", "-gone-after", "9s", "-join-settle", "2s",
	}
	ann, f, sd := announceWith(t, args...)
	local := f.coordinatorConfig(sd)
	cc := ann.Config

	for _, tc := range []struct {
		field string
		got   any
		want  any
	}{
		{"MaxDepth", local.MaxDepth, cc.MaxDepth},
		{"StreamKbps", local.StreamKbps, cc.StreamKbps},
		{"DefaultUploadKbps", local.DefaultUploadKbps, cc.DefaultUploadKbps},
		// Dereferenced: the pointer is a local API convenience and must never be
		// compared against the wire's plain float, which would compare an address.
		{"StickinessMs", *local.StickinessMs, cc.StickinessMs},
		{"Dwell", local.Dwell, cc.Dwell()},
		{"RecomputeCooldown", local.RecomputeCooldown, cc.RecomputeCooldown()},
		{"DegradedAfter", local.DegradedAfter, cc.DegradedAfter()},
		{"GoneAfter", local.GoneAfter, cc.GoneAfter()},
		{"JoinSettle", local.JoinSettle, cc.JoinSettle()},
		{"SocketDetection", local.SocketDetection, cc.SocketDetection()},
	} {
		t.Run(tc.field, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("local coordinator %s = %v, announced = %v; the meet would re-shape "+
					"the moment the role moved off the arbiter", tc.field, tc.got, tc.want)
			}
		})
	}

	// SelfName is the one field that must NOT travel: it is the holder's own identity,
	// and an elected peer's coordinator names itself.
	if local.SelfName != "" {
		t.Fatalf("local SelfName = %q, want empty (this coordinator runs inside the arbiter)", local.SelfName)
	}
}

// TestDerivedThresholdsSurviveTheWire pins that the two sentinels this server relies on
// reach a peer intact.
//
// 0 on DegradedAfterMs/GoneAfterMs means "derive per node from the cadence each peer
// declared" — the shipped design and the only thing keeping a slow-beating peer from
// being reaped. A translator that helpfully substituted the default cadence here would
// hand an elected peer a hard-coded threshold and reopen exactly that bug.
func TestDerivedThresholdsSurviveTheWire(t *testing.T) {
	ann, _, _ := announceWith(t, "-coordinate", "-elect")
	if ann.Config.DegradedAfterMs != 0 || ann.Config.GoneAfterMs != 0 {
		t.Fatalf("announced degraded/gone = %d/%d ms, want 0/0 (derive per peer)",
			ann.Config.DegradedAfterMs, ann.Config.GoneAfterMs)
	}
	if !ann.Config.Resolved {
		t.Fatal("Resolved must still be true: a zero threshold is a CHOICE here, and " +
			"Resolved is what says so without needing a per-field sentinel")
	}
}

// TestMemorylessStickinessSurvivesTheWire pins the value the CoordinatorConfig contract
// is most emphatic about: StickinessMs 0 means MEMORYLESS, never "unset".
//
// -stickiness-ms 0 is documented by §10 and explicitly permitted by startup validation.
// If the translator emitted overlay.DefaultStickinessMs for it — "the effective value
// after the coordinator's own normalize" — it would put a number the operator did not
// choose on the wire and delete the very ambiguity this type was designed to remove.
func TestMemorylessStickinessSurvivesTheWire(t *testing.T) {
	ann, _, _ := announceWith(t, "-coordinate", "-elect", "-stickiness-ms", "0")
	if ann.Config.StickinessMs != 0 {
		t.Fatalf("announced StickinessMs = %v for -stickiness-ms 0, want 0 (memoryless); "+
			"emitting %v would put a value the operator never chose on the wire",
			ann.Config.StickinessMs, overlay.DefaultStickinessMs)
	}
	if err := ann.Config.Validate(); err != nil {
		t.Fatalf("a memoryless config must still validate: %v", err)
	}
}

// TestStartupIsSilentAboutTheMeetConfig asserts the arbiter's breadcrumb does not fire.
//
// arbiter.New warns "coordinator configuration is unusable" when -elect is on and the
// config does not validate. That warning firing at startup IS the bug: it means every
// elected peer in this process will publish no tree.
func TestStartupIsSilentAboutTheMeetConfig(t *testing.T) {
	var buf bytes.Buffer
	// Debug level so nothing is filtered out by the handler before it can be seen.
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	f, err := parseFlags([]string{"-coordinate", "-elect"}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	res, err := f.resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	p, err := newPlane(log, f, res)
	if err != nil {
		t.Fatalf("newPlane: %v", err)
	}
	defer p.close()
	p.logStartup()

	if got := buf.String(); strings.Contains(got, "coordinator configuration is unusable") {
		t.Fatalf("the arbiter warned that its meet configuration is unusable:\n%s", got)
	}
}

// TestUnusableMeetConfigIsFatalAtStartup pins the fail-loud half.
//
// The arbiter only warns, deliberately: an arbiter that refuses to start is worse than
// one that says why every meet is uncoordinated. cmd/server is where that becomes
// fatal, because a server that would coordinate nothing should not start and pretend.
func TestUnusableMeetConfigIsFatalAtStartup(t *testing.T) {
	f, err := parseFlags([]string{"-elect"}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	// Reach past the flag layer to build a config no flag combination can produce, so
	// the check is proven to be on the config itself and not a duplicate of the flag
	// validation that happens to cover the same ground.
	f.streamKbps = 0
	if err := validateMeetConfig(f, signaling.WSLivenessBudget); err == nil {
		t.Fatal("a meet config BuildTree cannot use was accepted at startup")
	} else if !strings.Contains(err.Error(), "stream_kbps") {
		t.Fatalf("error %q does not name the offending key", err)
	}

	f2, _ := parseFlags(nil, io.Discard)
	if err := validateMeetConfig(f2, signaling.WSLivenessBudget); err != nil {
		t.Fatalf("the shipped defaults produce an unusable meet config: %v", err)
	}
}
