package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/logging"
	"github.com/SammyUrfen/conclave/internal/meetconfig"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/policy"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// defaultAllowedOrigins is the frozen -allowed-origins default (docs/PLAN.md §9.8).
//
// The three entries are the three places a browser legitimately loads the dashboard
// from: a local dev server on any port, the same by IP literal for a machine whose
// DNS does not map localhost to loopback, and the GitHub Pages origin the static
// frontend is published to. The '*' is a PORT wildcard only — policy.ParseOrigins
// rejects every looser shape, because a host wildcard is how an allow-list becomes
// silently permissive.
const defaultAllowedOrigins = "http://localhost:*,http://127.0.0.1:*,https://sammyurfen.github.io"

// defaultRoom is the meet a peer lands in when it dials /ws with no ?room=.
//
// It DUPLICATES signaling's unexported default, and the duplication is load-bearing
// rather than lazy: the evictor keys a live connection by (meet, name) taken from the
// upgrade request, and the demo surface names a meet by its id, so the two must
// normalise an absent ?room= identically or an evict would silently miss. Reported to
// the spec owner as a candidate for export (signaling.DefaultRoomID).
const defaultRoom = "default"

// shutdownGrace bounds how long a graceful shutdown waits for in-flight requests. It
// is comfortably longer than any handler here (all of them are memory reads) and
// short enough that a stuck client cannot hold a container restart hostage.
const shutdownGrace = 10 * time.Second

// serverFlags is the parsed -flag set of docs/PLAN.md §10, one field per flag and
// nothing derived. Keeping the raw values in one struct is what lets every validation
// and every config-building helper below be a pure function of it — testable without
// a FlagSet, a listener, or a clock.
type serverFlags struct {
	addr      string
	logLevel  string
	logFormat string

	// The coordinator's constraints and cadences (§5).
	coordinate           bool
	maxDepth             int
	streamKbps           int
	defaultUploadKbps    int
	stickinessMs         float64
	rootChangeMarginKbps int
	joinSettle           time.Duration
	dwell                time.Duration
	recomputeCooldown    time.Duration
	degradedAfter        time.Duration
	goneAfter            time.Duration

	// The arbiter's election policy (§6).
	elect         bool
	electionDwell time.Duration
	minTerm       time.Duration

	// The browser-facing surface (§9).
	dashboard      bool
	allowedOrigins string
	publicURL      string
	demo           bool
}

// parseFlags registers and parses §10's frozen flag set. It writes usage and parse
// errors to errOut (nil ⇒ the FlagSet's default) so a test can keep them off stderr.
//
// A dedicated FlagSet rather than flag.CommandLine keeps parsing self-contained and
// testable, and ContinueOnError returns the error instead of calling os.Exit behind
// our back.
func parseFlags(args []string, errOut io.Writer) (*serverFlags, error) {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	if errOut != nil {
		fs.SetOutput(errOut)
	}
	f := &serverFlags{}

	fs.StringVar(&f.addr, "addr", ":9000", "TCP address to listen on (host:port)")
	fs.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	fs.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")

	fs.BoolVar(&f.coordinate, "coordinate", false,
		"run the coordinator in this process: compute and push relay trees from peer telemetry")
	fs.IntVar(&f.maxDepth, "max-depth", 2,
		"coordinator: max relay-tree depth in hops root→leaf")
	fs.IntVar(&f.streamKbps, "stream-kbps", 2000,
		"coordinator: assumed per-stream upload cost, kbit/s (child capacity = upload budget / this)")
	fs.IntVar(&f.defaultUploadKbps, "default-upload-kbps", 0,
		"coordinator: upload budget assumed for a peer that has not reported yet; 0 ⇒ leaf until it reports")
	// The help text describes what the value MEANS and deliberately does not imply it
	// currently changes a live tree. Stickiness is observable only where pairwise RTT
	// is measured — `unknown + margin < unknown` is false for every margin — and the
	// coordinator's projection leaves Node.RTT nil because Phase 5 has no RTT source.
	// So the flag is presently inert on a running meet, which is a Limitation of the
	// telemetry rather than of this flag, and overstating it here would be the more
	// misleading of the two errors.
	fs.Float64Var(&f.stickinessMs, "stickiness-ms", overlay.DefaultStickinessMs,
		"coordinator: RTT margin (ms) by which a challenger must beat a node's incumbent parent before it is re-parented; 0 ⇒ no margin (memoryless)")
	fs.IntVar(&f.rootChangeMarginKbps, "root-change-margin-kbps", overlay.RootChangeMarginKbps,
		"coordinator: extra upload (kbit/s) a challenger needs before the subnet is re-rooted")
	fs.DurationVar(&f.joinSettle, "join-settle", coordinator.JoinSettle,
		"coordinator: how long to wait after a join for telemetry before building anyway")
	fs.DurationVar(&f.dwell, "dwell", coordinator.DegradationDwell,
		"coordinator: how long metrics must stay bad before sustained degradation counts")
	fs.DurationVar(&f.recomputeCooldown, "recompute-cooldown", coordinator.RecomputeCooldown,
		"coordinator: minimum interval between two published trees for one meet")
	// The two health thresholds default to 0, which is NOT "unset": coordinator.Config
	// defines 0 as "derive this threshold per node from the cadence the peer DECLARED
	// on the wire", and a non-zero default here would mean the server always passes a
	// fixed value and that derivation never executes. The consequence is concrete: a
	// peer running -heartbeat 5s would be declared gone after 8s of silence while it
	// is beating perfectly. A non-zero value keeps its documented meaning as an
	// explicit GLOBAL OVERRIDE of the per-peer derivation.
	fs.DurationVar(&f.degradedAfter, "degraded-after", 0,
		"coordinator: override the per-peer degraded threshold with a fixed silence; 0 ⇒ derive it from each peer's declared cadence")
	fs.DurationVar(&f.goneAfter, "gone-after", 0,
		"coordinator: override the per-peer gone threshold with a fixed silence; 0 ⇒ derive it from each peer's declared cadence")

	fs.BoolVar(&f.elect, "elect", false,
		"arbitrate the coordinator role among peers (Phase 6)")
	fs.DurationVar(&f.electionDwell, "election-dwell", arbiter.ElectionDwell,
		"arbiter: sustained window before a voluntary coordinator handover")
	fs.DurationVar(&f.minTerm, "min-term", arbiter.MinTermDuration,
		"arbiter: floor between two voluntary handovers")

	fs.BoolVar(&f.dashboard, "dashboard", true, "mount the /api/ dashboard surface")
	fs.StringVar(&f.allowedOrigins, "allowed-origins", defaultAllowedOrigins,
		"comma-separated browser origin allow-list for CORS and the WS upgrade; a trailing '*' is a PORT wildcard")
	fs.StringVar(&f.publicURL, "public-url", "",
		"externally reachable base URL used to build join commands; empty ⇒ derive from -addr")
	fs.BoolVar(&f.demo, "demo", false,
		"register the DESTRUCTIVE demo control routes (evict a peer, force an election); off by default")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return f, nil
}

// resolved holds the values that had to be parsed or validated before anything could
// be constructed. Returning them from resolve rather than re-parsing at each use is
// what stops two surfaces disagreeing about the same string.
type resolved struct {
	level   slog.Level
	format  logging.Format
	origins policy.Origins
}

// resolve validates every flag that can be checked without a running dependency and
// returns the parsed forms.
//
// Fail-loud discipline: an operator typo in a rarely-exercised value must break the
// process immediately, with a message naming the flag, rather than surface later as
// "the dashboard mysteriously cannot reach the arbiter". The one relationship this
// cannot see — the Hub's socket-death window — is checked by validateLivenessBudget
// once the Hub exists.
func (f *serverFlags) resolve() (resolved, error) {
	var r resolved

	level, err := logging.ParseLevel(f.logLevel)
	if err != nil {
		return r, err
	}
	r.level = level

	format, err := parseFormat(f.logFormat)
	if err != nil {
		return r, err
	}
	r.format = format

	origins, err := policy.ParseOrigins(f.allowedOrigins)
	if err != nil {
		return r, fmt.Errorf("-allowed-origins: %w", err)
	}
	// Empty means same-origin only. That is a legal posture for a pure signaling
	// relay (a Go peer sends no Origin header), but with the browser dashboard
	// mounted it guarantees every cross-origin request is refused — a server that
	// starts, looks healthy, and serves nobody.
	if f.dashboard && len(origins) == 0 {
		return r, fmt.Errorf("-allowed-origins is empty while -dashboard is on: " +
			"the dashboard is a cross-origin browser surface and would refuse every request")
	}
	r.origins = origins

	if err := f.validateTimings(); err != nil {
		return r, err
	}
	if err := validateCoordinatorFlags(f); err != nil {
		return r, err
	}
	return r, nil
}

// parseFormat validates -log-format.
//
// It exists because logging.New's default branch turns ANY unrecognised format into
// JSON: without this parse step "-log-format tex" would start a server logging JSON
// and say nothing about it. ParseLevel already rejects a bad level, so accepting a
// bad format was an inconsistency, not a policy. cmd/peer fixed the same bug.
func parseFormat(s string) (logging.Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case string(logging.FormatText):
		return logging.FormatText, nil
	case string(logging.FormatJSON):
		return logging.FormatJSON, nil
	default:
		return "", fmt.Errorf("logging: unknown -log-format %q (want text|json)", s)
	}
}

// validateTimings enforces §10's startup rules over the duration flags.
//
// Every one of them must be positive: the zero value is a legal "use the package
// default" inside coordinator.Config and arbiter.Config, but reaching that from argv
// would mean an operator who typed "0" got a value they did not ask for and no
// warning. Negative is worse still — arbiter reads it as "disable this brake".
func (f *serverFlags) validateTimings() error {
	for _, d := range []struct {
		flag string
		val  time.Duration
	}{
		{"-join-settle", f.joinSettle},
		{"-dwell", f.dwell},
		{"-recompute-cooldown", f.recomputeCooldown},
		{"-election-dwell", f.electionDwell},
		{"-min-term", f.minTerm},
	} {
		if d.val <= 0 {
			return fmt.Errorf("%s must be > 0, got %v", d.flag, d.val)
		}
	}
	// -degraded-after and -gone-after are the exception, and only in one direction:
	// 0 is their documented "derive per node" value and their default, but NEGATIVE
	// means nothing to the coordinator and would be read as 0 there — an operator who
	// typed "-1s" would silently get the derivation they did not ask for.
	for _, d := range []struct {
		flag string
		val  time.Duration
	}{
		{"-degraded-after", f.degradedAfter},
		{"-gone-after", f.goneAfter},
	} {
		if d.val < 0 {
			return fmt.Errorf("%s must be >= 0 (0 ⇒ derive from each peer's declared cadence), got %v",
				d.flag, d.val)
		}
	}
	// The health FSM is an ordered walk healthy → degraded → gone, so degraded must
	// fire strictly first. The check applies ONLY when both are explicit: with either
	// one derived, the comparison is between a fixed value and a per-peer one that no
	// startup check can see, and rejecting on it would refuse configurations that are
	// perfectly ordered for every peer that actually connects. The derived pair is
	// ordered by construction anyway — metrics.DegradedAfter is 3 beats and
	// metrics.GoneAfter is 8, of the same cadence.
	if f.degradedAfter > 0 && f.goneAfter > 0 && f.degradedAfter >= f.goneAfter {
		return fmt.Errorf("-degraded-after (%v) must be strictly less than -gone-after (%v)",
			f.degradedAfter, f.goneAfter)
	}
	return nil
}

// validateCoordinatorFlags rejects coordinator constraints that BuildTree cannot
// satisfy, but only when the coordinator is actually enabled (the flags are inert
// otherwise). These mirror overlay.BuildTree's own preconditions, checked here so the
// failure is a clear startup error rather than a silent per-recompute warning.
func validateCoordinatorFlags(f *serverFlags) error {
	if f.rootChangeMarginKbps < 0 {
		return fmt.Errorf("-root-change-margin-kbps must be >= 0, got %d", f.rootChangeMarginKbps)
	}
	if !f.coordinate {
		return nil
	}
	if f.streamKbps <= 0 {
		return fmt.Errorf("-stream-kbps must be > 0, got %d", f.streamKbps)
	}
	if f.maxDepth < 1 {
		return fmt.Errorf("-max-depth must be >= 1, got %d", f.maxDepth)
	}
	if f.stickinessMs < 0 {
		return fmt.Errorf("-stickiness-ms must be >= 0, got %v", f.stickinessMs)
	}
	// §10 froze the flag; overlay froze the value as a compile-time constant with no
	// Constraints field to carry it, so there is nowhere for a different value to go.
	// Refusing is the only honest answer: accepting it would leave an operator with a
	// re-rooting margin they configured, can see in `-h`, and are not getting.
	// REPORTED to the spec owner — the fix is one field on overlay.Constraints.
	if f.rootChangeMarginKbps != overlay.RootChangeMarginKbps {
		return fmt.Errorf("-root-change-margin-kbps=%d cannot be honoured: overlay froze the margin "+
			"as the compile-time constant overlay.RootChangeMarginKbps=%d and exposes no Constraints "+
			"field for it, so only %d is achievable (docs/PLAN.md §10 vs §3)",
			f.rootChangeMarginKbps, overlay.RootChangeMarginKbps, overlay.RootChangeMarginKbps)
	}
	return nil
}

// validateLivenessBudget checks the one relationship between conclave's two
// independent liveness detectors, and it is cmd/server's job because this is the only
// place that sees both the Hub and the control-plane thresholds.
//
// The Hub's socket reaper must not be SLOWER than the coordinator's gone threshold.
// If it were, a peer could be dropped from the tree while its socket is still
// registered — and since the join handshake only happens on a NEW connection, a peer
// whose socket never closed could never re-announce itself: permanently ejected from
// a meet it believes it is in, with no error anywhere.
//
// It reports against -gone-after because that is the flag an operator can act on. The
// WS ping family is deliberately compile-time only (§10): its two constants are bound
// by a relationship spanning two processes, so exposing them would be a footgun with
// no use case behind it.
func validateLivenessBudget(goneAfter, socketDetection time.Duration) error {
	if goneAfter <= 0 {
		// 0 means "derive per node from the cadence each peer declares", so there is no
		// fixed threshold to compare against and the best a startup check can do is
		// verify the DEFAULT cadence — which is exactly what
		// metrics.ValidateLivenessBudget is for.
		//
		// Its message is wrapped rather than returned bare because it names -heartbeat,
		// which is a PEER flag: correct advice, useless to whoever is starting THIS
		// process. Naming -gone-after gives them a remedy they can apply here, and
		// keeping the wrapped cause preserves the other one for the peer's operator.
		if err := metrics.ValidateLivenessBudget(metrics.HeartbeatInterval, socketDetection); err != nil {
			return fmt.Errorf("%w; on this server, set -gone-after to at least %v",
				err, socketDetection)
		}
		return nil
	}
	if socketDetection <= 0 {
		return fmt.Errorf("the WebSocket liveness budget %v must be positive", socketDetection)
	}
	if socketDetection > goneAfter {
		return fmt.Errorf("-gone-after=%v is shorter than the WebSocket liveness budget %v: "+
			"the control plane would declare a peer gone while the Hub still holds its socket, "+
			"and a peer whose socket never closed cannot re-announce itself; raise -gone-after to at least %v",
			goneAfter, socketDetection, socketDetection)
	}
	return nil
}

// meetCoordinatorConfig is THE tuning this server gives every meet it arbitrates.
//
// It is the single source for two consumers that must never disagree: the arbiter
// announces it to whichever peer is elected, and this process's own coordinator runs
// under it. Two literals would drift — that is the DefaultArbiterID/ServerID lesson —
// so coordinatorConfig below is a TRANSLATION of this value rather than a second
// spelling of the same flags.
//
// EVERY VALUE HERE IS THE RESOLVED EFFECTIVE ONE. arbiter.CoordinatorConfig makes
// absence unrepresentable per field precisely so no reader has to know which sense a
// zero carries, so emitting a raw flag that still needs defaulting would put the
// ambiguity back — on the wire, where it is worst. Two zeros survive deliberately and
// are not absences: DegradedAfterMs/GoneAfterMs 0 means "derive the threshold per node
// from the cadence each peer declared", and StickinessMs 0 means memoryless. Startup
// validation forces -dwell, -recompute-cooldown and -join-settle strictly positive, so
// none of them can reach this function as a zero needing interpretation.
//
// socketDetection is the one input that is not a flag: it is the Hub's own
// socket-death window, and it is the field an elected peer could not possibly supply
// for itself because it describes THIS server's transport. Without it a peer
// coordinator leaves the permanent-ejection window open.
func (f *serverFlags) meetCoordinatorConfig(socketDetection time.Duration) arbiter.CoordinatorConfig {
	return arbiter.CoordinatorConfig{
		Resolved:            true,
		MaxDepth:            f.maxDepth,
		StreamKbps:          f.streamKbps,
		DefaultUploadKbps:   f.defaultUploadKbps,
		StickinessMs:        f.stickinessMs,
		DwellMs:             millis(f.dwell),
		RecomputeCooldownMs: millis(f.recomputeCooldown),
		DegradedAfterMs:     millis(f.degradedAfter),
		GoneAfterMs:         millis(f.goneAfter),
		JoinSettleMs:        millis(f.joinSettle),
		SocketDetectionMs:   millis(socketDetection),
	}
}

// millis converts a duration to the wire's millisecond representation. Durations cross
// this boundary as milliseconds — matching metrics.Heartbeat.IntervalMs — because
// time.Duration marshals as nanoseconds, which round-trips fine and is unreadable in a
// log line at exactly the moment someone is debugging a mis-shaped meet.
func millis(d time.Duration) int64 { return int64(d / time.Millisecond) }

// coordinatorConfig builds this process's own coordinator Config by TRANSLATING the
// meet configuration above.
//
// Deriving rather than re-reading the flags is the whole point: the announced tuning
// and the tuning the local coordinator runs under are then the same values by
// construction, so the meet cannot silently re-shape the moment the role moves off the
// arbiter. What remains testable is the translation itself, which is a strictly
// smaller thing to guard than two independent literals.
func (f *serverFlags) coordinatorConfig(socketDetection time.Duration) coordinator.Config {
	// meetconfig.Coordinator is the SHARED translator, and sharing it is the point: an
	// elected peer runs the identical conversion on the announcement it receives, so
	// the arbiter's in-process coordinator and a peer's reach the same configuration
	// from the same frame. It lives in its own package because Go cannot import a main
	// package and the dependency graph forbids arbiter and coordinator importing each
	// other (docs/PLAN.md §2.3).
	//
	// SelfName is deliberately left as the translator leaves it — empty — because that
	// empty value is what marks a coordinator running inside the arbiter process (§5.8).
	// An elected peer sets its own name there.
	return meetconfig.Coordinator(f.meetCoordinatorConfig(socketDetection))
}

// validateMeetConfig fails startup on a meet configuration that cannot drive a
// coordinator.
//
// arbiter.New only WARNS about this, deliberately: an arbiter that refuses to start is
// worse than one that says why every meet is uncoordinated. cmd/server is where it
// becomes fatal, because a server whose every elected peer would publish nothing
// should not start and look healthy. The error names the offending JSON key, which is
// the vocabulary a peer's operator sees on the wire.
func validateMeetConfig(f *serverFlags, socketDetection time.Duration) error {
	if err := f.meetCoordinatorConfig(socketDetection).Validate(); err != nil {
		return fmt.Errorf("the coordinator configuration this server would announce is unusable: %w", err)
	}
	return nil
}

// arbiterConfig builds the arbiter's Config from the flags.
//
// ArbiterID is set EXPLICITLY to signaling.ServerID and that is the point of this
// function existing (§2.4). arbiter may not import signaling, so it carries its own
// "_server" literal as a test fallback; cmd/server is the one place that legitimately
// sees both constants, so it is the one place responsible for passing the wire value
// across the forbidden edge. Leaving the field empty would work by accident today and
// break silently the day either constant moves.
func (f *serverFlags) arbiterConfig(socketDetection time.Duration) arbiter.Config {
	return arbiter.Config{
		Elect:         f.elect,
		Coordinate:    f.coordinate,
		ArbiterID:     signaling.ServerID,
		ElectionDwell: f.electionDwell,
		MinTerm:       f.minTerm,
		// The tuning travels with the role. It is the SAME value coordinatorConfig
		// translates for this process's own coordinator, so an elected peer and the
		// arbiter shape the meet identically and a handover changes nothing but who
		// is doing the shaping.
		CoordinatorConfig: f.meetCoordinatorConfig(socketDetection),
		// ValidMeetID is wired explicitly rather than left nil, for the same reason:
		// nil resolves to policy.ValidMeetID inside arbiter, which is correct but
		// invisible. Naming it here makes the shared boundary predicate greppable
		// from the one file that wires every surface to it.
		ValidMeetID: policy.ValidMeetID,
	}
}

// hubConfig builds the signaling Hub's config. The origin allow-list is the SAME
// policy.Origins value the dashboard's CORS uses — injected in both places rather
// than written down twice, which is the entire reason internal/policy exists (§2.4).
func (f *serverFlags) hubConfig(log *slog.Logger, origins policy.Origins) signaling.HubConfig {
	return signaling.HubConfig{Log: log, AllowedOrigins: origins}
}
