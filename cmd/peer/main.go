// Command peer is a conclave participant node. It has two modes:
//
//   - probe (default): dial the server's /healthz and log the result — the
//     Phase 0 reachability check.
//   - call (-call): join a signaling room and establish a WebRTC call with the
//     other peer(s) in it, optionally sending a VP8 track and/or recording the
//     received one. This is the Phase 1 capture→signal→connect→media pipeline.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/logging"
	"github.com/SammyUrfen/conclave/internal/media"
	"github.com/SammyUrfen/conclave/internal/meetconfig"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "peer: "+err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	opts, err := parseArgs(args)
	if err != nil {
		return err
	}
	logger := logging.New(os.Stdout, opts.level, opts.format).With(
		slog.String("service", "conclave-peer"),
	)
	// A flag that silently does nothing is worse than one that errors: the operator
	// believes they configured something. These cannot be hard errors (their
	// defaults are non-zero, so every plain -call would fail), so they are named.
	for _, name := range opts.inert {
		logger.Warn("flag has no effect in this mode", slog.String("flag", name))
	}

	if opts.call {
		// The signal context is installed HERE, at the process edge, and not inside
		// runCall: runCall is then drivable from a test (and, later, from a harness)
		// with an ordinary cancellable context, which is the same
		// orchestration-vs-mechanism split every other seam in this project uses.
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runCall(ctx, logger, opts.cfg)
	}
	return runProbe(logger, opts.server, opts.timeout)
}

// options is the fully parsed and VALIDATED command line. Parsing and validation
// are one step, performed before anything is constructed, so a bad or
// self-contradictory flag combination fails at startup rather than half-way into a
// call — the same fail-loud discipline as logging.ParseLevel.
type options struct {
	server  string
	level   slog.Level
	format  logging.Format
	timeout time.Duration
	call    bool
	cfg     callConfig
	// inert names the flags the user set EXPLICITLY that this configuration cannot
	// act on. Warned about, never fatal; see run.
	inert []string
}

// callConfig is the parsed configuration for call mode.
type callConfig struct {
	server, room, mediaPath, recordPath, stun string
	name, topologyPath                        string
	send                                      bool
	managed                                   bool
	uploadKbps                                int
	nat                                       overlay.NATType
	// mediaPorts is the UDP range media is confined to; the zero value means
	// "ephemeral", which is what a peer run without -media-ports gets.
	mediaPorts [2]uint16

	// heartbeat is the liveness cadence this peer declares on every beat.
	//
	// It rides the wire (metrics.Heartbeat.IntervalMs) because the coordinator's
	// degraded/gone thresholds are MULTIPLES of the cadence a peer actually uses,
	// not of the default: a peer beating every 5 s against thresholds computed from
	// 1 s would be declared gone while beating perfectly.
	heartbeat time.Duration

	// backup is this peer's failover INTENT: on primary-parent failure, promote the
	// backup parent the coordinator precomputed, without asking anyone. Expressed
	// positively, because that is what -backup means to an operator;
	// routerConfigFor is the single place it is translated into media's field.
	backup bool

	// coordinatable declares whether this peer is WILLING to be elected coordinator
	// (a laptop on battery says false). It is a hard disqualifier in arbiter.Score,
	// not a penalty.
	//
	// The CLI default is true and the wire default is false, and that asymmetry is
	// deliberate. On the wire, absent decodes to false, so an older build is treated
	// as unwilling rather than silently conscripted — the conservative direction. On
	// the command line the conservative choice would be the wrong one: it would make
	// "no peer is ever elected" the default posture of the whole system, and the
	// failure is SILENT (arbiter.Score returns a hard 0, no error is raised, the meet
	// simply stays on the arbiter forever). A peer that ran this binary and said
	// nothing is an ordinary participant; a peer that must decline says so.
	coordinatable bool
}

// parseArgs parses argv into a validated options. It is separated from run — and
// takes args rather than reading os.Args — so every default and every rejection is
// table-testable without starting a process.
func parseArgs(args []string) (options, error) {
	fs := flag.NewFlagSet("peer", flag.ContinueOnError)
	server := fs.String("server", "http://localhost:9000", "base URL of the conclave server")
	logLevel := fs.String("log-level", "info", "log level: debug|info|warn|error")
	logFormat := fs.String("log-format", "text", "log format: json|text")
	timeout := fs.Duration("timeout", 5*time.Second, "overall timeout for the health probe (probe mode)")
	call := fs.Bool("call", false, "join a room and establish a WebRTC call instead of probing /healthz")
	room := fs.String("room", "default", "room to join (call mode)")
	send := fs.Bool("send", false, "send a video track to peers (call mode)")
	mediaPath := fs.String("media", "", "VP8 IVF file to send; empty sends synthetic frames (call mode; implies -send)")
	recordPath := fs.String("record", "", "write the first received track to this IVF file; empty just counts (call mode)")
	stun := fs.String("stun", "", "STUN server URL, e.g. stun:stun.l.google.com:19302 (call mode; empty is fine on one host)")
	name := fs.String("name", "", "stable topology name for this peer, e.g. relay|leaf-b (tree mode)")
	topology := fs.String("topology", "", "path to a tree topology JSON file; enables static tree mode (empty ⇒ full mesh)")
	managed := fs.Bool("managed", false, "join a coordinator-managed room: report telemetry and realise the pushed tree (requires -name; no static -topology)")
	uploadKbps := fs.Int("upload-kbps", 3000, "advertised upload budget for forwarding others' media, kbit/s (managed mode)")
	natType := fs.String("nat", "direct", "declared NAT class: direct|turn (turn ⇒ forced leaf) (managed mode)")
	heartbeat := fs.Duration("heartbeat", metrics.HeartbeatInterval,
		"liveness beat interval; declared on the wire, and what the coordinator computes its degraded/gone thresholds from (tree mode)")
	backup := fs.Bool("backup", true,
		"on primary-parent failure, promote the coordinator's precomputed backup parent without asking (tree mode)")
	coordinatable := fs.Bool("coordinatable", true,
		"may this peer be elected coordinator; false declines (a laptop on battery) (managed mode)")
	mediaPorts := fs.String("media-ports", "",
		"confine media (ICE/SRTP) to these UDP ports, e.g. 47000-47019; empty lets the OS choose (call mode)")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}

	ports, err := parseMediaPorts(*mediaPorts)
	if err != nil {
		return options{}, err
	}

	level, err := logging.ParseLevel(*logLevel)
	if err != nil {
		return options{}, err
	}
	format, err := parseFormat(*logFormat)
	if err != nil {
		return options{}, err
	}
	nat, err := parseNAT(*natType)
	if err != nil {
		return options{}, err
	}

	cfg := callConfig{
		server:        *server,
		room:          *room,
		mediaPath:     *mediaPath,
		recordPath:    *recordPath,
		stun:          *stun,
		name:          *name,
		topologyPath:  *topology,
		send:          *send || *mediaPath != "",
		managed:       *managed,
		uploadKbps:    *uploadKbps,
		nat:           nat,
		heartbeat:     *heartbeat,
		backup:        *backup,
		coordinatable: *coordinatable,
		mediaPorts:    ports,
	}
	if err := validate(cfg, *timeout); err != nil {
		return options{}, err
	}

	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	return options{
		server:  *server,
		level:   level,
		format:  format,
		timeout: *timeout,
		call:    *call,
		cfg:     cfg,
		inert:   inertFlags(set, *call, cfg),
	}, nil
}

// validate rejects every configuration this peer cannot honour. Each rule fails
// loud with the flag name in the message, because the alternative — a peer that
// starts and then behaves subtly differently from what was asked — is exactly the
// class of bug this project spends its budget avoiding.
func validate(cfg callConfig, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("invalid -timeout %v: must be positive", timeout)
	}
	if cfg.uploadKbps < 0 {
		return fmt.Errorf("invalid -upload-kbps %d: must not be negative", cfg.uploadKbps)
	}
	if cfg.heartbeat <= 0 {
		return fmt.Errorf("invalid -heartbeat %v: must be positive", cfg.heartbeat)
	}
	// Sub-millisecond cadences truncate to interval_ms = 0 on the wire, which
	// metrics.Heartbeat.Interval reads back as "the default" — so the coordinator
	// would size this peer's gone threshold from a cadence it is not using. Rejecting
	// is better than silently rounding: the operator asked for something the frame
	// cannot express.
	if cfg.heartbeat < time.Millisecond {
		return fmt.Errorf("invalid -heartbeat %v: the wire carries whole milliseconds, so anything under 1ms "+
			"would be sent as 0 and read back as the %v default", cfg.heartbeat, metrics.HeartbeatInterval)
	}
	// Tree mode has two flavours: a STATIC topology loaded from a file (Phase 3), or
	// a MANAGED meet where the coordinator computes and pushes the tree (Phase 4+).
	// Both need a -name so this peer can find itself; fail loud rather than silently
	// falling back to mesh.
	if cfg.managed && cfg.topologyPath != "" {
		return errors.New("-managed and -topology are mutually exclusive: a pushed tree or a static one, not both")
	}
	if cfg.topologyPath != "" && cfg.name == "" {
		return errors.New("-topology requires -name so this peer can locate itself in the tree")
	}
	if cfg.managed && cfg.name == "" {
		return errors.New("-managed requires -name so the coordinator can place this peer in the tree")
	}
	return nil
}

// managedOnlyFlags are the flags whose effect depends on the peer's mode, in a
// FIXED (sorted) order so the warning list is deterministic rather than dependent on
// the order they appeared in argv.
var managedOnlyFlags = []string{"backup", "coordinatable", "heartbeat", "nat", "upload-kbps"}

// inertFlags returns the subset of managedOnlyFlags the user set explicitly that
// this configuration cannot act on. Only explicitly-set flags count: a default that
// happens to be unused is not a mistake, whereas a value someone typed and that is
// then ignored is.
func inertFlags(set map[string]bool, call bool, cfg callConfig) []string {
	tree := cfg.managed || cfg.topologyPath != ""
	usable := map[string]bool{
		// Telemetry inputs: consumed only by a coordinator building a tree.
		"coordinatable": cfg.managed,
		"upload-kbps":   cfg.managed,
		"nat":           cfg.managed,
		"heartbeat":     shouldBeat(call, cfg),
		"backup":        tree,
	}
	var inert []string
	for _, name := range managedOnlyFlags {
		if set[name] && !usable[name] {
			inert = append(inert, name)
		}
	}
	return inert
}

// parseFormat validates the -log-format flag.
//
// It exists because logging.New treats any unrecognised format as JSON, so a typo
// would silently change the shape of every log line this process emits instead of
// saying anything. Same reasoning as logging.ParseLevel, applied at the flag.
func parseFormat(s string) (logging.Format, error) {
	switch logging.Format(strings.ToLower(strings.TrimSpace(s))) {
	case logging.FormatText:
		return logging.FormatText, nil
	case logging.FormatJSON:
		return logging.FormatJSON, nil
	default:
		return "", fmt.Errorf("invalid -log-format %q: must be text or json", s)
	}
}

// parseNAT validates the -nat flag into an overlay.NATType, failing loud on an
// unknown value rather than silently treating a typo as "direct" (which could
// wrongly make a TURN-bound peer eligible to relay).
func parseNAT(s string) (overlay.NATType, error) {
	switch s {
	case "direct":
		return overlay.NATDirect, nil
	case "turn":
		return overlay.NATRelayed, nil
	default:
		return "", fmt.Errorf("invalid -nat %q: must be direct or turn", s)
	}
}

// sampleReport is the telemetry snapshot shipped on every metrics tick. It is a
// function of the command line today; when real sensors land (RTT, loss, CPU) they
// fold in here and nothing else moves — which is why metrics.Reporter takes a
// sampler rather than a value.
// telemetry is the peer's live sensor bundle: everything a Report carries that is
// MEASURED rather than declared on the command line.
//
// Every sensor is a func seam and every one is optional, which is the whole shape of
// the type. A probe-mode peer has no Router, a non-Linux host has no /proc/stat, and
// a peer whose control link has just died has no fresh RTT — none of those may fault
// the peer, and none may be papered over with a zero. metrics.Reporter already
// applies that rule to a failed send; this applies it to a failed reading.
//
// Each sensor returns (value, ok) rather than a bare value, and the ok is
// load-bearing rather than defensive. Report.CPUPct and RTTServerMs are float64s
// whose zero means BOTH "not measured" and "perfect", and arbiter.Score reads the
// perfect meaning — so a sensor with nothing to say must leave the field untouched,
// not write its own zero.
type telemetry struct {
	// cfg is the declared half: the flags, unchanged.
	cfg callConfig
	// cpu reports host CPU busy percent (metrics.CPUSampler.Sample). nil off Linux.
	cpu func() (float64, bool)
	// rtt reports the last measured round trip to the arbiter
	// (metrics.RTTProbe.LastMs). nil before the probe is started.
	rtt func() (float64, bool)
	// links reports measured pairwise RTT and worst-leg uplink loss
	// (media.Router.LinkStats). nil until the Router exists.
	links func() ([]metrics.PeerRTT, float64)
}

// sample builds one Report: the declared half from the flags, plus whatever the
// sensors currently have to say.
func (t *telemetry) sample() metrics.Report {
	rep := sampleReport(t.cfg)
	if t.cpu != nil {
		if pct, ok := t.cpu(); ok {
			rep.CPUPct = pct
		}
	}
	if t.rtt != nil {
		if ms, ok := t.rtt(); ok {
			rep.RTTServerMs = ms
		}
	}
	if t.links != nil {
		rep.PeerRTT, rep.LossPct = t.links()
	}
	// LAST LINE OF DEFENCE, and it is here rather than only in the sensors because
	// this is where the value becomes a frame. A NaN or an infinity marshals to
	// INVALID JSON — encoding/json returns an error instead of bytes — so one bad
	// reading would end this peer's telemetry entirely and the coordinator would
	// watch it go quiet with nothing logged to say why. Each sensor clamps at its own
	// boundary too; a reader should not have to trust three packages to know that a
	// report is always sendable.
	rep.CPUPct = finite(rep.CPUPct)
	rep.RTTServerMs = finite(rep.RTTServerMs)
	rep.LossPct = finite(rep.LossPct)
	for i := range rep.PeerRTT {
		rep.PeerRTT[i].RTTMs = finite(rep.PeerRTT[i].RTTMs)
	}
	// Ordered before it goes out, as metrics.Report.PeerRTT requires: this value
	// becomes overlay.Node.RTT and shapes the tree, so two peers holding identical
	// measurements must emit identical frames.
	rep.Normalize()
	return rep
}

// finite maps a non-finite reading to 0 — "not measured" — because that is the only
// value that is both marshallable and honest about a sensor that has malfunctioned.
func finite(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

// parseMediaPorts reads the -media-ports flag: "lo-hi", or a bare port meaning a
// range of one, or empty for pion's ephemeral default.
//
// It fails loud on anything unusable, because the alternative is far worse than a
// startup error: a peer given an impossible range gathers no candidates, never
// connects, and reports nothing that points at the flag. Bad config should stop a
// peer at startup, not surface as a mysterious missing stream later.
func parseMediaPorts(s string) ([2]uint16, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return [2]uint16{}, nil
	}
	lo, hi, found := strings.Cut(s, "-")
	if !found {
		hi = lo
	}
	parse := func(field string) (uint16, error) {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		// Port 0 is excluded deliberately: to the kernel it means "pick any", so a
		// range starting at 0 would silently disable the confinement this flag exists
		// to provide.
		if err != nil || n < 1 || n > 65535 {
			return 0, fmt.Errorf("invalid -media-ports %q: %q is not a port in 1-65535", s, strings.TrimSpace(field))
		}
		return uint16(n), nil
	}
	min, err := parse(lo)
	if err != nil {
		return [2]uint16{}, err
	}
	max, err := parse(hi)
	if err != nil {
		return [2]uint16{}, err
	}
	if max < min {
		return [2]uint16{}, fmt.Errorf("invalid -media-ports %q: the high port %d is below the low port %d", s, max, min)
	}
	return [2]uint16{min, max}, nil
}

func sampleReport(cfg callConfig) metrics.Report {
	return metrics.Report{
		Name:       cfg.name,
		UploadKbps: cfg.uploadKbps,
		NAT:        cfg.nat,
		// Always emitted, never omitted: absent decodes to false, and a peer that
		// declines must look different on the wire from one whose build predates the
		// field. metrics.Report deliberately leaves omitempty off for the same reason.
		Coordinatable: cfg.coordinatable,
	}
}

// routerConfigFor builds the media policy for this peer. It is a function rather
// than an inline literal so the two translations that matter — operator intent to
// media's fields, and the mode gating on the control-plane callbacks — are testable
// without a socket or a PeerConnection.
func routerConfigFor(
	cfg callConfig,
	topo *overlay.Topology,
	iceServers []webrtc.ICEServer,
	clk clock.Clock,
	onReparented func(metrics.Reparented),
	onCoordinator func(payload []byte),
) media.RouterConfig {
	rc := media.RouterConfig{
		ICEServers:     iceServers,
		MediaPortRange: cfg.mediaPorts,
		SendMedia:      cfg.send,
		MediaPath:      cfg.mediaPath,
		RecordPath:     cfg.recordPath,
		Topology:       topo,
		SelfName:       cfg.name,
		Managed:        cfg.managed,
		// THE POLARITY FLIP, and the only one in the tree. -backup reads positively
		// to an operator ("do the failover"); media spells it negatively so that its
		// ZERO value is the safe case — a caller who forgets the field gets failover
		// enabled rather than silently disabled (§15.13). Both spellings are right
		// for their own side, and this line is where they meet.
		DisableBackup: !cfg.backup,
		// Injected, never clock.System() inline: every re-parent deadline and the
		// relay's PLI throttle hang off this, and a wall-clock call here would make
		// them undrivable from a test.
		Clock:        clk,
		OnReparented: onReparented,
	}
	// Announcements are only meaningful where there is a control plane to obey. A
	// mesh or static-tree peer applies no pushed topology at all, so adopting an
	// epoch would be bookkeeping with nothing downstream of it; media documents nil
	// as the correct value for those modes.
	if cfg.managed {
		rc.OnCoordinator = onCoordinator
	}
	return rc
}

// coordinatorAdopter raises this peer's fence to a newly announced epoch, reporting
// false for a stale or duplicate one. It is media.Router.AdoptCoordinator in
// production, and a bare overlay.Fence in tests — the seam exists so the wiring
// below can be tested against the very same fence type the Router holds.
type coordinatorAdopter func(epoch uint64, coordinatorID string) bool

// adoptAnnouncement decodes a TypeCoordinator body, hands it to the fence, and
// reports whether it was adopted and whether it names THIS peer as coordinator.
//
// This is the authorization half of §6.5, and it is the last link in it: media
// enforces the fence on every pushed topology, but a fence that never learns WHO the
// arbiter named refuses everything — the peer sits fully connected and receives no
// tree, with nothing logged above debug. Wiring this is what turns Fence.Accept from
// a gate that always says no into the real predicate.
//
// It deliberately does NOT reset the fence anywhere. media owns TypeJoined and
// resets there, on its Run goroutine; a second reset from here could land AFTER an
// adoption and silently unfence the peer, which is a privilege-escalation regression
// rather than a cosmetic one.
//
// The decoded announcement is returned along with the two predicates so the caller
// can drive the coordinator lifecycle without decoding the same bytes twice — and so
// self, which starts and stops an in-process coordinator, is a tested value rather
// than something rediscovered at the call site.
func adoptAnnouncement(log *slog.Logger, selfID func() string, adopt coordinatorAdopter, payload []byte) (ann arbiter.Announcement, adopted, self bool) {
	if err := json.Unmarshal(payload, &ann); err != nil {
		// A malformed announcement is dropped, not fatal: the arbiter re-broadcasts
		// the current announcement on every membership change, so the next one
		// repairs this peer.
		log.Warn("bad coordinator announcement", slog.Any("error", err))
		return arbiter.Announcement{}, false, false
	}
	// Compared by the SERVER-ASSIGNED ID, never by -name: an announcement names the
	// coordinator by id, so a peer matching it against its own name could never
	// recognise itself and would silently decline the job it was just given. The id
	// is "" until the joined frame lands, which reads as "not me" — the safe answer
	// in that window, and the reason the empty-id guard is here rather than implied.
	id := selfID()
	self = id != "" && id == ann.CoordinatorID
	if !adopt(ann.Epoch, ann.CoordinatorID) {
		return ann, false, self
	}
	log.Info("coordinator announced",
		slog.Uint64("epoch", ann.Epoch),
		slog.String("coordinator", ann.Coordinator),
		slog.String("coordinator_id", ann.CoordinatorID),
		slog.String("reason", string(ann.Reason)),
		slog.Bool("self", self))
	return ann, true, self
}

// overlayReporter is the slice of media.Router the heartbeat reads: this peer's
// control-plane authority and its REALIZED overlay position. Declared here, narrow
// and consumer-side, so the beat's contents are testable against a fake instead of
// against a live PeerConnection — and so a drift in the Router's signatures breaks
// the build rather than quietly beating stale data.
type overlayReporter interface {
	Fence() overlay.Fence
	Realized() (parent string, parentState string, children []metrics.ChildLink)
	StaleRejected() uint64
}

// realizedBeat samples the half of a heartbeat that only the media layer knows: the
// epoch and revision this peer has actually ADOPTED, and the parent and children it
// has actually CONNECTED.
//
// It reports exactly what Realized returns and adds nothing. That restraint is the
// contract, not laziness: media deliberately reports the pending target of a
// re-parent in flight as NEITHER parent nor child, because the obvious "every
// session that is not my parent is a child" fixup would hand a rebuilding
// coordinator an edge pointing the wrong way — our future parent listed as our
// current child. Our realized parent stays the OLD one until media arrives over the
// new edge. Any filtering, defaulting or repair here would re-introduce exactly the
// divergence rebuild-from-peers (§6.6) exists to eliminate.
//
// The same restraint governs StaleRejected, and there it is load-bearing in a
// second way: the counter is cumulative and monotonic within a session and RESETS ON
// REJOIN, because the fence producing it resets on TypeJoined (§5.7 rule 6). It is
// therefore read fresh from the Router on every beat and passed through — never
// remembered here, never seeded, never clamped or smoothed. The coordinator emits
// its stale event on an INCREASE, so a value carried across a rejoin would mask a
// genuine refusal (the new total never climbs past the old high-water mark), and any
// smoothing would manufacture one out of a peer that has refused nothing.
//
// Name, Seq and IntervalMs are stamped by beater.next after this returns, so a
// provider cannot misreport the identity a coordinator keys on.
func realizedBeat(r overlayReporter) metrics.Heartbeat {
	f := r.Fence()
	parent, parentState, children := r.Realized()
	return metrics.Heartbeat{
		Epoch:       f.Epoch,
		Rev:         f.Rev,
		Parent:      parent,
		ParentState: parentState,
		Children:    children,
		// The visible proof that the fence works, on the frame that carries the
		// fence itself — so the number and the state that produced it are consistent
		// by construction rather than by two frames happening to agree.
		StaleRejected: r.StaleRejected(),
	}
}

// shouldBeat reports whether this peer sends liveness heartbeats.
//
// The rule is "call mode with a -name", not "managed mode", and the name is the
// load-bearing half: the coordinator's resurrection rule CREATES a member record
// from the name in the frame body, so a nameless peer beating would file liveness
// under "" — and two of them would collide into one record. A named peer, static
// tree or managed, is one a coordinator may legitimately track.
//
// Beating is not optional where a coordinator is watching: it reaps a member that
// sends no frame of any kind within metrics.GoneBeats cadences of joining, so a
// silent peer is ejected from every meet it joins.
func shouldBeat(call bool, cfg callConfig) bool {
	return call && cfg.name != ""
}

// controlFrame wraps a control-plane body in the signaling frame that carries it.
// From and To are deliberately left empty: the server stamps the sender, and a
// self-reported id would let a peer file telemetry or liveness as someone else.
func controlFrame(t signaling.Type, body any) (signaling.Message, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return signaling.Message{}, fmt.Errorf("marshal %s payload: %w", t, err)
	}
	return signaling.Message{Type: t, Payload: payload}, nil
}

// planeCoordinator is the slice of coordinator.Coordinator an elected peer drives.
//
// Consumer-defined and narrow, for the same reason overlayReporter is: it lets the
// plane's routing and lifecycle be asserted against a fake instead of a real control
// loop, and it makes a drift in the coordinator's surface a compile error here rather
// than a peer that silently stops feeding it something.
type planeCoordinator interface {
	Run(ctx context.Context) error
	SetRoster(roomID string, members []coordinator.Member)
	PeerJoined(roomID, peerID, name string)
	PeerLeft(roomID, peerID string)
	Metrics(roomID, peerID string, payload []byte)
	Heartbeat(roomID, peerID string, payload []byte)
	Reparented(roomID, peerID string, payload []byte)
	SetEpoch(roomID string, epoch uint64)
	Yield(roomID string, newEpoch uint64)
}

// planeQueueDepth bounds the plane's inbound backlog.
//
// 64 is roomy for a meet's control traffic — a handful of peers beating at 1 Hz plus
// membership churn — and the depth matters far less than the discipline around it:
// this queue exists so media's Run goroutine never waits on the coordinator.
const planeQueueDepth = 64

// planeEvent is one thing for the plane's goroutine to do: a control frame to route,
// an election outcome to act on, or a test barrier. One queue rather than three
// channels because ORDER MATTERS — media delivers the announcement and the frames
// that follow it on one goroutine in wire order, and an election that arrived before
// a membership change must be applied before it.
type planeEvent struct {
	frame signaling.Message
	ann   *arbiter.Announcement
	self  bool
	ack   chan struct{}
}

// peerPlane hosts the coordinator when THIS peer is the elected one.
//
// It is the peer-side analogue of cmd/server's object graph, minus the arbiter: the
// same coordinator.Coordinator, fed from the frames the server forwards rather than
// from an Observer, publishing through the peer's one signaling client rather than
// through the Hub.
//
// It exists because an elected peer that hosts nothing is strictly worse than no
// election at all — the arbiter hands the role away and the meet is never repaired
// again, silently, because a working server-hosted coordinator was replaced by a peer
// that adopted the title and no duties.
type peerPlane struct {
	log      *slog.Logger
	room     string
	selfName string
	clk      clock.Clock
	send     func(signaling.Message) error

	// events is the queue-and-return seam. post writes here from media's Run
	// goroutine and returns immediately; everything below runs on the plane's own
	// goroutine, which is therefore the single owner of coord/holding/epoch.
	events  chan planeEvent
	dropped atomic.Uint64
	refused atomic.Uint64
	// hosts is read by hosting() from other goroutines, so it is atomic rather than
	// a plain bool guarded by the single-owner rule.
	hosts atomic.Bool

	// selfID reports this peer's server-assigned id, or "" before the joined frame
	// lands. It is how the host's own locally-injected frames get the attribution the
	// server would otherwise have stamped on them.
	selfID func() string

	// newCoordinator is the constructor, injected so a test can substitute a fake
	// and count how many are built. Nil is filled in by newPeerPlane.
	newCoordinator func(coordinator.Config) planeCoordinator

	// Owned by the run goroutine.
	coord    planeCoordinator
	coordCtx context.CancelFunc
	wg       sync.WaitGroup
}

// newPeerPlane builds an idle plane. Nothing is constructed until this peer is
// actually elected: before that there is no configuration to build one from, because
// the meet's tuning arrives on the announcement.
func newPeerPlane(log *slog.Logger, room, selfName string, clk clock.Clock,
	send func(signaling.Message) error) *peerPlane {
	p := &peerPlane{
		log:      log.With(slog.String("component", "peer-coordinator")),
		room:     room,
		selfName: selfName,
		clk:      clk,
		send:     send,
		selfID:   func() string { return "" },
		events:   make(chan planeEvent, planeQueueDepth),
	}
	p.newCoordinator = func(cfg coordinator.Config) planeCoordinator {
		return coordinator.New(p.log, cfg, planeSender{room: p.room, send: p.send}, nil)
	}
	return p
}

// post hands one control frame to the plane. Called from media's Run goroutine, so it
// MUST NOT block: that goroutine routes every session's offer/answer/candidate, and
// stalling it would break the media plane in order to feed the control plane.
//
// Overflow drops and counts. A dropped telemetry frame is self-correcting — the peer
// reports again on its next tick — whereas a wedged Run goroutine is not corrected by
// anything.
func (p *peerPlane) post(msg signaling.Message) {
	select {
	case p.events <- planeEvent{frame: msg}:
	default:
		p.dropped.Add(1)
		p.log.Warn("dropped a control frame: plane queue full", slog.String("type", string(msg.Type)))
	}
}

// announce reports an arbiter announcement to the plane.
//
// The (adopted, self) pair is exactly what the fence already computed, and BOTH halves
// are load-bearing. Acting on an unadopted announcement would let §6.2's
// re-announcement — broadcast on every membership change at an UNCHANGED epoch, which
// AdoptAnnouncement correctly refuses — restart a running coordinator for no reason.
func (p *peerPlane) announce(ann arbiter.Announcement, adopted, self bool) {
	if !adopted {
		return
	}
	a := ann
	select {
	case p.events <- planeEvent{ann: &a, self: self}:
	default:
		// An election is not telemetry: losing one leaves the meet without a
		// coordinator until the next membership change re-broadcasts it. Loud.
		p.dropped.Add(1)
		p.log.Error("dropped a coordinator announcement: plane queue full",
			slog.Uint64("epoch", ann.Epoch), slog.Bool("self", self))
	}
}

// selfReport and selfBeat inject the HOST's own telemetry into the coordinator this
// peer is hosting.
//
// They exist because of an asymmetry that is correct on both sides and leaves a hole
// in the middle. The server refuses to forward a peer's frames back to that same peer
// (§8 rule 2 skips coordID == peerID) — echoing them would double the traffic on the
// one socket least able to afford it. So an elected coordinator hears from every node
// in the meet EXCEPT the one it is running on, and nothing on the wire can ever fix
// that: the frame is correctly never sent. The host is a member like any other; it is
// just the one member its own coordinator cannot learn about remotely.
//
// Left unfed, the health FSM sees a member that never beats and declares it gone —
// the coordinator evicting its own host from the tree while that host is alive and
// publishing. Measured worst case: an elected peer that was the meet's only
// relay-capable node excluded itself, leaving no eligible root and no tree at all.
//
// They take the SAME value that goes on the wire and route it through the SAME path a
// remote peer's frame takes, marshalled and attributed exactly as the server would
// have stamped it. The round trip through JSON is deliberate: it costs nothing at 1 Hz
// and it means the host cannot drift into being a specially-shaped member that the
// health FSM treats differently from everyone else.
func (p *peerPlane) selfReport(rep metrics.Report) { p.postSelf(signaling.TypeMetrics, rep) }

func (p *peerPlane) selfBeat(hb metrics.Heartbeat) { p.postSelf(signaling.TypeHeartbeat, hb) }

// reparentSend builds the re-parent sender's ship function: hand the report to the
// local coordinator first, then put it on the wire.
//
// It is a named function rather than an inline closure because the tee is the part
// that can be silently missing, and a closure buried in runCall is reachable only by
// an end-to-end test that has to fail a real parent edge mid-call. Named, the tee is
// a two-line assertion. A nil plane means this peer hosts nothing and only the wire
// send happens, which is every peer that is not currently the coordinator.
func reparentSend(plane *peerPlane, send func(signaling.Message) error) func(metrics.Reparented) error {
	return func(rep metrics.Reparented) error {
		if plane != nil {
			plane.selfReparented(rep)
		}
		msg, err := controlFrame(signaling.TypeReparented, rep)
		if err != nil {
			return err
		}
		return send(msg)
	}
}

// selfReparented closes the same hole for the third forwarded frame type. A
// coordinator peer is a peer in the tree like any other: it can lose its own parent
// and promote its own backup, and §8 rule 2 skips coordID == peerID for reparented
// exactly as it does for metrics and heartbeats — so without this the one node whose
// job is to RATIFY a self-promotion is the one node that never hears about its own.
func (p *peerPlane) selfReparented(rep metrics.Reparented) {
	p.postSelf(signaling.TypeReparented, rep)
}

// postSelf queues one locally-originated frame as if the server had delivered it.
func (p *peerPlane) postSelf(kind signaling.Type, body any) {
	id := p.selfID()
	if id == "" {
		// Before the joined frame lands there is no id to attribute this to, and a
		// member record keyed on "" would be a phantom node the meet never had.
		return
	}
	msg, err := controlFrame(kind, body)
	if err != nil {
		p.log.Warn("could not encode a self-report", slog.String("type", string(kind)), slog.Any("error", err))
		return
	}
	msg.From = id
	p.post(msg)
}

// Dropped counts events shed by post/announce. Nonzero means the control link
// outran the plane, which is a diagnosis rather than an error.
func (p *peerPlane) Dropped() uint64 { return p.dropped.Load() }

// Refused counts elections declined because the announced configuration could not
// drive a coordinator. Nonzero is a CONFIGURATION fault on the arbiter, not a
// transient: every announcement carries the same config until a redeploy.
func (p *peerPlane) Refused() uint64 { return p.refused.Load() }

// hosting reports whether this peer is currently the coordinator for its meet.
func (p *peerPlane) hosting() bool { return p.hosts.Load() }

// sync blocks until every event posted before the call has been handled. It is the
// test barrier — the plane is inherently asynchronous, and polling for "has it
// happened yet" is how a suite becomes flaky. Same role as coordinator.Sync.
func (p *peerPlane) sync() {
	ack := make(chan struct{})
	p.events <- planeEvent{ack: ack}
	<-ack
}

// run owns every mutation of the plane's state until ctx is cancelled, then stops the
// hosted coordinator and joins its goroutine so nothing outlives the call.
func (p *peerPlane) run(ctx context.Context) {
	defer p.stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-p.events:
			p.handle(ctx, ev)
		}
	}
}

func (p *peerPlane) handle(ctx context.Context, ev planeEvent) {
	switch {
	case ev.ack != nil:
		close(ev.ack)
	case ev.ann != nil:
		p.onElection(ctx, *ev.ann, ev.self)
	case p.coord != nil:
		p.route(ev.frame)
	}
	// The last case tests coord != nil, NOT hosting(), and the difference is
	// deliberate: a YIELDED coordinator keeps being fed. Yield drops the tree — a
	// belief this node is no longer authoritative about — while telemetry, health and
	// membership came off the wire and are still true, so a re-elected node starts
	// warm instead of paying a full join settle over again.
	//
	// A frame arriving before this peer has EVER been elected is dropped rather than
	// buffered: there is nothing to hold it, and replaying it after a later election
	// would feed a stale view of a membership that has since moved on.
}

// route feeds one control frame to the hosted coordinator.
//
// Every telemetry frame is attributed to msg.From — the ORIGINAL peer's id, which the
// server preserves when it forwards (§8 rule 2). The coordinator keys its member
// records on that id, so attributing a forwarded frame to the server, or to this
// peer, would file another node's telemetry under the wrong node.
func (p *peerPlane) route(msg signaling.Message) {
	switch msg.Type {
	case signaling.TypeMembership:
		// The authoritative roster, and the reason TypeMembership exists at all: a
		// coordinator that is a PEER gets no Observer callbacks, so without this frame
		// it would learn joins only implicitly and graceful leaves not at all.
		p.coord.SetRoster(p.room, membersFrom(msg.Peers))
	case signaling.TypePeerJoined:
		p.coord.PeerJoined(p.room, msg.From, msg.Name)
	case signaling.TypePeerLeft:
		p.coord.PeerLeft(p.room, msg.From)
	case signaling.TypeMetrics:
		p.coord.Metrics(p.room, msg.From, msg.Payload)
	case signaling.TypeHeartbeat:
		p.coord.Heartbeat(p.room, msg.From, msg.Payload)
	case signaling.TypeReparented:
		p.coord.Reparented(p.room, msg.From, msg.Payload)
	}
}

// membersFrom translates the wire roster into the coordinator's own membership type.
// The translation lives here so coordinator imports nothing new — the same
// consumer-side discipline as every other seam in this binary.
func membersFrom(peers []signaling.Peer) []coordinator.Member {
	members := make([]coordinator.Member, 0, len(peers))
	for _, pr := range peers {
		members = append(members, coordinator.Member{ID: pr.ID, Name: pr.Name})
	}
	return members
}

// onElection applies one adopted announcement to the coordinator lifecycle.
//
// The coordinator is constructed ONCE and then kept, because Yield is designed to keep
// it warm: yielding drops the tree (a belief this node is no longer authoritative
// about) but preserves telemetry, health and membership (observations that came off
// the wire and are still true). Rebuilding on every handover would discard those and
// make a re-election pay a full join settle for nothing.
func (p *peerPlane) onElection(ctx context.Context, ann arbiter.Announcement, self bool) {
	if !self {
		if p.coord != nil && p.hosts.Load() {
			// newEpoch is the term that REPLACED this node, and passing it raises the
			// meet's epoch floor so the very announcement that demoted us can never
			// also resume us.
			p.coord.Yield(p.room, ann.Epoch)
			p.hosts.Store(false)
			p.log.Info("yielded the coordinator role",
				slog.Uint64("epoch", ann.Epoch), slog.String("to", ann.Coordinator))
		}
		return
	}

	if p.coord == nil {
		cfg, err := planeConfig(ann.Config, p.selfName, p.clk)
		if err != nil {
			// Refuse LOUDLY and start nothing. Nobody upstream will stop us: the
			// arbiter only warns about an unusable config, on the reasoning that an
			// arbiter which refuses to start is worse than one that says why every
			// meet is uncoordinated. A coordinator built from it would run, publish
			// nothing, and look healthy — which is the exact failure this whole item
			// exists to remove, relocated one layer down.
			p.refused.Add(1)
			p.log.Error("refusing the coordinator role: the announced configuration cannot drive one",
				slog.Uint64("epoch", ann.Epoch), slog.Any("error", err))
			return
		}
		p.coord = p.newCoordinator(cfg)
		coordCtx, cancel := context.WithCancel(ctx)
		p.coordCtx = cancel
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			if err := p.coord.Run(coordCtx); err != nil && !errors.Is(err, context.Canceled) {
				p.log.Error("hosted coordinator stopped", slog.Any("error", err))
			}
		}()
		p.log.Info("hosting the coordinator for this meet",
			slog.Uint64("epoch", ann.Epoch), slog.String("self_name", p.selfName),
			slog.Int("max_depth", cfg.MaxDepth), slog.Int("stream_kbps", cfg.StreamKbps))
	}

	// Both the first election and every later one: the term is what makes this node
	// authoritative again, and only a strictly higher SetEpoch resumes a yielded
	// coordinator.
	p.coord.SetEpoch(p.room, ann.Epoch)
	p.hosts.Store(true)

	// Admit the host NOW, not on its first self-report. The join settle can expire in
	// the gap, and a tree built in that window would exclude the very node computing
	// it. PeerJoined is the same call the wire would have made for any other member,
	// and the coordinator's roster handling is idempotent, so a later TypeMembership
	// naming this peer costs nothing.
	if id := p.selfID(); id != "" {
		p.coord.PeerJoined(p.room, id, p.selfName)
	} else {
		p.log.Warn("elected before this peer knows its own id; the host will be admitted by its first self-report")
	}
}

// stop tears the hosted coordinator down and joins its goroutine.
func (p *peerPlane) stop() {
	if p.coordCtx != nil {
		p.coordCtx()
	}
	p.wg.Wait()
	p.hosts.Store(false)
}

// planeConfig turns the meet's announced tuning into a coordinator.Config this peer
// can actually run, or refuses it.
//
// It validates FIRST, because arbiter.CoordinatorConfig's zero fields are meaningful
// values rather than absences: an unresolved config would translate cleanly into a
// coordinator that silently retunes the meet, and a zero StreamKbps into one whose
// every BuildTree call hard-errors.
//
// Then it fills in the two fields the wire deliberately cannot carry:
//
//   - SelfName, and this is the trap. The translator leaves it empty because it
//     differs per holder, and an EMPTY SelfName is precisely how a coordinator marks
//     itself as the one running inside the arbiter process. A peer that forgets it
//     gets a coordinator that believes it is the arbiter.
//   - Clock, a per-process capability rather than a value, so a test drives this
//     coordinator's timers instead of sleeping.
func planeConfig(cc arbiter.CoordinatorConfig, selfName string, clk clock.Clock) (coordinator.Config, error) {
	if err := cc.Validate(); err != nil {
		return coordinator.Config{}, fmt.Errorf("announced coordinator config: %w", err)
	}
	cfg := meetconfig.Coordinator(cc)
	cfg.SelfName = selfName
	cfg.Clock = clk
	return cfg, nil
}

// planeSender publishes computed topologies back through the peer's ONE signaling
// client (§8 forbids a second control link).
//
// It satisfies coordinator.Sender's "must not block, must not do network I/O on the
// calling goroutine" the same way the server's hubSender does: signaling.Client.Send
// writes to a buffered channel and returns, and the write pump does the socket work.
// It is also called from the coordinator's own sender goroutine, never its control
// loop, so even a full buffer delays pushes rather than stalling recomputation.
type planeSender struct {
	room string
	send func(signaling.Message) error
}

func (s planeSender) SendTopology(roomID, peerID string, topo *overlay.Topology) error {
	payload, err := json.Marshal(topo)
	if err != nil {
		return fmt.Errorf("marshal topology: %w", err)
	}
	// To addresses the recipient; From is left for the server to stamp with this
	// peer's id, which is what the receiving peer's fence checks against the
	// coordinator the arbiter named.
	return s.send(signaling.Message{Type: signaling.TypeTopology, To: peerID, Payload: payload})
}

// realizedFunc reports this peer's REALIZED topology state — the parent and children
// it has actually connected, as opposed to what the coordinator believes it told it
// to connect. That distinction is what makes rebuild-from-peers possible.
//
// Production supplies realizedBeat over the live media.Router. It is a seam rather
// than a direct call so the beat's contents can be asserted against a fake, and so a
// harness can beat without a PeerConnection.
type realizedFunc func() metrics.Heartbeat

// beater is the peer's liveness producer: one metrics.Heartbeat every cadence, on
// its own goroutine, off the injected clock.
type beater struct {
	log      *slog.Logger
	name     string
	interval time.Duration
	clk      clock.Clock
	realized realizedFunc
	send     func(metrics.Heartbeat) error
	// seq is touched only by the goroutine running run (and by tests calling next
	// directly), so it needs no lock — the same single-owner discipline media's
	// Router uses for its re-parent state.
	seq uint64
}

func newBeater(log *slog.Logger, name string, interval time.Duration, clk clock.Clock,
	realized realizedFunc, send func(metrics.Heartbeat) error) *beater {
	return &beater{
		log:      log.With(slog.String("component", "heartbeat")),
		name:     name,
		interval: interval,
		clk:      clk,
		realized: realized,
		send:     send,
	}
}

// next produces the frame for the next beat. The identity fields are stamped AFTER
// the realized-state provider runs, so a provider can never misreport the name a
// coordinator keys on or the cadence it sizes this peer's thresholds from.
func (b *beater) next() metrics.Heartbeat {
	b.seq++
	var h metrics.Heartbeat
	if b.realized != nil {
		h = b.realized()
	}
	h.Name = b.name
	h.Seq = b.seq
	h.IntervalMs = uint64(b.interval / time.Millisecond)
	// The sender normalizes: two peers reporting the same realized edges must
	// produce byte-identical frames, or a rebuild from them is not reproducible.
	h.Normalize()
	return h
}

// beat produces and ships one heartbeat. A send failure is logged and swallowed:
// the beat is best-effort on the wire, and a loop that stopped on a transient error
// would guarantee the ejection it exists to prevent.
func (b *beater) beat() {
	h := b.next()
	if b.send == nil {
		return
	}
	if err := b.send(h); err != nil {
		b.log.Debug("heartbeat dropped", slog.Uint64("seq", h.Seq), slog.Any("error", err))
	}
}

// run beats immediately and then every interval until ctx is cancelled.
//
// The ticker is armed BEFORE the first beat, not after, for two reasons: the cadence
// then excludes the cost of the first send, and a test can treat "the first beat
// arrived" as proof the ticker is live.
func (b *beater) run(ctx context.Context) {
	ticker := b.clk.NewTicker(b.interval)
	defer ticker.Stop()
	// Beat at once. A peer silent for one cadence after joining has already spent an
	// eighth of its gone budget before saying anything.
	b.beat()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			b.beat()
		}
	}
}

// reparentQueueDepth bounds the backlog of self-promotion reports.
//
// Eight is generous for an event that fires at most once per parent failure per
// peer, and the depth matters far less than the non-blocking discipline around it:
// this queue exists so the media Run goroutine never waits on the network.
const reparentQueueDepth = 8

// reparentSender ships metrics.Reparented frames without blocking the caller.
//
// The indirection is not ceremony. media invokes OnReparented from its Run
// goroutine — the same goroutine that routes every offer/answer/candidate — so a
// send that blocked there (a full outbound buffer, a stalled socket) would starve
// the negotiation completing the very promotion being reported. Posting is
// therefore non-blocking and DROPS on overflow: a lost ratification frame is
// repaired by the coordinator's gone backstop, whereas a wedged Run goroutine is not
// repaired by anything.
type reparentSender struct {
	log     *slog.Logger
	ch      chan metrics.Reparented
	send    func(metrics.Reparented) error
	dropped atomic.Uint64
}

func newReparentSender(log *slog.Logger, send func(metrics.Reparented) error) *reparentSender {
	return &reparentSender{
		log:  log.With(slog.String("component", "reparent-reporter")),
		ch:   make(chan metrics.Reparented, reparentQueueDepth),
		send: send,
	}
}

// post hands one report to the drain goroutine. Safe to call from any goroutine and
// guaranteed not to block.
func (s *reparentSender) post(rep metrics.Reparented) {
	select {
	case s.ch <- rep:
	default:
		s.dropped.Add(1)
		s.log.Warn("dropped a re-parent report: queue full",
			slog.String("from", rep.From), slog.String("to", rep.To), slog.Bool("ok", rep.OK))
	}
}

// Dropped counts reports shed by post. Nonzero means the control link could not keep
// up with local failover, which is a diagnosis, not an error.
func (s *reparentSender) Dropped() uint64 { return s.dropped.Load() }

// run drains the queue until ctx is cancelled.
func (s *reparentSender) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case rep := <-s.ch:
			if s.send == nil {
				continue
			}
			if err := s.send(rep); err != nil {
				s.log.Warn("re-parent report not delivered",
					slog.String("from", rep.From), slog.String("to", rep.To), slog.Any("error", err))
				continue
			}
			s.log.Info("reported a self-promotion",
				slog.String("from", rep.From), slog.String("to", rep.To),
				slog.Bool("ok", rep.OK), slog.String("reason", rep.Reason))
		}
	}
}

// runCall joins a signaling room and runs WebRTC calls with the peers in it
// until ctx is cancelled or the signaling stream closes. Everything — the signaling
// client, every session, the media pumps, and the three control-plane goroutines —
// hangs off ctx, so one cancellation (SIGINT in production) tears the whole thing
// down cleanly.
//
// The configuration reaching here has already been validated (see parseArgs), so
// this function constructs and wires; it does not second-guess flags.
func runCall(ctx context.Context, logger *slog.Logger, cfg callConfig) error {
	// ONE clock for the process, injected into everything that measures or waits.
	// Nothing below calls package time directly, so driving this peer from a virtual
	// clock later is a change at this line and nowhere else.
	clk := clock.System()

	var topo *overlay.Topology
	if cfg.topologyPath != "" {
		var err error
		if topo, err = overlay.LoadTopology(cfg.topologyPath); err != nil {
			return err
		}
	}

	// The dial timeout bounds only the WebSocket handshake, not the call.
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	client, err := signaling.Dial(dialCtx, logger, cfg.server, cfg.room, cfg.name)
	cancel()
	if err != nil {
		return err
	}
	defer client.Close()
	logger.Info("connected to signaling",
		slog.String("server", cfg.server), slog.String("room", cfg.room), slog.String("name", cfg.name))

	var iceServers []webrtc.ICEServer
	if cfg.stun != "" {
		iceServers = append(iceServers, webrtc.ICEServer{URLs: []string{cfg.stun}})
	}

	// callCtx bounds this call. Cancelling it — on SIGINT, or when router.Run returns
	// on its own because the signaling stream closed — also stops the reporter, the
	// beater and the re-parent drain, so no goroutine outlives runCall.
	callCtx, callCancel := context.WithCancel(ctx)
	defer callCancel()
	var wg sync.WaitGroup

	// plane hosts the coordinator if the arbiter elects this peer. It is built for
	// every managed peer and stays idle until then: an election can arrive at any
	// moment, and constructing the queue lazily would mean dropping the frames that
	// arrive in the same breath as the announcement.
	var plane *peerPlane
	if cfg.managed {
		plane = newPeerPlane(logger, cfg.room, cfg.name, clk, client.Send)
	}

	// The peer reports its OWN backup promotions so the coordinator can ratify them
	// (or, when OK is false, repair a stranded peer urgently). Queued rather than
	// sent inline: media invokes the callback from its Run goroutine.
	reparents := newReparentSender(logger, reparentSend(plane, client.Send))

	// router is captured by the callbacks below before it exists. That is safe, not a
	// race: NewRouter returns on this goroutine before Run is called on it, and
	// nothing invokes a callback until Run is executing — so the assignment
	// happens-before every read.
	var router *media.Router
	rcfg := routerConfigFor(cfg, topo, iceServers, clk, reparents.post, func(payload []byte) {
		ann, adopted, self := adoptAnnouncement(logger, router.SelfID, router.AdoptCoordinator, payload)
		if plane != nil {
			plane.announce(ann, adopted, self)
		}
	})
	if plane != nil {
		// Every control frame this peer's coordinator would need, queued and returned
		// immediately — media calls this on its Run goroutine.
		rcfg.OnControlFrame = plane.post
		plane.selfID = func() string { return router.SelfID() }
	}
	router = media.NewRouter(logger, client, rcfg)
	logger.Info("running call",
		slog.Bool("send", cfg.send), slog.String("media", cfg.mediaPath),
		slog.String("record", cfg.recordPath), slog.Bool("stun", cfg.stun != ""),
		slog.Bool("tree", topo != nil), slog.Bool("managed", cfg.managed),
		slog.Bool("backup", cfg.backup), slog.Bool("coordinatable", cfg.coordinatable))

	if topo != nil || cfg.managed {
		wg.Add(1)
		go func() { defer wg.Done(); reparents.run(callCtx) }()
	}

	if plane != nil {
		wg.Add(1)
		go func() { defer wg.Done(); plane.run(callCtx) }()
	}

	if cfg.managed {
		// The live sensors. Each is started or primed HERE, at the edge, and handed
		// to telemetry as a closure — no globals, and every seam visible at one site.
		// The first report carries no CPU reading, and deliberately nothing is done
		// about that. /proc/stat counts in 10 ms jiffies, so priming the baseline at
		// startup would diff two reads microseconds apart, accumulate zero jiffies,
		// and report nothing anyway — it only looks like it saves a cycle. The second
		// report, one interval later, carries the first real interval.
		cpu := &metrics.CPUSampler{}

		// The control-link probe. It runs on its own goroutine rather than inside the
		// sampler because Reporter.report calls sample() and then send() in sequence:
		// a probe that blocked inside sample would delay the telemetry frame by
		// exactly as long as the link is slow, which is when the frame matters most.
		probe := metrics.NewRTTProbe(logger, metrics.DefaultInterval, clk,
			func(ctx context.Context) error {
				// A bounded wait, not the call's whole lifetime: an unbounded ping on
				// a wedged socket would freeze RTTServerMs at its last good value
				// forever, which reads as a healthy link. The budget is the report
				// interval — waiting longer than the cadence that consumes the value
				// buys nothing.
				pingCtx, cancel := context.WithTimeout(ctx, metrics.DefaultInterval)
				defer cancel()
				return client.Ping(pingCtx)
			})
		wg.Add(1)
		go func() { defer wg.Done(); probe.Run(callCtx) }()

		tel := &telemetry{
			cfg: cfg,
			cpu: cpu.Sample,
			rtt: probe.LastMs,
			// Read through the variable, not captured by value: router is assigned
			// above but LinkStats must be resolved per call anyway, and a nil check
			// keeps a probe-mode peer from dereferencing one that was never built.
			links: func() ([]metrics.PeerRTT, float64) {
				if router == nil {
					return nil, 0
				}
				return router.LinkStats()
			},
		}

		// The reporter is decoupled from signaling: it samples a Report and ships it
		// through this send closure, the only thing that knows about the wire.
		// Telemetry is best-effort — a send error is the reporter's to log, never the
		// peer's to fail on.
		reporter := metrics.NewReporter(logger, metrics.DefaultInterval, clk,
			tel.sample,
			func(rep metrics.Report) error {
				// Tee to the local coordinator first. The server will not forward this
				// peer's own frames back to it, so if this process is hosting the
				// coordinator, THIS is the only path by which it learns about its own
				// host. Same value, same cadence as every other member's.
				if plane != nil {
					plane.selfReport(rep)
				}
				msg, err := controlFrame(signaling.TypeMetrics, rep)
				if err != nil {
					return err
				}
				return client.Send(msg)
			},
		)
		wg.Add(1)
		go func() { defer wg.Done(); reporter.Run(callCtx) }()
	}

	if shouldBeat(true, cfg) {
		beats := newBeater(logger, cfg.name, cfg.heartbeat, clk,
			// What this peer has actually adopted and actually connected — the
			// ground truth a coordinator promoted mid-call rebuilds the previous
			// tree from, rather than inheriting its predecessor's beliefs (§6.6).
			func() metrics.Heartbeat { return realizedBeat(router) },
			func(h metrics.Heartbeat) error {
				// Tee, for the same reason as the report above: a host that never
				// appears to beat is declared gone by its own coordinator.
				if plane != nil {
					plane.selfBeat(h)
				}
				msg, err := controlFrame(signaling.TypeHeartbeat, h)
				if err != nil {
					return err
				}
				return client.Send(msg)
			},
		)
		wg.Add(1)
		go func() { defer wg.Done(); beats.run(callCtx) }()
	}

	runErr := router.Run(callCtx)
	callCancel() // stop the control-plane goroutines even if Run returned via stream-close
	wg.Wait()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	// The session totals. stale_rejected also rides every heartbeat, so the control
	// plane and the dashboard see it live; this line is the local closing summary,
	// and it is proof the fence works rather than an error condition.
	ended := []any{
		slog.Uint64("stale_rejected", router.StaleRejected()),
		slog.Uint64("reparent_reports_dropped", reparents.Dropped()),
	}
	if plane != nil {
		ended = append(ended,
			slog.Uint64("control_frames_dropped", plane.Dropped()),
			slog.Uint64("elections_refused", plane.Refused()))
	}
	logger.Info("call ended", ended...)
	return nil
}

// runProbe is the Phase 0 reachability check: GET /healthz and validate it.
func runProbe(logger *slog.Logger, server string, timeout time.Duration) error {
	healthURL, err := healthURLFor(server)
	if err != nil {
		return err
	}

	// One context bounds the whole probe. context.WithTimeout derives a child
	// context that auto-cancels after the deadline; cancel() must always be
	// called (defer) to release the timer even on the happy path. Threading
	// this ctx into the request is what makes the -timeout flag actually bite —
	// a hung server can't wedge the peer forever.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	logger.Info("probing server health", slog.String("url", healthURL))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", healthURL, err)
	}
	// Always close the body, always via defer so it runs on every return path.
	// Leaking response bodies leaks connections — a classic Go footgun.
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server unhealthy: status %d, body %q", resp.StatusCode, string(body))
	}

	// Decode into an anonymous struct: we only need these two fields here, so
	// there's no reason to export a named type. The json tags map wire names to
	// fields exactly as on the server side.
	var health struct {
		Status  string `json:"status"`
		Service string `json:"service"`
	}
	if err := json.Unmarshal(body, &health); err != nil {
		return fmt.Errorf("decode health response: %w", err)
	}

	logger.Info("server is healthy",
		slog.Int("status_code", resp.StatusCode),
		slog.String("server_status", health.Status),
		slog.String("server_service", health.Service),
	)
	return nil
}

// healthURLFor turns a user-supplied -server value into a concrete /healthz URL.
// It accepts a bare "host:port" as shorthand for "http://host:port" and rejects
// anything without a real host. Pulling this out of run keeps it a small, pure,
// obviously-testable function — the same reasoning behind logging.ParseLevel.
//
// It parses first and validates the parsed structure, rather than trimming and
// concatenating strings: string surgery on URLs is how "http://" quietly
// becomes a request to a host literally named "http". Rebuilding from
// scheme+host also normalizes away any trailing slash or stray path.
func healthURLFor(server string) (string, error) {
	s := strings.TrimSpace(server)
	if s == "" {
		return "", errors.New("empty -server URL")
	}
	// No scheme? Treat it as the http shorthand ("localhost:8080").
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("parse -server %q: %w", server, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("invalid -server URL %q: scheme must be http or https", server)
	}
	// Guard on Hostname(), not Host. u.Host *includes* the port, so it is
	// non-empty for ":", ":9000", or "http://:9000" (empty hostname, port set) —
	// exactly the shapes that mirror the server's own "-addr :9000" and are easy
	// to type by mistake. Those would build a hostless URL that only explodes
	// deep in the HTTP client ("no Host in request URL"). u.Hostname() strips the
	// port, so it is the real "is there a host?" check. Fail loud and early, and
	// point at the likely fix.
	if u.Hostname() == "" {
		hint := ""
		if u.Port() != "" {
			hint = fmt.Sprintf(` (did you mean "localhost:%s"?)`, u.Port())
		}
		return "", fmt.Errorf("invalid -server URL %q: missing host%s", server, hint)
	}
	return u.Scheme + "://" + u.Host + "/healthz", nil
}
