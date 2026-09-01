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
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/logging"
	"github.com/SammyUrfen/conclave/internal/media"
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
	if err := fs.Parse(args); err != nil {
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
		ICEServers: iceServers,
		SendMedia:  cfg.send,
		MediaPath:  cfg.mediaPath,
		RecordPath: cfg.recordPath,
		Topology:   topo,
		SelfName:   cfg.name,
		Managed:    cfg.managed,
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
// self is returned as well as logged so the Phase 6 wiring that starts and stops an
// in-process coordinator has the predicate ready — and tested — rather than
// rediscovering it at the call site.
func adoptAnnouncement(log *slog.Logger, selfID func() string, adopt coordinatorAdopter, payload []byte) (adopted, self bool) {
	var ann arbiter.Announcement
	if err := json.Unmarshal(payload, &ann); err != nil {
		// A malformed announcement is dropped, not fatal: the arbiter re-broadcasts
		// the current announcement on every membership change, so the next one
		// repairs this peer.
		log.Warn("bad coordinator announcement", slog.Any("error", err))
		return false, false
	}
	// Compared by the SERVER-ASSIGNED ID, never by -name: an announcement names the
	// coordinator by id, so a peer matching it against its own name could never
	// recognise itself and would silently decline the job it was just given. The id
	// is "" until the joined frame lands, which reads as "not me" — the safe answer
	// in that window, and the reason the empty-id guard is here rather than implied.
	id := selfID()
	self = id != "" && id == ann.CoordinatorID
	if !adopt(ann.Epoch, ann.CoordinatorID) {
		return false, self
	}
	log.Info("coordinator announced",
		slog.Uint64("epoch", ann.Epoch),
		slog.String("coordinator", ann.Coordinator),
		slog.String("coordinator_id", ann.CoordinatorID),
		slog.String("reason", string(ann.Reason)),
		slog.Bool("self", self))
	return true, self
}

// overlayReporter is the slice of media.Router the heartbeat reads: this peer's
// control-plane authority and its REALIZED overlay position. Declared here, narrow
// and consumer-side, so the beat's contents are testable against a fake instead of
// against a live PeerConnection — and so a drift in the Router's signatures breaks
// the build rather than quietly beating stale data.
type overlayReporter interface {
	Fence() overlay.Fence
	Realized() (parent string, parentState string, children []metrics.ChildLink)
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

	// The peer reports its OWN backup promotions so the coordinator can ratify them
	// (or, when OK is false, repair a stranded peer urgently). Queued rather than
	// sent inline: media invokes the callback from its Run goroutine.
	reparents := newReparentSender(logger, func(rep metrics.Reparented) error {
		msg, err := controlFrame(signaling.TypeReparented, rep)
		if err != nil {
			return err
		}
		return client.Send(msg)
	})

	// router is captured by the two callbacks below before it exists. That is safe,
	// not a race: NewRouter returns on this goroutine before Run is called on it, and
	// nothing invokes a callback until Run is executing — so the assignment
	// happens-before every read.
	var router *media.Router
	rcfg := routerConfigFor(cfg, topo, iceServers, clk, reparents.post, func(payload []byte) {
		adoptAnnouncement(logger, router.SelfID, router.AdoptCoordinator, payload)
	})
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

	if cfg.managed {
		// The reporter is decoupled from signaling: it samples a Report and ships it
		// through this send closure, the only thing that knows about the wire.
		// Telemetry is best-effort — a send error is the reporter's to log, never the
		// peer's to fail on.
		reporter := metrics.NewReporter(logger, metrics.DefaultInterval, clk,
			func() metrics.Report { return sampleReport(cfg) },
			func(rep metrics.Report) error {
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
	// stale_rejected is proof the fence works, not an error condition, so it is
	// reported on the way out rather than warned about in flight.
	logger.Info("call ended",
		slog.Uint64("stale_rejected", router.StaleRejected()),
		slog.Uint64("reparent_reports_dropped", reparents.Dropped()))
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
