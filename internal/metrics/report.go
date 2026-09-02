package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// DefaultInterval is how often a peer re-reports its telemetry. A few seconds is
// the right order of magnitude: frequent enough that the coordinator learns a new
// peer's real capacity within one cycle of it joining, infrequent enough that the
// report stream is a trickle, not a load. Membership changes (join/leave) drive
// re-optimisation directly and immediately; this interval only refreshes the
// underlying numbers, so it need not be tight.
const DefaultInterval = 3 * time.Second

// Report is the telemetry one peer sends the coordinator — the payload of a
// TypeMetrics frame. It carries only what BuildTree consumes plus room to grow:
//
//   - UploadKbps and NAT are the load-bearing inputs today; together they decide
//     whether a node can be a relay and how many children it may serve.
//   - RTTServerMs/LossPct/CPUPct are declared now and populated later (Phase 5/7);
//     wiring the fields through the plane up front means adding a real sensor never
//     touches the protocol.
//
// It intentionally does NOT carry the peer's id: the server stamps the sender id
// on the frame, and trusting a self-reported id would let a peer file telemetry as
// someone else. The coordinator pairs this body with that server-known id.
type Report struct {
	// Name is the peer's stable label — how it appears in the computed tree.
	Name string `json:"name"`
	// UploadKbps is the upload budget this node offers for forwarding others'
	// media. Phase 4 declares it via a flag (an honest stand-in for the bandwidth
	// probe a real deployment would run); it is the primary tree-shaping input.
	UploadKbps int `json:"upload_kbps"`
	// NAT is the node's reachability class; NATRelayed (TURN-bound) forces it to a
	// leaf and disqualifies it as coordinator.
	//
	// MEASURED since Phase 7, and the claim is narrow: it says "this peer's media
	// paths are going through a relay", NOT what kind of NAT it is behind. The sender
	// reads the type of the local candidate in its nominated ICE candidate pairs
	// (media.relayedPath) and reports NATRelayed only when EVERY such path is
	// relay-typed. cmd/peer's -nat still FORCES a value when an operator sets it.
	//
	// A peer that has not connected to anybody has measured nothing and reports
	// NATDirect — the permissive class. Absence of evidence must not read as evidence
	// of constraint here, because the alternative demotes every freshly-joined peer to
	// a forced leaf and leaves a new meet's first tree unbuildable.
	NAT overlay.NATType `json:"nat,omitempty"`
	// RTTServerMs is the node's round-trip to the server, a cheap proxy for its
	// network quality. Unpopulated in Phase 4 (BuildTree tolerates missing RTT);
	// Phase 7 measures it. Kept for forward compatibility of the wire format.
	RTTServerMs float64 `json:"rtt_server_ms,omitempty"`
	// LossPct and CPUPct are headroom signals Phase 5's hysteresis will act on.
	LossPct float64 `json:"loss_pct,omitempty"`
	CPUPct  float64 `json:"cpu_pct,omitempty"`
	// Coordinatable declares whether this peer is WILLING to be elected coordinator
	// (the -coordinatable flag; a laptop on battery says false). It is a hard
	// disqualifier in arbiter.Score — not a penalty — because an unwilling peer is
	// not a worse candidate, it is not a candidate.
	//
	// Deliberately NOT omitempty: the key is emitted even when false, because a
	// peer that declines must look different on the wire from one that never had
	// the field. cmd/peer emits it explicitly for the same reason, even though its
	// flag defaults to true.
	//
	// FAILS CLOSED, and the silence is the hazard. Absent decodes to false, so a
	// peer running an older build is treated as unwilling rather than silently
	// conscripted — the right direction. But the failure is quiet: if NO peer ever
	// sets it, every candidate scores a hard 0, no peer is ever elected, and the
	// meet simply stays on the arbiter forever with nothing logged. A bool cannot
	// distinguish the three situations that matter ("willing", "declined", "has not
	// spoken"), and it must not try to: the consumer already holds the missing bit,
	// namely whether a report arrived AT ALL. So the rule is
	//
	//	eligible == reported && Coordinatable
	//
	// (arbiter gates exactly this way), and a control plane that finds itself with
	// reports in hand and no volunteers among them should say so out loud rather
	// than sit silent — that is a configuration fact, not a transient one.
	Coordinatable bool `json:"coordinatable"`
	// PeerRTT is this node's MEASURED round-trip to other peers, in milliseconds —
	// the input BuildTree's min-latency rank needs and, before this existed, never
	// had. The coordinator copies it into overlay.Node.RTT verbatim.
	//
	// It is an ORDERED SLICE and not the map[string]float64 that overlay.Node.RTT
	// itself uses, for exactly the reason metrics.Heartbeat.Children is a slice: the
	// value shapes the tree, and while encoding/json marshals a map in sorted key
	// order, it hands the PRODUCER a map to iterate — and Go randomises that. Two
	// peers holding identical measurements could then emit frames that differ, and
	// the first tree of an epoch would depend on a hash seed. Call Normalize before
	// sending.
	//
	// PARTIAL AND POSSIBLY STALE, by construction. A peer can only measure a path it
	// has a PeerConnection over, so this covers its current tree neighbours plus
	// neighbours it held recently enough to still remember (media.Router keeps them
	// for RTTMemory). A peer it has never been connected to is simply absent, and
	// BuildTree already treats a missing entry as unknown/worst-case. omitempty
	// because a peer with nothing measured — every peer, on its first report — must
	// emit no key at all rather than an empty array a receiver could read as
	// "measured, and all zero".
	PeerRTT []PeerRTT `json:"peer_rtt,omitempty"`
}

// PeerRTT is one measured round-trip from the reporting node to another peer.
type PeerRTT struct {
	Name  string  `json:"name"`
	RTTMs float64 `json:"rtt_ms"`
}

// Normalize puts PeerRTT into the canonical order (name ascending), so two peers
// holding the same measurements produce byte-identical frames. A sender must call
// it; a receiver may call it defensively. It is idempotent. Name is a total order
// because a meet rejects duplicate names.
func (r *Report) Normalize() {
	sort.Slice(r.PeerRTT, func(i, j int) bool { return r.PeerRTT[i].Name < r.PeerRTT[j].Name })
}

// The liveness family. HeartbeatInterval sets the cadence; everything else is
// expressed as a MULTIPLE of the cadence a peer actually declared, never of the
// default. That is deliberate: -heartbeat lets a peer pick its own interval, so a
// threshold hard-coded from the 1 s default would declare a peer beating every 5 s
// gone while it is beating perfectly.
const (
	// HeartbeatInterval is the DEFAULT cadence a peer beats at.
	//
	// 1 s is the floor that is still robust: a heartbeat is ~120 bytes, so 1 Hz
	// costs ~1 kbit/s per peer — nothing next to a 2 Mbit/s video stream. Faster
	// (250 ms) starts producing false misses from ordinary scheduler jitter and GC
	// pauses on a loaded home PC, the "too sensitive" failure mode; slower (5 s)
	// makes the backstop detection unusably sluggish.
	HeartbeatInterval = 1 * time.Second

	// DegradedBeats is how many consecutive beats may be missed before a node is
	// demoted from healthy to degraded. Three rather than one because a single
	// missed beat is a GC pause, a Wi-Fi retransmit, or a scheduler hiccup — not a
	// failure. Nothing acts on this transition; it colours the dashboard and arms
	// the degradation dwell.
	DegradedBeats = 3

	// GoneBeats is how many consecutive beats may be missed before a node is
	// declared gone, which DOES trigger repair.
	//
	// Eight looks slow for a system claiming keyframe-bounded failover, and it is —
	// on purpose. This is the BACKSTOP, not the fast path: a child sees its parent's
	// PeerConnection fail within a few seconds and promotes its precomputed backup
	// without asking anyone, so by the time this fires the tree has usually already
	// healed and the coordinator is merely ratifying. Keeping the backstop
	// conservative is therefore nearly free, and it buys immunity to the ugliest
	// false positive there is: declaring a healthy peer dead because the ARBITER's
	// own network hiccuped.
	GoneBeats = 8
)

// DegradedAfter is the silence that demotes a peer beating every interval from
// healthy to degraded. A non-positive interval falls back to HeartbeatInterval, so a
// caller that has not yet learned a peer's cadence still gets the sane default.
func DegradedAfter(interval time.Duration) time.Duration {
	return DegradedBeats * cadence(interval)
}

// GoneAfter is the silence that declares a peer beating every interval gone.
func GoneAfter(interval time.Duration) time.Duration {
	return GoneBeats * cadence(interval)
}

// cadence normalises a declared interval, so every threshold here defaults the same
// way rather than each caller inventing its own fallback.
func cadence(interval time.Duration) time.Duration {
	if interval <= 0 {
		return HeartbeatInterval
	}
	return interval
}

// ValidateLivenessBudget checks the one relationship between conclave's two
// independent liveness detectors, and is meant to be called at startup and treated
// as fatal.
//
// interval is the heartbeat cadence peers are configured to use; socketDetection is
// the worst case time the transport takes to notice a dead socket
// (signaling.Hub.LivenessBudget). The socket detector must NOT be the slower of the
// two. If it were, there would be a window in which the control plane has declared a
// peer gone and dropped it from the tree while its socket is still registered — and
// because the join handshake only happens on a NEW connection, a peer whose socket
// never closed could never re-announce itself. It would be permanently ejected from
// a meet it believes it is still in, with no error anywhere.
//
// (The resurrection contract on signaling.Observer.Heartbeat is the other half of
// this defence: it lets a still-live peer come back even inside that window. This
// check keeps the window from existing in the first place.)
func ValidateLivenessBudget(interval, socketDetection time.Duration) error {
	if socketDetection <= 0 {
		return fmt.Errorf("socket liveness budget %v must be positive", socketDetection)
	}
	if gone := GoneAfter(interval); socketDetection > gone {
		return fmt.Errorf("socket liveness budget %v exceeds the gone threshold %v for a %v heartbeat: "+
			"shorten the WebSocket ping interval/timeout or lengthen -heartbeat",
			socketDetection, gone, cadence(interval))
	}
	return nil
}

// RebuildWindow bounds how long a freshly promoted coordinator waits for peers to
// report in before it publishes its first tree. 3 s is about three heartbeats plus a
// metrics tick — enough for every peer present to have spoken at least once, so the
// first tree of a new term is built on measurements rather than on defaults.
//
// It lives HERE, not in arbiter where it was first written, and the move is the point
// rather than a tidy-up: it is declared as part of the election contract but consumed
// only by the coordinator, and coordinator -> arbiter is forbidden by the dependency
// graph. Left in arbiter it would force a second copy with nothing enforcing that the
// two agree. metrics is the correct home and not a stretch of this package's charter:
// RebuildWindow is literally "how long until every peer has reported at least once",
// a metrics-plane cadence sitting beside DefaultInterval, HeartbeatInterval,
// DegradedAfter and GoneAfter — and both arbiter and coordinator already import this
// package. It explicitly does NOT belong in policy, whose charter is untrusted input
// at the process boundary; a control-loop timing constant is neither.
const RebuildWindow = 3 * time.Second

// Heartbeat is the peer's liveness beat and its report of REALIZED topology state —
// what it has actually connected, as opposed to what the coordinator believes it
// told it to connect. That distinction is what makes rebuild-from-peers possible: a
// new coordinator reconstructs ground truth, not its predecessor's beliefs.
//
// Like Report it carries no id: the server stamps the sender, and a self-reported id
// would let a peer file liveness as someone else.
type Heartbeat struct {
	// Name is the peer's stable label.
	Name string `json:"name"`
	// Seq is a per-peer monotonic counter starting at 1, reset on rejoin. It lets
	// the coordinator detect gaps (missed beats) without depending on its own clock,
	// and lets a late duplicate be dropped idempotently.
	Seq uint64 `json:"seq"`
	// IntervalMs is the cadence THIS peer beats at, in milliseconds.
	//
	// It is on the wire because the thresholds are multiples of the cadence and the
	// peer is the only party that knows its own -heartbeat setting. Without it the
	// coordinator would have to assume the default and would declare a deliberately
	// slow peer gone while it is beating perfectly. Absent (0) means "the default";
	// read it through Interval rather than the field.
	IntervalMs uint64 `json:"interval_ms,omitempty"`
	// Epoch is the coordinator term this peer believes is current, and Rev the
	// topology revision it has realized. The coordinator uses them to spot a peer
	// running behind and re-push to it specifically.
	Epoch uint64 `json:"epoch"`
	Rev   uint64 `json:"rev"`
	// Parent is the peer's realized upstream neighbour name, "" if it is the root or
	// currently parentless. ParentState is the pion PeerConnectionState string of
	// that edge ("new"/"connecting"/"connected"/"disconnected"/"failed"/"closed").
	Parent      string `json:"parent,omitempty"`
	ParentState string `json:"parent_state,omitempty"`
	// Children are the realized downstream neighbours. Absent on a leaf.
	//
	// This is an ORDERED SLICE and not the map[string]string it obviously wants to
	// be, and that is load-bearing rather than fussy. A coordinator taking over a
	// meet rebuilds the previous tree's edge list from these heartbeats, and
	// overlay.Topology.Edges ORDER is an invariant the stability-preserving builder
	// replays. Go randomises map iteration, so a map here would make the first tree
	// of every new epoch depend on nothing but hash seed — silently destroying the
	// determinism the whole test strategy rests on. Call Normalize before sending.
	Children []ChildLink `json:"children,omitempty"`
	// StaleRejected is how many control-plane instructions this peer's fence has
	// REFUSED since it last joined (media.Router.StaleRejected()).
	//
	// It rides the heartbeat rather than Report because it is CONTROL state, not
	// telemetry: this frame already carries the peer's fence (Epoch, Rev), and this
	// counter is that same fence's refusal count — so the number and the fence that
	// produced it are consistent by construction rather than by two frames happening
	// to agree. Report's 3 s cadence would also be the wrong rhythm for something an
	// operator watches during a handover.
	//
	// CUMULATIVE and MONOTONIC within a session, and RESET ON REJOIN exactly like
	// Seq. That reset is a requirement, not a stylistic echo: the fence itself resets
	// on TypeJoined, so a counter that survived a rejoin would be reporting refusals
	// made under a fence that no longer exists. It matters because the coordinator
	// emits its stale event on an INCREASE, never per heartbeat — so a carried-over
	// total would either mask a genuine refusal (the new total never climbs past the
	// stale high-water mark) or manufacture one out of a peer that has refused
	// nothing. omitempty because the healthy case is 0 on every beat from every peer.
	StaleRejected uint64 `json:"stale_rejected,omitempty"`
}

// ChildLink is one realized downstream edge: the child's stable label and the pion
// PeerConnectionState string of the edge to it.
type ChildLink struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

// Interval is the cadence this peer declared, falling back to HeartbeatInterval when
// the frame does not say (a peer built before the field existed, or one that left it
// at the default). Read the cadence through this, never through IntervalMs.
func (h Heartbeat) Interval() time.Duration {
	if h.IntervalMs == 0 {
		return HeartbeatInterval
	}
	return time.Duration(h.IntervalMs) * time.Millisecond
}

// Normalize puts Children into the canonical order (name ascending) so that two
// peers reporting the same realized edges produce byte-identical frames. A sender
// must call it; a receiver may call it defensively. Name is a total order because a
// meet rejects duplicate names.
func (h *Heartbeat) Normalize() {
	sort.Slice(h.Children, func(i, j int) bool { return h.Children[i].Name < h.Children[j].Name })
}

// Reparented tells the control plane that a peer changed its own parent WITHOUT
// being told to — it promoted its precomputed backup after its primary failed, or it
// tried and could not. It is the peer half of the fast-failover path; the
// coordinator's job is to RATIFY the choice rather than fight it.
type Reparented struct {
	Name string `json:"name"`
	// From is the parent that was lost; To is the parent now in use ("" when OK is
	// false and the peer is parentless).
	From string `json:"from"`
	To   string `json:"to,omitempty"`
	// OK is false when the peer could not attach to any parent and is stranded. The
	// coordinator must treat that as an urgent threshold event: a stranded peer is
	// receiving nothing, so the anti-thrash cooldown is the wrong trade there.
	OK bool `json:"ok"`
	// Epoch/Rev of the topology the peer was acting under, so the coordinator can
	// tell a failover under the current tree from one under a tree it has replaced.
	Epoch  uint64 `json:"epoch"`
	Rev    uint64 `json:"rev"`
	Reason string `json:"reason,omitempty"` // free text for the dashboard; never parsed
}

// Reporter is the producer side of the metrics plane: on its own goroutine it
// samples the local node's telemetry every interval and ships it to the
// coordinator. It is deliberately decoupled from both signaling and media — it
// holds a sample function (what to report) and a send function (how to ship it),
// injected by the peer's main, so this package imports neither. That is what lets
// it be unit-tested with a fake sink and no sockets.
type Reporter struct {
	log      *slog.Logger
	interval time.Duration
	clk      clock.Clock
	sample   func() Report
	send     func(Report) error
}

// NewReporter builds a Reporter. sample is called each tick to snapshot the
// current telemetry (so live values can be folded in later without changing this
// type); send ships one report and may fail transiently — telemetry is
// best-effort and a dropped report must never fault the peer, so send errors are
// logged, not returned. A zero or negative interval falls back to DefaultInterval,
// and a nil clk to clock.System().
//
// clk is a parameter rather than a package-level var swapped in tests: a global
// would race between parallel tests and would make "which clock is this code on"
// invisible at the call site.
func NewReporter(log *slog.Logger, interval time.Duration, clk clock.Clock, sample func() Report, send func(Report) error) *Reporter {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if clk == nil {
		clk = clock.System()
	}
	return &Reporter{
		log:      log.With(slog.String("component", "metrics-reporter")),
		interval: interval,
		clk:      clk,
		sample:   sample,
		send:     send,
	}
}

// Run reports once immediately — so the coordinator learns this node's real
// capacity right after it joins, rather than a cycle later — then every interval
// until ctx is cancelled. The immediate-then-ticker shape is the standard Go
// periodic-task idiom: a select over the ticker and ctx.Done, with the first
// emission hoisted out of the loop so there is no initial interval of silence.
func (r *Reporter) Run(ctx context.Context) {
	r.report()

	ticker := r.clk.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			r.report()
		}
	}
}

// report samples once and ships it, swallowing (but logging) a transient send
// failure — a lost telemetry frame is self-correcting on the next tick, and must
// never propagate as a peer-level error.
func (r *Reporter) report() {
	rep := r.sample()
	if err := r.send(rep); err != nil {
		r.log.Debug("metrics report dropped", slog.Any("error", err))
		return
	}
	r.log.Debug("reported telemetry",
		slog.String("name", rep.Name), slog.Int("upload_kbps", rep.UploadKbps), slog.String("nat", string(rep.NAT)))
}
