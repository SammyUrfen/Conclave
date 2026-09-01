package coordinator

import (
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// DegradationDwell is how long a node's metrics must stay bad before "sustained
// degradation" counts as a threshold event. The timer is armed when a node's
// metrics first cross a threshold and RESET on every good sample — so only an
// unbroken run of bad samples ever fires it. A single bad RTT resets nothing.
//
// 10s ≈ 3 metrics ticks at metrics.DefaultInterval. There is no correct constant
// here (too short → thrash; too long → sluggish), so this is a starting point that
// is deliberately CONFIGURABLE via Config.Dwell and tested at both extremes. It is
// not presented as tuned.
const DegradationDwell = 10 * time.Second

// RecomputeCooldown is the minimum interval between two published trees for ONE
// meet. It is the last line of anti-thrash defence: even if threshold events arrive
// in a burst (a 4-peer group all leaving at once), the coordinator coalesces them
// into at most one rebuild per window. A recompute suppressed by the cooldown sets a
// dirty flag and runs when the window closes — it is never dropped.
//
// WHY 5s specifically: it is bounded below by the cost of the thing it suppresses
// and above by the joiner's patience.
//
// Below: a rebuild that changes an edge costs that edge a re-negotiation plus a
// keyframe wait — call it 1-2s of degraded video. Two rebuilds inside that window
// would overlap, so the floor is ~2s or the cooldown does not actually prevent the
// harm. Above: the most common threshold event is a join, and 5s of black screen is
// well past what a user tolerates — which is exactly why an unplaced JOINER bypasses
// this entirely and is bounded by JoinSettle instead. That bypass is what lets this
// constant be tuned for the churn case without being hostage to the join case.
// Within 2-8s the choice is not sharp; 5s sits in the middle, is a round number in
// logs, and is configurable precisely because it is not a derived value.
const RecomputeCooldown = 5 * time.Second

// JoinSettle bounds how long the coordinator waits after a join for the fleet's
// telemetry before building.
//
// 1500ms. metrics.Reporter emits its first report IMMEDIATELY on Run (the
// deliberate "immediate-then-ticker" shape), so a healthy peer's first report lands
// within roughly one round trip of its join. 1.5s is ~10x that even on a poor WAN
// path and is imperceptible as a join latency. A silent peer past the window is
// attached as a provisional leaf and refined by whatever it eventually says; it can
// never stall the meet, which is the bound that "wait for everyone" alone lacks.
const JoinSettle = 1500 * time.Millisecond

// MinBuildableMembers is the smallest meet worth computing a tree for.
//
// 2, because a 1-member tree is not a tree: zero edges, zero media, and — the reason
// this is a CORRECTNESS constant and not a micro-optimisation — publishing it would
// seed the stickiness baseline (prev) with a root chosen from a fleet of one. Sticky
// PickRoot would then defend that root against every later arrival inside
// overlay.RootChangeMarginKbps. A meet's root must never be decided by who dialled
// in first.
const MinBuildableMembers = 2

// The metric thresholds that ARM the degradation dwell. Crossing one starts (or
// leaves running) the dwell; falling back under all of them resets it.
const (
	// DegradedLossPct 5%: VP8 with NACK tolerates ~1-2% loss invisibly; past ~5%
	// the decoder starts showing artefacts that a keyframe cannot hide.
	DegradedLossPct = 5.0
	// DegradedRTTMs 400: the ARCHITECTURE's stated tolerable ceiling for
	// interactive conversation, applied to the round trip.
	DegradedRTTMs = 400.0
	// DegradedCPUPct 90: above this a peer cannot be relied on to keep its
	// forwarding loops scheduled, whatever its bandwidth says.
	DegradedCPUPct = 90.0
)

// MaxStrandedRepairs bounds how many times the coordinator will spend an urgent
// recompute on ONE peer that reports it cannot attach, before it stops trying and says
// so.
//
// WHY A BOUND EXISTS AT ALL. The stranded bypass answers "this peer is receiving
// nothing, serve it now" — and that is right the first time. But the coordinator's only
// move is to compute a tree, and a peer can be un-attachable for a reason no tree can
// fix: if the SOURCE is orphaned, nothing is flowing to hand it, so its confirmation can
// never arrive however many times it is re-parented. Without a bound the loop is a
// livelock — recompute, republish, renegotiate, fail, repeat — and it was observed live
// as rev churn with an error storm underneath it.
//
// WHAT COUNTS AS AN ATTEMPT is the load-bearing part: an attempt is UNPRODUCTIVE only
// when the recompute it triggered published nothing new. If the coordinator had a
// different tree to offer, the peer has something new to try and the budget resets. So
// this counts "times I answered and my answer did not change", not "times you
// complained" — which is what makes it a progress bound rather than a rate limit.
//
// 3, because an attempt is cheap for the control plane and expensive for the media
// plane: each one that DOES change an edge costs that edge a renegotiation and a
// keyframe wait. Three absorbs a genuine transient — a peer reporting failure once or
// twice while its old session finishes tearing down — and converges in well under a
// second on a structural fault, where by construction a fourth identical answer cannot
// help. Attaching successfully resets it, so this bounds one EPISODE and never a peer.
const MaxStrandedRepairs = 3

// ReasonUnratifiable is the Event.Reason on the EventUnbuildable that reports a repair
// loop giving up: the coordinator believes its tree is correct and the peer cannot
// confirm it.
//
// It is an unbuildable OUTCOME rather than a new kind because the audience is the same
// one §5.9 wrote that outcome for — an operator who has to change something, since no
// tree the coordinator can compute will fix an orphaned source. The distinct reason is
// what separates it from an over-constrained fleet, which is a different fix.
const ReasonUnratifiable = "repair loop gave up: the peer cannot confirm any parent, and recomputing no longer changes the tree"

// ReasonNoEligibleRoot is the Event.Reason a meet carries when overlay.PickRoot
// found nobody fit to root the tree.
//
// It is a named constant rather than an inline string because it marks the ONE
// unbuildable verdict reached without a relaxed retry (§5.9a): dropping the
// stability preference does not create upload capacity, so an unrootable fleet is
// over-constrained immediately. A test asserting the short-circuit has no other
// handle on "the second attempt was skipped", since both paths look identical from
// outside. NOT part of the frozen surface — see the WI-3 report.
const ReasonNoEligibleRoot = "no eligible root: no member can serve a child"

// The two ways rebuild-from-peers (§6.6) can degrade, carried on the first
// EventTopology of a term so the degradation is visible where its consequence is.
//
// Both produce the SAME visible symptom — a from-scratch tree, so nearly every peer
// re-parents — and they have completely different causes and different fixes. Collapsing
// them, or reporting neither, leaves an operator watching every handover reconfigure the
// whole meet with no way to tell a broken telemetry path from ordinary churn. This is
// the same discipline §5.9a forced one plane down, where "we have not heard from anyone
// yet" and "this fleet is over-constrained" had to stop sharing one warning.
//
// They are exported so a test can assert on the distinction; the dashboard renders
// Event.Reason as text and never parses it.
const (
	// ReasonNoRealizedState: the peers beat, but none of them named a parent, so there
	// was no realized topology to reconstruct. The likely cause is a peer build that
	// does not populate metrics.Heartbeat's Parent/Children — i.e. the input to §6.6 is
	// missing rather than contradictory — and the fix is on the peer, not the fleet.
	ReasonNoRealizedState = "rebuild-from-peers: no realized topology was reported; this handover degraded to a full rebuild"
	// ReasonRealizedStateInvalid prefixes the case where the peers DID describe a
	// topology and it is not a legal tree — the mid-flight re-parent captured
	// half-applied that §6.6 admits as residual. The validator's complaint is appended.
	ReasonRealizedStateInvalid = "rebuild-from-peers: the reported realized topology is not a legal tree"
)

// Config parameterises the coordinator. Every duration is honoured through Clock, so
// a simnet scenario can drive all of them in virtual time.
type Config struct {
	// MaxDepth and StreamKbps flow straight into overlay.Constraints.
	MaxDepth   int
	StreamKbps int
	// DefaultUploadKbps is the budget assumed for a peer that has joined but not
	// yet reported. It defaults to 0 — a not-yet-reported peer is treated as a LEAF
	// until its first report proves it can relay. That conservative choice is
	// load-bearing: promoting an unproven peer to relay could hand it children it
	// cannot serve. A caller may set a positive value to attach newcomers at an
	// assumed budget instead.
	DefaultUploadKbps int
	// StickinessMs is the re-parent margin. 0 ⇒ overlay.DefaultStickinessMs.
	StickinessMs float64
	// Dwell is the sustained-degradation window. 0 ⇒ DegradationDwell.
	Dwell time.Duration
	// RecomputeCooldown is the anti-thrash window. 0 ⇒ RecomputeCooldown.
	RecomputeCooldown time.Duration
	// DegradedAfter and GoneAfter OVERRIDE the per-node health thresholds with a
	// fixed duration. 0 — the normal case — derives them per node from the cadence
	// the peer DECLARED on the wire, via metrics.DegradedAfter/metrics.GoneAfter.
	// Deriving is the shipped design because -heartbeat lets a peer choose its own
	// cadence, and a threshold hard-coded from the default would declare a peer
	// beating every 5s gone while it is beating perfectly.
	DegradedAfter time.Duration
	GoneAfter     time.Duration
	// JoinSettle bounds the post-join wait for telemetry. 0 ⇒ JoinSettle.
	JoinSettle time.Duration
	// SocketDetection is the Hub's worst-case socket-death detection window
	// (signaling.Hub.LivenessBudget(); 7s by default). Every per-node gone
	// threshold is FLOORED at it, so a fast-beating peer cannot shrink its own
	// threshold below the window in which the Hub may still consider it present.
	//
	// Startup flag validation cannot cover this: GoneAfter is a function of a
	// cadence each PEER declares at run time, so `-heartbeat 500ms` yields a 4s
	// threshold under a 7s socket window and reopens the permanent-ejection bug
	// from a place no flag check can reach. The floor closes it unconditionally and
	// per node, with no rejection of the peer and no clamping of what it may
	// declare. 0 disables the floor — correct only for simnet, which models no
	// socket layer.
	SocketDetection time.Duration
	// Clock drives every deadline. nil ⇒ clock.System().
	Clock clock.Clock
	// SelfName is the coordinator's own peer name. Empty means the coordinator is
	// running inside the arbiter process (Phase 5) rather than on an elected peer.
	SelfName string
}

// normalize resolves every zero-valued knob to its documented default, so the rest
// of the package never repeats a "0 means X" check and a Config literal can carry
// only what the caller wants to change.
func normalize(cfg Config) Config {
	if cfg.DefaultUploadKbps < 0 {
		cfg.DefaultUploadKbps = 0
	}
	if cfg.StickinessMs == 0 {
		cfg.StickinessMs = overlay.DefaultStickinessMs
	}
	if cfg.Dwell <= 0 {
		cfg.Dwell = DegradationDwell
	}
	if cfg.RecomputeCooldown <= 0 {
		cfg.RecomputeCooldown = RecomputeCooldown
	}
	if cfg.JoinSettle <= 0 {
		cfg.JoinSettle = JoinSettle
	}
	if cfg.SocketDetection < 0 {
		cfg.SocketDetection = 0
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.System()
	}
	return cfg
}

// degradedThreshold is the silence after which a node is demoted to degraded.
//
// The socket-detection floor deliberately does NOT apply here. Degraded is
// advisory — nothing re-parents on it — so a threshold shorter than the Hub's
// detection window cannot eject anybody; it only colours the dashboard earlier,
// which for a fast-beating peer is exactly right.
func (c *Coordinator) degradedThreshold(ns *nodeState) time.Duration {
	if c.cfg.DegradedAfter > 0 {
		return c.cfg.DegradedAfter
	}
	return metrics.DegradedAfter(ns.interval)
}

// goneThreshold is the silence after which a node is declared gone — a threshold
// event that removes it from the tree. It is floored at Config.SocketDetection so
// the health FSM can never be FASTER than the Hub's own socket reaper.
//
// The direction matters: the health FSM must be the slower of the two, so whenever
// the coordinator declares a peer gone, either the Hub has already reaped the socket
// (a real death) or the socket is genuinely live (a transient — and then the
// resurrection rule re-admits the peer on its next frame). Reversing it ejects a
// peer from a tree the Hub still considers it part of, with no event able to bring
// it back.
func (c *Coordinator) goneThreshold(ns *nodeState) time.Duration {
	d := c.cfg.GoneAfter
	if d <= 0 {
		d = metrics.GoneAfter(ns.interval)
	}
	if c.cfg.SocketDetection > d {
		return c.cfg.SocketDetection
	}
	return d
}

// isDegradedSample reports whether one telemetry sample crosses any of the three
// degradation thresholds. It is the predicate that arms and resets the dwell, kept
// as a pure function of a Report so the thresholds are testable without a clock.
func isDegradedSample(rep metrics.Report) bool {
	return rep.LossPct >= DegradedLossPct ||
		rep.RTTServerMs >= DegradedRTTMs ||
		rep.CPUPct >= DegradedCPUPct
}
