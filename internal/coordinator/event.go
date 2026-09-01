package coordinator

import (
	"time"

	"github.com/SammyUrfen/conclave/internal/overlay"
)

// Health is a node's liveness classification. It is deliberately three states, not
// two: collapsing "degraded" into "gone" would make every GC pause a re-parenting
// event, and collapsing it into "healthy" would leave the operator blind to a node
// that is about to fail. Degraded is the state that is VISIBLE but not ACTIONABLE.
type Health string

const (
	// HealthHealthy: beating. In the tree, green on the dashboard.
	HealthHealthy Health = "healthy"
	// HealthDegraded: three missed beats. STILL IN THE TREE and still pushed to,
	// rendered amber. Advisory only — degraded never re-parents anything.
	HealthDegraded Health = "degraded"
	// HealthGone: eight missed beats. Excluded from the tree projection and a
	// threshold event. The RECORD IS NOT DELETED — see the resurrection rule on
	// Coordinator.Heartbeat.
	HealthGone Health = "gone"
)

// BuildOutcome classifies why a recompute did or did not publish a tree. It exists
// because the cases have different audiences: one is normal, one is a startup
// detail, one is a rare expensive success, and one is an operator-actionable fault.
type BuildOutcome string

const (
	// OutcomeBuilt: a tree was computed, validated, and pushed. Logged at Info.
	OutcomeBuilt BuildOutcome = "built"
	// OutcomeSettling: the meet has not met the eligibility rule yet — too few
	// members, or a join settle still running. Entirely normal, and lasts at most
	// JoinSettle per join episode (unbounded only while a meet has one member).
	// Logged at DEBUG, never Warn: a warning that fires on every healthy startup
	// teaches operators to ignore warnings.
	OutcomeSettling BuildOutcome = "settling"
	// OutcomeRelaxed: the sticky build FAILED but a from-scratch build (prev = nil)
	// succeeded, so a tree WAS published — at the cost of dropping the stability
	// preference for this round. Expect a large re-parent. Logged at Warn because
	// it is rare, expensive, and worth seeing; it is a SUCCESS, not a fault.
	OutcomeRelaxed BuildOutcome = "relaxed"
	// OutcomeUnbuildable: no tree exists for this fleet EVEN WITHOUT the stability
	// preference — PickRoot found no eligible root, or both attempts failed. A
	// real, operator-actionable condition (not enough aggregate upload for this
	// many peers, or a depth bound too shallow); the previous tree is retained.
	//
	// Because the relaxed retry runs first, this now means "genuinely
	// over-constrained", never "stickiness painted us into a corner".
	OutcomeUnbuildable BuildOutcome = "unbuildable"
)

// EventKind enumerates what the coordinator observes.
type EventKind string

const (
	EventTopology EventKind = "topology"       // a new tree was computed and pushed
	EventMember   EventKind = "member"         // a member joined (Present) or left (!Present)
	EventHealth   EventKind = "health"         // a node's Health changed
	EventReparent EventKind = "reparent"       // a peer self-promoted to its backup
	EventFailover EventKind = "failover"       // a node was declared gone; repair ran
	EventStale    EventKind = "stale_rejected" // a stale-epoch instruction was refused

	// EventSettling is emitted ONCE per transition into ineligibility, not on every
	// suppressed threshold event — the latter would be a frame storm at exactly the
	// moment the dashboard is connecting. Event.Reason names what is being waited
	// on and Event.Waiting names who.
	EventSettling EventKind = "settling"
	// EventUnbuildable is emitted on every failed post-settle build, with
	// Event.Reason carrying the builder's error text. The dashboard renders it as a
	// persistent banner: some participant is receiving nothing and a human has to
	// change something (drop a peer, raise -max-depth, lower -stream-kbps).
	EventUnbuildable EventKind = "unbuildable"
)

// Event is one observable control-plane fact. Fields not relevant to a Kind are
// zero.
//
// Every ordered field (Orphans, Waiting) is sorted ascending. That is not cosmetic:
// the dashboard replays this stream, and a collection whose order comes from Go's
// randomised map iteration makes two replays of the same history differ.
type Event struct {
	Kind   EventKind
	RoomID string
	At     time.Time // stamped from the injected clock, never the wall clock
	Epoch  uint64
	Rev    uint64
	Node   string // the node this event is about (its stable NAME)
	// NodeID is the server-assigned peer id for Node, populated on member, health,
	// reparent, and stale events.
	//
	// It exists because a consumer was otherwise forced to key a membership delta on
	// the peer-SUPPLIED name — the one field a peer controls — while keying the
	// snapshot on the server-stamped id, so the two could not be joined reliably. The
	// coordinator holds the id already (nodeState.id); this was a plumbing gap, not a
	// missing fact. Deliberately absent on failover, which names a node in a tree and
	// has no id to speak of for a node that may already be gone.
	NodeID     string
	Parent     string // new parent (reparent/failover)
	PrevParent string // parent that was lost (reparent/failover)
	Health     Health // EventHealth: the NEW value
	// PrevHealth is the value Health transitioned FROM, for the same reason as NodeID:
	// the coordinator performs the transition, so it is the only party that knows it
	// without guessing. A consumer remembering the previous value in-process is wrong
	// across a restart and wrong for a fresh subscriber, whose very first transition
	// would report "".
	//
	// ONE RULE WORTH KNOWING: sustained degradation and its recovery have no event kind
	// of their own, so they ride EventHealth — and they set PrevHealth EQUAL to Health,
	// because a node's LIVENESS did not change. A consumer can therefore separate a
	// liveness transition from an impairment with a comparison, rather than by parsing
	// Reason, which this package promises never to make parseable.
	PrevHealth Health
	// Count is a monotonic total carried by counting events. EventStale sets it to the
	// peer's cumulative refusal count; Reason carries no numbers.
	Count   uint64
	Present bool         // EventMember
	Orphans []string     // EventFailover: the subtree that had to move, ascending
	Reroot  bool         // EventFailover: the root itself was lost
	Waiting []string     // EventSettling: members not yet heard from, ascending
	Outcome BuildOutcome // EventTopology/EventSettling/EventUnbuildable
	Reason  string       // human-readable; never parsed
	// Topo is the tree just published (EventTopology). It is not copied: the
	// coordinator never mutates a published Topology in place, so sharing the
	// pointer is safe and avoids a deep copy per subscriber. DO NOT MUTATE IT.
	Topo *overlay.Topology
}

// Publisher is the SEAM through which the coordinator emits observable events for
// the dashboard. It lives HERE, in coordinator, and its method set mentions only
// stdlib and overlay types — so declaring it adds no import to this package and
// creates no cycle with internal/dashboard, which implements it.
//
// One method taking a struct, rather than one method per event kind. The struct
// wins for the same reason signaling.Message is one frame type with omitempty
// fields rather than a union of nine: adding a new event kind later is a
// non-breaking change for every implementer, whereas adding a method breaks all of
// them. What it gives up is compile-time exhaustiveness on the consumer side —
// named, and accepted, because the dashboard fans every kind into one JSON envelope
// anyway.
//
// Publish is called from the coordinator's single Run goroutine and MUST NOT BLOCK:
// an implementation that needs to do work should enqueue and return. A nil Publisher
// is legal and means "publish nothing".
type Publisher interface {
	Publish(Event)
}

// publish stamps an event with the room's identity and the injected clock, then
// hands it to the Publisher. Centralising the stamping is what keeps At off the wall
// clock: no call site sets it, so no call site can reach for package time.
func (c *Coordinator) publish(rs *roomState, ev Event) {
	if c.pub == nil {
		return
	}
	ev.RoomID = rs.id
	ev.At = c.cfg.Clock.Now()
	// Most events describe the MEET, so an unset stamp defaults to the meet's term and
	// revision. EventStale is the exception and must not be defaulted: it reports the
	// state of ONE PEER's fence, and a peer that has adopted nothing genuinely is at
	// epoch 0 — which is the most diagnostic case there is. Substituting the meet's
	// epoch there would hide the exact mismatch the event exists to show.
	if ev.Kind != EventStale {
		if ev.Epoch == 0 {
			ev.Epoch = rs.epoch
		}
		if ev.Rev == 0 {
			ev.Rev = rs.rev
		}
	}
	c.pub.Publish(ev)
}
