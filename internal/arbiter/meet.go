package arbiter

import (
	"sort"
	"time"

	"github.com/SammyUrfen/conclave/internal/metrics"
)

// Meet is the arbiter-owned description of one meet, and the payload of the
// dashboard's /api/meets endpoints.
//
// It is owned by its producer rather than by the dashboard on purpose: a type
// defined in the dashboard and returned by the arbiter would invert the dependency
// and drag the browser-facing surface into the control plane.
//
// Relays and Depth describe the REALIZED subnet — reconstructed from the heartbeats
// the arbiter already terminates, not from the coordinator's beliefs. The arbiter may
// not import the coordinator, and asking the coordinator would reintroduce exactly the
// circularity Fitness.Live exists to avoid; reporting ground truth is also the more
// honest thing for a dashboard to draw, which is why the dashboard labels every field
// here provenance "realized".
//
// Rev is deliberately ABSENT. Rev is a coordinator counter, not an observable, so a
// heartbeat-derived Rev would be a fabricated number wearing an authoritative name.
// Epoch stays because the arbiter MINTS it and is its single writer.
type Meet struct {
	ID        string
	CreatedAt time.Time
	// There is deliberately no EndedAt here. A reaped meet leaves the registry, so
	// the field could never hold a value, and a field that is structurally always
	// zero states something false about the type. The end time lives on the
	// tombstone, EndedMeet.EndedAt, which is the only place it can be observed.
	Members int
	// Epoch is the current term. 0 means no coordinator has ever been named here.
	Epoch uint64
	// Coordinator is the peer name; "" means vacant, or the arbiter itself.
	// Distinguish the two with ArbiterIsCoord, never by testing this for "".
	Coordinator string
	// CoordinatorID is the peer id, or the arbiter's reserved id when it coordinates.
	CoordinatorID string
	// Relays are the members reporting at least one realized child, ascending.
	Relays []string
	// Depth is the realized hop count from root to the deepest leaf, or -1 when no
	// tree exists yet.
	Depth          int
	ArbiterIsCoord bool
}

// EndedMeet is the tombstone left behind when a meet is reaped, so an operator can
// still review the demo they just ran instead of watching it vanish the instant the
// last peer disconnects. Deliberately small and fixed-size: it carries counts, never
// telemetry, because an unbounded history is the same denial of service in a
// different map.
type EndedMeet struct {
	ID        string
	CreatedAt time.Time
	// EndedAt is when the meet WENT EMPTY — the moment its last participant left —
	// not when it was reaped.
	//
	// The distinction is not pedantic. Reaping is LAZY: it happens on a registry
	// mutation or a listing, so on a quiet server nothing triggers a sweep and a
	// reap-time stamp could be late by hours rather than by MeetTTL. "Ended 14:20"
	// for a call that finished at 11:59 is the kind of wrong nobody notices and
	// nobody can debug, whereas a tombstone that merely APPEARS late is visibly late.
	//
	// The value is the FINAL emptiness: a meet that empties, is rejoined, and empties
	// again carries the last one. That falls out of emptyAt being cleared on rejoin
	// rather than needing its own rule. Reap time is an implementation artifact and
	// is deliberately not recorded at all — no consumer needs it, and a second
	// timestamp would only invite the same confusion back.
	EndedAt     time.Time
	PeakMembers int
	FinalEpoch  uint64
	Elections   int
}

// peerState is everything the arbiter knows about one member. It is owned by the Run
// goroutine and never escapes it except as a copied value.
type peerState struct {
	id   string
	name string
	// joinedAt drives Fitness.UptimeSec. Reset on a rejoin, because a peer that
	// dropped and came back has not proven it stays.
	joinedAt time.Time
	// lastSeen is the arbiter's INDEPENDENT liveness evidence: the instant of the
	// last frame of any kind from this peer. Every frame counts, not just a
	// heartbeat, because a peer that is reporting telemetry is self-evidently alive
	// and refusing that evidence would only invent false failovers.
	lastSeen time.Time
	// report/reported/coordinatable are the last telemetry declaration. reported
	// gates coordinatable so a peer that has never spoken is never elected.
	report        metrics.Report
	reported      bool
	coordinatable bool
	// beat is the last heartbeat, kept for its declared cadence (which sets this
	// peer's own liveness threshold) and for the realized topology it carries.
	beat    metrics.Heartbeat
	hasBeat bool
	// lagging is the LATCHED fence-repair state: this peer's declared epoch has been
	// behind the meet's for longer than the propagation window. It exists so the
	// dashboard sees two events per episode (entering, leaving) rather than one per
	// heartbeat — the transition-not-sample discipline. The wire repair still fires
	// every beat; only the event is latched.
	lagging bool
}

// interval is the cadence this peer declared, defaulting when it has not spoken. It
// matters that the threshold is a multiple of the peer's OWN cadence: a peer
// deliberately beating every 5 s must not be declared gone for doing exactly that.
func (ps *peerState) interval() time.Duration {
	if !ps.hasBeat {
		return 0 // metrics.GoneAfter treats this as "use the default cadence"
	}
	return ps.beat.Interval()
}

// meetState is one meet's authoritative control state. Every field is written only
// by the arbiter's Run goroutine, which is what makes the epoch counter a
// single-writer value with no lock to get wrong and no agreement to reach.
type meetState struct {
	id        string
	createdAt time.Time
	members   map[string]*peerState

	// epoch is the fencing token. Incremented on, and only on, a change of holder —
	// including a change TO nobody, because a vacancy fences the outgoing coordinator.
	epoch uint64
	// who currently holds the role. A struct rather than three loose fields so that
	// "is this the same holder we already announced" is one comparison and cannot be
	// got half right.
	who holder

	// emptyAt is when the meet last became memberless, zero while it has members. A
	// meet is reaped only after MeetTTL of CONTINUOUS emptiness, so a rejoin clears
	// this rather than merely restarting a countdown.
	emptyAt time.Time
	// peakMembers and elections are the only history a tombstone keeps.
	peakMembers int
	elections   int
	// noVolunteers latches "reports are in hand and nobody is willing to coordinate",
	// so the operator-facing warning fires on the transition rather than per report.
	noVolunteers bool

	// lastAnn is replayed verbatim on a membership change so a joiner adopts the
	// current authority. Keeping the original frame rather than minting a fresh one
	// is what makes the re-announcement idempotent at every peer already at that
	// epoch.
	lastAnn Announcement
	hasAnn  bool

	// termStart is when the sitting coordinator took office, for MinTermDuration.
	termStart time.Time

	// The dwell state for a VOLUNTARY handover: which change has been wanted, at
	// whom, and since when. A change of wanted reason or target restarts the clock,
	// which is what "sustained" means.
	pendingReason Reason
	pendingTarget string
	pendingSince  time.Time
}

func newMeetState(id string, now time.Time) *meetState {
	return &meetState{id: id, createdAt: now, members: map[string]*peerState{}}
}

// hasHolder reports whether anyone currently coordinates this meet.
func (ms *meetState) hasHolder() bool { return ms.who.kind != holderNone }

// abandon drops both the holder and the retained announcement. It is used ONLY when
// the meet has no members left: there is nobody to announce a vacancy to, and keeping
// an announcement that names a departed coordinator would fence the next joiner to a
// corpse. While the meet still has members the correct move is the opposite — announce
// the vacancy (see elect) — because a live peer may still believe it coordinates.
func (ms *meetState) abandon() {
	ms.who = holder{}
	ms.lastAnn, ms.hasAnn = Announcement{}, false
}

func (ms *meetState) clearPending() {
	ms.pendingReason, ms.pendingTarget, ms.pendingSince = "", "", time.Time{}
}

// snapshot copies this meet out for a caller on another goroutine.
func (ms *meetState) snapshot(arbiterID string) Meet {
	relays, depth := ms.realized()
	m := Meet{
		ID:             ms.id,
		CreatedAt:      ms.createdAt,
		Members:        len(ms.members),
		Epoch:          ms.epoch,
		Coordinator:    ms.who.name,
		CoordinatorID:  ms.who.id,
		Relays:         relays,
		Depth:          depth,
		ArbiterIsCoord: ms.who.kind == holderArbiter,
	}
	if m.ArbiterIsCoord {
		m.CoordinatorID = arbiterID
	}
	return m
}

// tombstone renders this meet as the record left behind when it is reaped.
//
// It takes no instant: the end time is ms.emptyAt, already recorded when the last
// member left. Passing the reap time in would make the caller's clock the source of a
// value that is not about the caller's moment at all.
func (ms *meetState) tombstone() EndedMeet {
	return EndedMeet{
		ID:          ms.id,
		CreatedAt:   ms.createdAt,
		EndedAt:     ms.emptyAt,
		PeakMembers: ms.peakMembers,
		FinalEpoch:  ms.epoch,
		Elections:   ms.elections,
	}
}

// realized reconstructs the subnet from the members' last heartbeats.
//
// The reconstruction is defensive rather than trusting: heartbeats from different
// peers are snapshots taken at different instants, so the edge set can be
// momentarily torn or even cyclic mid-reparent. Every walk is therefore bounded by a
// visited set, and a parent naming a peer that has not reported is treated as the
// top of the chain rather than an error — this is a display value, and a torn frame
// must degrade to a slightly stale number, never to a hang or a panic.
func (ms *meetState) realized() (relays []string, depth int) {
	parent := make(map[string]string, len(ms.members))
	known := make(map[string]bool, len(ms.members))
	// Iterating the member map is safe here because every result is either sorted
	// (relays) or an order-independent reduction (a max). Nothing downstream can
	// observe the iteration order.
	for _, ps := range ms.members {
		if !ps.hasBeat {
			continue
		}
		known[ps.name] = true
		if ps.beat.Parent != "" {
			parent[ps.name] = ps.beat.Parent
		}
		if len(ps.beat.Children) > 0 {
			relays = append(relays, ps.name)
		}
	}
	sort.Strings(relays)

	if len(parent) == 0 {
		return relays, -1
	}
	depth = 0
	for name := range known {
		hops := 0
		seen := map[string]bool{name: true}
		for cur := name; ; {
			p, ok := parent[cur]
			if !ok || !known[p] || seen[p] {
				break
			}
			seen[p] = true
			hops++
			cur = p
		}
		if hops > depth {
			depth = hops
		}
	}
	return relays, depth
}
