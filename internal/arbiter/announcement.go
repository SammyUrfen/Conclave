package arbiter

import "time"

// Reason explains a coordinator change, for the dashboard and the logs. Frozen enum.
type Reason string

const (
	// ReasonBootstrap is the first coordinator for this meet, or the first after a
	// vacancy. It is also the reason carried by every re-announcement of a
	// bootstrapped term, because a re-announcement replays the original frame
	// verbatim rather than minting a new one.
	ReasonBootstrap Reason = "bootstrap"
	// ReasonFailover means the incumbent was lost — its socket closed, or the
	// arbiter stopped hearing from it. No dwell, no minimum term.
	ReasonFailover Reason = "failover"
	// ReasonPromotion means a materially fitter peer took over, voluntarily.
	ReasonPromotion Reason = "promotion"
	// ReasonDemotion means the incumbent fell below the absolute fitness floor.
	ReasonDemotion Reason = "demotion"
	// ReasonManual means the change was forced through the demo control surface.
	ReasonManual Reason = "manual"
	// ReasonVacated means the coordinator was lost or demoted and NO eligible
	// candidate exists. Coordinator and CoordinatorID are both "", and the epoch is
	// still BUMPED — because the epoch is a fencing token, not a term counter, and
	// the thing being fenced is the old coordinator. Announcing a vacancy is the
	// only way to tell a live-but-demoted coordinator to stop; silence cannot.
	ReasonVacated Reason = "vacated"
)

// Announcement is the arbiter's authoritative statement of who coordinates a meet,
// under which epoch.
//
// It is the ONLY thing in the system that may advance a peer's epoch, and the
// arbiter is its only author. That asymmetry is the whole fence: a peer learns about
// a new coordinator only from the arbiter, never from the would-be coordinator's own
// message, so stamping a bigger number on a topology authenticates nothing.
//
// On the wire it is a signaling TypeCoordinator frame with this struct marshalled
// into the payload, broadcast to every member of the meet.
type Announcement struct {
	RoomID string `json:"room_id"`
	// Epoch is the term. Strictly increasing per meet, never reissued — including
	// across a broadcast that failed to reach anyone, because reusing a consumed
	// epoch is exactly the state in which two coordinators could hold one term.
	Epoch uint64 `json:"epoch"`
	// Coordinator is the peer NAME. "" means either the arbiter itself or a
	// vacancy; CoordinatorID disambiguates (the arbiter's reserved id versus "").
	Coordinator string `json:"coordinator"`
	// CoordinatorID is the server-assigned peer id, or the arbiter's reserved id.
	// This is the value a peer fences topology pushes against, because the Hub
	// stamps the sender id on every frame and a peer cannot forge it.
	CoordinatorID string `json:"coordinator_id"`
	// Prev is the outgoing coordinator's name, for the dashboard's narrative.
	Prev string `json:"prev,omitempty"`
	// Reason explains the change. Never parsed by a peer; peers act on Epoch alone.
	Reason Reason `json:"reason"`
	// Config is the tuning the named coordinator must serve this meet under.
	//
	// It rides here because the tuning belongs to the MEET, not to whichever node
	// holds the role: an elected peer otherwise has no way to learn what constraints
	// to coordinate under, and a handover would silently re-shape the subnet. See
	// CoordinatorConfig.
	//
	// It is present on every announcement, including a vacancy — the frame is one
	// shape, and a re-announcement or a unicast repair replays it verbatim along with
	// everything else.
	Config CoordinatorConfig `json:"config"`
	// IssuedAt comes from the injected clock, never the wall clock, so a replayed
	// scenario produces identical frames. It is a time.Time rather than the
	// *_unix_ms convention used elsewhere because that convention is scoped to
	// dashboard bodies, which JavaScript consumes; this is a Go-to-Go signaling
	// payload, and the dashboard re-serializes it into its own envelope anyway.
	IssuedAt time.Time `json:"issued_at"`
}

// Announcer ships an Announcement to every member of a meet.
//
// Consumer-defined here, and named only in terms of arbiter-owned and stdlib types,
// so this package never imports signaling; cmd/server adapts it to the Hub's
// room broadcast. Like the coordinator's Sender, an implementation MUST NOT block
// and MUST NOT do network I/O on the calling goroutine: it is called from the
// arbiter's single Run goroutine, and one stalled peer would otherwise stall every
// meet in the process. An implementation that must do real I/O enqueues and returns.
//
// A returned error is transient and never fatal — but note that it does NOT undo the
// epoch, which has already been minted and consumed. The failure is repaired by the
// next membership change, which re-broadcasts the same announcement.
type Announcer interface {
	AnnounceCoordinator(roomID string, a Announcement) error
	// RepairCoordinator re-sends a's CURRENT announcement to ONE peer whose fence
	// has fallen behind. It is a separate method because the repair must be unicast:
	// broadcasting would turn one wedged peer into N frames per second, and the
	// broadcast method has no way to name a recipient.
	//
	// The frame is byte-identical to the announcement that peer missed, on purpose.
	// Minting a fresh one with a different Reason would leave two peers disagreeing
	// about why the current coordinator holds the role.
	RepairCoordinator(roomID, peerID string, a Announcement) error
}

// Publisher is the arbiter's dashboard seam, separate from the coordinator's because
// each interface is owned by its emitter — which is what keeps both packages free of
// any import of the dashboard. A nil Publisher means "publish nothing".
//
// Only a real election is published. A re-announcement (the same epoch rebroadcast
// after a membership change) is deliberately not, because it is not a transition and
// an event log that shows one per join is an event log nobody reads.
type Publisher interface {
	PublishElection(Announcement)
	// PublishRepair reports that a peer's fence has fallen behind the meet's epoch,
	// and again when it catches up.
	//
	// It is emitted on TRANSITION only — entering and leaving the lagging state —
	// never once per heartbeat, which is the same transition-not-sample discipline
	// the health FSM uses. 1 Hz of wire repair therefore produces two events per
	// episode instead of drowning the log at exactly the moment an operator is
	// trying to read it.
	//
	// It is deliberately NOT folded into PublishElection: the election log's whole
	// value is that it lists actual leadership changes, and a repair is not one.
	PublishRepair(roomID, name string, peerEpoch, meetEpoch uint64, resolved bool)
}

// nopAnnouncer is what a nil Announcer becomes, so the election logic never has to
// nil-check its own output seam.
type nopAnnouncer struct{}

func (nopAnnouncer) AnnounceCoordinator(string, Announcement) error       { return nil }
func (nopAnnouncer) RepairCoordinator(string, string, Announcement) error { return nil }
