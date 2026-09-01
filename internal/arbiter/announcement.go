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
	// Coordinator is the peer NAME; "" means the arbiter itself coordinates.
	Coordinator string `json:"coordinator"`
	// CoordinatorID is the server-assigned peer id, or the arbiter's reserved id.
	// This is the value a peer fences topology pushes against, because the Hub
	// stamps the sender id on every frame and a peer cannot forge it.
	CoordinatorID string `json:"coordinator_id"`
	// Prev is the outgoing coordinator's name, for the dashboard's narrative.
	Prev string `json:"prev,omitempty"`
	// Reason explains the change. Never parsed by a peer; peers act on Epoch alone.
	Reason Reason `json:"reason"`
	// IssuedAtUnixMs comes from the injected clock, never the wall clock, so a
	// replayed scenario produces byte-identical frames.
	IssuedAtUnixMs int64 `json:"issued_at_unix_ms"`
}

// IssuedAt is IssuedAtUnixMs as a time.Time, for callers that would otherwise each
// reinvent the conversion.
func (a Announcement) IssuedAt() time.Time { return time.UnixMilli(a.IssuedAtUnixMs) }

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
}

// nopAnnouncer is what a nil Announcer becomes, so the election logic never has to
// nil-check its own output seam.
type nopAnnouncer struct{}

func (nopAnnouncer) AnnounceCoordinator(string, Announcement) error { return nil }
