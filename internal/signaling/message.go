package signaling

import "encoding/json"

// Type is the discriminator carried on every signaling frame. Go has no enum or
// sum type, so the idiom is a named string type plus a block of typed constants:
// the compiler still catches a TypeOffer-vs-"ofer" typo at any call site typed
// as Type, and the value round-trips over JSON as a plain string.
type Type string

const (
	// --- peer → server → peer: relayed verbatim, routed by To. The server never
	// inspects SDP/Candidate; it only forwards the frame to the addressed peer. ---

	// TypeOffer carries an SDP offer in Message.SDP.
	TypeOffer Type = "offer"
	// TypeAnswer carries an SDP answer in Message.SDP.
	TypeAnswer Type = "answer"
	// TypeCandidate carries one trickled ICE candidate in Message.Candidate.
	TypeCandidate Type = "candidate"

	// --- server → peer: control frames the Hub originates. ---

	// TypeJoined is sent once to a newcomer. Message.To is the id the server just
	// assigned it (this frame is addressed to you, and its address *is* your new
	// id), and Message.Peers lists the members already present.
	TypeJoined Type = "joined"
	// TypePeerJoined tells the incumbents a newcomer arrived; From is its id.
	TypePeerJoined Type = "peer-joined"
	// TypePeerLeft tells the remaining members someone left; From is its id.
	TypePeerLeft Type = "peer-left"
	// TypeError reports a rejected request back to its sender (e.g. a relay to an
	// unknown peer). Message.Error holds a human-readable reason.
	TypeError Type = "error"

	// --- Phase 4 control plane: metrics in, computed topology out. ---

	// TypeMetrics carries a peer's telemetry to the coordinator in Message.Payload
	// (an opaque metrics.Report — the server decodes it rather than relaying it, the
	// one frame the Hub consumes instead of forwarding). Peer → server.
	TypeMetrics Type = "metrics"
	// TypeTopology carries a coordinator-computed relay tree to a peer in
	// Message.Payload (an opaque overlay.Topology the peer decodes and realises).
	// It has TWO legitimate origins: the server (Phase 5, From == ServerID) and an
	// elected coordinator peer (Phase 6, From == that peer's id). The Hub relays a
	// peer-originated one without checking who sent it — see route.
	TypeTopology Type = "topology"

	// --- Phase 5/6 liveness and election plane. ---

	// TypeHeartbeat carries a peer's 1 Hz liveness beat and its REALIZED topology
	// state in Message.Payload (an opaque metrics.Heartbeat). Peer → server, where
	// it terminates: like TypeMetrics it is consumed by the Observer, never relayed.
	TypeHeartbeat Type = "heartbeat"
	// TypeReparented reports that a peer changed its own parent without being told
	// to — a backup promotion, successful or not — in Message.Payload (an opaque
	// metrics.Reparented). Peer → server, terminating.
	TypeReparented Type = "reparented"
	// TypeCoordinator announces who holds the coordinator role for an epoch, in
	// Message.Payload (an opaque arbiter.Announcement). Server → every member of the
	// meet; a peer that sends one is refused, because the arbiter is the sole source
	// of truth for this fact and a peer-minted announcement would defeat the fence.
	TypeCoordinator Type = "coordinator"
	// TypeMembership carries the meet's AUTHORITATIVE roster in Message.Peers.
	// Server → every member, broadcast on every membership change.
	//
	// It exists for the Phase 6 coordinator, which is a PEER: such a coordinator has
	// no Observer callbacks, so without this frame it would learn joins only
	// implicitly (from a stranger's next heartbeat) and learn graceful leaves not at
	// all — the join and leave threshold events would be unreachable, and a peer that
	// quit cleanly would only be noticed by the multi-second "gone" backstop. The
	// delta frames (TypePeerJoined / TypePeerLeft) still flow; this is the periodic
	// snapshot that makes a missed delta self-correcting rather than permanent, and
	// gives a newly elected coordinator ground truth without a handover protocol.
	TypeMembership Type = "membership"
)

// ServerID is the reserved From value the Hub stamps on frames the server
// ORIGINATES (as opposed to relays). It lets a peer apply one uniform fencing rule
// in both phases: in Phase 5 the coordinator IS the server, so a topology arrives
// with From == ServerID; in Phase 6 it arrives with From == the elected peer's id.
// Without the reserved value a peer would need two different checks.
//
// It is prefixed with '_' because peer ids are "p1", "p2", … — a collision is
// impossible by construction, and the prefix makes that obvious in a log line.
const ServerID = "_server"

// Peer identifies one room member on the wire: the server-assigned id (the
// authoritative address used for routing) plus the peer's self-declared name (a
// stable label). The two are distinct on purpose — ids are unique and
// server-controlled, names are peer-supplied and used only so a peer can locate
// itself and its neighbours in a hardcoded topology authored ahead of time (the
// server assigns p1, p2, … at connect time, but a tree is written in names like
// "relay"/"leaf-b"). Name is empty when a peer joined without -name.
type Peer struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

// Message is the single frame type on conclave's signaling WebSocket. Both the
// server (this package) and every peer speak it.
//
// It is deliberately pion-free: SDP and Candidate are json.RawMessage — still
// encoded bytes the server forwards without decoding. Only the peer, which
// imports pion, marshals a webrtc.SessionDescription / webrtc.ICECandidateInit
// into them and back out. Keeping the control-plane server ignorant of media
// details is the plane split from docs/ROADMAP.md, enforced at the type level.
//
// Every field but Type is omitempty, so one struct serves both control frames
// (joined / peer-joined / peer-left / error) and media frames
// (offer / answer / candidate) without a fat union — absent fields simply do
// not appear on the wire.
type Message struct {
	Type Type `json:"type"`
	// From is the sender's id. It is STAMPED BY THE SERVER on every relayed
	// frame and is never trusted from the client — that is what stops a peer
	// impersonating another.
	From string `json:"from,omitempty"`
	// To is the intended recipient's id: the relay target, or the addressee of a
	// control frame.
	To string `json:"to,omitempty"`
	// Name is the peer's stable label. It is set by the server on peer-joined /
	// peer-left frames (naming the peer that From identifies) so that peers can
	// resolve topology names to runtime ids. Like From, it is server-stamped from
	// the connection's declared name, never trusted off the wire from a relay.
	Name string `json:"name,omitempty"`
	// SDP is an opaque webrtc.SessionDescription for offer/answer frames.
	SDP json.RawMessage `json:"sdp,omitempty"`
	// Candidate is an opaque webrtc.ICECandidateInit for candidate frames.
	Candidate json.RawMessage `json:"candidate,omitempty"`
	// Peers lists the members already in the room (id + name); set only on a
	// joined frame.
	Peers []Peer `json:"peers,omitempty"`
	// Error is a human-readable reason; set only on an error frame.
	Error string `json:"error,omitempty"`
	// Payload is an opaque control-plane body for metrics/topology frames — a
	// json.RawMessage for the same reason SDP is: the wire layer stays ignorant of
	// its shape (a metrics.Report going up, an overlay.Topology coming down), so
	// signaling never imports the control-plane packages. The endpoints that own
	// those types marshal into and out of it; the Type field says which is inside.
	Payload json.RawMessage `json:"payload,omitempty"`
}
