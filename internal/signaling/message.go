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
)

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
}
