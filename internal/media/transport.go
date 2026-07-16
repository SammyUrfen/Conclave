package media

import "github.com/SammyUrfen/conclave/internal/signaling"

// Transport is the signaling channel a Session uses to exchange SDP and ICE with
// its one remote peer. The consumer (Session) defines it — "accept interfaces,
// return structs" — so the real *signaling.Client (via Router's per-peer adapter)
// and an in-memory test double are interchangeable, and the Session's negotiation
// logic can be tested with no sockets and no server.
//
// A Session only ever acts on offer/answer/candidate frames; lifecycle frames
// (joined/peer-joined/peer-left) are handled by whoever owns the transport (the
// Router), which is why Incoming here carries only the frames meant for this
// peer's session.
type Transport interface {
	// Send delivers one signaling frame toward the remote peer. The
	// implementation is responsible for addressing (setting To).
	Send(signaling.Message) error
	// Incoming is the stream of signaling frames from the remote peer.
	Incoming() <-chan signaling.Message
}
