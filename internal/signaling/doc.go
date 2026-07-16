// Package signaling is conclave's control plane: the WebSocket rendezvous that
// lets peers discover one another and exchange the out-of-band setup — SDP
// offer/answer plus trickled ICE candidates — needed before any media flows.
//
// It is deliberately separate from the data plane. Media travels peer-to-peer
// over WebRTC (SRTP/UDP) and never passes through here; this package only moves
// small JSON control frames. SDP and ICE payloads ride as opaque
// json.RawMessage (see Message), so the server relays them without importing
// pion or understanding media at all — the control/data-plane split from
// docs/ROADMAP.md, enforced by the type system.
//
// A peer dials GET /ws?room=<id>, is assigned an id, and thereafter addresses
// other peers in the room by id. The Hub multiplexes peers into rooms and
// relays frames between them. Identity is server-authoritative: the Hub stamps
// Message.From on every relayed frame, so a peer cannot spoof another's id.
//
// Concurrency shape: one Hub, its room map guarded by a single sync.Mutex; per
// connection, a reader goroutine (the http handler itself) and a writer
// goroutine draining a buffered channel, torn down together by one context
// cancellation. Populated in Phase 1.
package signaling
