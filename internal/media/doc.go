// Package media is conclave's data plane: the pion/webrtc wiring that carries
// audio/video (SRTP over UDP/ICE) directly between overlay neighbors, never
// through the central server.
//
// A Session owns one webrtc.PeerConnection to one remote peer and negotiates it
// with a single-offerer scheme over a Transport — the signaling channel it uses to
// exchange SDP and ICE. Per pair the higher id offers and the other only answers,
// so there is never glare to reconcile; this is deliberate, because pion cannot
// roll back a local offer (so textbook perfect-negotiation-with-rollback is not
// implementable on it). The Transport interface is defined here, by the consumer,
// so the real *signaling.Client and an in-memory test double are interchangeable
// and this package never imports a concrete transport for its core logic.
//
// A Router adapts a *signaling.Client into per-peer Transports, demultiplexing
// the single inbound signaling stream (joined/peer-joined/peer-left plus routed
// offer/answer/candidate) into one Session per remote peer, and meters aggregate
// upload across all of them. For Phase 2 that is a full mesh — every peer holds a
// PeerConnection to every other — which is what makes the O(N) upload cost visible.
//
// Media sources (PlayIVF, SendSynthetic) feed an outbound track; sinks
// (RecordVP8, DrainAndCount) consume a remote track. Codecs are pinned to VP8 in
// the MediaEngine so both ends agree without depending on default ordering.
//
// Phase 3 grows this into the novel core: reading RTP off a *webrtc.TrackRemote
// and forwarding it into per-downstream TrackLocalStaticRTP (with RTCP/PLI
// plumbing) so a *participant* relays others' media — a tiny, tree-shaped SFU.
package media
