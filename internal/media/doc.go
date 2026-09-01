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
// upload across all of them. With no topology it runs a full mesh — every peer
// holds a PeerConnection to every other — which is what makes the O(N) upload cost
// visible.
//
// Given a Topology it instead runs in tree mode: a peer connects only to its
// topology neighbours, and a relay (a peer with children) becomes the novel core —
// a tiny, tree-shaped SFU. The forwarder reads RTP off each source's
// *webrtc.TrackRemote and fans it, without re-encoding, into a per-downstream
// TrackLocalStaticRTP, plumbing keyframe requests (PLI) back upstream to the
// original sender with the SSRC translated. That is what lets a *participant* relay
// others' media, so the sender's upload stays O(1) and the relay bears the fan-out.
//
// From Phase 5 the tree CHANGES underneath all of this. A pushed topology is
// diffed against what is live and only the difference is applied, which brings in
// three things that are easy to get wrong and are documented where they live:
//
//   - Negotiation is serialized (session.go). Tracks are added and removed
//     mid-call now, so at most one offer is outstanding per session; pion's own
//     signaling state is the primary guard, because pion re-fires
//     negotiation-needed itself on the return to stable.
//   - A parent change is an ASYNCHRONOUS state machine (reparent.go), never a
//     blocking sequence. Nothing on the Router's Run goroutine, and nothing in a
//     pion callback, may wait on I/O — those goroutines are what FEED the
//     negotiation a wait would be waiting for.
//   - Make-before-break means two upstreams briefly feed the same downstream leg,
//     so each leg's outgoing RTP is rewritten (rewrite.go) to stay one continuous
//     sequence/timestamp series across the switch. A keyframe does not repair a
//     broken transport ordering; it repairs reference state, one layer up.
//
// Media sources (PlayIVF, SendSynthetic) feed an outbound track; sinks
// (RecordVP8, DrainAndCount) consume a remote track. Codecs are pinned to VP8 in
// the MediaEngine so both ends agree without depending on default ordering.
package media
