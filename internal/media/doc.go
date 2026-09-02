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
// A pushed tree is also AUTHORIZED before it is applied: overlay.Fence answers "may
// I act on this" (the sender is the coordinator the arbiter named, at exactly the
// epoch this peer was told is current, with a strictly newer revision), which is a
// different question from Topology.Supersedes' "is this newer". Only the arbiter's
// announcement — carried up to the host by OnCoordinator and back down through
// AdoptCoordinator, so this package never learns the arbiter's types — may raise a
// peer's epoch.
//
// A source is identified by the peer that ORIGINATED it, never by the neighbour
// that handed it over. One edge therefore carries as many forwarded tracks as there
// are participants behind it — which is every edge more than one hop from a sender —
// and each relay forwards, toward each neighbour, exactly what is not already on
// that neighbour's side of the cut.
//
// One edge deliberately sits outside the tree: the child that promoted this peer as
// its backup parent. It is accepted on the coordinator's own backup assignment, and
// it is held — exempt from the diff's ordinary "close what the tree does not name" —
// until the coordinator rules on the promotion, one way or the other.
//
// Being outside the tree, that edge cannot derive its offerer from Topology.Offers,
// so the roles are assigned by the promotion path instead — and the intuitive
// assignment is the wrong one. The promoter is the end that knows the failure
// happened, but only an OFFER can add forwarded m-lines, so a backup parent that
// answers can never publish the tracks it was promoted to carry. The child therefore
// ASKS, with a signaling.TypeBackupPromote frame, and answers; the parent OFFERS,
// after checking its own topology names it that child's backup. Exactly one end can
// promote and exactly one end authorizes, so on any tree overlay.Validate would
// accept — one where a backup is never already a neighbour — no second offer exists
// to collide with. On a tree it would not accept, neither end's assigned role takes
// (startPeerOpt will not re-role a session it already holds), both say so at WARN,
// and the promoter does not send the frame at all: asking for an offer while still
// holding the offerer role itself is the one glare pion cannot recover from.
//
// Because the Router owns the peer's one signaling stream, it is also the seam the
// control plane reaches its host through: OnCoordinator carries the arbiter's
// announcement (which tells a peer it has the coordinator job) and OnControlFrame
// carries the membership, metrics, heartbeat and reparent frames it needs to do it.
// Both run on the Run goroutine, so a host queues and returns.
//
// Ratifying a re-parent needs evidence that the new edge carries media, but a peer
// that subscribes to nothing has no such evidence to offer and never will — for it,
// connecting IS the whole observable outcome. And a move that cannot be VERIFIED is
// not a move that failed: the leg stays up and the report says "unverified", because
// destroying a live path on an unobservable condition guarantees the outage it was
// trying to avoid.
//
// The Router also reports its REALIZED overlay position (Realized) — the parent it
// is actually attached to and the children it is actually serving, in topology
// names — which is the ground truth a newly promoted coordinator rebuilds the
// previous tree from, instead of inheriting its predecessor's beliefs.
//
// The Router is also where three of the peer's telemetry signals are measured
// (LinkStats): pairwise RTT, worst-leg uplink loss, and — since Phase 7 — the peer's
// NAT class.
// That last one is a BEHAVIOURAL classification and not NAT-type discovery: the
// nominated ICE candidate pair names the local candidate media leaves through, and a
// relay-typed one means a TURN allocation, so the answer is "this peer's media paths
// are going through a relay" and nothing more. It is the only question conclave asks
// NAT for (may this node be a parent), and it fails safe — a peer that has not
// connected to anybody yet is unclassified and reports the permissive class, the same
// structural blind spot RTTMemory records for first attachment.
//
// Media sources (PlayIVF, SendSynthetic) feed an outbound track; sinks
// (RecordVP8, DrainAndCount) consume a remote track. Codecs are pinned to VP8 in
// the MediaEngine so both ends agree without depending on default ordering.
package media
