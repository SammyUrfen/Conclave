package media

import (
	"context"
	"log/slog"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// ReparentConnectTimeout bounds how long a peer waits for a new parent's session to
// reach Connected. 5s covers ICE gathering + connectivity checks + DTLS on a normal
// path with a STUN round trip; a path that has not connected in 5s is usually not
// going to (a blocked candidate, a dead peer), and holding the old parent open
// longer than that means paying double upload for a leg that will not arrive.
const ReparentConnectTimeout = 5 * time.Second

// ReparentMediaTimeout is how long a peer waits, after its new parent's session
// reaches Connected, for actual media to arrive before declaring the promotion
// failed.
//
// 3s: one keyframe interval plus slack. The relay requests an upstream keyframe the
// moment a child connects, so on a working path the first track arrives well inside
// this. Its whole job is to distinguish "I can reach B" from "B can reach the root",
// which is exactly the distinction that makes ratification safe — ratifying on ICE
// alone would let the coordinator stickily defend an edge carrying nothing.
const ReparentMediaTimeout = 3 * time.Second

// ParentDisconnectGrace is how long a parent edge may sit in
// PeerConnectionStateDisconnected before we treat it as failed and promote the
// backup. pion reaches `disconnected` on a short consent-freshness lapse and
// frequently recovers on its own; `failed` is the definitive state and we act on it
// immediately. This grace only covers the case where the edge sits in
// `disconnected` indefinitely without ever declaring `failed`.
const ParentDisconnectGrace = 2 * time.Second

// internalQueue is the depth of the Router's own event channel. 32 is far more than
// the handful of pion callbacks and timers that can be in flight at once; the depth
// matters less than the NON-BLOCKING send, because blocking a pion dispatch
// goroutine is precisely the failure this channel exists to fix.
const internalQueue = 32

// routerEventKind names the things that happen TO a Router asynchronously — on a
// pion callback goroutine or a timer — and that must be handled on the Run
// goroutine, which is the single owner of the topology and the peer map.
type routerEventKind int

const (
	// evPeerState is a PeerConnection state transition, posted from pion's callback.
	evPeerState routerEventKind = iota
	// evPeerTrack says a remote media track arrived from a peer. It is the evidence
	// that distinguishes "connected to the backup" from "the backup is attached to
	// the root", and is therefore what ratifies a promotion.
	evPeerTrack
	// evReparentConnect / evReparentMedia are the two re-parent deadlines.
	evReparentConnect
	evReparentMedia
	// evParentGrace fires when a parent edge has sat in `disconnected` for
	// ParentDisconnectGrace without recovering or declaring itself failed.
	evParentGrace
	// evNegotiationFailed says a session has spent its whole retry ladder without
	// completing an offer/answer exchange. Its PeerConnection is stuck in
	// have-local-offer and can never offer again, so the edge is mute until it is
	// re-created.
	evNegotiationFailed
)

// routerEvent is one such happening. It carries names, not pointers, so handling it
// on the Run goroutine re-resolves everything against the topology in force THEN —
// the world may have moved on between posting and handling.
type routerEvent struct {
	kind     routerEventKind
	peerName string
	state    webrtc.PeerConnectionState
	// gen fences a timer against the re-parent it was armed for: a superseded
	// re-parent's deadline must not abort its successor.
	gen uint64
}

// reparentPhase is the state of the at-most-one re-parent in flight.
type reparentPhase int

const (
	rpOpening       reparentPhase = iota // new parent's session opened, waiting for Connected
	rpAwaitingMedia                      // Connected; waiting for a track to prove the path
)

// reparent is the per-Router state machine for a parent change in flight.
//
// It exists because NOTHING in this path may block. applyTopology and every pion
// callback run on goroutines that also FEED the negotiation they would be waiting
// for: waiting for `Connected` on the Run goroutine starves the offer/answer frames
// deliver() must route, so the wait times out every single time. Every wait is
// therefore a timer plus a state transition, resumed on the Run goroutine.
//
// At most one is live. A topology arriving mid-re-parent supersedes it: the new
// target is adopted and the old pending session is closed.
type reparent struct {
	gen       uint64
	phase     reparentPhase
	oldParent string
	newParent string
	// viaBackup marks a SELF-promotion (§7.5) rather than a coordinator-directed
	// move. Only a self-promotion is reported to the control plane, because only a
	// self-promotion is news to it.
	viaBackup bool
	// triedBackup records that the backup has already been attempted for this
	// failure, so a second timeout abandons instead of looping.
	triedBackup bool
	// cancel retires this re-parent's deadline goroutines. Superseding or finishing
	// a re-parent must not leave a timer alive that can abort its successor.
	cancel context.CancelFunc
}

// postEvent hands an event to the Run goroutine. It NEVER blocks: it is called from
// pion callbacks and from timer goroutines, and blocking either is the failure mode
// being fixed. A full queue drops the event and says so — the coordinator's next
// push re-drives the same decision, so a dropped event costs latency, not
// correctness.
func (r *Router) postEvent(ev routerEvent) {
	select {
	case r.internal <- ev:
	default:
		r.log.Warn("internal event queue full; dropping",
			slog.Int("kind", int(ev.kind)), slog.String("peer_name", ev.peerName))
	}
}

// armDeadline posts ev after d unless ctx is cancelled first. The goroutine is
// joined by the Router's WaitGroup, so shutdown waits for it; ctx is the re-parent's
// own, so superseding it retires the timer.
func (r *Router) armDeadline(ctx context.Context, d time.Duration, ev routerEvent) {
	timer := r.clk.NewTimer(d)
	r.spawnTracked(func() {
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C():
			r.postEvent(ev)
		}
	})
}

// handleInternal dispatches one asynchronous event. Run's select owns this, so
// everything below runs on the single goroutine that mutates r.topo and r.peers —
// which is why none of it takes a lock over the state machine itself.
func (r *Router) handleInternal(ctx context.Context, ev routerEvent) {
	switch ev.kind {
	case evPeerState:
		r.onPeerState(ctx, ev.peerName, ev.state)
	case evPeerTrack:
		r.onPeerTrack(ev.peerName)
	case evReparentConnect:
		if r.rp != nil && r.rp.gen == ev.gen && r.rp.phase == rpOpening {
			r.reparentFailed(ctx, "new parent never connected")
		}
	case evReparentMedia:
		if r.rp != nil && r.rp.gen == ev.gen && r.rp.phase == rpAwaitingMedia {
			r.reparentFailed(ctx, "new parent connected but carried no media")
		}
	case evNegotiationFailed:
		r.onNegotiationExhausted(ctx, ev.peerName)
	case evParentGrace:
		// Still our parent, still not connected: the edge is not coming back.
		if r.isParentEdge(ev.peerName) && r.connectionStateByName(ev.peerName) != webrtc.PeerConnectionStateConnected {
			r.onParentLost(ctx, ev.peerName)
		}
	}
}

// onPeerState handles a PeerConnection transition on the Run goroutine.
func (r *Router) onPeerState(ctx context.Context, name string, st webrtc.PeerConnectionState) {
	switch st {
	case webrtc.PeerConnectionStateConnected:
		// A relay asks upstream for a keyframe the moment a child connects, so the
		// joiner gets an I-frame instead of black video. This used to run inline on
		// the pion callback; it runs here now because no pion callback may do work
		// that can block.
		if r.fwd != nil && r.isRelayNow() {
			r.fwd.keyframeForChild(name)
		}
		if r.rp != nil && r.rp.phase == rpOpening && r.rp.newParent == name {
			// §5.6 step 4 wants an ARRIVING track before ratifying, and for a peer
			// that subscribes to nothing that is not an unmet condition but an
			// UNDEFINED one: no track will ever arrive over this edge however healthy
			// it is. Waiting can then only ever time out, and the timeout used to
			// tear down the very leg the peer was successfully sending on.
			//
			// Only the peer can know this — the coordinator has no field that says
			// "I receive nothing" — and it reads it off the tree: the set of origins
			// that reach us through this neighbour. Empty means there is nothing to
			// wait for, and the evidence that the move worked is that our own
			// outbound sender is live on the new leg.
			//
			// This does NOT weaken the rule for anyone who does expect media. A relay
			// orphaned by a correlated failure has healthy PeerConnections and nothing
			// to forward, which is exactly the case step 4 exists to reject — it
			// expects media, so it still waits for it.
			if !r.expectsMediaFrom(name) {
				r.log.Info("re-parent needs no media evidence; this peer receives nothing over the edge",
					slog.String("new_parent", name))
				r.commitReparent(true, "no media expected over this edge")
				return
			}
			r.rp.phase = rpAwaitingMedia
			r.hookReparent("connected", r.rp.oldParent)
			r.armDeadline(r.rpCtx, ReparentMediaTimeout,
				routerEvent{kind: evReparentMedia, gen: r.rp.gen})
			// Ask for a keyframe on everything we expect from this parent. It is
			// best-effort: until the new upstream's track arrives we do not know its
			// SSRC, so this only fires for sources already learned. The parent's own
			// keyframeForChild covers the rest from the other side.
			if r.fwd != nil {
				r.fwd.keyframeForChild(r.selfName)
				r.fwd.requestUpstreamKeyframe(name)
			}
		}
	case webrtc.PeerConnectionStateFailed:
		if r.isParentEdge(name) {
			r.onParentLost(ctx, name)
		}
	case webrtc.PeerConnectionStateDisconnected:
		if r.isParentEdge(name) {
			// `disconnected` is frequently a consent-freshness blip that heals
			// itself; only a sustained one is a failure.
			r.armDeadline(ctx, ParentDisconnectGrace, routerEvent{kind: evParentGrace, peerName: name})
		}
	}
}

// onPeerTrack handles "media arrived from name". For a re-parent in flight from
// this peer, it is the ratifying evidence: reaching Connected proves we can reach
// the new parent, an arrived track proves the new parent can reach the root.
func (r *Router) onPeerTrack(name string) {
	if r.rp == nil || r.rp.phase != rpAwaitingMedia || r.rp.newParent != name {
		return
	}
	r.commitReparent(true, "media arrived over the new edge")
}

// isParentEdge reports whether name is the peer we currently take media from. All
// three answers matter and they can disagree: the tree's parent, the parent we are
// really attached to (an abandoned re-parent leaves those different), and the old
// parent of a re-parent still in flight — which is still our live upstream until
// the new one carries media.
func (r *Router) isParentEdge(name string) bool {
	if name == "" {
		return false
	}
	if r.rp != nil && r.rp.oldParent == name {
		return true
	}
	return name == r.parentInUse || name == r.parentName()
}

// parentName is our upstream under the topology in force, or "" if we are the root
// (or not yet attached).
func (r *Router) parentName() string {
	topo := r.currentTopo()
	if topo == nil {
		return ""
	}
	return topo.ParentOf(r.selfName)
}

// startReparent opens the session to newParent and returns IMMEDIATELY, leaving the
// old parent fully intact and still receiving. Break-before-make would guarantee a
// gap of ICE + DTLS + first keyframe (1–3s); make-before-break costs one transient
// extra PeerConnection and one duplicated inbound stream for at most
// ReparentConnectTimeout + ReparentMediaTimeout.
func (r *Router) startReparent(ctx context.Context, oldParent, newParent string, viaBackup bool) {
	if newParent == "" {
		r.reportReparent(oldParent, "", false, "no parent to move to")
		return
	}
	// RESOLVE BEFORE DISTURBING ANYTHING. Superseding first and resolving second
	// means an unresolvable name tears down the move already in flight and then
	// returns, leaving r.rp non-nil, cancelled, naming a closed session, with no
	// deadline armed and pendingParent stale. onParentLost then sees a move
	// "already under way" and refuses to promote a backup — so the next parent death
	// is unrecoverable until some later push happens to name a different parent.
	// Failing to start must cost nothing.
	peerID := r.idForName(newParent)
	if peerID == "" {
		// The new parent is not in the roster yet. maybeStartPeer picks it up when
		// its peer-joined arrives, and the coordinator will push again; abandoning
		// here keeps whatever we have, which is the one outcome that is never wrong.
		r.log.Warn("re-parent target not in the roster yet", slog.String("new_parent", newParent))
		r.reportReparent(oldParent, "", false, "new parent not present")
		return
	}

	if r.rp != nil {
		// Superseded. Close the pending session unless the new target is the same
		// one, and retire its deadlines before they can abort the successor.
		if r.rp.newParent != newParent {
			r.stopPeerByName(r.rp.newParent)
		}
		r.rp.cancel()
		if oldParent == "" {
			oldParent = r.rp.oldParent
		}
	}

	r.rpGen++
	rpCtx, cancel := context.WithCancel(ctx)
	r.rpCtx = rpCtx
	r.rp = &reparent{
		gen: r.rpGen, phase: rpOpening,
		oldParent: oldParent, newParent: newParent, viaBackup: viaBackup,
	}
	r.rp.cancel = cancel
	// The new parent is now OPEN but not realized: it must show up in a heartbeat as
	// neither our parent nor our child until media arrives over it.
	r.setParent(oldParent, newParent)
	r.log.Info("re-parenting",
		slog.String("old_parent", oldParent), slog.String("new_parent", newParent),
		slog.Bool("via_backup", viaBackup))
	r.hookReparent("opening", r.rp.oldParent)

	// A backup edge is not in the tree, so Topology.Offers says nothing useful about
	// it and the far end has no reason to expect a connection at all. The roles are
	// therefore assigned by this path rather than derived: WE ANSWER, and we ask the
	// backup parent to offer.
	//
	// Making the promoter the offerer is the obvious reading — it is the end that
	// knows the failure happened — and it is wrong, because only an OFFER can add
	// forwarded m-lines (§5.12). A backup parent that answers cannot publish the
	// tracks it was promoted to carry, so ReparentMediaTimeout expires on every
	// backup edge whose peer expects media: the failure §9.5 measured live.
	//
	// This is NOT the "please offer me" handshake §5.12 rejected for the invert
	// bucket. That one was rejected for creating a new glare surface, because BOTH
	// ends of an inverting tree edge could decide to ask. Here exactly one end can:
	// only the child that lost its parent promotes, only it sends this frame, and the
	// parent's response is gated on its own topology naming it that child's backup.
	// One asker, one authorizer, no second offer to collide with.
	opts := peerOpts{}
	if viaBackup {
		no := false
		opts.offerer = &no
	}
	// A role override is silently dropped when a session to this peer is already
	// open (startPeerOpt is idempotent per peer), and on a backup edge that means we
	// keep whatever role the TREE gave the edge while asking the far end to offer.
	// Under a Validate-clean tree it cannot happen — a backup is never already a
	// neighbour — but applyTopology does not call Validate (§8.6). The move is not
	// abandoned here: ReparentConnectTimeout and ReparentMediaTimeout still own the
	// outcome, exactly as they do for a refused or lost promote. It is logged because
	// a promotion that kept the wrong role fails in a completely different way from
	// one that took, and nothing else in the log tells the two apart.
	if !r.startPeerOpt(ctx, peerID, opts) && viaBackup {
		r.log.Warn("promoting onto a peer we already hold a session to: no session was created, "+
			"so the backup edge's answerer role was not applied",
			slog.String("new_parent", newParent), slog.Bool("wanted_offerer", false))
	}
	if viaBackup {
		// AFTER the session exists, so the parent's offer has an inbox to land in —
		// deliver() would otherwise drop it as a frame for an unknown peer, and the
		// promotion would fail on the connect timeout for no reason at all. Both run
		// on the Run goroutine, so this ordering is a guarantee, not a race we win.
		//
		// A send failure is not fatal and not retried here: the promotion is already
		// bounded by ReparentConnectTimeout, which is the same ladder a frame lost on
		// the wire lands in.
		//
		// The nil check is for the state-machine tests, which drive this Router
		// entirely by events and hold no signaling client. It cannot mask a
		// production bug: Run reads r.client.Incoming() on its first line, so a
		// clientless Router never gets as far as losing a parent.
		if r.client == nil {
			r.log.Debug("no signaling client; not asking the backup parent to offer",
				slog.String("new_parent", newParent))
		} else if err := r.client.Send(signaling.Message{
			Type: signaling.TypeBackupPromote, To: peerID,
		}); err != nil {
			r.log.Warn("could not ask the backup parent to offer",
				slog.String("new_parent", newParent), slog.Any("error", err))
		}
	}
	r.armDeadline(rpCtx, ReparentConnectTimeout, routerEvent{kind: evReparentConnect, gen: r.rp.gen})

	// A former child can become our parent after a re-root, in which case the
	// session already exists and no state transition is coming. Synthesize one.
	if r.connectionStateByName(newParent) == webrtc.PeerConnectionStateConnected {
		r.postEvent(routerEvent{kind: evPeerState, peerName: newParent,
			state: webrtc.PeerConnectionStateConnected})
	}
}

// commitReparent is the point of no return: the new upstream carries media, so the
// forwarder is spliced onto it and the old parent is torn down LAST, so nothing is
// lost if any earlier step failed.
//
// Downstream sessions are NOT touched. Their forwarded tracks are the same objects
// they always were; only the upstream feeding them changes, and rtpRewriter keeps
// each one's RTP series continuous across the splice. That is what keeps a
// re-parent local to one hop instead of cascading down the whole subtree.
func (r *Router) commitReparent(verified bool, reason string) {
	rp := r.rp
	r.hookReparent("committing", rp.oldParent)

	if r.fwd != nil {
		if next := r.sessionByName(rp.newParent); next != nil {
			// Re-point every source that was arriving through the old parent. Source
			// keys are origins, so they survive the move unchanged and the children
			// keep the very same forwarded tracks.
			r.fwd.rebindUpstreamSession(r.sessionByName(rp.oldParent), next)
		}
	}
	if id := r.idForName(rp.oldParent); id != "" {
		r.stopPeer(id)
	}
	rp.cancel()
	r.rp = nil
	r.setParent(rp.newParent, "")
	r.log.Info("re-parent complete",
		slog.String("old_parent", rp.oldParent), slog.String("new_parent", rp.newParent))
	r.hookReparent("committed", rp.oldParent)
	switch {
	case !verified:
		// Attached but unverified. The report says so — the coordinator corroborates
		// from the parent's heartbeat rather than abandoning us — while the leg
		// itself stays up, because "I cannot confirm this works" is not "this does
		// not work".
		r.reportReparent(rp.oldParent, rp.newParent, false, reason)
	case rp.viaBackup:
		r.reportReparent(rp.oldParent, rp.newParent, true, reason)
	}
}

// expectsMediaFrom reports whether anything is supposed to reach us THROUGH this
// neighbour under the tree in force. It is the peer-local answer to "do I subscribe
// to anything here", and it is empty for a pure source: every other node on that
// side of the cut is a relay, and a relay publishes no media of its own.
func (r *Router) expectsMediaFrom(name string) bool {
	topo := r.currentTopo()
	if topo == nil {
		return true // unknown tree: fall back to demanding evidence
	}
	return len(sourcesFrom(topo, r.selfName, name)) > 0
}

// reparentFailed is the abandon path. It tries the precomputed backup ONCE, and
// otherwise KEEPS the old parent if it is still alive: the one outcome that is
// never acceptable is ending up parentless because we obeyed an instruction we
// could not carry out.
func (r *Router) reparentFailed(ctx context.Context, reason string) {
	rp := r.rp
	r.log.Warn("re-parent failed",
		slog.String("new_parent", rp.newParent), slog.String("reason", reason))

	// The two timeouts are NOT the same failure, and treating them alike is what
	// turned an unverifiable move into a real outage.
	//
	// Reaching awaiting-media means the new parent CONNECTED and only the evidence
	// is missing. That leg is a live transport path — very possibly the one carrying
	// this peer's own outbound media — and destroying it converts "I cannot confirm
	// this works" into "this definitely does not work", guaranteeing the gap it was
	// trying to avoid. So the move is committed and reported honestly as unverified;
	// the coordinator corroborates it from the parent's heartbeat, and repairs it if
	// nobody can.
	//
	// Still in `opening` means it never connected. There is no working leg to
	// protect, and leaving the half-open session behind would strand a peer that is
	// in nobody's topology — so that one is torn down as before.
	if rp.phase == rpAwaitingMedia {
		r.hookReparent("unverified", rp.oldParent)
		r.commitReparent(false, reason)
		return
	}

	r.hookReparent("abandoning", rp.oldParent)
	r.stopPeerByName(rp.newParent)

	backup := r.backupParent()
	if !r.cfg.DisableBackup && !rp.triedBackup && backup != "" && backup != rp.newParent && backup != rp.oldParent {
		rp.cancel()
		r.rp = nil
		r.clearLocalBackup()
		r.startReparent(ctx, rp.oldParent, backup, true)
		if r.rp != nil {
			r.rp.triedBackup = true
		}
		return
	}

	rp.cancel()
	r.rp = nil
	// Reality is still the OLD parent, whatever the pushed tree says. Recording it
	// is what lets the next push see the move as still outstanding and retry it,
	// rather than reading the tree back as if it had been realised.
	r.setParent(rp.oldParent, "")
	r.hookReparent("abandoned", rp.oldParent)
	r.reportReparent(rp.oldParent, "", false, reason)
}

// onParentLost is the local failover trigger: our upstream died, so we promote the
// precomputed backup OURSELVES, without asking the coordinator. The coordinator's
// job afterwards is to ratify the choice rather than fight it.
func (r *Router) onParentLost(ctx context.Context, parent string) {
	if r.rp != nil && r.rp.oldParent == parent {
		return // already moving off this parent
	}
	if r.cfg.DisableBackup {
		r.reportReparent(parent, "", false, "backup promotion disabled")
		return
	}
	backup := r.backupParent()
	if backup == "" || backup == parent {
		r.log.Warn("parent lost with no usable backup", slog.String("parent", parent))
		r.reportReparent(parent, "", false, "no backup parent")
		return
	}
	// §5.6 step 3: clear the local backup on promotion. The pushed Backups still
	// name the node we are moving TO — it was computed before the failure — so a
	// naive second attempt would re-target the parent we already have and burn a
	// whole ReparentConnectTimeout doing it. A backup is regained on the
	// coordinator's next push, which this promotion's ratification triggers.
	r.clearLocalBackup()
	r.startReparent(ctx, parent, backup, true)
}

// backupParent is the precomputed secondary parent for this peer under the topology
// in force, or "" if it has none — a normal answer, not an error: the root's own
// children have no legal backup by construction.
func (r *Router) backupParent() string {
	topo := r.currentTopo()
	if topo == nil {
		return ""
	}
	return topo.BackupOf(r.selfName)
}

// clearLocalBackup drops our own entry from the topology in force. It replaces the
// Backups slice rather than mutating it in place because Stats and the remote-track
// sinks read the topology from other goroutines.
func (r *Router) clearLocalBackup() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.topo == nil {
		return
	}
	kept := make([]overlay.Backup, 0, len(r.topo.Backups))
	for _, b := range r.topo.Backups {
		if b.Node != r.selfName {
			kept = append(kept, b)
		}
	}
	next := *r.topo
	next.Backups = kept
	r.topo = &next
}

// reportReparent ships the outcome to the control plane through the injected
// callback, so media never learns the wire format. Nil callback ⇒ no report, which
// is the right default for the static-tree and mesh modes that have no coordinator.
func (r *Router) reportReparent(from, to string, ok bool, reason string) {
	if r.cfg.OnReparented == nil {
		return
	}
	var epoch, rev uint64
	if topo := r.currentTopo(); topo != nil {
		epoch, rev = topo.Epoch, topo.Rev
	}
	r.cfg.OnReparented(metrics.Reparented{
		Name: r.selfName, From: from, To: to, OK: ok,
		Epoch: epoch, Rev: rev, Reason: reason,
	})
}

// hookReparent notifies the test observer, if any, of a state transition and
// whether the old parent's session is still open at that moment — the one fact that
// distinguishes make-before-break from break-before-make and that a poller cannot
// reliably catch. oldParent is passed explicitly so the terminal transitions still
// report after r.rp has been cleared.
func (r *Router) hookReparent(phase, oldParent string) {
	if r.reparentHook == nil {
		return
	}
	r.reparentHook(phase, r.hasSession(oldParent))
}

// onNegotiationExhausted re-creates an edge whose session gave up negotiating.
//
// Without this the failure is permanent AND silent: the pc sits in have-local-offer,
// both serializer guards then refuse every future offer, and the diff has no reason
// to touch the session — the neighbour is still live, still wanted, still the same
// role — so no later topology push heals it either. Re-creating is the same cure the
// recreate bucket applies for the other two ways a session can end up built wrong.
//
// A re-parent's pending target is deliberately left alone: its own
// ReparentConnectTimeout already owns that outcome, and pulling the session out from
// under the state machine would race it.
func (r *Router) onNegotiationExhausted(ctx context.Context, name string) {
	if r.rp != nil && r.rp.newParent == name {
		r.log.Debug("negotiation exhausted on a pending new parent; the re-parent deadline owns it",
			slog.String("peer_name", name))
		return
	}
	topo := r.currentTopo()
	if topo == nil || !neighborOf(topo, r.selfName, name) {
		return // no longer ours to keep alive
	}
	id := r.idForName(name)
	if id == "" {
		return
	}
	r.log.Warn("re-creating a session that exhausted its negotiation ladder",
		slog.String("peer_name", name))
	r.stopPeer(id)
	r.startPeer(ctx, id)
}
