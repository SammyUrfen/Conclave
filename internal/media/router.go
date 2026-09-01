package media

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// RouterConfig is the media policy for a peer process: what to send and where to
// put what it receives. Kept small and declarative on purpose — the Router owns
// the mechanism, this owns the choices.
type RouterConfig struct {
	ICEServers []webrtc.ICEServer
	SendMedia  bool   // add an outbound video track toward each peer
	MediaPath  string // IVF file to send; empty ⇒ synthetic frames
	RecordPath string // write the first received track here; empty ⇒ just count

	// Topology, when non-nil, switches the Router from full mesh to tree mode: a
	// peer opens a session only to its topology neighbours (not to every other
	// peer), and a relay forwards media among them. SelfName is this peer's label
	// in that topology (how it finds itself among the edges). Nil ⇒ mesh (Phase 2),
	// unless Managed is set.
	Topology *overlay.Topology
	SelfName string

	// Managed puts the Router in tree mode with NO static topology: it waits for the
	// coordinator to push one over signaling (a TypeTopology frame) and realises it
	// at run time (Phase 4). A managed peer connects to nobody until its first
	// topology arrives, then behaves exactly as a static-tree peer would. Ignored
	// when Topology is non-nil (an explicit tree wins over a pushed one).
	Managed bool

	// Backup enables honouring the coordinator's precomputed backup parents on a
	// primary-parent failure, without asking anyone. NOTE the zero value is false,
	// so a managed peer that wants the fast failover path must set it explicitly —
	// see the report on §7.5, which specifies "default true" for a plain bool.
	Backup bool

	// Clock is the time source for every re-parent deadline and the relay's PLI
	// throttle. Injected so a test drives them instead of sleeping. Nil ⇒
	// clock.System().
	Clock clock.Clock

	// OnReparented is called after this peer promotes its own backup parent, with
	// the outcome the control plane needs to ratify (or urgently repair) the
	// decision. Declared as a callback so media never learns the wire format — the
	// same seam discipline as Transport. Nil ⇒ no report.
	OnReparented func(metrics.Reparented)
}

// Router owns a signaling.Client and turns its single inbound frame stream into
// one Session per remote peer. It handles the lifecycle frames (joined /
// peer-joined / peer-left) itself and routes offer/answer/candidate frames to
// the matching Session's inbox by sender id.
//
// For Phase 1 this manages a single 2-peer call; the same structure holds for
// the Phase 2 mesh, where several peers are live at once.
type Router struct {
	log    *slog.Logger
	client *signaling.Client
	cfg    RouterConfig
	meter  *uploadMeter
	// fwd is the relay forwarder; non-nil only when this peer is a relay in tree
	// mode. It owns the RTP fan-out and the upstream-PLI plumbing.
	fwd *forwarder

	clk clock.Clock

	// internal is how pion callbacks and clock timers reach the Run goroutine.
	// They POST and return immediately; all handling happens on Run, which is also
	// the only goroutine that mutates topo, peers and the re-parent state machine.
	// That keeps the existing "one owner" discipline intact instead of adding locks
	// — and, critically, means no pion dispatch goroutine ever blocks on us.
	internal chan routerEvent

	// rp is the at-most-one re-parent in flight; rpCtx and rpGen belong to it. All
	// three are touched ONLY from the Run goroutine, so they need no lock.
	rp    *reparent
	rpCtx context.Context
	rpGen uint64
	// parentInUse is the parent we are ACTUALLY attached to, which is not always
	// the one the pushed tree names: a re-parent that was abandoned keeps the old
	// one. The diff compares against reality, not against the last tree, so an
	// unrealised instruction is retried by the next push instead of being read back
	// as done.
	parentInUse string

	// staleRejected counts pushed topologies dropped by the ordering fence. A
	// nonzero count is visible proof the fence works, not an error condition.
	staleRejected atomic.Uint64

	// reparentHook, when non-nil, observes re-parent state transitions. It exists
	// for tests: the sequence is inherently asynchronous, and whether the old parent
	// was still open at the moment the new one connected — the whole difference
	// between make-before-break and break-before-make — cannot be caught reliably by
	// a poller. Nil in production.
	reparentHook func(phase string, oldParentOpen bool)

	// wg joins every goroutine the Router itself spawns — the upload-meter sampler,
	// one media pump per outbound peer, and each forwarder RTCP drain — so Run does
	// not return until they have all stopped. Paired with per-link context
	// cancellation, this is the Phase 2 lesson in explicit goroutine lifecycle:
	// cancel to signal, WaitGroup to join, no leaks.
	wg sync.WaitGroup

	mu       sync.Mutex
	selfID   string
	selfName string
	peers    map[string]*peerLink
	// topo is the CURRENT topology driving tree decisions. In static tree mode it is
	// set once at construction; in managed mode it starts nil and is replaced each
	// time the coordinator pushes a new one (applyTopology). Guarded by mu because
	// applyTopology runs on the Run goroutine while Stats/remote-track sinks read it
	// from others.
	topo *overlay.Topology
	// name↔id maps translate between the stable topology names and the runtime ids
	// the server assigns. Filled from the joined roster + peer-joined/peer-left, so
	// every tree decision is made in names and resolved to an id here.
	nameByID map[string]string
	idByName map[string]string
}

// peerLink is the Router's per-peer bookkeeping: the Session, the inbox we feed
// its routed frames into, and the cancel that tears its goroutines down.
type peerLink struct {
	session  *Session
	inbox    chan signaling.Message
	cancel   context.CancelFunc
	received bool // has a remote media track arrived from this peer? (guarded by Router.mu)
}

// NewRouter constructs a Router over an already-dialed signaling client.
func NewRouter(log *slog.Logger, client *signaling.Client, cfg RouterConfig) *Router {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.System()
	}
	r := &Router{
		log:      log.With(slog.String("component", "media-router")),
		client:   client,
		cfg:      cfg,
		clk:      clk,
		meter:    &uploadMeter{},
		internal: make(chan routerEvent, internalQueue),
		selfName: cfg.SelfName,
		topo:     cfg.Topology,
		peers:    make(map[string]*peerLink),
		nameByID: make(map[string]string),
		idByName: make(map[string]string),
	}
	// Create the forwarder now if this peer is (static tree) or may become (managed)
	// a relay. Building it up front — rather than lazily when a pushed topology first
	// promotes a managed leaf — keeps r.fwd write-once and so free of any read/write
	// race with the pion goroutines that consult it; an unused forwarder on a leaf is
	// inert (empty maps, no goroutines). Whether it is ACTUALLY used for a given
	// track is gated on the live topology via isRelayNow(), not on fwd != nil.
	if cfg.Managed || (cfg.Topology != nil && cfg.Topology.IsRelay(cfg.SelfName)) {
		r.fwd = newForwarder(r.log, r.meter, r.spawnTracked, clk)
	}
	return r
}

// treeMode reports whether the Router runs as a tree (static or coordinator-managed)
// rather than a full mesh.
func (r *Router) treeMode() bool { return r.cfg.Topology != nil || r.cfg.Managed }

// currentTopo returns the topology in force right now (nil in mesh mode, or in
// managed mode before the first push). Safe to call concurrently with applyTopology.
func (r *Router) currentTopo() *overlay.Topology {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.topo
}

// isRelayNow reports whether this peer forwards media under the CURRENT topology.
// It — not "do we hold a forwarder" — is the authority on whether an arriving track
// should be fanned out, because a managed peer always holds a forwarder but is only
// sometimes a relay.
func (r *Router) isRelayNow() bool {
	t := r.currentTopo()
	return t != nil && t.IsRelay(r.selfName)
}

// spawnTracked runs fn as a goroutine joined by the Router's WaitGroup, so shutdown
// waits for it to return. The forwarder uses it for its per-child RTCP drains.
func (r *Router) spawnTracked(fn func()) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		fn()
	}()
}

// Run consumes the signaling stream until ctx is cancelled or the connection
// ends, then tears down every live session. It blocks, so callers run it as the
// peer's main loop.
func (r *Router) Run(ctx context.Context) error {
	// runCtx is the lifetime of everything this Router spawns. Deriving our own
	// cancel (rather than leaning on the parent ctx) matters because Run also
	// returns when the signaling stream closes on its own — a path where ctx is
	// NOT cancelled. cancel() then still unblocks the meter and every media pump so
	// the WaitGroup join below can complete instead of hanging forever.
	//
	// Deferred teardown, in the order it must happen (defers run LIFO, so they are
	// registered in reverse): first closeAll() cancels each peer link and closes
	// its session; then cancel() stops the meter and any pump not tied to a link;
	// finally wg.Wait() blocks until all of them have actually exited.
	runCtx, cancel := context.WithCancel(ctx)
	defer r.wg.Wait()
	defer cancel()
	defer r.closeAll()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.meter.run(runCtx, r.log, uploadSampleInterval)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-r.internal:
			r.handleInternal(runCtx, ev)
		case msg, ok := <-r.client.Incoming():
			if !ok {
				r.log.Info("signaling stream closed")
				return nil
			}
			r.handle(runCtx, msg)
		}
	}
}

// Stats is a point-in-time snapshot of the mesh this peer maintains: the
// PeerConnection state of every remote peer, and how many outbound media pumps are
// currently running (≈ N-1 when sending in a full mesh). It is a read-only view for
// tests and operators today, and the seed of the Phase 4 metrics plane.
type Stats struct {
	Peers       map[string]webrtc.PeerConnectionState // remote peer id → connection state
	Received    map[string]bool                       // remote peer id → have we received a track from them
	UploadPeers int                                   // live outbound media pumps
}

// Stats snapshots the current mesh state. Safe to call concurrently with Run.
func (r *Router) Stats() Stats {
	r.mu.Lock()
	peers := make(map[string]webrtc.PeerConnectionState, len(r.peers))
	received := make(map[string]bool, len(r.peers))
	for id, link := range r.peers {
		if link.session != nil {
			peers[id] = link.session.ConnectionState()
		} else {
			peers[id] = webrtc.PeerConnectionStateNew
		}
		received[id] = link.received
	}
	r.mu.Unlock()
	return Stats{Peers: peers, Received: received, UploadPeers: r.meter.livePeers()}
}

// markReceived records that a media track has arrived from peerID. Called from the
// remote-track sink, which runs on a pion goroutine.
func (r *Router) markReceived(peerID string) {
	r.mu.Lock()
	if link := r.peers[peerID]; link != nil {
		link.received = true
	}
	r.mu.Unlock()
}

func (r *Router) handle(ctx context.Context, msg signaling.Message) {
	switch msg.Type {
	case signaling.TypeJoined:
		r.mu.Lock()
		r.selfID = msg.To
		if r.selfName != "" {
			r.nameByID[msg.To] = r.selfName
			r.idByName[r.selfName] = msg.To
		}
		r.mu.Unlock()
		r.log.Info("joined room",
			slog.String("self_id", msg.To), slog.String("self_name", r.selfName), slog.Any("peers", msg.Peers))
		// Existing members are already here; learn their names, then start sessions.
		for _, p := range msg.Peers {
			r.learnPeer(p.ID, p.Name)
			r.maybeStartPeer(ctx, p.ID)
		}
	case signaling.TypePeerJoined:
		r.learnPeer(msg.From, msg.Name)
		r.maybeStartPeer(ctx, msg.From)
	case signaling.TypePeerLeft:
		r.stopPeer(msg.From)
		r.forgetPeer(msg.From)
	case signaling.TypeOffer, signaling.TypeAnswer, signaling.TypeCandidate:
		r.deliver(ctx, msg)
	case signaling.TypeTopology:
		r.applyTopology(ctx, msg.From, msg.Payload)
	case signaling.TypeError:
		r.log.Warn("signaling error frame", slog.String("error", msg.Error))
	}
}

// applyTopology realises a coordinator-pushed topology. It DIFFS the incoming tree
// against the one currently in force and applies the difference: open sessions to
// new neighbours, close sessions to dropped ones, re-parent, add and remove
// forwarding legs, and request keyframes where a source changed.
//
// It returns IMMEDIATELY. A re-parent is handed to an asynchronous state machine
// rather than performed inline, because this runs on the Run goroutine — the same
// goroutine whose deliver() feeds every session's offer/answer/candidate inbox.
// Blocking here to wait for the new parent to connect would starve the very frames
// that connection depends on.
//
// Ordering within one apply is fixed: fence, then role inversions (they are
// tear-down-and-recreate, so earliest), then adds, then leg adds BEFORE leg removes
// — a child must never lose a source it is about to regain — then removes, then the
// re-parent hand-off. The serializer folds each session's adds and removes into ONE
// renegotiation, which is why the order within a session does not cost an extra
// interruption.
func (r *Router) applyTopology(ctx context.Context, from string, payload []byte) {
	if !r.cfg.Managed {
		r.log.Debug("ignoring pushed topology (not in managed mode)")
		return
	}
	var topo overlay.Topology
	if err := json.Unmarshal(payload, &topo); err != nil {
		r.log.Warn("bad topology payload", slog.Any("error", err))
		return
	}
	// Ordering fence: a duplicate or reordered push is dropped and counted rather
	// than applied, so a late frame cannot undo a newer tree.
	//
	// NOTE this is only the ORDERING half of the §6.5 fence. The AUTHORIZATION half
	// — "from is the coordinator the arbiter named for the epoch I currently believe
	// in" — needs overlay.Fence, which is not yet shipped; see the report.
	if !topo.Supersedes(r.currentTopo()) {
		r.staleRejected.Add(1)
		r.log.Debug("rejected a non-advancing topology",
			slog.String("from", from), slog.Uint64("epoch", topo.Epoch), slog.Uint64("rev", topo.Rev))
		return
	}

	live := r.liveState()
	diff := diffTopology(r.selfName, &topo, live)

	r.mu.Lock()
	r.topo = &topo
	r.mu.Unlock()
	if !diff.reparent {
		r.noteParent(&topo)
	}

	r.log.Info("applying pushed topology",
		slog.String("from", from), slog.Uint64("epoch", topo.Epoch), slog.Uint64("rev", topo.Rev),
		slog.Bool("relay", topo.IsRelay(r.selfName)),
		slog.Any("add", diff.add), slog.Any("remove", diff.remove), slog.Any("invert", diff.invert),
		slog.Bool("reparent", diff.reparent))
	if diff.empty() {
		return
	}

	// (1) Inversions first: the session must be re-created before anything else
	// decides what legs it carries. Tearing down and re-creating is the ONLY cure
	// pion allows — an answerer has no way to add m-lines, so a mutable role would
	// need a "please offer me" control frame, a new round trip, and a brand-new
	// glare surface in a design built entirely on never having glare.
	for _, name := range diff.invert {
		id := r.idForName(name)
		if id == "" {
			continue
		}
		r.log.Info("re-creating an edge whose offerer role inverted", slog.String("peer_name", name))
		r.stopPeer(id)
		r.startPeer(ctx, id)
	}

	// (2) New neighbours.
	for _, name := range diff.add {
		if id := r.idForName(name); id != "" {
			r.startPeer(ctx, id)
		}
	}

	// (3) Legs: adds before removes, so a child never loses a source it is about to
	// regain. The overlap is harmless; a gap is not.
	changed := map[string]bool{}
	for _, l := range diff.addLegs {
		if r.addLegLive(l) {
			changed[l.child] = true
		}
	}
	for _, l := range diff.removeLegs {
		if r.removeLegLive(l) {
			changed[l.child] = true
		}
	}

	// (4) Departures. stopPeer drops the peer in BOTH forwarding roles and hands
	// back the senders that were carrying its media, which we remove from the
	// surviving children so the serializer folds them into one renegotiation each.
	for _, name := range diff.remove {
		if id := r.idForName(name); id != "" {
			for _, child := range r.stopPeer(id) {
				changed[child] = true
			}
		}
	}

	// (5) Every child whose source set moved starts mid-GOP; ask upstream.
	if r.fwd != nil {
		for child := range changed {
			r.fwd.keyframeForChild(child)
		}
	}

	// (6) Finally the re-parent, asynchronously.
	if diff.reparent {
		r.startReparent(ctx, diff.oldParent, diff.newParent, false)
	}
}

// noteParent records the parent this tree attaches us to, unless a re-parent is
// still in flight — in which case the state machine records reality when it
// settles, one way or the other.
func (r *Router) noteParent(topo *overlay.Topology) {
	if r.rp == nil {
		r.parentInUse = topo.ParentOf(r.selfName)
	}
}

// liveState snapshots what the Router currently holds, in topology names, for the
// diff to compare against. Reality — not the previous topology — is the baseline,
// so a push that was partly applied (a neighbour that had not joined yet) converges
// on the next one instead of drifting.
func (r *Router) liveState() liveState {
	st := liveState{roles: map[string]bool{}, parent: r.parentInUse}
	// A re-parent in flight means the tree already says our parent is the new one
	// while reality is still the old one. The diff must see reality.
	if r.rp != nil {
		st.parent = r.rp.oldParent
	}
	r.mu.Lock()
	for id, link := range r.peers {
		name := r.nameByID[id]
		if name == "" || link.session == nil {
			continue
		}
		st.roles[name] = link.session.Offerer()
	}
	r.mu.Unlock()
	if r.fwd != nil {
		st.legs = r.fwd.legs()
	}
	return st
}

// addLegLive creates one (source → child) forwarding leg on a session that is
// already running: a new forwarded track plus its bookkeeping. The serializer emits
// the renegotiating offer; we do not offer by hand.
func (r *Router) addLegLive(l leg) bool {
	if r.fwd == nil {
		return false
	}
	session := r.sessionByName(l.child)
	if session == nil {
		return false
	}
	track, sender, err := session.AddForwardTrack(forwardTrackPrefix+l.src, "conclave")
	if err != nil {
		r.log.Error("add forward track mid-call",
			slog.String("source", l.src), slog.String("child", l.child), slog.Any("error", err))
		return false
	}
	r.fwd.addOutLive(l.src, l.child, track, sender)
	return true
}

// removeLegLive drops one (source → child) leg from a running session.
func (r *Router) removeLegLive(l leg) bool {
	if r.fwd == nil {
		return false
	}
	sender := r.fwd.removeOut(l.src, l.child)
	if sender == nil {
		return false
	}
	if session := r.sessionByName(l.child); session != nil {
		if err := session.RemoveTrack(sender); err != nil {
			r.log.Warn("remove forward track",
				slog.String("source", l.src), slog.String("child", l.child), slog.Any("error", err))
		}
	}
	return true
}

// learnPeer records a runtime id↔name mapping from the roster or a peer-joined
// frame. Names are only present in tree mode; a nameless peer is a no-op.
func (r *Router) learnPeer(id, name string) {
	if name == "" {
		return
	}
	r.mu.Lock()
	r.nameByID[id] = name
	r.idByName[name] = id
	r.mu.Unlock()
}

// forgetPeer drops a departed peer's id↔name mapping.
func (r *Router) forgetPeer(id string) {
	r.mu.Lock()
	if name, ok := r.nameByID[id]; ok {
		delete(r.idByName, name)
		delete(r.nameByID, id)
	}
	r.mu.Unlock()
}

// maybeStartPeer starts a session with peerID, except in tree mode where a peer
// only connects to its topology neighbours — everyone else is reached *through* the
// tree, which is the entire point of the relay (a leaf holds one connection, not
// N−1). In managed mode with no topology pushed yet, it connects to nobody.
func (r *Router) maybeStartPeer(ctx context.Context, peerID string) {
	if r.treeMode() {
		topo := r.currentTopo()
		if topo == nil {
			return
		}
		r.mu.Lock()
		peerName := r.nameByID[peerID]
		r.mu.Unlock()
		if peerName == "" || !neighborOf(topo, r.selfName, peerName) {
			return
		}
	}
	r.startPeer(ctx, peerID)
}

// neighborOf reports whether name is directly linked to self in topo.
func neighborOf(topo *overlay.Topology, self, name string) bool {
	for _, n := range topo.NeighborsOf(self) {
		if n == name {
			return true
		}
	}
	return false
}

// forwardTrackPrefix names a forwarded track after the SOURCE whose media it
// carries, so both ends can talk about a leg without a second naming scheme.
const forwardTrackPrefix = "fwd-"

// peerOpts are the per-call overrides startPeer normally derives from the topology.
type peerOpts struct {
	// offerer overrides Topology.Offers for this edge. It is set on exactly one
	// path: a backup-parent promotion, whose edge is NOT in the tree, so Offers says
	// nothing meaningful about it and the far end has no reason to initiate. The
	// peer that promotes offers; the far end answers.
	offerer *bool
}

// startPeer creates and starts a Session toward peerID (idempotent per peer),
// attaches an outbound track if configured, and wires the remote-track sink.
func (r *Router) startPeer(ctx context.Context, peerID string) {
	r.startPeerOpt(ctx, peerID, peerOpts{})
}

func (r *Router) startPeerOpt(ctx context.Context, peerID string, opts peerOpts) {
	r.mu.Lock()
	if _, exists := r.peers[peerID]; exists {
		r.mu.Unlock()
		return
	}
	selfID := r.selfID
	peerName := r.nameByID[peerID]
	inbox := make(chan signaling.Message, 64)
	linkCtx, cancel := context.WithCancel(ctx)
	r.peers[peerID] = &peerLink{inbox: inbox, cancel: cancel}
	r.mu.Unlock()

	tr := &clientTransport{client: r.client, peerID: peerID, inbox: inbox}

	// Role + hooks depend on mode. Tree mode fixes the offerer from the topology
	// (the relay offers every edge, because only an offer can add its forwarded
	// m-lines), and a relay proactively asks upstream for a keyframe when a child
	// connects, so the joiner gets an I-frame instead of black video.
	topo := r.currentTopo()
	var offererOverride *bool
	if topo != nil {
		o := topo.Offers(r.selfName, peerName)
		offererOverride = &o
	}
	if opts.offerer != nil {
		offererOverride = opts.offerer
	}
	// The state callback runs on a pion dispatch goroutine, so it does exactly one
	// thing: post. Everything it used to do inline (the relay's keyframe request,
	// and now the whole re-parent state machine) happens on the Run goroutine.
	name := peerName
	onState := func(st webrtc.PeerConnectionState) {
		r.postEvent(routerEvent{kind: evPeerState, peerName: name, state: st})
	}

	session, err := NewSession(SessionConfig{
		Log:           r.log,
		SelfID:        selfID,
		PeerID:        peerID,
		Transport:     tr,
		ICEServers:    r.cfg.ICEServers,
		Offerer:       offererOverride,
		Clock:         r.clk,
		Spawn:         r.spawnTracked,
		OnRemoteTrack: r.remoteTrackSink(linkCtx, peerID),
		OnState:       onState,
	})
	if err != nil {
		r.log.Error("create session", slog.String("peer_id", peerID), slog.Any("error", err))
		cancel()
		r.mu.Lock()
		delete(r.peers, peerID)
		r.mu.Unlock()
		return
	}

	r.mu.Lock()
	r.peers[peerID].session = session
	r.mu.Unlock()

	// Wire tracks/transceivers BEFORE Start (add-track-before-Start ⇒ the first
	// offer carries them, and the answerer folds its own track into the offered
	// m-line). Three roles:
	//   relay            → receive this neighbour's media + offer the others' forwarded tracks
	//   leaf/mesh sender → add own camera (it folds into the answer)
	//   offerer, no media → a recvonly transceiver so it still has something to offer
	var track *webrtc.TrackLocalStaticSample
	switch {
	case r.isRelayNow():
		r.setupRelayEdge(session, topo, peerName)
	case r.cfg.SendMedia:
		track, err = session.AddVideoTrack("video", "conclave")
		if err != nil {
			r.log.Error("add outbound track", slog.String("peer_id", peerID), slog.Any("error", err))
		}
	case session.Offerer():
		if err := session.AddRecvOnlyVideo(); err != nil {
			r.log.Warn("add recvonly transceiver", slog.String("peer_id", peerID), slog.Any("error", err))
		}
	}

	session.Start(linkCtx)
	r.log.Info("session started",
		slog.String("peer_id", peerID), slog.String("peer_name", peerName), slog.Bool("offerer", session.Offerer()))

	if track != nil {
		r.pumpOutbound(linkCtx, track, peerID)
	}
}

// setupRelayEdge wires the relay's side of the edge toward neighbour peerName,
// before Start: a recvonly transceiver to receive that neighbour's media (so the
// forwarder can fan it out), and one forwarded track per OTHER neighbour carrying
// that neighbour's media toward this one. Adding them all before Start means they
// are present when negotiation runs; pion usually folds them into a single
// offer/answer, but its OnNegotiationNeeded is asynchronous, so it may instead split
// into a first offer plus one renegotiation. Either way this converges without glare
// — the relay is the sole offerer on the edge, so there is never a colliding offer
// to reconcile (which pion could not roll back anyway).
func (r *Router) setupRelayEdge(session *Session, topo *overlay.Topology, peerName string) {
	// Receive peerName's own media; the reader is wired in remoteTrackSink.
	if err := session.AddRecvOnlyVideo(); err != nil {
		r.log.Warn("relay recvonly transceiver", slog.String("peer_name", peerName), slog.Any("error", err))
	}
	r.fwd.setUpstream(peerName, session)

	// This edge also carries every OTHER neighbour's media down to peerName.
	for _, src := range topo.NeighborsOf(r.selfName) {
		if src == peerName {
			continue
		}
		fwdTrack, sender, err := session.AddForwardTrack(forwardTrackPrefix+src, "conclave")
		if err != nil {
			r.log.Error("add forward track",
				slog.String("source", src), slog.String("child", peerName), slog.Any("error", err))
			continue
		}
		r.fwd.addOut(src, peerName, fwdTrack, sender)
	}
}

// pumpOutbound streams the configured media source into an already-added track on
// its own goroutine. The source writes through the upload meter, and the peer gauge
// is held up for exactly the pump's lifetime, so what the meter reports as "peers"
// is precisely the number of live outbound streams. The pump is registered with the
// WaitGroup so a shutdown joins it.
func (r *Router) pumpOutbound(ctx context.Context, track *webrtc.TrackLocalStaticSample, peerID string) {
	w := meteredTrack{track: track, meter: r.meter}

	r.meter.addPeer(1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer r.meter.addPeer(-1)

		var err error
		if r.cfg.MediaPath != "" {
			err = PlayIVF(ctx, r.log, r.cfg.MediaPath, w)
		} else {
			err = SendSynthetic(ctx, w, 30)
		}
		if err != nil && ctx.Err() == nil {
			r.log.Warn("media source ended", slog.String("peer_id", peerID), slog.Any("error", err))
		}
	}()
}

// remoteTrackSink returns the OnRemoteTrack handler for one peer: record to file
// if RecordPath is set, otherwise just count. It runs on a pion goroutine and
// blocks reading the track until the session closes.
func (r *Router) remoteTrackSink(ctx context.Context, peerID string) func(*webrtc.TrackRemote, *webrtc.RTPReceiver) {
	// A leaf can receive several forwarded tracks over its ONE session (one per
	// source the relay sends it), each firing this handler on its own goroutine.
	// recording claims the single record file for the first track so the others
	// don't clobber it; subsequent tracks are counted instead.
	var recording atomic.Bool
	return func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		r.mu.Lock()
		peerName := r.nameByID[peerID]
		r.mu.Unlock()
		log := r.log.With(slog.String("peer_id", peerID),
			slog.String("codec", track.Codec().MimeType), slog.Any("ssrc", track.SSRC()))
		log.Info("remote track arrived")
		r.markReceived(peerID)
		// Media arriving is the evidence a pending re-parent waits for; the Run
		// goroutine decides what it means.
		r.postEvent(routerEvent{kind: evPeerTrack, peerName: peerName})

		// Relay: this is a source's media — hand the single reader to the forwarder
		// to fan out. A TrackRemote has exactly ONE reader, so the relay must not
		// also record/count it here; the forwarder's read loop IS the reader. Gated
		// on the LIVE topology, not on holding a forwarder: a managed peer always
		// holds one but is only a relay while the current tree gives it children.
		if r.isRelayNow() {
			r.fwd.forward(peerName, track)
			return
		}

		if r.cfg.RecordPath != "" && recording.CompareAndSwap(false, true) {
			f, err := os.Create(r.cfg.RecordPath)
			if err != nil {
				log.Error("create record file", slog.Any("error", err))
				return
			}
			defer f.Close()
			n, err := RecordVP8(log, track, f)
			if err != nil && ctx.Err() == nil {
				log.Warn("recording ended with error", slog.Int("packets", n), slog.Any("error", err))
				return
			}
			log.Info("recording finished", slog.Int("packets", n), slog.String("path", r.cfg.RecordPath))
			return
		}
		if r.cfg.RecordPath != "" {
			log.Debug("record file already claimed by another track; counting instead")
		}

		n, err := DrainAndCount(log, track)
		if err != nil && ctx.Err() == nil {
			log.Warn("track drain ended with error", slog.Int("packets", n), slog.Any("error", err))
			return
		}
		log.Info("track finished", slog.Int("packets", n))
	}
}

// deliver routes a media-signaling frame to the session for its sender.
//
// One frame legitimately arrives for a peer we hold no session to: the first offer
// from a child promoting US as its precomputed backup parent. That edge is not in
// the tree — it exists precisely because the tree is momentarily wrong — so the
// ordinary neighbour check would drop it and the failover could never complete.
// Accepting it is gated on the topology naming us as that peer's backup, so it is
// not an open door.
func (r *Router) deliver(ctx context.Context, msg signaling.Message) {
	r.mu.Lock()
	link := r.peers[msg.From]
	r.mu.Unlock()
	if link == nil && msg.Type == signaling.TypeOffer && r.acceptsBackupChild(msg.From) {
		no := false
		r.startPeerOpt(ctx, msg.From, peerOpts{offerer: &no})
		r.mu.Lock()
		link = r.peers[msg.From]
		r.mu.Unlock()
	}
	if link == nil {
		r.log.Debug("frame for unknown peer", slog.String("from", msg.From), slog.String("type", string(msg.Type)))
		return
	}
	select {
	case link.inbox <- msg:
	default:
		// Buffer is generous (64); a full inbox means the session is wedged.
		r.log.Warn("peer inbox full; dropping frame", slog.String("from", msg.From), slog.String("type", string(msg.Type)))
	}
}

// stopPeer closes the session to peerID and returns the names of the children whose
// forwarded source set changed as a result, so the caller can ask upstream for a
// keyframe on each.
func (r *Router) stopPeer(peerID string) []string {
	r.mu.Lock()
	link := r.peers[peerID]
	delete(r.peers, peerID)
	peerName := r.nameByID[peerID] // still mapped: stopPeer runs before forgetPeer
	r.mu.Unlock()
	if link == nil {
		return nil
	}
	link.cancel()
	if link.session != nil {
		_ = link.session.Close()
	}
	if r.fwd == nil || peerName == "" {
		r.log.Info("session stopped", slog.String("peer_id", peerID))
		return nil
	}
	// Relay: drop the departed peer in BOTH forwarding roles. removeChild trims the
	// legs INTO it (its own session closing reaps their drains); removeSource drops
	// the forwardSource for its media AND hands back every downstream sender that
	// was carrying it, so those tracks are actually removed from the surviving
	// children instead of lingering as m-lines that will never carry a packet again.
	var affected []string
	for _, l := range r.fwd.legs() {
		if l.src == peerName {
			affected = append(affected, l.child)
		}
	}
	r.fwd.removeChild(peerName)
	for _, sender := range r.fwd.removeSource(peerName) {
		r.removeSenderFromItsSession(sender)
	}
	r.log.Info("session stopped", slog.String("peer_id", peerID))
	return affected
}

// removeSenderFromItsSession finds the live session holding sender and removes the
// track. The forwarder deliberately does not know which session a sender belongs to
// — it stores legs, not sessions — so the lookup lives here, where the peer map is.
func (r *Router) removeSenderFromItsSession(sender *webrtc.RTPSender) {
	r.mu.Lock()
	sessions := make([]*Session, 0, len(r.peers))
	for _, link := range r.peers {
		if link.session != nil {
			sessions = append(sessions, link.session)
		}
	}
	r.mu.Unlock()
	for _, s := range sessions {
		if s.HasSender(sender) {
			if err := s.RemoveTrack(sender); err != nil {
				r.log.Warn("remove departed source track", slog.Any("error", err))
			}
			return
		}
	}
}

// idForName resolves a topology name to the runtime id the server assigned, or ""
// if that peer is not in the roster (yet, or any more).
func (r *Router) idForName(name string) string {
	if name == "" {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.idByName[name]
}

// sessionByName returns the live Session toward a topology name, or nil.
func (r *Router) sessionByName(name string) *Session {
	id := r.idForName(name)
	if id == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if link := r.peers[id]; link != nil {
		return link.session
	}
	return nil
}

// hasSession reports whether a peerLink for name is still registered — which is
// what "the old parent is still open" means during a re-parent.
func (r *Router) hasSession(name string) bool {
	id := r.idForName(name)
	if id == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peers[id] != nil
}

// connectionStateByName reports a neighbour's PeerConnection state, or "new" when
// there is no session at all.
func (r *Router) connectionStateByName(name string) webrtc.PeerConnectionState {
	if s := r.sessionByName(name); s != nil {
		return s.ConnectionState()
	}
	return webrtc.PeerConnectionStateNew
}

// stopPeerByName is stopPeer keyed by topology name; a no-op when there is no such
// session, so abandon paths can call it unconditionally.
func (r *Router) stopPeerByName(name string) {
	if id := r.idForName(name); id != "" {
		r.stopPeer(id)
	}
}

// acceptsBackupChild reports whether the topology in force names US as peerID's
// backup parent — the one case where an unsolicited offer from a non-neighbour is
// legitimate.
func (r *Router) acceptsBackupChild(peerID string) bool {
	topo := r.currentTopo()
	if topo == nil {
		return false
	}
	r.mu.Lock()
	name := r.nameByID[peerID]
	r.mu.Unlock()
	return name != "" && topo.BackupOf(name) == r.selfName
}

// StaleRejected reports how many pushed topologies the ordering fence dropped. A
// nonzero count is proof the fence works, not an error condition.
func (r *Router) StaleRejected() uint64 { return r.staleRejected.Load() }

func (r *Router) closeAll() {
	r.mu.Lock()
	links := make([]*peerLink, 0, len(r.peers))
	for id, link := range r.peers {
		links = append(links, link)
		delete(r.peers, id)
	}
	r.mu.Unlock()
	for _, link := range links {
		link.cancel()
		if link.session != nil {
			_ = link.session.Close()
		}
	}
}

// clientTransport adapts the shared signaling.Client into a per-peer Transport:
// outbound frames are addressed to peerID; inbound frames are the ones the Router
// routed into inbox.
type clientTransport struct {
	client *signaling.Client
	peerID string
	inbox  chan signaling.Message
}

func (t *clientTransport) Send(msg signaling.Message) error {
	msg.To = t.peerID
	return t.client.Send(msg)
}

func (t *clientTransport) Incoming() <-chan signaling.Message { return t.inbox }
