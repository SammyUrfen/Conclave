package media

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pion/webrtc/v4"

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
	r := &Router{
		log:      log.With(slog.String("component", "media-router")),
		client:   client,
		cfg:      cfg,
		meter:    &uploadMeter{},
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
		r.fwd = newForwarder(r.log, r.meter, r.spawnTracked)
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
		r.deliver(msg)
	case signaling.TypeTopology:
		r.applyTopology(ctx, msg.Payload)
	case signaling.TypeError:
		r.log.Warn("signaling error frame", slog.String("error", msg.Error))
	}
}

// applyTopology realises a topology the coordinator pushed (managed mode). It
// swaps in the new tree and opens a session to every neighbour it can already
// resolve to an id; a neighbour not yet in the roster is picked up later, when its
// peer-joined frame runs maybeStartPeer against this now-current topology. The two
// orders — topology-then-peer and peer-then-topology — both converge, because both
// funnel through maybeStartPeer and startPeer is idempotent per peer.
//
// Phase 4 is deliberately ADDITIVE: it connects new neighbours but does not tear
// down a session to a peer the new tree drops. Mid-call re-parenting and teardown
// (and the renegotiation a new upstream source forces onto existing children) are
// the churn problem Phase 5 owns; doing them here would be a half-built version of
// that phase. A dropped-neighbour is logged so the gap is visible, not silent.
func (r *Router) applyTopology(ctx context.Context, payload []byte) {
	if !r.cfg.Managed {
		r.log.Debug("ignoring pushed topology (not in managed mode)")
		return
	}
	var topo overlay.Topology
	if err := json.Unmarshal(payload, &topo); err != nil {
		r.log.Warn("bad topology payload", slog.Any("error", err))
		return
	}
	r.mu.Lock()
	r.topo = &topo
	r.mu.Unlock()

	neighbors := topo.NeighborsOf(r.selfName)
	r.log.Info("applied pushed topology",
		slog.Int("edges", len(topo.Edges)), slog.Bool("relay", topo.IsRelay(r.selfName)),
		slog.Any("neighbors", neighbors))

	for _, name := range neighbors {
		r.mu.Lock()
		id := r.idByName[name]
		r.mu.Unlock()
		if id != "" {
			r.maybeStartPeer(ctx, id)
		}
	}
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

// startPeer creates and starts a Session toward peerID (idempotent per peer),
// attaches an outbound track if configured, and wires the remote-track sink.
func (r *Router) startPeer(ctx context.Context, peerID string) {
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
	var onState func(webrtc.PeerConnectionState)
	if topo != nil {
		o := topo.Offers(r.selfName, peerName)
		offererOverride = &o
	}
	if r.isRelayNow() {
		childName := peerName
		onState = func(st webrtc.PeerConnectionState) {
			if st == webrtc.PeerConnectionStateConnected {
				r.fwd.keyframeForChild(childName)
			}
		}
	}

	session, err := NewSession(SessionConfig{
		Log:           r.log,
		SelfID:        selfID,
		PeerID:        peerID,
		Transport:     tr,
		ICEServers:    r.cfg.ICEServers,
		Offerer:       offererOverride,
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
		fwdTrack, sender, err := session.AddForwardTrack("fwd-"+src, "conclave")
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
func (r *Router) deliver(msg signaling.Message) {
	r.mu.Lock()
	link := r.peers[msg.From]
	r.mu.Unlock()
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

func (r *Router) stopPeer(peerID string) {
	r.mu.Lock()
	link := r.peers[peerID]
	delete(r.peers, peerID)
	peerName := r.nameByID[peerID] // still mapped: stopPeer runs before forgetPeer
	r.mu.Unlock()
	if link == nil {
		return
	}
	link.cancel()
	if link.session != nil {
		_ = link.session.Close()
	}
	// Relay: drop the departed peer in BOTH forwarding roles. removeChild trims the
	// legs INTO it (its own session closing reaps their drains); removeSource drops
	// the forwardSource for its media so f.sources can't grow without bound as senders
	// churn. (Its downstream legs on surviving children linger until they leave — a
	// Phase-5 teardown item, see removeSource.)
	if r.fwd != nil && peerName != "" {
		r.fwd.removeChild(peerName)
		r.fwd.removeSource(peerName)
	}
	r.log.Info("session stopped", slog.String("peer_id", peerID))
}

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
