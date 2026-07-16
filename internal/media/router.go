package media

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/pion/webrtc/v4"

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

	// wg joins every goroutine the Router itself spawns — the upload-meter sampler
	// and one media pump per outbound peer — so Run does not return until they have
	// all stopped. Paired with per-link context cancellation, this is the
	// Phase 2 lesson in explicit goroutine lifecycle: cancel to signal, WaitGroup
	// to join, no leaks.
	wg sync.WaitGroup

	mu     sync.Mutex
	selfID string
	peers  map[string]*peerLink
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
	return &Router{
		log:    log.With(slog.String("component", "media-router")),
		client: client,
		cfg:    cfg,
		meter:  &uploadMeter{},
		peers:  make(map[string]*peerLink),
	}
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
		r.mu.Unlock()
		r.log.Info("joined room", slog.String("self_id", msg.To), slog.Any("peers", msg.Peers))
		// Existing members are already here; start a session with each.
		for _, peerID := range msg.Peers {
			r.startPeer(ctx, peerID)
		}
	case signaling.TypePeerJoined:
		r.startPeer(ctx, msg.From)
	case signaling.TypePeerLeft:
		r.stopPeer(msg.From)
	case signaling.TypeOffer, signaling.TypeAnswer, signaling.TypeCandidate:
		r.deliver(msg)
	case signaling.TypeError:
		r.log.Warn("signaling error frame", slog.String("error", msg.Error))
	}
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
	inbox := make(chan signaling.Message, 64)
	linkCtx, cancel := context.WithCancel(ctx)
	r.peers[peerID] = &peerLink{inbox: inbox, cancel: cancel}
	r.mu.Unlock()

	tr := &clientTransport{client: r.client, peerID: peerID, inbox: inbox}
	session, err := NewSession(SessionConfig{
		Log:           r.log,
		SelfID:        selfID,
		PeerID:        peerID,
		Transport:     tr,
		ICEServers:    r.cfg.ICEServers,
		OnRemoteTrack: r.remoteTrackSink(linkCtx, peerID),
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

	// Wire negotiation-relevant transceivers BEFORE Start, so they exist before the
	// consume loop can process any inbound offer. This is what makes the single-
	// offerer scheme carry media both ways: the answerer's sendonly track must be
	// present when it applies the offer, or pion answers recvonly and the answerer's
	// media is stranded (an answer can't add m-lines). An offerer that sends no
	// media still needs a transceiver to have something to offer at all.
	var track *webrtc.TrackLocalStaticSample
	if r.cfg.SendMedia {
		track, err = session.AddVideoTrack("video", "conclave")
		if err != nil {
			r.log.Error("add outbound track", slog.String("peer_id", peerID), slog.Any("error", err))
		}
	} else if session.Offerer() {
		if err := session.AddRecvOnlyVideo(); err != nil {
			r.log.Warn("add recvonly transceiver", slog.String("peer_id", peerID), slog.Any("error", err))
		}
	}

	session.Start(linkCtx)
	r.log.Info("session started", slog.String("peer_id", peerID), slog.Bool("offerer", session.Offerer()))

	if track != nil {
		r.pumpOutbound(linkCtx, track, peerID)
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
	return func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		log := r.log.With(slog.String("peer_id", peerID),
			slog.String("codec", track.Codec().MimeType), slog.Any("ssrc", track.SSRC()))
		log.Info("remote track arrived")
		r.markReceived(peerID)

		if r.cfg.RecordPath != "" {
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
	r.mu.Unlock()
	if link == nil {
		return
	}
	link.cancel()
	if link.session != nil {
		_ = link.session.Close()
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
