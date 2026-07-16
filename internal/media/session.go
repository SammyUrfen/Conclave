package media

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/signaling"
)

// SessionConfig configures one peer-to-peer WebRTC session.
type SessionConfig struct {
	Log       *slog.Logger
	SelfID    string    // our peer id (server-assigned)
	PeerID    string    // the remote peer's id
	Transport Transport // signaling channel to the remote peer

	// ICEServers are STUN/TURN servers for candidate gathering. Empty is fine on
	// loopback / same-host (host candidates connect directly); a public STUN URL
	// is what lets two machines behind NAT find a path.
	ICEServers []webrtc.ICEServer

	// OnRemoteTrack fires (on a pion goroutine) when a remote media track
	// arrives. The callee owns draining it — see RecordVP8 / DrainAndCount.
	OnRemoteTrack func(*webrtc.TrackRemote, *webrtc.RTPReceiver)
	// OnState fires on every PeerConnection state transition. Optional.
	OnState func(webrtc.PeerConnectionState)
}

// Session drives one webrtc.PeerConnection to one remote peer, exchanging SDP and
// ICE over a Transport. It uses a single-offerer scheme (one deterministic offerer
// per pair, see the offerer field) so simultaneous offers (glare) can never arise —
// which matters because pion cannot roll back a local offer to recover from them.
type Session struct {
	log    *slog.Logger
	selfID string
	peerID string
	tr     Transport
	pc     *webrtc.PeerConnection

	// offerer decides who initiates. It is derived deterministically from the two
	// ids, so exactly one side of every pair offers and the other only answers.
	// This is glare-free by construction — we never have two simultaneous offers to
	// reconcile, which matters because pion cannot roll back a local offer (the
	// SetLocal+rollback transition simply isn't in its state machine). Both peers'
	// media still flows over a single sendrecv m-line: when the answerer applies the
	// offer, pion folds the answerer's own sendonly track into that m-line.
	offerer bool

	// mu guards the buffered-candidate queue. It is touched from both the pion
	// callback goroutine (OnICECandidate) and the inbound-consume goroutine.
	mu        sync.Mutex
	remoteSet bool                      // has SetRemoteDescription succeeded at least once?
	pending   []webrtc.ICECandidateInit // candidates that arrived before remoteSet
}

// NewSession builds a Session and its PeerConnection with VP8 pinned in the
// MediaEngine. It wires the pion callbacks but does not start consuming inbound
// signaling — call Start for that.
func NewSession(cfg SessionConfig) (*Session, error) {
	if cfg.Log == nil || cfg.Transport == nil {
		return nil, fmt.Errorf("media: SessionConfig requires Log and Transport")
	}

	// Pin codecs: register ONLY VP8, so both ends negotiate it without depending
	// on default-codec ordering (the roadmap's "pin codecs in the MediaEngine").
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("register vp8: %w", err)
	}
	// Default interceptors add NACK, RTCP reports, and TWCC — the feedback plumbing
	// a real connection expects. Cheap to include now; load-bearing from Phase 3.
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil {
		return nil, fmt.Errorf("register interceptors: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))

	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: cfg.ICEServers})
	if err != nil {
		return nil, fmt.Errorf("new peer connection: %w", err)
	}

	// Deterministic role: the peer with the higher id offers, the other answers.
	// Any total order on the (distinct) ids works; both sides compute the same one.
	offerer := cfg.SelfID > cfg.PeerID
	s := &Session{
		log: cfg.Log.With(
			slog.String("component", "media"),
			slog.String("peer_id", cfg.PeerID),
			slog.Bool("offerer", offerer),
		),
		selfID:  cfg.SelfID,
		peerID:  cfg.PeerID,
		tr:      cfg.Transport,
		pc:      pc,
		offerer: offerer,
	}

	pc.OnICECandidate(s.onLocalCandidate)
	pc.OnNegotiationNeeded(s.onNegotiationNeeded)
	// Log every state transition on both peers — per the roadmap, the first thing
	// to do when ICE/DTLS won't reach connected is diff the two timelines.
	pc.OnICEConnectionStateChange(func(st webrtc.ICEConnectionState) {
		s.log.Info("ice connection state", slog.String("state", st.String()))
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		s.log.Info("peer connection state", slog.String("state", st.String()))
		if cfg.OnState != nil {
			cfg.OnState(st)
		}
	})
	if cfg.OnRemoteTrack != nil {
		pc.OnTrack(cfg.OnRemoteTrack)
	}
	return s, nil
}

// Start begins consuming inbound signaling frames until ctx is cancelled.
func (s *Session) Start(ctx context.Context) {
	go s.consume(ctx)
}

// AddVideoTrack adds an outbound VP8 track and returns it for sample writing.
// Adding a track fires OnNegotiationNeeded (which offers, on the offerer side). The
// Router calls this BEFORE Start so the answerer's transceiver exists before any
// inbound offer is applied — see the Router for why that ordering carries media both
// ways over a single m-line.
func (s *Session) AddVideoTrack(id, streamID string) (*webrtc.TrackLocalStaticSample, error) {
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, id, streamID)
	if err != nil {
		return nil, fmt.Errorf("new local track: %w", err)
	}
	if _, err := s.pc.AddTrack(track); err != nil {
		return nil, fmt.Errorf("add track: %w", err)
	}
	return track, nil
}

// Offerer reports whether this side initiates negotiation (vs only answering).
func (s *Session) Offerer() bool { return s.offerer }

// AddRecvOnlyVideo adds a receive-only video transceiver. The Router calls this on
// the offerer side when this peer sends no media of its own, so that (a) there is a
// transceiver to trigger the initial offer, and (b) the offer carries an m-line the
// answerer can fold its outbound track into. When this peer does send media,
// AddVideoTrack already provides both, so this isn't needed.
func (s *Session) AddRecvOnlyVideo() error {
	if _, err := s.pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		return fmt.Errorf("add recvonly video: %w", err)
	}
	return nil
}

// ConnectionState reports the current PeerConnection state.
func (s *Session) ConnectionState() webrtc.PeerConnectionState { return s.pc.ConnectionState() }

// Close tears down the PeerConnection (unblocking any track reads/writes).
func (s *Session) Close() error { return s.pc.Close() }

// onNegotiationNeeded creates and sends an offer. Only the offerer acts on it: the
// answerer's transceivers (added before it answers) ride the offerer's offer, so it
// has nothing to initiate. Suppressing the answerer here is what keeps the exchange
// glare-free — the two sides can never both be mid-offer.
func (s *Session) onNegotiationNeeded() {
	if !s.offerer {
		return
	}

	offer, err := s.pc.CreateOffer(nil)
	if err != nil {
		s.log.Error("create offer", slog.Any("error", err))
		return
	}
	if err := s.pc.SetLocalDescription(offer); err != nil {
		s.log.Error("set local description (offer)", slog.Any("error", err))
		return
	}
	if err := s.sendDescription(s.pc.LocalDescription(), signaling.TypeOffer); err != nil {
		s.log.Warn("send offer", slog.Any("error", err))
		return
	}
	s.log.Debug("sent offer")
}

// onLocalCandidate trickles one locally-gathered ICE candidate to the peer. A nil
// candidate marks end-of-gathering; we don't send an explicit end-of-candidates.
func (s *Session) onLocalCandidate(c *webrtc.ICECandidate) {
	if c == nil {
		return
	}
	raw, err := json.Marshal(c.ToJSON())
	if err != nil {
		s.log.Error("marshal candidate", slog.Any("error", err))
		return
	}
	if err := s.tr.Send(signaling.Message{Type: signaling.TypeCandidate, From: s.selfID, Candidate: raw}); err != nil {
		s.log.Warn("send candidate", slog.Any("error", err))
	}
}

func (s *Session) consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.tr.Incoming():
			if !ok {
				return
			}
			// Defensive for >2 peers on a shared transport: only act on frames
			// from our own peer.
			if msg.From != "" && msg.From != s.peerID {
				continue
			}
			switch msg.Type {
			case signaling.TypeOffer, signaling.TypeAnswer:
				s.onRemoteDescription(msg)
			case signaling.TypeCandidate:
				s.onRemoteCandidate(msg)
			default:
				// Not a media-signaling frame; the Router handles lifecycle.
			}
		}
	}
}

// onRemoteDescription applies an inbound offer or answer. Because roles are fixed
// (one offerer per pair), the exchange is a plain half-duplex handshake: the
// answerer only ever gets offers and replies with an answer; the offerer only ever
// gets answers. Anything else means both sides disagree on their role — impossible
// under the deterministic rule — so we log and drop it rather than risk a bad state
// transition.
func (s *Session) onRemoteDescription(msg signaling.Message) {
	var desc webrtc.SessionDescription
	if err := json.Unmarshal(msg.SDP, &desc); err != nil {
		s.log.Error("decode remote sdp", slog.Any("error", err))
		return
	}

	switch desc.Type {
	case webrtc.SDPTypeOffer:
		if s.offerer {
			s.log.Warn("offerer received an offer; dropping (role disagreement)")
			return
		}
		s.answerOffer(desc)
	case webrtc.SDPTypeAnswer:
		if !s.offerer {
			s.log.Warn("answerer received an answer; dropping (role disagreement)")
			return
		}
		if err := s.pc.SetRemoteDescription(desc); err != nil {
			s.log.Error("set remote description (answer)", slog.Any("error", err))
			return
		}
		s.flushPending()
	default:
		s.log.Warn("unexpected remote description type", slog.String("sdp_type", desc.Type.String()))
	}
}

// answerOffer applies a remote offer and replies with an answer. If this peer added
// its own outbound track first (see the Router), pion folds that track into the
// offered m-line and the answer is sendrecv — so a single offer establishes media
// in both directions.
func (s *Session) answerOffer(desc webrtc.SessionDescription) {
	if err := s.pc.SetRemoteDescription(desc); err != nil {
		s.log.Error("set remote description (offer)", slog.Any("error", err))
		return
	}
	s.flushPending()

	answer, err := s.pc.CreateAnswer(nil)
	if err != nil {
		s.log.Error("create answer", slog.Any("error", err))
		return
	}
	if err := s.pc.SetLocalDescription(answer); err != nil {
		s.log.Error("set local description (answer)", slog.Any("error", err))
		return
	}
	if err := s.sendDescription(s.pc.LocalDescription(), signaling.TypeAnswer); err != nil {
		s.log.Warn("send answer", slog.Any("error", err))
		return
	}
	s.log.Debug("sent answer")
}

// onRemoteCandidate adds a trickled remote candidate. Candidates that arrive
// before the remote description is set are buffered — pion rejects AddICECandidate
// with no remote description — and flushed by flushPending once it lands.
func (s *Session) onRemoteCandidate(msg signaling.Message) {
	var init webrtc.ICECandidateInit
	if err := json.Unmarshal(msg.Candidate, &init); err != nil {
		s.log.Error("decode remote candidate", slog.Any("error", err))
		return
	}
	s.mu.Lock()
	if !s.remoteSet {
		s.pending = append(s.pending, init)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	if err := s.pc.AddICECandidate(init); err != nil {
		s.log.Warn("add ice candidate", slog.Any("error", err))
	}
}

// flushPending marks the remote description as set and applies any candidates
// that were buffered while it was still nil.
func (s *Session) flushPending() {
	s.mu.Lock()
	s.remoteSet = true
	pending := s.pending
	s.pending = nil
	s.mu.Unlock()
	for _, init := range pending {
		if err := s.pc.AddICECandidate(init); err != nil {
			s.log.Warn("add buffered ice candidate", slog.Any("error", err))
		}
	}
}

func (s *Session) sendDescription(desc *webrtc.SessionDescription, t signaling.Type) error {
	raw, err := json.Marshal(desc)
	if err != nil {
		return fmt.Errorf("marshal sdp: %w", err)
	}
	return s.tr.Send(signaling.Message{Type: t, From: s.selfID, SDP: raw})
}
