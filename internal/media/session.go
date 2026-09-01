package media

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// NegotiationRetryDelay is how long a Session waits before re-attempting a
// negotiation that failed. 250ms is long enough for a transient pion state to
// settle and short enough that a human does not perceive the extra wait on top of
// the ICE/DTLS time already being spent.
const NegotiationRetryDelay = 250 * time.Millisecond

// NegotiationRetries bounds the retries after the initial attempt before the
// Session gives up and logs at Error. Five retries is ~1.25s of waiting. Giving up
// is safe rather than fatal: the coordinator re-pushes the topology on the next
// threshold event, and applyTopology is idempotent, so a wedged session self-heals
// on the next tree. Retrying forever would hide a real bug.
const NegotiationRetries = 5

// NegotiationAnswerTimeout is how long the offerer waits for the answer before
// re-sending the offer it already applied.
//
// It exists because an offer can be legitimately DROPPED by a correct peer: during
// a role inversion the two ends re-create their session at different moments, and
// for that window the far end still holds a session baked as the offerer, which
// (correctly) refuses an inbound offer as a role disagreement. Without a re-send
// the edge deadlocks — our side sits in have-local-offer forever, theirs sits
// waiting for an offer that was already sent and thrown away.
//
// 2s is comfortably longer than a loopback or LAN offer/answer round trip, so it
// never fires on a healthy edge, and short enough that the inversion window costs
// one re-send rather than a stalled call. The re-send re-uses the retry ladder, so
// it is bounded by NegotiationRetries like every other failure here.
const NegotiationAnswerTimeout = 2 * time.Second

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

	// Offerer, when non-nil, fixes who initiates negotiation instead of deriving it
	// from id order. The Router sets it from the topology in tree mode — the relay
	// must offer on every edge, because only a fresh offer can add the forwarded
	// m-lines it publishes, and an SDP answer cannot. Nil ⇒ mesh default (the
	// higher id offers).
	Offerer *bool

	// Clock is the time source for the negotiation retry timer. Injected rather
	// than taken from package time so a test can drive the retry ladder in a
	// microsecond instead of 1.25 real seconds. Nil ⇒ clock.System().
	Clock clock.Clock

	// Spawn runs a function as a goroutine the OWNER joins on shutdown — the Router
	// passes its WaitGroup-tracked spawner. The Session uses it for the retry timer
	// wait, which must not outlive the process it belongs to. Nil ⇒ a bare `go`,
	// which is right for a Session constructed directly in a test.
	Spawn func(func())
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

	clk   clock.Clock
	spawn func(func())
	// done is closed by Close and is what retires a pending retry: a Session that
	// has been torn down must not resurrect its negotiation a quarter second later.
	done     chan struct{}
	closeOne sync.Once

	// negotiationProbe, when non-nil, reports what the negotiation handler observed
	// on entry. It exists for one test, which pins an ORDERING that is otherwise
	// invisible: pion sets its own negotiation-needed flag BEFORE invoking the
	// handler, so a handler that returns early because OUR flag is still set loses
	// that notification permanently. Nil in production.
	negotiationProbe func(inFlight bool)

	// mu guards the buffered-candidate queue and the negotiation bookkeeping. It is
	// touched from the pion callback goroutine (OnICECandidate, OnNegotiationNeeded)
	// and the inbound-consume goroutine.
	mu        sync.Mutex
	remoteSet bool                      // has SetRemoteDescription succeeded at least once?
	pending   []webrtc.ICECandidateInit // candidates that arrived before remoteSet
	// negotiating means an offer is outstanding: we have called SetLocalDescription
	// and have not yet applied the answer.
	//
	// It is a GUARD, not a SCHEDULER. There is deliberately no companion "please
	// renegotiate when this lands" flag: pion v4 implements the W3C
	// update-the-negotiation-needed-flag algorithm, so a track added while the pc is
	// not `stable` sets pion's OWN flag and pion re-fires OnNegotiationNeeded on the
	// return to `stable` (peerconnection.go setDescription). A re-run of our own
	// would race pion's and the usual outcome is two offers for one change — exactly
	// the bug the serializer exists to prevent, reintroduced by the fix.
	negotiating bool
	// negGen increments every time an offer stops being outstanding, so a pending
	// answer-deadline goroutine can tell "my offer is still unanswered" from "a
	// later negotiation is in flight".
	negGen uint64
	// pendingLocalChange means a LOCAL mutation has been made that pion will not
	// re-fire negotiation for on its own.
	//
	// Measured against pion v4.2.16, not assumed: RemoveTrack changes the
	// transceiver from sendrecv to recvonly and then calls onNegotiationNeeded, but
	// checkNegotiationNeeded concludes no renegotiation is required and the handler
	// is never invoked — while a manual CreateOffer at that moment DOES produce a
	// correctly changed offer (a=recvonly). So for removals, and only for removals,
	// this Session must schedule its own follow-up. It is NOT the "renegotiate
	// later" flag the contract removed: that one raced pion on the path where pion
	// genuinely does re-fire (AddTrack), and this one covers the path where it
	// provably does not.
	pendingLocalChange bool
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
	// In tree mode the Router overrides this so the relay offers on every edge.
	offerer := cfg.SelfID > cfg.PeerID
	if cfg.Offerer != nil {
		offerer = *cfg.Offerer
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.System()
	}
	spawn := cfg.Spawn
	if spawn == nil {
		spawn = func(fn func()) { go fn() }
	}
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
		clk:     clk,
		spawn:   spawn,
		done:    make(chan struct{}),
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

// HasSender reports whether sender belongs to this session's PeerConnection. The
// Router needs it because the forwarder tracks legs, not sessions: when a departed
// source's senders come back for removal, this is what says which session owns
// each one.
func (s *Session) HasSender(sender *webrtc.RTPSender) bool {
	for _, t := range s.pc.GetTransceivers() {
		if t.Sender() == sender {
			return true
		}
	}
	return false
}

// AddForwardTrack adds an outbound VP8 track that carries RAW RTP packets
// (TrackLocalStaticRTP), for a relay to forward another peer's media without
// re-encoding, and returns the track plus its RTPSender. The sender matters: the
// caller MUST drain sender.ReadRTCP() or the interceptor chain stalls — and that
// drain loop is also where downstream PLIs are caught to forward upstream. Called
// before Start (add-track-before-Start) so the forwarded m-line rides the one
// offer with no renegotiation.
func (s *Session) AddForwardTrack(id, streamID string) (*webrtc.TrackLocalStaticRTP, *webrtc.RTPSender, error) {
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, id, streamID)
	if err != nil {
		return nil, nil, fmt.Errorf("new forward track: %w", err)
	}
	sender, err := s.pc.AddTrack(track)
	if err != nil {
		return nil, nil, fmt.Errorf("add forward track: %w", err)
	}
	return track, sender, nil
}

// WriteRTCP sends application-generated RTCP (a keyframe request) toward this
// session's peer. A relay uses it on the UPSTREAM session to ask the original
// sender for a keyframe when a downstream needs one. It silently no-ops if the
// peer is gone, so a nil return is not proof of delivery.
func (s *Session) WriteRTCP(pkts []rtcp.Packet) error { return s.pc.WriteRTCP(pkts) }

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

// Close tears down the PeerConnection (unblocking any track reads/writes) and
// retires any pending negotiation retry. It is idempotent: the Router closes a
// session on teardown and a superseded re-parent closes its pending one, and both
// paths can reach the same Session.
func (s *Session) Close() error {
	s.closeOne.Do(func() { close(s.done) })
	return s.pc.Close()
}

// RemoveTrack removes a previously added outbound track from this session. It fires
// OnNegotiationNeeded, so the serializer emits the renegotiating offer; the caller
// does not offer by hand.
//
// A closed PeerConnection returns webrtc.ErrConnectionClosed, which is mapped to
// nil: a closed session has already removed every track, so "remove from a closed
// session" is a satisfied postcondition, not a failure. Making the caller
// special-case it at every site would be noise, and would tempt callers to ignore
// ALL errors from this method — including the one that matters, a sender that was
// never created by this connection.
func (s *Session) RemoveTrack(sender *webrtc.RTPSender) error {
	if sender == nil {
		return nil
	}
	err := s.pc.RemoveTrack(sender)
	if errors.Is(err, webrtc.ErrConnectionClosed) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove track: %w", err)
	}
	s.mu.Lock()
	s.pendingLocalChange = true
	s.mu.Unlock()
	// Nudge the serializer, off this goroutine so a caller holding a lock (the
	// Router's Run loop does) never runs CreateOffer inline. The serializer decides
	// whether an offer actually goes out, so several removals in one apply still
	// cost exactly one renegotiation — SDP is a full snapshot.
	s.spawn(s.onNegotiationNeeded)
	return nil
}

// onNegotiationNeeded creates and sends an offer, at most one at a time. Only the
// offerer acts on it: the answerer's transceivers (added before it answers) ride
// the offerer's offer, so it has nothing to initiate. Suppressing the answerer here
// is what keeps the exchange glare-free — the two sides can never both be mid-offer.
//
// Phase 5 is the first time this can fire more than once on a live session (tracks
// are added and removed mid-call), so it needs a guard. There are TWO, in this
// order, and the order is the design:
//
//  1. pc.SignalingState() != stable — pion's OWN truth, and therefore the primary
//     guard. It covers every state change, including ones we did not initiate.
//  2. s.negotiating — our intent, secondary. It closes the window between deciding
//     to offer and pion actually leaving `stable`.
//
// What is deliberately NOT here: the polite/impolite roles, ignoreOffer, and
// rollback. pion v4 cannot roll back a local offer, which is why this codebase
// picked a single deterministic offerer per edge in the first place; with no
// colliding offer there is nothing to reconcile, only our own successive offers to
// serialize.
func (s *Session) onNegotiationNeeded() {
	if !s.offerer {
		return
	}
	s.mu.Lock()
	inFlight := s.negotiating
	if probe := s.negotiationProbe; probe != nil {
		probe(inFlight)
	}
	if inFlight {
		s.mu.Unlock()
		return // pion will call us again when the pc returns to stable
	}
	if st := s.pc.SignalingState(); st != webrtc.SignalingStateStable {
		s.mu.Unlock()
		s.log.Debug("negotiation deferred; signaling state not stable",
			slog.String("state", st.String()))
		return
	}
	s.negotiating = true
	s.mu.Unlock()

	s.negotiate(0)
}

// negotiate performs one negotiation attempt and, on failure, schedules the next.
// It runs with s.negotiating already set.
func (s *Session) negotiate(attempt int) {
	// A previous attempt may have applied a local offer and failed only to SEND it.
	// pion has no stable->... path back and no have-local-offer->SetLocal(offer)
	// transition (signalingstate.go checkNextSignalingState), so the ONLY legal
	// recovery from that state is to re-send the description already applied.
	if s.pc.SignalingState() == webrtc.SignalingStateHaveLocalOffer {
		if desc := s.pc.LocalDescription(); desc != nil {
			if err := s.sendDescription(desc, signaling.TypeOffer); err != nil {
				s.negotiationFailed(attempt, "resend offer", err)
				return
			}
			s.log.Debug("re-sent offer", slog.Int("attempt", attempt))
			return
		}
	}

	offer, err := s.pc.CreateOffer(nil)
	if err != nil {
		s.negotiationFailed(attempt, "create offer", err)
		return
	}
	if err := s.pc.SetLocalDescription(offer); err != nil {
		s.negotiationFailed(attempt, "set local description (offer)", err)
		return
	}
	// This offer is a full snapshot of the pc, so it carries every local change
	// made so far.
	s.mu.Lock()
	s.pendingLocalChange = false
	s.mu.Unlock()
	if err := s.sendDescription(s.pc.LocalDescription(), signaling.TypeOffer); err != nil {
		s.negotiationFailed(attempt, "send offer", err)
		return
	}
	s.log.Debug("sent offer")
	s.armAnswerDeadline(attempt)
}

// armAnswerDeadline re-sends an unanswered offer once the wait exceeds
// NegotiationAnswerTimeout. It is a no-op on a healthy edge; see the constant for
// the one case where a correct peer legitimately drops our offer.
func (s *Session) armAnswerDeadline(attempt int) {
	if attempt >= NegotiationRetries {
		return
	}
	s.mu.Lock()
	gen := s.negGen
	s.mu.Unlock()

	timer := s.clk.NewTimer(NegotiationAnswerTimeout)
	s.spawn(func() {
		defer timer.Stop()
		select {
		case <-s.done:
			return
		case <-timer.C():
		}
		s.mu.Lock()
		stale := !s.negotiating || s.negGen != gen
		s.mu.Unlock()
		if stale {
			return // answered, superseded, or abandoned
		}
		s.log.Warn("no answer for our offer; re-sending", slog.Int("attempt", attempt))
		s.negotiate(attempt + 1)
	})
}

// negotiationFailed clears the in-flight flag and schedules a bounded retry. pion
// will NOT re-fire for an error inside our own handler — its negotiation-needed
// flag is already set by the time we are called — which is precisely why the retry
// path survives even though the "renegotiate later" flag does not.
func (s *Session) negotiationFailed(attempt int, what string, cause error) {
	s.mu.Lock()
	s.negotiating = false
	s.negGen++
	s.mu.Unlock()

	if attempt >= NegotiationRetries {
		s.log.Error("giving up on negotiation",
			slog.String("op", what), slog.Int("attempts", attempt+1), slog.Any("error", cause))
		return
	}
	s.log.Warn("negotiation attempt failed; retrying",
		slog.String("op", what), slog.Int("attempt", attempt), slog.Any("error", cause))

	timer := s.clk.NewTimer(NegotiationRetryDelay)
	s.spawn(func() {
		defer timer.Stop()
		select {
		case <-s.done:
			return
		case <-timer.C():
		}
		s.mu.Lock()
		if s.negotiating {
			s.mu.Unlock()
			return // something else got there first
		}
		s.negotiating = true
		s.mu.Unlock()
		s.negotiate(attempt + 1)
	})
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
		// Clear the in-flight flag BEFORE applying the answer, not after.
		//
		// SetRemoteDescription reaches `stable` inside this call, and pion's
		// setDescription then clears its own negotiation-needed flag, sets it again,
		// and invokes our handler — on its operations goroutine, concurrently with
		// this one. A handler that finds our flag still set returns early, and pion
		// will not fire again until the NEXT return to stable, which never comes
		// because we never offered. The renegotiation would be lost silently and
		// permanently. (The contract specifies the opposite order; this is the
		// correction, and the failure mode it prevents is exactly the
		// "half-negotiated session" the serializer exists to rule out.)
		s.mu.Lock()
		s.negotiating = false
		s.negGen++
		pending := s.pendingLocalChange
		s.mu.Unlock()
		if pending {
			// A removal landed while this offer was in flight. pion will not
			// re-fire for it, so we must.
			s.spawn(s.onNegotiationNeeded)
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
