package media

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	pmedia "github.com/pion/webrtc/v4/pkg/media"

	"github.com/SammyUrfen/conclave/internal/signaling"
)

// memTransport is an in-memory Transport: Send delivers to the paired peer's
// inbox, stamping From the way the real server does. It lets two Sessions
// negotiate with no sockets and no signaling server — only the media path (ICE +
// DTLS + SRTP) uses real loopback UDP.
type memTransport struct {
	selfID string
	out    chan<- signaling.Message
	in     <-chan signaling.Message
}

func (t *memTransport) Send(msg signaling.Message) error {
	msg.From = t.selfID // the server stamps identity; emulate it here
	t.out <- msg
	return nil
}
func (t *memTransport) Incoming() <-chan signaling.Message { return t.in }

func newMemPair(aID, bID string) (a, b *memTransport) {
	aIn := make(chan signaling.Message, 64)
	bIn := make(chan signaling.Message, 64)
	a = &memTransport{selfID: aID, out: bIn, in: aIn}
	b = &memTransport{selfID: bID, out: aIn, in: bIn}
	return a, b
}

// TestSessionConnectsAndForwardsTrack is the Phase-1 acceptance test in miniature:
// two Sessions complete real ICE+DTLS over loopback and one receives the other's
// media track. It is media-free of cameras and files (synthetic VP8) yet exercises
// the whole pion path, and runs under -race to catch the callback/goroutine races
// the negotiation code invites. Deterministic and fast (~tens of ms to connect).
func TestSessionConnectsAndForwardsTrack(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	callerID, calleeID := "caller", "callee"
	callerTr, calleeTr := newMemPair(callerID, calleeID)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	gotPacket := make(chan struct{})
	var packetOnce sync.Once
	calleeConnected := make(chan struct{})
	var connOnce sync.Once

	callee, err := NewSession(SessionConfig{
		Log: logger, SelfID: calleeID, PeerID: callerID, Transport: calleeTr,
		OnRemoteTrack: func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			for {
				if _, _, err := track.ReadRTP(); err != nil {
					return
				}
				packetOnce.Do(func() { close(gotPacket) })
			}
		},
		OnState: func(st webrtc.PeerConnectionState) {
			if st == webrtc.PeerConnectionStateConnected {
				connOnce.Do(func() { close(calleeConnected) })
			}
		},
	})
	if err != nil {
		t.Fatalf("new callee session: %v", err)
	}
	defer callee.Close()

	caller, err := NewSession(SessionConfig{
		Log: logger, SelfID: callerID, PeerID: calleeID, Transport: callerTr,
	})
	if err != nil {
		t.Fatalf("new caller session: %v", err)
	}
	defer caller.Close()

	// Both consume loops must run before the track is added, so the offer the
	// track triggers can be received and answered.
	caller.Start(ctx)
	callee.Start(ctx)

	track, err := caller.AddVideoTrack("video", "conclave")
	if err != nil {
		t.Fatalf("add video track: %v", err)
	}

	// Pump synthetic frames until the receiver sees a packet. Early writes (before
	// the track binds post-negotiation) are harmless no-ops.
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		frame := make([]byte, 128)
		for {
			select {
			case <-ctx.Done():
				return
			case <-gotPacket:
				return
			case <-ticker.C:
				_ = track.WriteSample(pmedia.Sample{Data: frame, Duration: 33 * time.Millisecond})
			}
		}
	}()

	select {
	case <-gotPacket:
	case <-ctx.Done():
		t.Fatal("receiver never got an RTP packet — media did not flow end to end")
	}
	select {
	case <-calleeConnected:
	case <-time.After(2 * time.Second):
		t.Fatalf("callee PeerConnection never reached connected (state=%s)", callee.ConnectionState())
	}
}
