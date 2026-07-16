package signaling

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// TestHubRelaysBetweenPeers drives the whole Hub through two real WebSocket
// connections against an httptest server: join + roster, relay with a
// server-stamped From, rejection of a spoofed From, and leave notification. It
// is media-free and deterministic, and is meant to be run under -race — the
// point is to prove the goroutine/channel plumbing has no data races or
// deadlocks, which a unit test of pure functions could never show.
func TestHubRelaysBetweenPeers(t *testing.T) {
	srv, wsURL := newTestServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Peer A joins an empty room.
	connA := dial(ctx, t, wsURL)
	defer connA.CloseNow()

	joinedA := readMsg(ctx, t, connA)
	if joinedA.Type != TypeJoined {
		t.Fatalf("A first frame = %q, want %q", joinedA.Type, TypeJoined)
	}
	if len(joinedA.Peers) != 0 {
		t.Fatalf("A joined an empty room but sees peers %v", joinedA.Peers)
	}
	idA := joinedA.To
	if idA == "" {
		t.Fatal("A was not assigned an id")
	}

	// Peer B joins: B's roster must contain A, and A must be told B arrived.
	connB := dial(ctx, t, wsURL)
	defer connB.CloseNow()

	joinedB := readMsg(ctx, t, connB)
	if joinedB.Type != TypeJoined || len(joinedB.Peers) != 1 || joinedB.Peers[0] != idA {
		t.Fatalf("B joined = %+v, want joined with peers=[%q]", joinedB, idA)
	}
	idB := joinedB.To

	peerJoined := readMsg(ctx, t, connA)
	if peerJoined.Type != TypePeerJoined || peerJoined.From != idB {
		t.Fatalf("A peer-joined = %+v, want %q from %q", peerJoined, TypePeerJoined, idB)
	}

	// A sends an offer addressed to B; B must receive it, From stamped to A.
	if err := wsjson.Write(ctx, connA, Message{
		Type: TypeOffer, To: idB, SDP: json.RawMessage(`"fake-sdp"`),
	}); err != nil {
		t.Fatalf("A write offer: %v", err)
	}
	offer := readMsg(ctx, t, connB)
	if offer.Type != TypeOffer {
		t.Fatalf("B got %q, want %q", offer.Type, TypeOffer)
	}
	if offer.From != idA {
		t.Errorf("B offer From = %q, want %q (server must stamp identity)", offer.From, idA)
	}
	if string(offer.SDP) != `"fake-sdp"` {
		t.Errorf("B offer SDP = %s, want %q", offer.SDP, `"fake-sdp"`)
	}

	// Spoof attempt: A claims From="impostor". The server must overwrite it.
	if err := wsjson.Write(ctx, connA, Message{
		Type: TypeCandidate, From: "impostor", To: idB, Candidate: json.RawMessage(`"cand"`),
	}); err != nil {
		t.Fatalf("A write candidate: %v", err)
	}
	cand := readMsg(ctx, t, connB)
	if cand.From != idA {
		t.Errorf("spoofed From leaked: got %q, want %q", cand.From, idA)
	}

	// B leaves; A must be told.
	if err := connB.Close(websocket.StatusNormalClosure, "bye"); err != nil {
		t.Fatalf("B close: %v", err)
	}
	left := readMsg(ctx, t, connA)
	if left.Type != TypePeerLeft || left.From != idB {
		t.Fatalf("A peer-left = %+v, want %q from %q", left, TypePeerLeft, idB)
	}
}

// TestHubRelayToUnknownPeerErrors verifies a relay to a nonexistent peer comes
// back as an error frame rather than being silently dropped.
func TestHubRelayToUnknownPeerErrors(t *testing.T) {
	srv, wsURL := newTestServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dial(ctx, t, wsURL)
	defer conn.CloseNow()

	joined := readMsg(ctx, t, conn)
	if joined.Type != TypeJoined {
		t.Fatalf("first frame = %q, want %q", joined.Type, TypeJoined)
	}

	if err := wsjson.Write(ctx, conn, Message{
		Type: TypeOffer, To: "nobody", SDP: json.RawMessage(`"x"`),
	}); err != nil {
		t.Fatalf("write offer: %v", err)
	}
	got := readMsg(ctx, t, conn)
	if got.Type != TypeError {
		t.Fatalf("got %q, want %q", got.Type, TypeError)
	}
	if !strings.Contains(got.Error, "nobody") {
		t.Errorf("error message %q does not mention the unknown peer", got.Error)
	}
}

// newTestServer mounts a fresh Hub on an httptest server and returns it plus the
// ws:// URL of the /ws endpoint (room "demo").
func newTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	hub := NewHub(slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?room=demo"
	return srv, wsURL
}

func dial(ctx context.Context, t *testing.T, wsURL string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", wsURL, err)
	}
	return conn
}

// readMsg reads one Message with a short deadline so a missing frame fails the
// test fast instead of hanging until the outer context expires.
func readMsg(ctx context.Context, t *testing.T, conn *websocket.Conn) Message {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var msg Message
	if err := wsjson.Read(readCtx, conn, &msg); err != nil {
		t.Fatalf("read: %v", err)
	}
	return msg
}
