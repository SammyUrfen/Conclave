package signaling

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
)

// wireBackupPromote is the WIRE VALUE of the promote frame, written as a literal
// rather than as the Type constant on purpose: what two peers agree on is the
// string, and a test spelled with the constant would keep passing if the constant
// were renamed to something the other end does not send.
const wireBackupPromote = Type("backup-promote")

// TestHubRelaysBackupPromote pins the routing of the one new wire type the
// backup-edge offerer inversion needs.
//
// A child that has lost its parent cannot simply wait for its precomputed backup to
// offer — the backup has no way to know the failure happened. So it asks, with a
// frame the Hub must treat exactly like an offer or a candidate: relayed verbatim,
// peer to peer, with a server-stamped From. It is NOT a control-plane frame: nothing
// on the server acts on it, and handing it to the Observer would put a peer-to-peer
// negotiation step into the coordinator's event stream.
//
// The mutation each assertion catches:
//
//   - relay: leave the type out of route's relay arm and it falls to the default arm
//     ("ignoring unexpected frame type"), so B never receives it and the read times
//     out. This is the assertion that fails against the pre-fix code, where the type
//     does not exist at all.
//   - From-stamping: a Hub that forwarded the client's own From would let any peer
//     claim to be the child whose parent died — and From is the one credential the
//     receiver's backup authorization is checked against.
//   - observer: routing the frame to the Observer AS WELL AS relaying it (the shape a
//     copy-paste from the TypeMetrics arm would take) leaves the first two assertions
//     green and only this one red.
//   - missing To: pins that the frame goes through the relay arm's addressing check,
//     so a mis-addressed promote is diagnosable instead of looking like a lost packet.
func TestHubRelaysBackupPromote(t *testing.T) {
	obs := &recordingObserver{}
	hub := NewHub(slog.New(slog.NewTextHandler(io.Discard, nil)))
	hub.SetObserver(obs)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?room=demo"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connA := dial(ctx, t, wsURL)
	defer connA.CloseNow()
	idA := readNext(ctx, t, connA).To

	connB := dial(ctx, t, wsURL)
	defer connB.CloseNow()
	idB := readNext(ctx, t, connB).To
	if pj := readNext(ctx, t, connA); pj.Type != TypePeerJoined {
		t.Fatalf("A saw %q, want %q", pj.Type, TypePeerJoined)
	}

	// A has lost its parent and asks B, its precomputed backup, to offer.
	if err := wsjson.Write(ctx, connA, Message{
		Type: wireBackupPromote, To: idB, From: "impostor",
	}); err != nil {
		t.Fatalf("A write backup-promote: %v", err)
	}
	got := readNext(ctx, t, connB)
	if got.Type != wireBackupPromote {
		t.Fatalf("B got %q, want %q relayed peer-to-peer", got.Type, wireBackupPromote)
	}
	if got.From != idA {
		t.Errorf("B backup-promote From = %q, want %q — identity is server-stamped, and "+
			"From is the credential the receiver's backup authorization reads", got.From, idA)
	}

	// Nothing on the server acts on this frame.
	consumed := obs.count(TypeMetrics) + obs.count(TypeHeartbeat) + obs.count(TypeReparented)
	if consumed != 0 {
		t.Errorf("the Observer was handed %d relayed frames; a peer-to-peer negotiation "+
			"step must not enter the coordinator's event stream", consumed)
	}

	// Mis-addressed frames are reported, not dropped.
	if err := wsjson.Write(ctx, connA, Message{Type: wireBackupPromote}); err != nil {
		t.Fatalf("A write unaddressed backup-promote: %v", err)
	}
	errFrame := readNext(ctx, t, connA)
	if errFrame.Type != TypeError || !strings.Contains(errFrame.Error, string(wireBackupPromote)) {
		t.Errorf("unaddressed backup-promote got %+v, want an error naming the type", errFrame)
	}
}
