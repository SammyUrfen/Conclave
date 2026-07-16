package media

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/signaling"
)

// TestMeshThreePeersFullyConnect is the Phase 2 acceptance test: three real peers
// join one room through the real signaling Hub and must form a full mesh — every
// peer holds a connected PeerConnection to each of the other two (3 pairs, 6
// directed sessions). Unlike the Phase 1 test's in-memory transport, this drives
// the actual Hub fan-out (joined roster + peer-joined) over loopback WebSockets,
// which is what exercises N>2 signaling and per-pair glare resolution at once.
//
// It also asserts the upload meter observes N-1 outbound peers per sender, and that
// cancelling the context joins every Router goroutine (the WaitGroup returns) — a
// leak would hang this test, and it runs under -race to catch the callback races
// the mesh invites.
func TestMeshThreePeersFullyConnect(t *testing.T) {
	const N = 3
	// Silent by default; set MESH_DEBUG=1 to watch the offer/answer/ICE timeline —
	// the first thing you want when a mesh won't converge.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("MESH_DEBUG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	hub := signaling.NewHub(logger)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	routers := make([]*Router, N)
	var wg sync.WaitGroup
	for i := range routers {
		client, err := signaling.Dial(ctx, logger, srv.URL, "mesh")
		if err != nil {
			t.Fatalf("peer %d dial: %v", i, err)
		}
		defer client.Close()

		// Every peer sends synthetic media, so each opens an outbound track toward
		// each other peer — the O(N) upload fan-out this phase is about.
		r := NewRouter(logger, client, RouterConfig{SendMedia: true})
		routers[i] = r
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Run(ctx)
		}()
	}

	// Poll until the mesh converges: every peer reports N-1 peers, all connected.
	// Loopback ICE+DTLS settles in well under a second, but joins race and glare
	// takes a round trip, so give it a generous window and fail with a snapshot.
	deadline := time.Now().Add(20 * time.Second)
	for !meshConnected(routers, N-1) {
		if time.Now().After(deadline) {
			t.Fatalf("mesh did not fully connect:\n%s", describeMesh(routers))
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Each sender should have exactly N-1 live outbound pumps feeding the meter.
	for i, r := range routers {
		if got := r.Stats().UploadPeers; got != N-1 {
			t.Errorf("peer %d: UploadPeers = %d, want %d", i, got, N-1)
		}
	}

	// Clean shutdown: cancelling the context must drain every Router's goroutines.
	// If any pump or the meter leaked, wg.Wait blocks and the test times out.
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("routers did not shut down cleanly — a goroutine leaked")
	}
}

// meshConnected reports whether every router sees exactly want peers, all in the
// connected state AND with a media track received from each — i.e. a full mesh with
// media flowing both ways over every pair (the single-offerer sendrecv m-line).
func meshConnected(routers []*Router, want int) bool {
	for _, r := range routers {
		st := r.Stats()
		if len(st.Peers) != want {
			return false
		}
		for id, state := range st.Peers {
			if state != webrtc.PeerConnectionStateConnected || !st.Received[id] {
				return false
			}
		}
	}
	return true
}

// describeMesh renders each router's peer states for a failure message.
func describeMesh(routers []*Router) string {
	var b strings.Builder
	for i, r := range routers {
		fmt.Fprintf(&b, "  peer %d: ", i)
		for id, state := range r.Stats().Peers {
			fmt.Fprintf(&b, "%s=%s ", id, state)
		}
		b.WriteByte('\n')
	}
	return b.String()
}
