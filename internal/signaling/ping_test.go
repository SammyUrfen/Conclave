package signaling

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestClientPingRoundTrips proves the peer side can measure its control link at all:
// coder/websocket's Conn.Ping blocks until the matching pong arrives, so a returned
// nil error IS the round trip. It needs a concurrent reader to consume the pong, and
// the Client's readPump is it — which is precisely what this test would fail to prove
// if Ping were bolted onto a Client whose read pump had stopped.
func TestClientPingRoundTrips(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := Dial(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), srv.URL, "demo", "prober")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	for i := range 3 {
		if err := c.Ping(ctx); err != nil {
			t.Fatalf("ping %d: %v", i, err)
		}
	}
}

// TestClientPingFailsOnAClosedClient pins the error path the RTT probe depends on. A
// probe that cannot distinguish "pong came back" from "the socket is gone" would
// report a dead link as a fast one — see metrics.TestFailedProbeReportsTheUnreachableCeiling
// for the consequence.
//
// The mutation this catches: a Ping implementation that swallows the transport error
// and returns nil.
func TestClientPingFailsOnAClosedClient(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := Dial(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), srv.URL, "demo", "prober")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("setup ping: %v", err)
	}
	c.Close()

	if err := c.Ping(ctx); err == nil {
		t.Fatal("ping on a closed client returned nil; a dead control link would be reported as a healthy one")
	}
}

// TestClientPingRespectsItsDeadline pins that the caller's context bounds the wait.
// The RTT probe hands Ping a short deadline and treats expiry as "unreachable", so a
// Ping that ignored the context would hang the probe goroutine for the life of the
// process and freeze RTTServerMs at its last good value.
func TestClientPingRespectsItsDeadline(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := Dial(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), srv.URL, "demo", "prober")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	dead, cancelDead := context.WithCancel(ctx)
	cancelDead() // already expired before the call
	if err := c.Ping(dead); err == nil {
		t.Fatal("ping with an expired context returned nil")
	}
}
