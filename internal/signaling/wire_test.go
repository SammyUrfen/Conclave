package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/SammyUrfen/conclave/internal/policy"
)

// TestFrameVocabulary pins the wire values. They are the one thing a peer built
// from a different commit still has to agree on, so a rename must fail here rather
// than in a cross-version call.
func TestFrameVocabulary(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "heartbeat", got: string(TypeHeartbeat), want: "heartbeat"},
		{name: "reparented", got: string(TypeReparented), want: "reparented"},
		{name: "coordinator", got: string(TypeCoordinator), want: "coordinator"},
		{name: "membership", got: string(TypeMembership), want: "membership"},
		{name: "server id", got: ServerID, want: "_server"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}

// TestHubRoutesControlFrames covers the frozen routing rules: liveness frames
// terminate at the server and reach the Observer with a SERVER-STAMPED sender id,
// a peer-originated topology is relayed (the Hub does not police the control
// plane), and server-originated types are refused from a peer.
func TestHubRoutesControlFrames(t *testing.T) {
	obs := &recordingObserver{}
	srv, wsURL, _ := newWireServer(t, HubConfig{}, obs)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connA := dial(ctx, t, wsURL+"&name=a")
	defer connA.CloseNow()
	idA := readMsg(ctx, t, connA).To
	readMsg(ctx, t, connA) // A's own membership snapshot

	connB := dial(ctx, t, wsURL+"&name=b")
	defer connB.CloseNow()
	idB := readMsg(ctx, t, connB).To
	drain(ctx, t, connA, 2) // peer-joined + membership
	readMsg(ctx, t, connB)  // B's membership snapshot

	t.Run("heartbeat terminates at the server", func(t *testing.T) {
		body := json.RawMessage(`{"name":"a","seq":1}`)
		write(ctx, t, connA, Message{Type: TypeHeartbeat, Payload: body})
		waitFor(t, "observer sees the heartbeat", func() bool { return obs.count(TypeHeartbeat) == 1 })
		room, peer, payload := obs.last(TypeHeartbeat)
		if room != "demo" || peer != idA {
			t.Errorf("heartbeat attributed to (%q, %q), want (%q, %q)", room, peer, "demo", idA)
		}
		if string(payload) != string(body) {
			t.Errorf("payload = %s, want %s", payload, body)
		}
		assertNotRelayed(ctx, t, connA, connB, idB, "heartbeat")
	})

	t.Run("reparented terminates at the server", func(t *testing.T) {
		write(ctx, t, connA, Message{Type: TypeReparented, Payload: json.RawMessage(`{"name":"a","ok":true}`)})
		waitFor(t, "observer sees the reparent", func() bool { return obs.count(TypeReparented) == 1 })
		if _, peer, _ := obs.last(TypeReparented); peer != idA {
			t.Errorf("reparent attributed to %q, want %q", peer, idA)
		}
		assertNotRelayed(ctx, t, connA, connB, idB, "reparented")
	})

	t.Run("peer topology is relayed", func(t *testing.T) {
		write(ctx, t, connA, Message{Type: TypeTopology, To: idB, Payload: json.RawMessage(`{"rev":7}`)})
		got := readMsg(ctx, t, connB)
		if got.Type != TypeTopology {
			t.Fatalf("B got %q, want %q", got.Type, TypeTopology)
		}
		if got.From != idA {
			t.Errorf("relayed topology From = %q, want the stamped sender %q", got.From, idA)
		}
	})

	t.Run("server-originated types are refused from a peer", func(t *testing.T) {
		for _, ty := range []Type{TypeCoordinator, TypeMembership, TypeJoined} {
			write(ctx, t, connA, Message{Type: ty, To: idB, Payload: json.RawMessage(`{}`)})
			assertNotRelayed(ctx, t, connA, connB, idB, string(ty))
		}
	})
}

// TestMembershipFrame covers the membership snapshot an ELECTED coordinator peer
// needs. Without it a peer-hosted coordinator learns joins only implicitly from the
// next heartbeat and learns graceful leaves not at all, so the join/leave threshold
// events would be unreachable in Phase 6 — a fidelity gap between the two
// coordinator hosts that would only surface after a handover.
func TestMembershipFrame(t *testing.T) {
	obs := &recordingObserver{}
	srv, wsURL, hub := newWireServer(t, HubConfig{}, obs)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connA := dial(ctx, t, wsURL+"&name=relay")
	defer connA.CloseNow()
	readMsg(ctx, t, connA) // joined

	t.Run("a join broadcasts the full roster", func(t *testing.T) {
		got := readMsg(ctx, t, connA)
		if got.Type != TypeMembership {
			t.Fatalf("first frame after joined = %q, want %q", got.Type, TypeMembership)
		}
		if got.From != ServerID {
			t.Errorf("membership From = %q, want %q", got.From, ServerID)
		}
		if len(got.Peers) != 1 || got.Peers[0].Name != "relay" {
			t.Errorf("roster = %+v, want exactly [relay]", got.Peers)
		}
	})

	connB := dial(ctx, t, wsURL+"&name=leaf")
	defer connB.CloseNow()
	readMsg(ctx, t, connB) // joined

	t.Run("incumbents see the newcomer in a fresh snapshot", func(t *testing.T) {
		readMsg(ctx, t, connA) // peer-joined
		got := readMsg(ctx, t, connA)
		if got.Type != TypeMembership || len(got.Peers) != 2 {
			t.Fatalf("A membership after B joined = %+v, want a 2-member roster", got)
		}
		readMsg(ctx, t, connB) // B's own snapshot
	})

	t.Run("a graceful leave broadcasts the shrunken roster", func(t *testing.T) {
		if err := connB.Close(websocket.StatusNormalClosure, "bye"); err != nil {
			t.Fatalf("B close: %v", err)
		}
		readMsg(ctx, t, connA) // peer-left
		got := readMsg(ctx, t, connA)
		if got.Type != TypeMembership {
			t.Fatalf("A got %q after B left, want %q", got.Type, TypeMembership)
		}
		if len(got.Peers) != 1 || got.Peers[0].Name != "relay" {
			t.Errorf("roster after leave = %+v, want exactly [relay]", got.Peers)
		}
	})

	t.Run("Roster is a deterministic total order", func(t *testing.T) {
		first := hub.Roster("demo")
		for i := 0; i < 20; i++ {
			again := hub.Roster("demo")
			if len(again) != len(first) {
				t.Fatalf("roster size changed: %d then %d", len(first), len(again))
			}
			for j := range first {
				if again[j] != first[j] {
					t.Fatalf("roster order is not stable at %d: %+v vs %+v", j, again, first)
				}
			}
		}
	})

	t.Run("Roster of an unknown meet is empty", func(t *testing.T) {
		if got := hub.Roster("no-such-room"); len(got) != 0 {
			t.Errorf("Roster of an unknown meet = %+v, want empty", got)
		}
	})
}

// TestHeartbeatCanResurrectAGonePeer pins the seam for the "declared gone but the
// socket never closed" hole. The Hub keeps no health state: it delivers a heartbeat
// for every LIVE socket, and Roster reports every live socket, whatever verdict the
// control plane has reached. So a control plane that declared this peer gone can
// resurrect it from the heartbeat alone — the callback carries the server-stamped
// id and the payload carries the name, which is everything a re-join needs.
func TestHeartbeatCanResurrectAGonePeer(t *testing.T) {
	obs := &recordingObserver{}
	srv, wsURL, hub := newWireServer(t, HubConfig{}, obs)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dial(ctx, t, wsURL+"&name=relay")
	defer conn.CloseNow()
	id := readMsg(ctx, t, conn).To

	// Simulate the control plane having given up on this peer: it forgets everything
	// it was ever told. The Hub must still be able to tell it the peer is back.
	obs.forget()

	write(ctx, t, conn, Message{Type: TypeHeartbeat, Payload: json.RawMessage(`{"name":"relay","seq":9}`)})
	waitFor(t, "a heartbeat after the control plane gave up", func() bool { return obs.count(TypeHeartbeat) == 1 })

	_, peer, payload := obs.last(TypeHeartbeat)
	if peer != id {
		t.Errorf("heartbeat peer id = %q, want %q", peer, id)
	}
	if !strings.Contains(string(payload), `"name":"relay"`) {
		t.Errorf("heartbeat payload %s carries no name, so no resurrection is possible", payload)
	}
	if r := hub.Roster("demo"); len(r) != 1 || r[0].ID != id {
		t.Errorf("Roster = %+v, want the live socket regardless of any health verdict", r)
	}
}

// TestSendRoomBroadcasts covers Hub.SendRoom: every member of the meet gets the
// frame, the count is returned, and a server-originated frame is stamped ServerID
// so a peer can apply one uniform fencing rule in both phases.
func TestSendRoomBroadcasts(t *testing.T) {
	srv, wsURL, hub := newWireServer(t, HubConfig{}, nil)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connA := dial(ctx, t, wsURL)
	defer connA.CloseNow()
	drain(ctx, t, connA, 2) // joined + membership
	connB := dial(ctx, t, wsURL)
	defer connB.CloseNow()
	drain(ctx, t, connB, 2)
	drain(ctx, t, connA, 2) // peer-joined + membership

	t.Run("reaches every member", func(t *testing.T) {
		if n := hub.SendRoom("demo", Message{Type: TypeCoordinator, Payload: json.RawMessage(`{"epoch":3}`)}); n != 2 {
			t.Fatalf("SendRoom reached %d members, want 2", n)
		}
		for name, conn := range map[string]*websocket.Conn{"A": connA, "B": connB} {
			got := readMsg(ctx, t, conn)
			if got.Type != TypeCoordinator {
				t.Errorf("%s got %q, want %q", name, got.Type, TypeCoordinator)
			}
			if got.From != ServerID {
				t.Errorf("%s From = %q, want %q", name, got.From, ServerID)
			}
		}
	})

	t.Run("unknown meet reaches nobody", func(t *testing.T) {
		if n := hub.SendRoom("no-such-room", Message{Type: TypeCoordinator}); n != 0 {
			t.Errorf("SendRoom into an empty meet reached %d, want 0", n)
		}
	})
}

// TestSendToStamping proves SendTo stamps ServerID only when the frame really is
// server-originated. A frame the server FORWARDS on behalf of a peer (the Phase 6
// path where telemetry is relayed to an elected coordinator) keeps the original
// sender's id, or the receiver's fence would attribute it to the arbiter.
func TestSendToStamping(t *testing.T) {
	srv, wsURL, hub := newWireServer(t, HubConfig{}, nil)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dial(ctx, t, wsURL)
	defer conn.CloseNow()
	id := readMsg(ctx, t, conn).To
	readMsg(ctx, t, conn) // membership

	tests := []struct {
		name     string
		from     string
		wantFrom string
	}{
		{name: "server originated", from: "", wantFrom: ServerID},
		{name: "forwarded on behalf of a peer", from: "p9", wantFrom: "p9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !hub.SendTo("demo", id, Message{Type: TypeTopology, From: tt.from}) {
				t.Fatal("SendTo reported the peer absent")
			}
			if got := readMsg(ctx, t, conn); got.From != tt.wantFrom {
				t.Errorf("From = %q, want %q", got.From, tt.wantFrom)
			}
		})
	}
}

// TestRoomIDValidation closes the gap between the meet id the HTTP API will accept
// and the ?room= value /ws accepts. A meet that only one of the two admits is a meet
// the dashboard can create but never render, or vice versa.
func TestRoomIDValidation(t *testing.T) {
	// RULING A (docs/PLAN.md §2.4): RoomIDPattern/ValidRoomID now DELEGATE to
	// internal/policy rather than owning the rule, so the dashboard's future
	// meet-creation endpoint can share it without importing this package. Pin the
	// delegation itself — not just matching behaviour on a few sample ids — so a
	// future edit that reintroduces a second copy of the pattern fails here even if
	// nobody thinks to update the table below.
	t.Run("RoomIDPattern and ValidRoomID are policy aliases, not a second copy", func(t *testing.T) {
		if RoomIDPattern != policy.MeetIDPattern {
			t.Errorf("RoomIDPattern = %q, want the literal policy.MeetIDPattern %q", RoomIDPattern, policy.MeetIDPattern)
		}
		for _, id := range []string{"demo", "", "Demo", "a_b-c9", "café"} {
			if got, want := ValidRoomID(id), policy.ValidMeetID(id); got != want {
				t.Errorf("ValidRoomID(%q) = %v, policy.ValidMeetID(%q) = %v; they diverged", id, got, id, want)
			}
		}
	})

	t.Run("ValidRoomID", func(t *testing.T) {
		tests := []struct {
			id   string
			want bool
		}{
			{id: "demo", want: true},
			{id: "default", want: true},
			{id: "a", want: true},
			{id: "9lives", want: true},
			{id: "a_b-c9", want: true},
			{id: strings.Repeat("a", 64), want: true},
			{id: "", want: false},
			{id: strings.Repeat("a", 65), want: false},
			{id: "Demo", want: false},
			{id: "-lead", want: false},
			{id: "_lead", want: false},
			{id: "a b", want: false},
			{id: "a/b", want: false},
			{id: "a.b", want: false},
			{id: "café", want: false},
		}
		for _, tt := range tests {
			t.Run(tt.id, func(t *testing.T) {
				if got := ValidRoomID(tt.id); got != tt.want {
					t.Errorf("ValidRoomID(%q) = %v, want %v", tt.id, got, tt.want)
				}
			})
		}
	})

	srv, wsURL, _ := newWireServer(t, HubConfig{}, nil)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("an invalid room is refused loudly", func(t *testing.T) {
		conn := dial(ctx, t, strings.Replace(wsURL, "room=demo", "room=BAD/ROOM", 1))
		defer conn.CloseNow()
		got := readMsg(ctx, t, conn)
		if got.Type != TypeError {
			t.Fatalf("got %q, want %q", got.Type, TypeError)
		}
		if !strings.Contains(got.Error, "BAD/ROOM") {
			t.Errorf("error %q does not name the rejected meet", got.Error)
		}
	})

	t.Run("an absent room still defaults", func(t *testing.T) {
		conn := dial(ctx, t, strings.Replace(wsURL, "?room=demo", "", 1))
		defer conn.CloseNow()
		if got := readMsg(ctx, t, conn); got.Type != TypeJoined {
			t.Fatalf("got %q, want %q", got.Type, TypeJoined)
		}
	})
}

// TestHubOriginPolicy covers the origin allow-list: the browser dashboard lives on a
// different origin than the arbiter, so the upgrade must be gated on an explicit
// list rather than left same-origin-only or opened with InsecureSkipVerify.
func TestHubOriginPolicy(t *testing.T) {
	srv, wsURL, _ := newWireServer(t, HubConfig{
		AllowedOrigins: []string{"https://sammyurfen.github.io", "http://localhost:*"},
	}, nil)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name    string
		origin  string
		wantErr bool
	}{
		{name: "no origin (a Go peer)", origin: ""},
		{name: "listed pages origin", origin: "https://sammyurfen.github.io"},
		{name: "port wildcard", origin: "http://localhost:5173"},
		{name: "unlisted origin", origin: "https://evil.example", wantErr: true},
		{name: "right host wrong scheme", origin: "http://sammyurfen.github.io", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts *websocket.DialOptions
			if tt.origin != "" {
				opts = &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{tt.origin}}}
			}
			conn, _, err := websocket.Dial(ctx, wsURL, opts)
			if err == nil {
				defer conn.CloseNow()
			}
			if tt.wantErr && err == nil {
				t.Fatalf("origin %q was accepted, want the upgrade refused", tt.origin)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("origin %q was refused: %v", tt.origin, err)
			}
		})
	}
}

// TestOriginsMatchAgreesWithWebsocketAccept is the one assertion that is the
// entire justification for internal/policy existing (docs/PLAN.md §2.4, §12.2):
// Origins.Match — used by the future dashboard CORS surface — MUST accept exactly
// the set that Origins.Patterns() makes the REAL websocket.Accept accept. This
// package may import both signaling and policy, so the check runs against the
// genuine library rather than a second copy of the matching algorithm.
//
// Empty origin is deliberately excluded: websocket.Accept always allows an absent
// Origin header (it means a non-browser client, like our Go peer, sent no CORS
// preflight at all), whereas Origins.Match(("")) correctly refuses to match
// anything — there is no dashboard use case for CORS-approving a request with no
// Origin. That is an intentional difference in what the two are FOR, not a case
// the "same set" property covers; TestHubOriginPolicy's "no origin" case already
// exercises the websocket.Accept side of it.
func TestOriginsMatchAgreesWithWebsocketAccept(t *testing.T) {
	origins, err := policy.ParseOrigins("https://sammyurfen.github.io,http://localhost:*")
	if err != nil {
		t.Fatalf("ParseOrigins: %v", err)
	}
	srv, wsURL, _ := newWireServer(t, HubConfig{AllowedOrigins: origins}, nil)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	candidates := []string{
		"https://sammyurfen.github.io",
		"http://localhost:5173",
		"http://localhost",
		"https://evil.example",
		"http://sammyurfen.github.io",
		"HTTPS://SAMMYURFEN.GITHUB.IO",
		"https://sammyurfen.github.io.evil.example",
	}
	for _, origin := range candidates {
		t.Run(origin, func(t *testing.T) {
			_, wantAllowed := origins.Match(origin)

			opts := &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{origin}}}
			conn, _, err := websocket.Dial(ctx, wsURL, opts)
			gotAllowed := err == nil
			if conn != nil {
				conn.CloseNow()
			}
			if gotAllowed != wantAllowed {
				t.Errorf("origin %q: policy.Origins.Match says allowed=%v, but websocket.Accept (via Patterns()) says allowed=%v",
					origin, wantAllowed, gotAllowed)
			}
		})
	}
}

// TestHubPingReapsDeadSocket is the whole point of the WS keepalive: a TCP socket
// that is dead but never FIN'd leaves wsjson.Read blocked forever, so without a
// protocol ping the Hub would carry a ghost member in the roster indefinitely and
// the control plane would keep computing a tree around a peer that is gone.
func TestHubPingReapsDeadSocket(t *testing.T) {
	obs := &recordingObserver{}
	// Tiny, named durations: the reap must be observable inside a unit test, and the
	// production constants would make this a multi-second test.
	const pingEvery = 60 * time.Millisecond
	const pongWithin = 30 * time.Millisecond
	srv, wsURL, hub := newWireServer(t, HubConfig{PingInterval: pingEvery, PingTimeout: pongWithin}, obs)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Dial and then NEVER read. coder/websocket answers a ping from inside a Read
	// call, so a client that never reads never pongs — a faithful stand-in for a
	// sleeping laptop whose socket is still open at the TCP layer.
	conn := dial(ctx, t, wsURL)
	defer conn.CloseNow()

	waitFor(t, "the mute peer to be registered", func() bool { return obs.joins() == 1 })
	waitFor(t, "the mute peer to be reaped by the ping", func() bool { return obs.leaves() == 1 })
	if r := hub.Roster("demo"); len(r) != 0 {
		t.Errorf("reaped peer still in the roster: %+v", r)
	}
}

// TestHubJoinsItsPerConnectionGoroutines proves the third (ping) goroutine is joined
// rather than merely cancelled: a leak of one goroutine per connection is invisible
// in a two-peer test and fatal in a long-lived server.
func TestHubJoinsItsPerConnectionGoroutines(t *testing.T) {
	obs := &recordingObserver{}
	srv, wsURL, _ := newWireServer(t, HubConfig{}, obs)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const rounds = 8
	base := runtime.NumGoroutine()
	for i := 0; i < rounds; i++ {
		conn := dial(ctx, t, wsURL)
		readMsg(ctx, t, conn)
		if err := conn.Close(websocket.StatusNormalClosure, "bye"); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}
	waitFor(t, "every connection to be unregistered", func() bool { return obs.leaves() == rounds })
	// Slack of 4 absorbs the http server's own transient goroutines; a per-connection
	// leak would show up as +8 and could not hide under it.
	waitFor(t, "the per-connection goroutines to be joined", func() bool {
		return runtime.NumGoroutine() <= base+4
	})
}

// TestHubConfigDefaults proves NewHub still works unchanged (every Phase 1-4 call
// site keeps compiling), that a zero HubConfig is a usable Hub rather than a
// nil-clock panic waiting to happen, and that an inconsistent ping config fails
// loud at construction instead of silently never detecting a dead socket.
func TestHubConfigDefaults(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("NewHub is unchanged", func(t *testing.T) {
		h := NewHub(log)
		if h.clk == nil || h.pingInterval != WSPingInterval || h.pingTimeout != WSPingTimeout {
			t.Errorf("NewHub gave clk=%v interval=%v timeout=%v, want the defaults", h.clk, h.pingInterval, h.pingTimeout)
		}
	})

	t.Run("zero config gets defaults", func(t *testing.T) {
		h, err := NewHubWithConfig(HubConfig{Log: log})
		if err != nil {
			t.Fatalf("NewHubWithConfig(zero config): %v", err)
		}
		if h.clk == nil || h.pingInterval != WSPingInterval || h.pingTimeout != WSPingTimeout {
			t.Errorf("zero config gave clk=%v interval=%v timeout=%v, want the defaults", h.clk, h.pingInterval, h.pingTimeout)
		}
	})

	t.Run("the liveness budget is the sum", func(t *testing.T) {
		if WSLivenessBudget != WSPingInterval+WSPingTimeout {
			t.Errorf("WSLivenessBudget = %v, want %v", WSLivenessBudget, WSPingInterval+WSPingTimeout)
		}
		if WSPingTimeout >= WSPingInterval {
			t.Errorf("WSPingTimeout %v must be shorter than WSPingInterval %v", WSPingTimeout, WSPingInterval)
		}
	})

	// RULING B (docs/PLAN.md §2.4/§4.2, §15.4): PingInterval/PingTimeout are
	// operator input (they arrive from flags, same family as -allowed-origins), not
	// a programmer error, so a self-contradictory pair must be reported as an error
	// value — the project's fail-loud-at-startup rule translated by run() error into
	// one sentence on stderr — rather than a panic whose stack trace names no flag.
	t.Run("an inconsistent ping config is a reported error, not a panic", func(t *testing.T) {
		h, err := NewHubWithConfig(HubConfig{Log: log, PingInterval: time.Second, PingTimeout: 2 * time.Second})
		if err == nil {
			t.Fatal("a pong timeout longer than the ping interval was accepted")
		}
		if h != nil {
			t.Errorf("got a non-nil Hub alongside the error: %+v", h)
		}
		if !errors.Is(err, ErrInvalidPingConfig) {
			t.Errorf("error %v does not wrap ErrInvalidPingConfig", err)
		}
		// Actionable: names the two config values and their values, not just "bad
		// config" — the whole point of moving off panic.
		for _, want := range []string{"PingInterval", "PingTimeout", "1s", "2s"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err.Error(), want)
			}
		}
	})

	t.Run("an inconsistent config never panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("NewHubWithConfig panicked: %v", r)
			}
		}()
		_, _ = NewHubWithConfig(HubConfig{Log: log, PingInterval: time.Second, PingTimeout: time.Second})
	})
}

// --- helpers -------------------------------------------------------------

// recordingObserver is a test double for the full Observer surface. The
// compile-time assertion below is the real test of the frozen method set.
type recordingObserver struct {
	mu     sync.Mutex
	joined int
	left   int
	frames map[Type][]observed
}

type observed struct {
	room, peer string
	payload    []byte
}

var _ Observer = (*recordingObserver)(nil)

func (o *recordingObserver) PeerJoined(roomID, peerID, name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.joined++
}

func (o *recordingObserver) PeerLeft(roomID, peerID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.left++
}

func (o *recordingObserver) Metrics(roomID, peerID string, payload []byte) {
	o.record(TypeMetrics, roomID, peerID, payload)
}

func (o *recordingObserver) Heartbeat(roomID, peerID string, payload []byte) {
	o.record(TypeHeartbeat, roomID, peerID, payload)
}

func (o *recordingObserver) Reparented(roomID, peerID string, payload []byte) {
	o.record(TypeReparented, roomID, peerID, payload)
}

func (o *recordingObserver) record(t Type, room, peer string, payload []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frames == nil {
		o.frames = make(map[Type][]observed)
	}
	o.frames[t] = append(o.frames[t], observed{room: room, peer: peer, payload: append([]byte(nil), payload...)})
}

// forget drops everything this observer was ever told — the control plane having
// declared a peer gone and dropped it from its node set.
func (o *recordingObserver) forget() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.joined, o.left, o.frames = 0, 0, nil
}

func (o *recordingObserver) count(t Type) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.frames[t])
}

func (o *recordingObserver) last(t Type) (room, peer string, payload []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f := o.frames[t]
	if len(f) == 0 {
		return "", "", nil
	}
	l := f[len(f)-1]
	return l.room, l.peer, l.payload
}

func (o *recordingObserver) joins() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.joined
}

func (o *recordingObserver) leaves() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.left
}

// newWireServer mounts a Hub built from cfg (Log is filled in) on an httptest
// server and returns it, the ws:// URL for room "demo", and the Hub itself.
func newWireServer(t *testing.T, cfg HubConfig, obs Observer) (*httptest.Server, string, *Hub) {
	t.Helper()
	cfg.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	hub, err := NewHubWithConfig(cfg)
	if err != nil {
		t.Fatalf("NewHubWithConfig: %v", err)
	}
	if obs != nil {
		hub.SetObserver(obs)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", hub.ServeWS)
	srv := httptest.NewServer(mux)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?room=demo", hub
}

func write(ctx context.Context, t *testing.T, conn *websocket.Conn, msg Message) {
	t.Helper()
	if err := wsjson.Write(ctx, conn, msg); err != nil {
		t.Fatalf("write %s: %v", msg.Type, err)
	}
}

func drain(ctx context.Context, t *testing.T, conn *websocket.Conn, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		readMsg(ctx, t, conn)
	}
}

// assertNotRelayed proves a frame just sent by from was NOT forwarded to to, by
// chasing it with a sentinel that definitely IS forwarded: if the next frame the
// receiver sees is the sentinel, nothing slipped in ahead of it.
//
// The obvious alternative — read with a short timeout and expect it to expire —
// cannot be used here: coder/websocket closes the connection when a read context
// expires (it cannot resynchronise a half-read stream), so the negative assertion
// would destroy the socket the rest of the test needs.
func assertNotRelayed(ctx context.Context, t *testing.T, from, to *websocket.Conn, toID, what string) {
	t.Helper()
	const sentinel = `"sentinel"`
	write(ctx, t, from, Message{Type: TypeOffer, To: toID, SDP: json.RawMessage(sentinel)})
	got := readMsg(ctx, t, to)
	if got.Type != TypeOffer || string(got.SDP) != sentinel {
		t.Errorf("%s leaked to another peer: received %+v before the sentinel", what, got)
	}
}

// waitFor polls pred until it holds. Polling (rather than sleeping a fixed
// duration) keeps the fast path fast and the slow path non-flaky.
func waitFor(t *testing.T, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
