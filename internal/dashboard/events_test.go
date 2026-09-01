package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// wireFrame mirrors the §9.4 envelope exactly, as a SEPARATE declaration from the
// production type. Decoding into the same struct the handler encodes would make a
// renamed json tag invisible to every test in this file.
type wireFrame struct {
	APIVersion int             `json:"api_version"`
	Seq        uint64          `json:"seq"`
	AtUnixMs   int64           `json:"at_unix_ms"`
	MeetID     string          `json:"meet_id"`
	Epoch      uint64          `json:"epoch"`
	Rev        uint64          `json:"rev"`
	Kind       string          `json:"kind"`
	Data       json.RawMessage `json:"data"`
}

// wsTimeout bounds every blocking read in this file. Generous enough that a loaded CI
// box does not flake, short enough that a genuinely wedged handler fails the test
// instead of hanging the suite until the go test deadline.
const wsTimeout = 5 * time.Second

// dialEvents opens the meet's event stream. origin, when non-empty, is sent as the
// Origin header so the upgrade-time origin check can be exercised.
func dialEvents(t *testing.T, url, meetID, origin string) *websocket.Conn {
	t.Helper()
	conn, res, err := dialEventsErr(t, url, meetID, origin)
	if err != nil {
		status := 0
		if res != nil {
			status = res.StatusCode
		}
		t.Fatalf("dial %s: %v (http status %d)", meetID, err, status)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

func dialEventsErr(t *testing.T, url, meetID, origin string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	wsURL := strings.Replace(url, "http://", "ws://", 1) + "/api/meets/" + meetID + "/events"
	opts := &websocket.DialOptions{HTTPHeader: http.Header{}}
	if origin != "" {
		opts.HTTPHeader.Set("Origin", origin)
	}
	ctx, cancel := context.WithTimeout(context.Background(), wsTimeout)
	defer cancel()
	return websocket.Dial(ctx, wsURL, opts)
}

func readFrame(t *testing.T, conn *websocket.Conn) wireFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wsTimeout)
	defer cancel()
	_, raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	var f wireFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("frame is not JSON (%v): %s", err, raw)
	}
	if f.APIVersion != APIVersion {
		t.Errorf("frame api_version = %d, want %d", f.APIVersion, APIVersion)
	}
	if f.Data == nil {
		t.Errorf("frame %q has no data object; §9.4 says data is always present", f.Kind)
	}
	return f
}

func writeOp(t *testing.T, conn *websocket.Conn, op string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wsTimeout)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"op":"`+op+`"}`)); err != nil {
		t.Fatalf("write %s: %v", op, err)
	}
}

func standupServer(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()
	if cfg.Meets == nil {
		cfg.Meets = &fakeMeets{live: []arbiter.Meet{sampleMeet()}}
	}
	if cfg.Subnet == nil {
		sub := &fakeSubnet{}
		sub.set(sampleSnapshot())
		cfg.Subnet = sub
	}
	s, ts := newTestServer(t, cfg)
	return s, ts.URL
}

// TestEventStreamSnapshotFirst pins §9.3: the first frame is ALWAYS a snapshot, it is
// seq 1 for this connection, and it carries the same body GET /api/meets/{id} returns.
// Deltas follow at 2, 3, … per connection (§9.4).
func TestEventStreamSnapshotFirst(t *testing.T) {
	s, url := standupServer(t, Config{})
	conn := dialEvents(t, url, "standup", "https://sammyurfen.github.io")

	first := readFrame(t, conn)
	if first.Kind != "snapshot" {
		t.Fatalf("first frame kind = %q, want snapshot", first.Kind)
	}
	if first.Seq != 1 {
		t.Errorf("first frame seq = %d, want 1", first.Seq)
	}
	if first.MeetID != "standup" {
		t.Errorf("meet_id = %q", first.MeetID)
	}
	var snap map[string]any
	if err := json.Unmarshal(first.Data, &snap); err != nil {
		t.Fatalf("snapshot data: %v", err)
	}
	if snap["id"] != "standup" || snap["root"] != "alice" || snap["rev"] != float64(11) {
		t.Errorf("snapshot data does not look like the GET body: %v", snap)
	}

	s.Publish(coordinator.Event{Kind: coordinator.EventMember, RoomID: "standup",
		Node: "frank", Present: true, Epoch: 3, Rev: 12, At: testAt})
	s.Publish(coordinator.Event{Kind: coordinator.EventUnbuildable, RoomID: "standup",
		Reason: "no root", Epoch: 3, Rev: 12, At: testAt})

	for i, want := range []struct {
		seq  uint64
		kind string
	}{{2, "member_joined"}, {3, "unbuildable"}} {
		f := readFrame(t, conn)
		if f.Seq != want.seq || f.Kind != want.kind {
			t.Errorf("delta %d = seq %d kind %q, want seq %d kind %q", i, f.Seq, f.Kind, want.seq, want.kind)
		}
	}
}

// TestEventStreamPerConnectionSeq pins that seq is PER CONNECTION (§9.4's resolution of
// v1's self-contradiction), not per meet: a second socket starts its own numbering at 1.
func TestEventStreamPerConnectionSeq(t *testing.T) {
	s, url := standupServer(t, Config{})
	a := dialEvents(t, url, "standup", "")
	if f := readFrame(t, a); f.Seq != 1 {
		t.Fatalf("conn A first seq = %d", f.Seq)
	}
	s.Publish(coordinator.Event{Kind: coordinator.EventMember, RoomID: "standup", Node: "x", Present: true})
	if f := readFrame(t, a); f.Seq != 2 {
		t.Fatalf("conn A second seq = %d", f.Seq)
	}
	b := dialEvents(t, url, "standup", "")
	if f := readFrame(t, b); f.Seq != 1 {
		t.Errorf("conn B first seq = %d, want 1 — seq is per connection, not per meet", f.Seq)
	}
	s.Publish(coordinator.Event{Kind: coordinator.EventMember, RoomID: "standup", Node: "y", Present: true})
	if f := readFrame(t, a); f.Seq != 3 {
		t.Errorf("conn A third seq = %d, want 3", f.Seq)
	}
	if f := readFrame(t, b); f.Seq != 2 {
		t.Errorf("conn B second seq = %d, want 2", f.Seq)
	}
}

// TestEventStreamSeqGapOnDrop is the discriminating test for §9.4's recovery story.
// The server drops frames for a slow consumer RATHER THAN BUFFER WITHOUT BOUND, and
// the client's only way to notice is a gap in seq. So seq must be minted when a frame
// is PRODUCED, not when it is written — a write-time counter would renumber the
// survivors contiguously and hide the loss forever.
//
// The writer is held still by blocking the very first Snapshot call, which makes the
// overflow deterministic instead of timing-dependent.
func TestEventStreamSeqGapOnDrop(t *testing.T) {
	sub := &fakeSubnet{block: make(chan struct{})}
	sub.set(sampleSnapshot())
	s, url := standupServer(t, Config{Subnet: sub})
	conn := dialEvents(t, url, "standup", "")

	// Wait until the writer is parked inside Snapshot, so nothing drains the queue.
	waitFor(t, func() bool {
		sub.mu.Lock()
		defer sub.mu.Unlock()
		return sub.calls > 0
	}, "writer to reach Snapshot")

	const overflow = 5
	total := eventBuffer + overflow
	for i := 0; i < total; i++ {
		s.Publish(coordinator.Event{Kind: coordinator.EventMember, RoomID: "standup",
			Node: fmt.Sprintf("n%02d", i), Present: true, Epoch: 3, Rev: 11, At: testAt})
	}
	close(sub.block)

	// seq 1 is the snapshot; the queue holds the next eventBuffer frames; the last
	// `overflow` were dropped.
	if f := readFrame(t, conn); f.Seq != 1 || f.Kind != "snapshot" {
		t.Fatalf("first frame = seq %d kind %q", f.Seq, f.Kind)
	}
	var last uint64 = 1
	for i := 0; i < eventBuffer; i++ {
		f := readFrame(t, conn)
		if f.Seq != last+1 {
			t.Fatalf("queued frame %d: seq = %d, want %d", i, f.Seq, last+1)
		}
		last = f.Seq
	}

	// The next event a live connection receives must expose the hole.
	sub.mu.Lock()
	sub.block = nil
	sub.mu.Unlock()
	s.Publish(coordinator.Event{Kind: coordinator.EventUnbuildable, RoomID: "standup", Reason: "after the drop"})
	f := readFrame(t, conn)
	if f.Seq == last+1 {
		t.Fatalf("seq %d follows %d with no gap: %d frames were dropped and the client "+
			"can never learn it. seq must be minted at PRODUCE time, not at write time.",
			f.Seq, last, overflow)
	}
	if want := last + uint64(overflow) + 1; f.Seq != want {
		t.Errorf("seq after the drop = %d, want %d (%d dropped)", f.Seq, want, overflow)
	}
}

// TestEventStreamResync pins the gap-recovery path a client takes after seeing that
// hole: {"op":"resync"} answers with a FRESH snapshot, numbered with the next seq so
// the client's own counter stays consistent.
func TestEventStreamResync(t *testing.T) {
	sub := &fakeSubnet{}
	sub.set(sampleSnapshot())
	s, url := standupServer(t, Config{Subnet: sub})
	conn := dialEvents(t, url, "standup", "")
	if f := readFrame(t, conn); f.Seq != 1 {
		t.Fatalf("first seq = %d", f.Seq)
	}
	s.Publish(coordinator.Event{Kind: coordinator.EventMember, RoomID: "standup", Node: "frank", Present: true})
	if f := readFrame(t, conn); f.Seq != 2 {
		t.Fatalf("delta seq = %d", f.Seq)
	}

	// Change the world, then resync: the answer must reflect the NEW state, proving
	// the handler re-reads the source rather than replaying a cached first frame.
	snap := sampleSnapshot()
	snap.Rev = 99
	snap.Topo.Rev = 99
	sub.set(snap)

	writeOp(t, conn, "resync")
	f := readFrame(t, conn)
	if f.Kind != "snapshot" {
		t.Fatalf("resync answered with kind %q, want snapshot", f.Kind)
	}
	if f.Seq != 3 {
		t.Errorf("resync snapshot seq = %d, want 3 (the next per-connection seq)", f.Seq)
	}
	var body map[string]any
	if err := json.Unmarshal(f.Data, &body); err != nil {
		t.Fatal(err)
	}
	if body["rev"] != float64(99) {
		t.Errorf("resync snapshot rev = %v, want 99 — it must be freshly read", body["rev"])
	}
}

// TestEventStreamPingPong pins the client's keepalive: {"op":"ping"} is answered with
// a normal envelope of kind "pong" and data {}, so the client needs no special parse
// path (§9.4a).
func TestEventStreamPingPong(t *testing.T) {
	_, url := standupServer(t, Config{})
	conn := dialEvents(t, url, "standup", "")
	readFrame(t, conn)
	writeOp(t, conn, "ping")
	f := readFrame(t, conn)
	if f.Kind != "pong" {
		t.Fatalf("kind = %q, want pong", f.Kind)
	}
	if string(f.Data) != "{}" {
		t.Errorf("pong data = %s, want {}", f.Data)
	}
	if f.Seq != 2 {
		t.Errorf("pong seq = %d, want 2 — a pong is an envelope like everything else", f.Seq)
	}
}

// TestEventStreamCloseCodes pins the frozen §9.4a close-code table, because the client
// branches on these to decide whether reconnecting can possibly help.
func TestEventStreamCloseCodes(t *testing.T) {
	t.Run("malformed op is 1008", func(t *testing.T) {
		_, url := standupServer(t, Config{})
		conn := dialEvents(t, url, "standup", "")
		readFrame(t, conn)
		ctx, cancel := context.WithTimeout(context.Background(), wsTimeout)
		defer cancel()
		if err := conn.Write(ctx, websocket.MessageText, []byte(`{"op":"drop-everything"}`)); err != nil {
			t.Fatal(err)
		}
		if got := readUntilClose(t, conn); got != websocket.StatusPolicyViolation {
			t.Errorf("close status = %d, want 1008 (client must NOT reconnect)", got)
		}
	})

	t.Run("non-json op is 1008", func(t *testing.T) {
		_, url := standupServer(t, Config{})
		conn := dialEvents(t, url, "standup", "")
		readFrame(t, conn)
		ctx, cancel := context.WithTimeout(context.Background(), wsTimeout)
		defer cancel()
		if err := conn.Write(ctx, websocket.MessageText, []byte(`not json`)); err != nil {
			t.Fatal(err)
		}
		if got := readUntilClose(t, conn); got != websocket.StatusPolicyViolation {
			t.Errorf("close status = %d, want 1008", got)
		}
	})

	t.Run("unknown meet is 4404", func(t *testing.T) {
		_, url := standupServer(t, Config{})
		conn := dialEvents(t, url, "nosuchmeet", "")
		if got := readUntilClose(t, conn); got != websocket.StatusCode(4404) {
			t.Errorf("close status = %d, want 4404", got)
		}
	})

	t.Run("malformed meet id is 4404", func(t *testing.T) {
		_, url := standupServer(t, Config{})
		conn := dialEvents(t, url, "NOT-A-MEET", "")
		if got := readUntilClose(t, conn); got != websocket.StatusCode(4404) {
			t.Errorf("close status = %d, want 4404", got)
		}
	})

	t.Run("shutdown is 1000", func(t *testing.T) {
		s, url := standupServer(t, Config{})
		conn := dialEvents(t, url, "standup", "")
		readFrame(t, conn)
		s.Close()
		if got := readUntilClose(t, conn); got != websocket.StatusNormalClosure {
			t.Errorf("close status = %d, want 1000 (reconnect with backoff)", got)
		}
	})
}

// TestEventStreamOriginRejected pins §9.8: the WS upgrade is origin-checked against
// the SAME allow-list the REST surface uses, and a non-matching origin gets the
// upgrade REFUSED — not a 200 with an empty stream. Both halves matter: the deny case
// proves the check exists, the allow case proves it is not simply deny-everything.
func TestEventStreamOriginRejected(t *testing.T) {
	_, url := standupServer(t, Config{})

	if _, res, err := dialEventsErr(t, url, "standup", "https://evil.example"); err == nil {
		t.Errorf("upgrade succeeded from a non-allowed origin")
	} else if res != nil && res.StatusCode == http.StatusSwitchingProtocols {
		t.Errorf("upgrade returned 101 for a rejected origin")
	}

	conn, _, err := dialEventsErr(t, url, "standup", "https://sammyurfen.github.io")
	if err != nil {
		t.Fatalf("upgrade REFUSED for an allowed origin (%v) — the check is deny-everything", err)
	}
	conn.CloseNow()
}

// TestEventKindMapping is the exhaustive §9.4 fan-in table: every coordinator and
// arbiter event, and the exact `data` shape the frontend's event log destructures.
func TestEventKindMapping(t *testing.T) {
	topo := &overlay.Topology{
		Epoch: 4, Rev: 17, Root: "alice",
		Edges: []overlay.Edge{
			{Parent: "alice", Child: "bob"},
			{Parent: "bob", Child: "carol"},
		},
		Backups: []overlay.Backup{{Node: "carol", Parent: "alice"}},
	}
	tests := []struct {
		name string
		emit func(s *Server)
		kind string
		want map[string]any
	}{
		{
			name: "member_joined",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventMember, RoomID: "standup", Node: "frank", Present: true})
			},
			kind: "member_joined", want: map[string]any{"name": "frank", "id": ""},
		},
		{
			name: "member_left",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventMember, RoomID: "standup", Node: "frank", Present: false})
			},
			kind: "member_left", want: map[string]any{"name": "frank", "id": ""},
		},
		{
			name: "health_changed",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventHealth, RoomID: "standup",
					Node: "bob", Health: coordinator.HealthDegraded, Reason: "3 missed beats"})
			},
			kind: "health_changed",
			want: map[string]any{"name": "bob", "health": "degraded", "prev_health": ""},
		},
		{
			name: "topology",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventTopology, RoomID: "standup",
					Outcome: coordinator.OutcomeRelaxed, Reason: "sticky attempt failed", Topo: topo})
			},
			kind: "topology",
			want: map[string]any{
				"root": "alice", "depth": float64(2), "outcome": "relaxed",
				"reason": "sticky attempt failed",
				"relays": []any{"alice", "bob"},
				"edges": []any{
					map[string]any{"parent": "alice", "child": "bob"},
					map[string]any{"parent": "bob", "child": "carol"},
				},
				"backups": []any{map[string]any{"node": "carol", "parent": "alice"}},
			},
		},
		{
			name: "reparent",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventReparent, RoomID: "standup",
					Node: "dave", PrevParent: "bob", Parent: "carol", Reason: "primary gone"})
			},
			kind: "reparent",
			want: map[string]any{"name": "dave", "from": "bob", "to": "carol",
				"self_promoted": true, "reason": "primary gone"},
		},
		{
			name: "failover",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventFailover, RoomID: "standup",
					Node: "bob", Orphans: []string{"dave", "erin"}, Reroot: true, Reason: "gone"})
			},
			kind: "failover",
			want: map[string]any{"name": "bob", "orphans": []any{"dave", "erin"},
				"reroot": true, "reason": "gone"},
		},
		{
			name: "stale_rejected",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventStale, RoomID: "standup",
					Node: "frank", Epoch: 3, Rev: 15, Reason: "epoch behind current"})
			},
			kind: "stale_rejected",
			want: map[string]any{"name": "frank", "epoch": float64(3), "rev": float64(15),
				"reason": "epoch behind current"},
		},
		{
			name: "settling",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventSettling, RoomID: "standup",
					Waiting: []string{"frank", "gina"}, Reason: "awaiting first heartbeat"})
			},
			kind: "settling",
			want: map[string]any{"waiting": []any{"frank", "gina"}, "reason": "awaiting first heartbeat"},
		},
		{
			name: "unbuildable",
			emit: func(s *Server) {
				s.Publish(coordinator.Event{Kind: coordinator.EventUnbuildable, RoomID: "standup",
					Reason: "no peer can root the tree"})
			},
			kind: "unbuildable", want: map[string]any{"reason": "no peer can root the tree"},
		},
		{
			name: "election",
			emit: func(s *Server) {
				s.PublishElection(arbiter.Announcement{RoomID: "standup", Epoch: 4,
					Coordinator: "alice", CoordinatorID: "p2", Prev: "bob",
					Reason: arbiter.ReasonPromotion, IssuedAt: testAt})
			},
			kind: "election",
			want: map[string]any{"epoch": float64(4), "coordinator": "alice",
				"prev": "bob", "reason": "promotion"},
		},
		{
			name: "election vacated carries an empty coordinator",
			emit: func(s *Server) {
				s.PublishElection(arbiter.Announcement{RoomID: "standup", Epoch: 5,
					Prev: "alice", Reason: arbiter.ReasonVacated, IssuedAt: testAt})
			},
			kind: "election",
			want: map[string]any{"epoch": float64(5), "coordinator": "", "prev": "alice", "reason": "vacated"},
		},
		{
			name: "announce_repair",
			emit: func(s *Server) { s.PublishRepair("standup", "frank", 2, 4, false) },
			kind: "announce_repair",
			want: map[string]any{"name": "frank", "peer_epoch": float64(2),
				"meet_epoch": float64(4), "resolved": false},
		},
		{
			name: "announce_repair resolved",
			emit: func(s *Server) { s.PublishRepair("standup", "frank", 4, 4, true) },
			kind: "announce_repair",
			want: map[string]any{"name": "frank", "peer_epoch": float64(4),
				"meet_epoch": float64(4), "resolved": true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, url := standupServer(t, Config{})
			conn := dialEvents(t, url, "standup", "")
			readFrame(t, conn) // snapshot
			tt.emit(s)
			f := readFrame(t, conn)
			if f.Kind != tt.kind {
				t.Fatalf("kind = %q, want %q", f.Kind, tt.kind)
			}
			var got map[string]any
			if err := json.Unmarshal(f.Data, &got); err != nil {
				t.Fatalf("data: %v", err)
			}
			for k, want := range tt.want {
				if fmt.Sprint(got[k]) != fmt.Sprint(want) {
					t.Errorf("data.%s = %#v, want %#v", k, got[k], want)
				}
			}
		})
	}
}

// TestHealthPrevTracking pins the one field §9.4 asks for that coordinator.Event does
// not carry: prev_health. The dashboard is the only place that sees the whole health
// series, so it remembers the last value per node and fills the field itself.
func TestHealthPrevTracking(t *testing.T) {
	s, url := standupServer(t, Config{})
	conn := dialEvents(t, url, "standup", "")
	readFrame(t, conn)
	for _, step := range []struct {
		h        coordinator.Health
		wantPrev string
	}{
		{coordinator.HealthDegraded, ""},
		{coordinator.HealthGone, "degraded"},
		{coordinator.HealthHealthy, "gone"},
	} {
		s.Publish(coordinator.Event{Kind: coordinator.EventHealth, RoomID: "standup",
			Node: "bob", Health: step.h})
		f := readFrame(t, conn)
		var d map[string]any
		if err := json.Unmarshal(f.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d["prev_health"] != step.wantPrev {
			t.Errorf("health→%s: prev_health = %#v, want %q", step.h, d["prev_health"], step.wantPrev)
		}
		if d["health"] != string(step.h) {
			t.Errorf("health = %#v, want %q", d["health"], step.h)
		}
	}
}

// TestEventsAreMeetScoped pins that a socket sees only its own meet. The path scopes
// the subscription (§9.4a: there is no subscribe/unsubscribe), so a leak here would
// show one meet's failover in another meet's log.
func TestEventsAreMeetScoped(t *testing.T) {
	meets := &fakeMeets{live: []arbiter.Meet{sampleMeet(), {ID: "retro"}}}
	s, url := standupServer(t, Config{Meets: meets})
	conn := dialEvents(t, url, "standup", "")
	readFrame(t, conn)

	s.Publish(coordinator.Event{Kind: coordinator.EventUnbuildable, RoomID: "retro", Reason: "other meet"})
	s.Publish(coordinator.Event{Kind: coordinator.EventUnbuildable, RoomID: "standup", Reason: "mine"})

	f := readFrame(t, conn)
	var d map[string]any
	if err := json.Unmarshal(f.Data, &d); err != nil {
		t.Fatal(err)
	}
	if d["reason"] != "mine" {
		t.Errorf("received another meet's event: %v", d)
	}
	if f.Seq != 2 {
		t.Errorf("seq = %d, want 2 — another meet's event must not consume this connection's seq", f.Seq)
	}
}

// TestStaleRejectedCount pins the counter §9.3 puts in the snapshot. coordinator's
// RoomSnapshot has no such field, so the dashboard owns it — and the count captured
// for a snapshot must be consistent with the deltas that follow: exactly once each,
// never double-counted and never lost.
func TestStaleRejectedCount(t *testing.T) {
	sub := &fakeSubnet{}
	sub.set(sampleSnapshot())
	s, url := standupServer(t, Config{Subnet: sub})

	// Two rejections before anyone is watching.
	for i := 0; i < 2; i++ {
		s.Publish(coordinator.Event{Kind: coordinator.EventStale, RoomID: "standup", Node: "frank"})
	}
	conn := dialEvents(t, url, "standup", "")
	f := readFrame(t, conn)
	var snap map[string]any
	if err := json.Unmarshal(f.Data, &snap); err != nil {
		t.Fatal(err)
	}
	if snap["stale_rejected"] != float64(2) {
		t.Errorf("snapshot stale_rejected = %v, want 2", snap["stale_rejected"])
	}

	// One more after: it arrives as a delta the client adds to the snapshot value.
	s.Publish(coordinator.Event{Kind: coordinator.EventStale, RoomID: "standup", Node: "frank"})
	if got := readFrame(t, conn); got.Kind != "stale_rejected" {
		t.Fatalf("kind = %q", got.Kind)
	}
	writeOp(t, conn, "resync")
	f = readFrame(t, conn)
	if err := json.Unmarshal(f.Data, &snap); err != nil {
		t.Fatal(err)
	}
	if snap["stale_rejected"] != float64(3) {
		t.Errorf("after 3 rejections the resynced snapshot says %v", snap["stale_rejected"])
	}
}

// TestPublishNeverBlocks is the availability contract from §5.8: Publish runs on the
// coordinator's single Run goroutine, so a wedged dashboard consumer must never stall
// it. Here the subscriber's writer is parked inside Snapshot and its queue is long
// past full — Publish must still return promptly, dropping instead of waiting.
func TestPublishNeverBlocks(t *testing.T) {
	sub := &fakeSubnet{block: make(chan struct{})}
	sub.set(sampleSnapshot())
	s, url := standupServer(t, Config{Subnet: sub})
	conn := dialEvents(t, url, "standup", "")
	defer conn.CloseNow()
	waitFor(t, func() bool {
		sub.mu.Lock()
		defer sub.mu.Unlock()
		return sub.calls > 0
	}, "writer to reach Snapshot")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < eventBuffer*10; i++ {
			s.Publish(coordinator.Event{Kind: coordinator.EventMember, RoomID: "standup",
				Node: "n", Present: true})
		}
		s.PublishElection(arbiter.Announcement{RoomID: "standup", Epoch: 9, Reason: arbiter.ReasonManual})
		s.PublishRepair("standup", "n", 1, 9, false)
	}()
	select {
	case <-done:
	case <-time.After(wsTimeout):
		t.Fatal("Publish blocked on a wedged subscriber — this would stall the coordinator's " +
			"Run goroutine and with it every meet in the process")
	}
	close(sub.block)
}

// TestSnapshotCallDoesNotHoldTheLock is the deadlock guard for the same seam from the
// other side: the connection goroutine calls into the coordinator (Snapshot) while a
// publisher is concurrently fanning out. If the dashboard held its own mutex across
// that call, Publish would block behind a goroutine that is itself waiting on the
// coordinator's Run loop — a deadlock that only shows up under load.
func TestSnapshotCallDoesNotHoldTheLock(t *testing.T) {
	sub := &fakeSubnet{block: make(chan struct{})}
	sub.set(sampleSnapshot())
	s, url := standupServer(t, Config{Subnet: sub})
	conn := dialEvents(t, url, "standup", "")
	defer conn.CloseNow()
	waitFor(t, func() bool {
		sub.mu.Lock()
		defer sub.mu.Unlock()
		return sub.calls > 0
	}, "writer to reach Snapshot")

	// Every publish entry point, while a Snapshot is in flight.
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Publish(coordinator.Event{Kind: coordinator.EventTopology, RoomID: "standup"})
		s.PublishElection(arbiter.Announcement{RoomID: "standup", Epoch: 2})
		s.PublishRepair("standup", "x", 1, 2, false)
	}()
	select {
	case <-done:
	case <-time.After(wsTimeout):
		t.Fatal("a publisher blocked while a Snapshot was in flight: the dashboard is " +
			"holding its lock across a call into the control plane")
	}
	close(sub.block)
}

// TestDemoEventPublished pins §9.6's observability half: a demo action emits a `demo`
// event on the meet's stream, stamped with the CURRENT epoch/rev. Stamping it with
// zeroes would be worse than omitting it — web/js/state.js copies frame.epoch onto the
// snapshot for every delta, so a zero would blank the epoch the operator is watching.
func TestDemoEventPublished(t *testing.T) {
	demo := &fakeDemo{}
	_, url := standupServer(t, Config{Demo: demo})
	conn := dialEvents(t, url, "standup", "")
	snap := readFrame(t, conn)
	if snap.Epoch != 3 || snap.Rev != 11 {
		t.Fatalf("snapshot epoch/rev = %d/%d, want 3/11", snap.Epoch, snap.Rev)
	}

	res, err := http.Post(url+"/api/demo/meets/standup/evict", "application/json",
		strings.NewReader(`{"name":"bob"}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	f := readFrame(t, conn)
	if f.Kind != "demo" {
		t.Fatalf("kind = %q, want demo", f.Kind)
	}
	if f.Epoch != 3 || f.Rev != 11 {
		t.Errorf("demo frame epoch/rev = %d/%d, want the meet's current 3/11", f.Epoch, f.Rev)
	}
	var d map[string]any
	if err := json.Unmarshal(f.Data, &d); err != nil {
		t.Fatal(err)
	}
	if d["action"] != "evict" || d["target"] != "bob" {
		t.Errorf("demo data = %v", d)
	}
	// §9.4a renamed v1's `by` to `by_remote_addr` and documented it as ADVISORY: it
	// is the RemoteAddr as the server saw it, not an identity.
	if addr, _ := d["by_remote_addr"].(string); addr == "" {
		t.Errorf("by_remote_addr is empty: %v", d)
	}
	if _, present := d["by"]; present {
		t.Errorf("data carries `by`; §9.4a renamed it to by_remote_addr because it is "+
			"not an identity: %v", d)
	}
}

// TestFrameJSONIsStable is the replayability guard for the stream: two connections
// given the identical event must receive byte-identical data objects.
func TestFrameJSONIsStable(t *testing.T) {
	s, url := standupServer(t, Config{})
	conns := make([]*websocket.Conn, 8)
	for i := range conns {
		conns[i] = dialEvents(t, url, "standup", "")
		readFrame(t, conns[i])
	}
	s.Publish(coordinator.Event{Kind: coordinator.EventFailover, RoomID: "standup",
		Node: "bob", Orphans: []string{"dave", "erin", "frank"}, Reason: "gone"})
	var first string
	for i, c := range conns {
		f := readFrame(t, c)
		if i == 0 {
			first = string(f.Data)
		} else if string(f.Data) != first {
			t.Fatalf("frame data differs between subscribers:\n %s\n %s", first, f.Data)
		}
	}
}

// readUntilClose drains frames until the socket closes and returns the close status.
func readUntilClose(t *testing.T, conn *websocket.Conn) websocket.StatusCode {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wsTimeout)
	defer cancel()
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return websocket.CloseStatus(err)
		}
	}
}

// waitFor spins until cond holds, so a test can synchronise on a state change without
// a sleep whose duration is a guess.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(wsTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
