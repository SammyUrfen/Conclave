package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/dashboard"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestArbiterIDMatchesSignalingServerID is the test docs/PLAN.md §2.4 REQUIRES to
// live here, and it has two halves that prove different things.
//
// The arbiter may not import signaling, so arbiter.DefaultArbiterID hard-codes
// "_server" as a test fallback while cmd/server passes signaling.ServerID explicitly.
// internal/arbiter's external test already asserts the two CONSTANTS agree. That is
// not enough: constants agreeing while main forgets to pass the value is exactly the
// failure a single test misses, because the arbiter would then silently fall back to
// its own copy and nothing would ever notice the wiring was absent.
//
// So the second half asserts the WIRING — that the plane cmd/server actually builds
// carries signaling.ServerID in the arbiter config it used, and carries it
// EXPLICITLY rather than by leaving the field empty and inheriting the fallback.
func TestArbiterIDMatchesSignalingServerID(t *testing.T) {
	t.Run("constants agree", func(t *testing.T) {
		if arbiter.DefaultArbiterID != signaling.ServerID {
			t.Fatalf("arbiter.DefaultArbiterID = %q, signaling.ServerID = %q; the duplication has drifted",
				arbiter.DefaultArbiterID, signaling.ServerID)
		}
	})

	t.Run("the wiring passes it", func(t *testing.T) {
		f, err := parseFlags([]string{"-coordinate", "-elect"}, io.Discard)
		if err != nil {
			t.Fatalf("parseFlags: %v", err)
		}
		res, err := f.resolve()
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		p, err := newPlane(testLogger(), f, res)
		if err != nil {
			t.Fatalf("newPlane: %v", err)
		}
		defer p.close()

		// Empty would compile, would run, and would produce the right announcements
		// from the fallback — while leaving the wiring this test exists to pin absent.
		if p.arbCfg.ArbiterID == "" {
			t.Fatal("plane.arbCfg.ArbiterID is empty: main is relying on arbiter.DefaultArbiterID " +
				"instead of passing signaling.ServerID, which §2.4 forbids")
		}
		if p.arbCfg.ArbiterID != signaling.ServerID {
			t.Fatalf("plane.arbCfg.ArbiterID = %q, want signaling.ServerID (%q)",
				p.arbCfg.ArbiterID, signaling.ServerID)
		}
	})
}

// fakeObserver records the Observer callbacks a fan-out delivered, so a test can
// assert BOTH halves received the same event rather than only the one it looked at.
type fakeObserver struct {
	mu    sync.Mutex
	calls []string
}

func (o *fakeObserver) record(s string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, s)
}

func (o *fakeObserver) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.calls...)
}

func (o *fakeObserver) PeerJoined(roomID, peerID, name string) {
	o.record("join:" + roomID + ":" + peerID)
}
func (o *fakeObserver) PeerLeft(roomID, peerID string) { o.record("left:" + roomID + ":" + peerID) }
func (o *fakeObserver) Metrics(roomID, peerID string, _ []byte) {
	o.record("metrics:" + roomID + ":" + peerID)
}
func (o *fakeObserver) Heartbeat(roomID, peerID string, _ []byte) {
	o.record("beat:" + roomID + ":" + peerID)
}
func (o *fakeObserver) Reparented(roomID, peerID string, _ []byte) {
	o.record("reparent:" + roomID + ":" + peerID)
}

// fakeRelay captures what the fan-out forwarded to an elected coordinator peer.
type fakeRelay struct {
	mu   sync.Mutex
	sent []signaling.Message
	room []string
	to   []string
	ok   bool
}

func (r *fakeRelay) SendTo(roomID, peerID string, msg signaling.Message) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.room = append(r.room, roomID)
	r.to = append(r.to, peerID)
	r.sent = append(r.sent, msg)
	return r.ok
}

func (r *fakeRelay) snapshot() ([]string, []signaling.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.to...), append([]signaling.Message(nil), r.sent...)
}

// TestObserverFansOutToBothPlanes pins the property §2.4 and §6 rest on: the Hub's
// single Observer reaches the arbiter AND the coordinator.
//
// That fan-out is what gives the arbiter a liveness view of the coordinator without
// importing it — if the coordinator wedges, the arbiter still sees peers beating and
// can still elect. Wiring only one of them compiles and looks fine right up until a
// failover never happens.
func TestObserverFansOutToBothPlanes(t *testing.T) {
	arb, coord := &fakeObserver{}, &fakeObserver{}
	obs := &planeObserver{log: testLogger(), arb: arb, coord: coord, roles: newRoleTable()}

	obs.PeerJoined("standup", "p1", "alice")
	obs.Metrics("standup", "p1", []byte(`{}`))
	obs.Heartbeat("standup", "p1", []byte(`{}`))
	obs.Reparented("standup", "p1", []byte(`{}`))
	obs.PeerLeft("standup", "p1")

	// The two planes see DIFFERENT slices on purpose. The arbiter's is a liveness
	// view — membership, telemetry, beats — and a self-promotion says nothing about
	// whether a peer could coordinate, so Reparented is not part of it. Asserting the
	// same list for both would force the arbiter to grow a method that ignores its
	// argument, which is a worse lie than a narrower interface.
	for _, side := range []struct {
		name string
		obs  *fakeObserver
		want []string
	}{
		{"arbiter", arb, []string{
			"join:standup:p1", "metrics:standup:p1", "beat:standup:p1", "left:standup:p1",
		}},
		{"coordinator", coord, []string{
			"join:standup:p1", "metrics:standup:p1", "beat:standup:p1",
			"reparent:standup:p1", "left:standup:p1",
		}},
	} {
		got := side.obs.snapshot()
		if len(got) != len(side.want) {
			t.Fatalf("%s saw %v, want %v", side.name, got, side.want)
		}
		for i := range side.want {
			if got[i] != side.want[i] {
				t.Fatalf("%s call %d = %q, want %q", side.name, i, got[i], side.want[i])
			}
		}
	}
}

// TestObserverForwardsTelemetryToElectedCoordinator pins §8 routing rule 2: metrics,
// heartbeats and reparents terminate at the server, and when the coordinator is an
// ELECTED PEER it is the server's Observer that forwards them onward — as the same
// frame type, with From left as the original peer's id so the elected coordinator
// does not attribute every peer's telemetry to the arbiter and fence it away.
func TestObserverForwardsTelemetryToElectedCoordinator(t *testing.T) {
	roles := newRoleTable()
	relay := &fakeRelay{ok: true}
	obs := &planeObserver{log: testLogger(), arb: &fakeObserver{}, coord: &fakeObserver{},
		roles: roles, relay: relay}

	t.Run("no coordinator yet: nothing is forwarded", func(t *testing.T) {
		obs.Metrics("standup", "p1", []byte(`{"a":1}`))
		if to, _ := relay.snapshot(); len(to) != 0 {
			t.Fatalf("forwarded %v with no coordinator elected", to)
		}
	})

	t.Run("arbiter coordinates: nothing is forwarded", func(t *testing.T) {
		roles.set("standup", signaling.ServerID)
		obs.Metrics("standup", "p1", []byte(`{"a":1}`))
		if to, _ := relay.snapshot(); len(to) != 0 {
			t.Fatalf("forwarded %v while the arbiter itself coordinates", to)
		}
	})

	t.Run("elected peer: forwarded once, verbatim", func(t *testing.T) {
		roles.set("standup", "p9")
		obs.Metrics("standup", "p1", []byte(`{"a":1}`))
		obs.Heartbeat("standup", "p1", []byte(`{"b":2}`))
		obs.Reparented("standup", "p1", []byte(`{"c":3}`))
		to, msgs := relay.snapshot()
		if len(to) != 3 {
			t.Fatalf("forwarded %d frames, want 3 (metrics, heartbeat, reparented)", len(to))
		}
		wantTypes := []signaling.Type{signaling.TypeMetrics, signaling.TypeHeartbeat, signaling.TypeReparented}
		for i, m := range msgs {
			if to[i] != "p9" {
				t.Fatalf("frame %d addressed to %q, want the elected coordinator %q", i, to[i], "p9")
			}
			if m.Type != wantTypes[i] {
				t.Fatalf("frame %d type = %q, want %q", i, m.Type, wantTypes[i])
			}
			if m.From != "p1" {
				t.Fatalf("frame %d From = %q, want the original sender %q", i, m.From, "p1")
			}
			if m.To != "p9" {
				t.Fatalf("frame %d To = %q, want %q", i, m.To, "p9")
			}
		}
	})

	t.Run("the coordinator's own telemetry is not echoed back to it", func(t *testing.T) {
		relay.mu.Lock()
		relay.to, relay.sent, relay.room = nil, nil, nil
		relay.mu.Unlock()
		obs.Heartbeat("standup", "p9", []byte(`{}`))
		if to, _ := relay.snapshot(); len(to) != 0 {
			t.Fatalf("echoed the coordinator's own beat back to it: %v", to)
		}
	})
}

// fakeBroadcaster captures the Announcer's two deliveries.
type fakeBroadcaster struct {
	mu        sync.Mutex
	rooms     []string
	broadcast []signaling.Message
	unicastTo []string
	unicast   []signaling.Message
	unicastOK bool
}

func (b *fakeBroadcaster) SendRoom(roomID string, msg signaling.Message) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rooms = append(b.rooms, roomID)
	b.broadcast = append(b.broadcast, msg)
	return 1
}

func (b *fakeBroadcaster) SendTo(roomID, peerID string, msg signaling.Message) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unicastTo = append(b.unicastTo, peerID)
	b.unicast = append(b.unicast, msg)
	return b.unicastOK
}

// fakeTerm records the epoch calls the announcer drives into the in-process
// coordinator. Adopting and yielding are the two halves of §6.5 that cmd/server owns.
type fakeTerm struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeTerm) SetEpoch(roomID string, epoch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "adopt:"+roomID+":"+itoa(epoch))
}

func (f *fakeTerm) Yield(roomID string, epoch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "yield:"+roomID+":"+itoa(epoch))
}

func (f *fakeTerm) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

// TestAnnouncerBroadcastsAndDrivesTheLocalCoordinator pins the seam the arbiter's
// Announcer is adapted to, and the in-process consequence of an announcement.
//
// The ordering assertion is the load-bearing one: the announcement must be QUEUED on
// every peer's outbound channel BEFORE the local coordinator is told to serve the
// epoch, because the coordinator's first push rides the same FIFO channel. Reverse
// them and a peer can be handed a topology stamped with an epoch it has not adopted
// yet, which its fence rejects — and nothing retries until the next rebuild.
func TestAnnouncerBroadcastsAndDrivesTheLocalCoordinator(t *testing.T) {
	t.Run("arbiter holds the role: adopt the epoch", func(t *testing.T) {
		b, term, roles := &fakeBroadcaster{}, &fakeTerm{}, newRoleTable()
		ann := &hubAnnouncer{log: testLogger(), bus: b, roles: roles, term: term}

		err := ann.AnnounceCoordinator("standup", arbiter.Announcement{
			RoomID: "standup", Epoch: 1, CoordinatorID: signaling.ServerID,
			Reason: arbiter.ReasonBootstrap, IssuedAt: time.Unix(0, 0),
		})
		if err != nil {
			t.Fatalf("AnnounceCoordinator: %v", err)
		}
		if len(b.broadcast) != 1 {
			t.Fatalf("broadcast %d frames, want 1", len(b.broadcast))
		}
		if got := b.broadcast[0].Type; got != signaling.TypeCoordinator {
			t.Fatalf("broadcast type = %q, want %q", got, signaling.TypeCoordinator)
		}
		var decoded arbiter.Announcement
		if err := json.Unmarshal(b.broadcast[0].Payload, &decoded); err != nil {
			t.Fatalf("the broadcast payload is not an Announcement: %v", err)
		}
		if decoded.Epoch != 1 || decoded.CoordinatorID != signaling.ServerID {
			t.Fatalf("broadcast announcement = %+v, want epoch 1 held by %q", decoded, signaling.ServerID)
		}
		if got := roles.get("standup"); got != signaling.ServerID {
			t.Fatalf("roles.get = %q, want %q", got, signaling.ServerID)
		}
		if got := term.snapshot(); len(got) != 1 || got[0] != "adopt:standup:1" {
			t.Fatalf("term calls = %v, want [adopt:standup:1]", got)
		}
	})

	t.Run("a peer takes the role: yield", func(t *testing.T) {
		b, term, roles := &fakeBroadcaster{}, &fakeTerm{}, newRoleTable()
		ann := &hubAnnouncer{log: testLogger(), bus: b, roles: roles, term: term}
		if err := ann.AnnounceCoordinator("standup", arbiter.Announcement{
			RoomID: "standup", Epoch: 4, Coordinator: "alice", CoordinatorID: "p2",
			Reason: arbiter.ReasonPromotion,
		}); err != nil {
			t.Fatalf("AnnounceCoordinator: %v", err)
		}
		if got := roles.get("standup"); got != "p2" {
			t.Fatalf("roles.get = %q, want p2", got)
		}
		if got := term.snapshot(); len(got) != 1 || got[0] != "yield:standup:4" {
			t.Fatalf("term calls = %v, want [yield:standup:4]", got)
		}
	})

	t.Run("a vacancy also yields", func(t *testing.T) {
		b, term, roles := &fakeBroadcaster{}, &fakeTerm{}, newRoleTable()
		ann := &hubAnnouncer{log: testLogger(), bus: b, roles: roles, term: term}
		if err := ann.AnnounceCoordinator("standup", arbiter.Announcement{
			RoomID: "standup", Epoch: 5, Reason: arbiter.ReasonVacated,
		}); err != nil {
			t.Fatalf("AnnounceCoordinator: %v", err)
		}
		if got := term.snapshot(); len(got) != 1 || got[0] != "yield:standup:5" {
			t.Fatalf("term calls = %v, want [yield:standup:5]", got)
		}
	})

	t.Run("repair is unicast and verbatim", func(t *testing.T) {
		b, term, roles := &fakeBroadcaster{unicastOK: true}, &fakeTerm{}, newRoleTable()
		ann := &hubAnnouncer{log: testLogger(), bus: b, roles: roles, term: term}
		a := arbiter.Announcement{RoomID: "standup", Epoch: 3, Coordinator: "alice",
			CoordinatorID: "p2", Reason: arbiter.ReasonFailover}
		if err := ann.RepairCoordinator("standup", "p7", a); err != nil {
			t.Fatalf("RepairCoordinator: %v", err)
		}
		if len(b.broadcast) != 0 {
			t.Fatal("a repair broadcast; §6.8 requires a unicast so one wedged peer costs one frame")
		}
		if len(b.unicastTo) != 1 || b.unicastTo[0] != "p7" {
			t.Fatalf("unicast targets = %v, want [p7]", b.unicastTo)
		}
		var decoded arbiter.Announcement
		if err := json.Unmarshal(b.unicast[0].Payload, &decoded); err != nil {
			t.Fatalf("repair payload is not an Announcement: %v", err)
		}
		if decoded != a {
			t.Fatalf("repair announcement = %+v, want it verbatim: %+v", decoded, a)
		}
		// A repair must NOT move the local term: it re-sends an epoch already in
		// force, and adopting it again would reset the coordinator's rev for nothing.
		if got := term.snapshot(); len(got) != 0 {
			t.Fatalf("a repair drove the local term: %v", got)
		}
	})

	t.Run("an absent peer is a reported failure, not a silent drop", func(t *testing.T) {
		b := &fakeBroadcaster{unicastOK: false}
		ann := &hubAnnouncer{log: testLogger(), bus: b, roles: newRoleTable(), term: &fakeTerm{}}
		if err := ann.RepairCoordinator("standup", "gone", arbiter.Announcement{}); err == nil {
			t.Fatal("RepairCoordinator to an absent peer returned nil")
		}
	})
}

// TestRoleTableForgetsEmptyMeets pins the bound on the one map cmd/server owns. It is
// fed from an unauthenticated create surface, so "grows forever" is a slow leak with
// a name rather than an aesthetic complaint.
func TestRoleTableForgetsEmptyMeets(t *testing.T) {
	roles := newRoleTable()
	roles.set("standup", "p2")
	roles.set("retro", signaling.ServerID)
	if got := roles.len(); got != 2 {
		t.Fatalf("roles.len = %d, want 2", got)
	}
	roles.forget("standup")
	if got := roles.get("standup"); got != "" {
		t.Fatalf("roles.get after forget = %q, want empty", got)
	}
	if got := roles.len(); got != 1 {
		t.Fatalf("roles.len = %d, want 1", got)
	}
}

// TestEvictorClosesOnlyItsTarget pins the demo eviction primitive.
//
// signaling exports no way to close one peer's socket, so cmd/server obtains the
// capability the only way that does not reach into another package: it owns the
// per-connection request context ServeWS derives its member context from. Cancelling
// that context is precisely what a dead socket does.
func TestEvictorClosesOnlyItsTarget(t *testing.T) {
	ev := newEvictor()
	aliceCtx, releaseAlice := ev.track("standup", "alice", context.Background())
	bobCtx, releaseBob := ev.track("standup", "bob", context.Background())
	defer releaseAlice()
	defer releaseBob()

	if !ev.evict("standup", "alice") {
		t.Fatal("evict(standup, alice) reported no tracked connection")
	}
	select {
	case <-aliceCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("alice's connection context was not cancelled by the eviction")
	}
	select {
	case <-bobCtx.Done():
		t.Fatal("evicting alice also cancelled bob")
	default:
	}

	if ev.evict("standup", "carol") {
		t.Fatal("evict reported success for a peer it never tracked")
	}
	if ev.evict("retro", "alice") {
		t.Fatal("evict crossed meets: a name is only unique WITHIN a meet")
	}

	releaseAlice()
	if ev.len() != 1 {
		t.Fatalf("evictor still tracks %d connections after a release, want 1", ev.len())
	}
}

// fakeMeets is the arbiter half of DemoControl, so the eviction error mapping can be
// tested without an arbiter goroutine.
type fakeMeets struct {
	known map[string]bool
	elect error
}

func (m *fakeMeets) GetMeet(_ context.Context, id string) (arbiter.Meet, error) {
	if !m.known[id] {
		return arbiter.Meet{}, arbiter.ErrMeetNotFound
	}
	return arbiter.Meet{ID: id}, nil
}

func (m *fakeMeets) ForceElection(context.Context, string, string) error { return m.elect }

type fakeRoster struct{ names map[string][]string }

func (r *fakeRoster) Roster(roomID string) []signaling.Peer {
	out := make([]signaling.Peer, 0, len(r.names[roomID]))
	for i, n := range r.names[roomID] {
		out = append(out, signaling.Peer{ID: "p" + string(rune('1'+i)), Name: n})
	}
	return out
}

// TestDemoControlErrorMapping pins the sentinels the dashboard's boundary matches on.
// It matches with errors.Is and never on message text, so an Evict that returns a
// bare fmt.Errorf would silently become a 500 where §9.4a promises a 404.
func TestDemoControlErrorMapping(t *testing.T) {
	ev := newEvictor()
	_, release := ev.track("standup", "alice", context.Background())
	defer release()

	dc := &demoControl{
		meets:  &fakeMeets{known: map[string]bool{"standup": true}},
		roster: &fakeRoster{names: map[string][]string{"standup": {"alice", "bob"}}},
		ev:     ev,
	}
	ctx := context.Background()

	t.Run("unknown meet", func(t *testing.T) {
		err := dc.Evict(ctx, "nope", "alice")
		if !errors.Is(err, arbiter.ErrMeetNotFound) {
			t.Fatalf("Evict on an unknown meet = %v, want it to wrap arbiter.ErrMeetNotFound", err)
		}
	})
	t.Run("unknown member", func(t *testing.T) {
		err := dc.Evict(ctx, "standup", "carol")
		if !errors.Is(err, dashboard.ErrMemberNotFound) {
			t.Fatalf("Evict on an unknown member = %v, want it to wrap dashboard.ErrMemberNotFound", err)
		}
	})
	t.Run("member present but untracked", func(t *testing.T) {
		// bob is in the roster but has no tracked connection — a race with his own
		// disconnect. It is a 404, not a 500: the target is gone either way.
		err := dc.Evict(ctx, "standup", "bob")
		if !errors.Is(err, dashboard.ErrMemberNotFound) {
			t.Fatalf("Evict on an untracked member = %v, want dashboard.ErrMemberNotFound", err)
		}
	})
	t.Run("success", func(t *testing.T) {
		if err := dc.Evict(ctx, "standup", "alice"); err != nil {
			t.Fatalf("Evict: %v", err)
		}
	})
	t.Run("force election delegates", func(t *testing.T) {
		dc.meets = &fakeMeets{known: map[string]bool{"standup": true}, elect: arbiter.ErrNoCandidate}
		err := dc.ForceElection(ctx, "standup", "")
		if !errors.Is(err, arbiter.ErrNoCandidate) {
			t.Fatalf("ForceElection = %v, want arbiter.ErrNoCandidate passed through", err)
		}
	})
}

// TestHubSenderMarshalsTopology keeps the Phase 4 push seam honest through the rewire.
func TestHubSenderMarshalsTopology(t *testing.T) {
	b := &fakeBroadcaster{unicastOK: true}
	s := hubSender{bus: b}
	if err := s.SendTopology("standup", "p1", nil); err != nil {
		t.Fatalf("SendTopology: %v", err)
	}
	if len(b.unicast) != 1 || b.unicast[0].Type != signaling.TypeTopology {
		t.Fatalf("SendTopology sent %+v, want one TypeTopology frame", b.unicast)
	}
	if b.unicast[0].To != "p1" {
		t.Fatalf("SendTopology addressed %q, want p1", b.unicast[0].To)
	}

	b.unicastOK = false
	if err := s.SendTopology("standup", "gone", nil); err == nil {
		t.Fatal("SendTopology to an absent peer returned nil")
	}
}

// TestLivenessBudgetIsWiredFromTheHub proves the startup check reads the REAL hub
// rather than a constant transcribed next to it. Config.SocketDetection and the
// validation must both come from the constructed Hub, or a future HubConfig change
// would leave the control plane fenced against a window that no longer exists.
func TestLivenessBudgetIsWiredFromTheHub(t *testing.T) {
	f, err := parseFlags([]string{"-coordinate"}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	res, err := f.resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	p, err := newPlane(testLogger(), f, res)
	if err != nil {
		t.Fatalf("newPlane: %v", err)
	}
	defer p.close()

	budget := p.hub.LivenessBudget()
	if budget != signaling.WSLivenessBudget {
		t.Fatalf("hub.LivenessBudget = %v, want %v", budget, signaling.WSLivenessBudget)
	}
	if p.coordCfg.SocketDetection != budget {
		t.Fatalf("coordinator SocketDetection = %v, want the hub's %v",
			p.coordCfg.SocketDetection, budget)
	}
	if err := metrics.ValidateLivenessBudget(metrics.HeartbeatInterval, budget); err != nil {
		t.Fatalf("the shipped defaults do not satisfy the liveness budget: %v", err)
	}
}
