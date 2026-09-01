package arbiter_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// epoch0 is the instant every scenario starts from. A fixed, arbitrary, non-zero
// wall time: fixed so a failure message is reproducible, non-zero so a bug that
// leaves a time.Time at its zero value is visible rather than accidentally correct.
var epoch0 = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// syncWait bounds how long a test waits for the Run goroutine to drain. It is a
// REAL duration and the only one in this package: it guards against a deadlock
// hanging the suite, and is never used to sequence anything.
const syncWait = 5 * time.Second

// deadline is syncWait as a channel. It goes through context rather than
// time.After because `make check-determinism` greps this package's whole source
// tree, tests included, for wall-clock calls — and a guard that a test file is
// allowed to defeat is not a guard.
func deadline(t *testing.T) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), syncWait)
	t.Cleanup(cancel)
	return ctx.Done()
}

// fakeClock is a virtual clock advanced explicitly by the test.
//
// NewTimer/NewTicker/After deliberately PANIC. The arbiter is specified to be
// purely event-driven (it acts only when a live peer produces a frame, and a peer
// that produces no frames is by definition one it can take no action about), so
// arming a timer would be a design regression that silently reintroduces
// wall-clock-shaped behaviour. Panicking here turns that regression into a failing
// test instead of a review note.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: epoch0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) NewTimer(time.Duration) clock.Timer {
	panic("arbiter armed a timer: it is specified to be event-driven (see harness_test.go)")
}

func (c *fakeClock) NewTicker(time.Duration) clock.Ticker {
	panic("arbiter armed a ticker: it is specified to be event-driven (see harness_test.go)")
}

func (c *fakeClock) After(time.Duration) <-chan time.Time {
	panic("arbiter called After: it is specified to be event-driven (see harness_test.go)")
}

// sent is one announcement as the wire saw it.
type sent struct {
	roomID string
	ann    arbiter.Announcement
}

// recAnnouncer records every announcement and can be made to fail, which is how the
// "the epoch was minted but the broadcast failed" case is exercised.
type recAnnouncer struct {
	mu   sync.Mutex
	got  []sent
	fail error
}

func (r *recAnnouncer) AnnounceCoordinator(roomID string, a arbiter.Announcement) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, sent{roomID: roomID, ann: a})
	return r.fail
}

func (r *recAnnouncer) all() []sent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sent(nil), r.got...)
}

// forRoom returns the announcements addressed to roomID, in issue order.
func (r *recAnnouncer) forRoom(roomID string) []arbiter.Announcement {
	var out []arbiter.Announcement
	for _, s := range r.all() {
		if s.roomID == roomID {
			out = append(out, s.ann)
		}
	}
	return out
}

func (r *recAnnouncer) setFail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = err
}

// recPublisher records the dashboard-facing side of an election.
type recPublisher struct {
	mu  sync.Mutex
	got []arbiter.Announcement
}

func (p *recPublisher) PublishElection(a arbiter.Announcement) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.got = append(p.got, a)
}

func (p *recPublisher) all() []arbiter.Announcement {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]arbiter.Announcement(nil), p.got...)
}

// peerOpts is the telemetry a test wants a peer to declare.
type peerOpts struct {
	name          string
	cpuPct        float64
	rttMs         float64
	lossPct       float64
	nat           overlay.NATType
	uploadKbps    int
	coordinatable bool
}

// wireReport is metrics.Report plus the coordinatable declaration.
//
// metrics.Report does NOT yet carry Coordinatable (PLAN §5.2 assigns that field to
// WI-0, which has not shipped it), so the flag is written as a sibling JSON key
// here exactly as the arbiter reads it. Collapse this into metrics.Report the
// moment WI-0 lands the field.
type wireReport struct {
	metrics.Report
	Coordinatable bool `json:"coordinatable"`
}

// harness owns one Arbiter under a virtual clock, plus the peers a scenario has
// introduced so the test can beat all of them without restating the roster.
type harness struct {
	t     *testing.T
	a     *arbiter.Arbiter
	clk   *fakeClock
	ann   *recAnnouncer
	pub   *recPublisher
	stop  context.CancelFunc
	done  chan error
	mu    sync.Mutex
	peers map[string][]string // roomID -> peer ids, in join order
	names map[string]string   // peerID -> name
	beats map[string]uint64   // peerID -> next Seq
}

func newHarness(t *testing.T, cfg arbiter.Config) *harness {
	t.Helper()
	clk := newFakeClock()
	cfg.Clock = clk
	ann := &recAnnouncer{}
	pub := &recPublisher{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := arbiter.New(log, cfg, ann, pub)

	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{
		t: t, a: a, clk: clk, ann: ann, pub: pub, stop: cancel,
		done:  make(chan error, 1),
		peers: map[string][]string{},
		names: map[string]string{},
		beats: map[string]uint64{},
	}
	go func() { h.done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-h.done:
		case <-deadline(t):
			t.Error("arbiter Run did not return after cancellation")
		}
	})
	return h
}

// electing is the standard Phase 6 posture: the arbiter arbitrates among peers and
// never coordinates itself.
func electing() arbiter.Config {
	return arbiter.Config{Elect: true, ArbiterID: arbiter.DefaultArbiterID}
}

func (h *harness) sync() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), syncWait)
	defer cancel()
	if err := h.a.Sync(ctx); err != nil {
		h.t.Fatalf("Sync: %v", err)
	}
}

// join introduces a peer and immediately declares its telemetry, which is what a
// real peer does (Reporter.Run reports once on start).
func (h *harness) join(roomID, peerID string, o peerOpts) {
	h.t.Helper()
	if o.name == "" {
		o.name = peerID
	}
	h.mu.Lock()
	h.peers[roomID] = append(h.peers[roomID], peerID)
	h.names[peerID] = o.name
	h.mu.Unlock()
	h.a.PeerJoined(roomID, peerID, o.name)
	h.report(roomID, peerID, o)
	h.sync()
}

// joinSilent introduces a peer that never declares telemetry — the conservative
// "older build" case, which must never be elected.
func (h *harness) joinSilent(roomID, peerID, name string) {
	h.t.Helper()
	h.mu.Lock()
	h.peers[roomID] = append(h.peers[roomID], peerID)
	h.names[peerID] = name
	h.mu.Unlock()
	h.a.PeerJoined(roomID, peerID, name)
	h.sync()
}

func (h *harness) report(roomID, peerID string, o peerOpts) {
	h.t.Helper()
	if o.name == "" {
		o.name = h.nameOf(peerID)
	}
	if o.nat == "" {
		o.nat = overlay.NATDirect
	}
	body, err := json.Marshal(wireReport{
		Report: metrics.Report{
			Name:        o.name,
			UploadKbps:  o.uploadKbps,
			NAT:         o.nat,
			RTTServerMs: o.rttMs,
			LossPct:     o.lossPct,
			CPUPct:      o.cpuPct,
		},
		Coordinatable: o.coordinatable,
	})
	if err != nil {
		h.t.Fatalf("marshal report: %v", err)
	}
	h.a.Metrics(roomID, peerID, body)
	h.sync()
}

func (h *harness) leave(roomID, peerID string) {
	h.t.Helper()
	h.mu.Lock()
	kept := h.peers[roomID][:0]
	for _, id := range h.peers[roomID] {
		if id != peerID {
			kept = append(kept, id)
		}
	}
	h.peers[roomID] = kept
	h.mu.Unlock()
	h.a.PeerLeft(roomID, peerID)
	h.sync()
}

func (h *harness) nameOf(peerID string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.names[peerID]
}

func (h *harness) roster(roomID string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.peers[roomID]...)
}

// beat sends one heartbeat for peerID with no realized topology state.
func (h *harness) beat(roomID, peerID string) {
	h.t.Helper()
	h.beatTree(roomID, peerID, "", nil, 0, 0)
}

// beatTree sends a heartbeat declaring a realized parent, children, and the
// epoch/rev the peer has realized — which is how a test drives the
// Meet.Relays/Depth/Rev derivation.
func (h *harness) beatTree(roomID, peerID, parent string, children []string, epoch, rev uint64) {
	h.t.Helper()
	h.mu.Lock()
	h.beats[peerID]++
	seq := h.beats[peerID]
	name := h.names[peerID]
	h.mu.Unlock()
	hb := metrics.Heartbeat{Name: name, Seq: seq, Parent: parent, Epoch: epoch, Rev: rev}
	for _, c := range children {
		hb.Children = append(hb.Children, metrics.ChildLink{Name: c, State: "connected"})
	}
	hb.Normalize()
	body, err := json.Marshal(hb)
	if err != nil {
		h.t.Fatalf("marshal heartbeat: %v", err)
	}
	h.a.Heartbeat(roomID, peerID, body)
	h.sync()
}

// beatWith sends a heartbeat carrying an explicit epoch/rev, for the stale-actor
// cases.
func (h *harness) beatWith(roomID, peerID string, epoch, rev uint64) {
	h.t.Helper()
	h.mu.Lock()
	h.beats[peerID]++
	seq := h.beats[peerID]
	name := h.names[peerID]
	h.mu.Unlock()
	body, err := json.Marshal(metrics.Heartbeat{Name: name, Seq: seq, Epoch: epoch, Rev: rev})
	if err != nil {
		h.t.Fatalf("marshal heartbeat: %v", err)
	}
	h.a.Heartbeat(roomID, peerID, body)
	h.sync()
}

// elapse advances virtual time by d in one-second steps, beating every peer in
// roomID at each step. This is what "sustained for 20 s" actually looks like on the
// wire: the arbiter is event-driven, so a condition can only be observed as
// sustained by peers continuing to speak.
func (h *harness) elapse(roomID string, d time.Duration) {
	h.t.Helper()
	steps := int(d / metrics.HeartbeatInterval)
	for i := 0; i < steps; i++ {
		h.clk.Advance(metrics.HeartbeatInterval)
		for _, id := range h.roster(roomID) {
			h.beat(roomID, id)
		}
	}
}

// elapseOnly is elapse restricted to the named peers: every other member stays
// silent, which is how a test starves exactly one peer of liveness evidence
// without stopping the event stream the arbiter needs to act at all.
func (h *harness) elapseOnly(roomID string, d time.Duration, ids ...string) {
	h.t.Helper()
	steps := int(d / metrics.HeartbeatInterval)
	for i := 0; i < steps; i++ {
		h.clk.Advance(metrics.HeartbeatInterval)
		for _, id := range ids {
			h.beat(roomID, id)
		}
	}
}

// silence advances virtual time by d with NO frames from anyone in roomID, then
// delivers one beat from prod so the arbiter has an event to act on. prod may be ""
// to advance with no event at all.
func (h *harness) silence(roomID string, d time.Duration, prod string) {
	h.t.Helper()
	h.clk.Advance(d)
	if prod != "" {
		h.beat(roomID, prod)
	}
	h.sync()
}

func (h *harness) meet(roomID string) arbiter.Meet {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), syncWait)
	defer cancel()
	m, err := h.a.GetMeet(ctx, roomID)
	if err != nil {
		h.t.Fatalf("GetMeet(%q): %v", roomID, err)
	}
	return m
}

// wantCoord asserts who the arbiter believes coordinates roomID.
func (h *harness) wantCoord(roomID, name string, epoch uint64) {
	h.t.Helper()
	m := h.meet(roomID)
	if m.Coordinator != name || m.Epoch != epoch {
		h.t.Fatalf("meet %q: coordinator=%q epoch=%d, want coordinator=%q epoch=%d",
			roomID, m.Coordinator, m.Epoch, name, epoch)
	}
}

// lastAnn returns the most recent announcement for roomID.
func (h *harness) lastAnn(roomID string) arbiter.Announcement {
	h.t.Helper()
	got := h.ann.forRoom(roomID)
	if len(got) == 0 {
		h.t.Fatalf("no announcement for meet %q", roomID)
	}
	return got[len(got)-1]
}
