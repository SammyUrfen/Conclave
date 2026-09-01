package coordinator_test

// The Phase 5 control-loop test harness.
//
// It lives in package coordinator_test (the EXTERNAL test package) for one
// structural reason: these tests drive the loop through simnet's VirtualClock, and
// docs/PLAN.md §2.3 permits `simnet -> coordinator`. An internal (white-box) test
// file importing simnet would therefore become an import cycle the moment simnet
// grows a scenario over the real coordinator — a landmine for the next work item.
// White-box unit tests that need no clock live in internal_test.go, which imports
// nothing that could ever point back here.
//
// Two rules every test in this package obeys, from §4.7:
//
//  1. No wall clock. Time moves only when a test calls VirtualClock.Advance. The
//     one real-time reference permitted anywhere in the control-plane test surface
//     is a deadlock guard, and here that is context.WithTimeout around Sync —
//     exactly the shape simnet.Scenario.Settle uses, and for the same reason (a
//     virtual clock nobody is advancing would never expire a virtual guard).
//  2. Every assertion is made after a Sync, so it observes a quiesced loop.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/simnet"
)

// guard bounds a Sync in REAL time so a wedged loop fails the test instead of
// hanging it with no stack. It never affects a trace — only whether a deadlock is
// reported. Mirrors simnet.SettleTimeout's role and its justification.
const guard = 10 * time.Second

// push is one recorded SendTopology call, in call order.
type push struct {
	roomID string
	peerID string
	topo   *overlay.Topology
}

// fakeSender records what the coordinator decided to send, without any signaling.
// It is safe for concurrent use because the coordinator's sender goroutine calls it
// while the test goroutine reads.
//
// DELIBERATE DUPLICATION of simnet.Capture, and simnet.Recorder below. Adopting the
// shared versions would drop ~60 lines here, and it was offered — but they are
// documented as never blocking and never failing, which is right for a scenario harness
// and is precisely what two tests in this package need them NOT to be:
//
//   - TestSenderStallCannotStallTheLoop needs a Sender that WEDGES, to prove a blocking
//     adapter cannot take the control loop with it;
//   - TestAmbiguityWindowStalePushIsNeitherSentNorAccepted needs one that wedges AND
//     announces its entry, so a term can be ended at a moment when there is provably one
//     push inside the network call and three queued behind it. That construction is the
//     whole test; without it the interleaving is hoped for rather than made.
//
// Adopting simnet.Capture for the easy cases would leave two Sender fakes in one package
// — and, because the internal tests cannot import simnet at all (simnet imports
// coordinator), a third publisher fake alongside them. One local pair that can do
// everything beats three that each do part.
type fakeSender struct {
	mu      sync.Mutex
	pushes  []push
	block   chan struct{} // non-nil ⇒ every send blocks on it (the stall test)
	entered chan struct{} // non-nil ⇒ signalled once, on entry to the first blocked send
	failAll bool
}

func newFakeSender() *fakeSender { return &fakeSender{} }

func (f *fakeSender) SendTopology(roomID, peerID string, topo *overlay.Topology) error {
	f.mu.Lock()
	blocked, fail, entered := f.block, f.failAll, f.entered
	f.pushes = append(f.pushes, push{roomID: roomID, peerID: peerID, topo: topo})
	f.mu.Unlock()
	if entered != nil {
		// Non-blocking: only the FIRST wedged send needs to announce itself, and a
		// send on a full channel here would deadlock the very goroutine the test is
		// trying to park.
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if blocked != nil {
		<-blocked
	}
	if fail {
		return errAlwaysFails
	}
	return nil
}

// last returns the most recent topology pushed to peerID, or nil.
func (f *fakeSender) last(peerID string) *overlay.Topology {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.pushes) - 1; i >= 0; i-- {
		if f.pushes[i].peerID == peerID {
			return f.pushes[i].topo
		}
	}
	return nil
}

func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pushes)
}

// sinceRev returns every push carrying a topology newer than rev, in call order. It is
// how a test asks "what actually went out after this point" without depending on how
// many members happened to be in the meet.
func (f *fakeSender) sinceRev(rev uint64) []push {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []push
	for _, p := range f.pushes {
		if p.topo != nil && p.topo.Rev > rev {
			out = append(out, p)
		}
	}
	return out
}

type constErr string

func (e constErr) Error() string { return string(e) }

const errAlwaysFails = constErr("fake sender always fails")

// fakePublisher records the coordinator's observable event stream — the dashboard's
// view. Assertions are made against it rather than against internal state, because
// §12.3 requires the coordinator to be LEGIBLE about what it did, not merely correct.
type fakePublisher struct {
	mu     sync.Mutex
	events []coordinator.Event
}

func newFakePublisher() *fakePublisher { return &fakePublisher{} }

func (p *fakePublisher) Publish(ev coordinator.Event) {
	p.mu.Lock()
	p.events = append(p.events, ev)
	p.mu.Unlock()
}

func (p *fakePublisher) all() []coordinator.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]coordinator.Event(nil), p.events...)
}

// of returns every recorded event of one kind, in order.
func (p *fakePublisher) of(kind coordinator.EventKind) []coordinator.Event {
	var out []coordinator.Event
	for _, ev := range p.all() {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

func (p *fakePublisher) countOf(kind coordinator.EventKind) int { return len(p.of(kind)) }

// lastOf returns the most recent event of a kind and whether one exists.
func (p *fakePublisher) lastOf(kind coordinator.EventKind) (coordinator.Event, bool) {
	evs := p.of(kind)
	if len(evs) == 0 {
		return coordinator.Event{}, false
	}
	return evs[len(evs)-1], true
}

func (p *fakePublisher) reset() {
	p.mu.Lock()
	p.events = nil
	p.mu.Unlock()
}

// harness is one coordinator under a virtual clock, plus the fakes it talks to.
type harness struct {
	t   *testing.T
	c   *coordinator.Coordinator
	clk *simnet.VirtualClock
	fs  *fakeSender
	fp  *fakePublisher

	cancel context.CancelFunc
	done   chan struct{}
}

// baseConfig is the fleet shape most tests use: a two-hop tree at the default
// 2000 kbit/s stream cost, with SocketDetection disabled (0) because simnet models
// no socket layer — exactly the case §5.3's floor documents as the zero value's
// purpose.
//
// Liveness is pushed out of reach here ON PURPOSE. These tests advance virtual time
// to exercise the cooldown and the settle, and a fleet that never heartbeats would
// otherwise be reaped mid-test by the very backstop that is correct in production —
// turning an assertion about the cooldown into an accidental assertion about the
// health FSM. liveness_test.go configures real thresholds and drives real beats.
func baseConfig() coordinator.Config {
	return coordinator.Config{
		MaxDepth:      2,
		StreamKbps:    2000,
		DegradedAfter: time.Hour,
		GoneAfter:     time.Hour,
	}
}

func newHarness(t *testing.T, cfg coordinator.Config) *harness {
	t.Helper()
	clk := simnet.NewVirtualClock(simnet.DefaultStart)
	cfg.Clock = clk
	fs, fp := newFakeSender(), newFakePublisher()
	c := coordinator.New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, fs, fp)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()

	h := &harness{t: t, c: c, clk: clk, fs: fs, fp: fp, cancel: cancel, done: done}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.cancel()
	<-h.done
}

// sync blocks until the loop has processed everything enqueued so far. Every
// assertion in this package is preceded by one.
func (h *harness) sync() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), guard)
	defer cancel()
	if err := h.c.Sync(ctx); err != nil {
		h.t.Fatalf("Sync: %v", err)
	}
}

// advance moves virtual time and settles the loop afterwards.
func (h *harness) advance(d time.Duration) {
	h.t.Helper()
	h.clk.Advance(d)
	h.sync()
}

// advanceNoSync moves virtual time WITHOUT settling. It exists for the tests that must
// observe the loop mid-reaction — a Sync there would be the very barrier the test is
// trying to race past.
func (h *harness) advanceNoSync(d time.Duration) {
	h.t.Helper()
	h.clk.Advance(d)
}

func (h *harness) snapshot(roomID string) coordinator.RoomSnapshot {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), guard)
	defer cancel()
	snap, err := h.c.Snapshot(ctx, roomID)
	if err != nil {
		h.t.Fatalf("Snapshot(%q): %v", roomID, err)
	}
	return snap
}

// join adds a named member and syncs.
func (h *harness) join(roomID, peerID, name string) {
	h.t.Helper()
	h.c.PeerJoined(roomID, peerID, name)
	h.sync()
}

// report delivers one metrics frame and syncs.
func (h *harness) report(roomID, peerID string, rep metrics.Report) {
	h.t.Helper()
	h.c.Metrics(roomID, peerID, mustJSON(h.t, rep))
	h.sync()
}

// member joins a peer and immediately delivers its first report, which is the
// common case: metrics.Reporter emits immediately on Run (§5.9).
func (h *harness) member(roomID, peerID, name string, uploadKbps int) {
	h.t.Helper()
	h.c.PeerJoined(roomID, peerID, name)
	h.c.Metrics(roomID, peerID, mustJSON(h.t, metrics.Report{Name: name, UploadKbps: uploadKbps}))
	h.sync()
}

func (h *harness) beat(roomID, peerID string, hb metrics.Heartbeat) {
	h.t.Helper()
	h.c.Heartbeat(roomID, peerID, mustJSON(h.t, hb))
	h.sync()
}

func (h *harness) reparented(roomID, peerID string, rp metrics.Reparented) {
	h.t.Helper()
	h.c.Reparented(roomID, peerID, mustJSON(h.t, rp))
	h.sync()
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// published returns the tree the coordinator most recently pushed for roomID, via
// the snapshot surface (never internal state).
func (h *harness) published(roomID string) *overlay.Topology {
	h.t.Helper()
	return h.snapshot(roomID).Topo
}

// healthOf reads one member's Health out of a snapshot.
func (h *harness) healthOf(roomID, name string) (coordinator.Health, bool) {
	h.t.Helper()
	for _, m := range h.snapshot(roomID).Members {
		if m.Name == name {
			return m.Health, true
		}
	}
	return "", false
}

// nodesFor projects a snapshot back into overlay.Node values so a test can run the
// independent oracles (Validate / ValidateLocalRepair) over what the coordinator
// actually decided. It mirrors the coordinator's own projection deliberately: the
// oracle needs the same fleet the builder saw.
func nodesFor(snap coordinator.RoomSnapshot, impaired map[string]bool) []overlay.Node {
	out := make([]overlay.Node, 0, len(snap.Members))
	for _, m := range snap.Members {
		if m.Name == "" || m.Health == coordinator.HealthGone {
			continue
		}
		out = append(out, overlay.Node{
			Name:        m.Name,
			UploadKbps:  m.Report.UploadKbps,
			NAT:         natOrDirect(m.Report.NAT),
			LossPct:     m.Report.LossPct,
			Provisional: !m.Reported,
			Impaired:    impaired[m.Name],
		})
	}
	return out
}

func natOrDirect(n overlay.NATType) overlay.NATType {
	if n == "" {
		return overlay.NATDirect
	}
	return n
}
