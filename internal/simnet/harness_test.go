package simnet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// modelCoordinator is a media-free, socket-free stand-in for internal/coordinator,
// living in _test.go so simnet itself never depends on it (internal/coordinator is
// owned by WI-3 and does not compile on this branch). It reproduces exactly the
// three properties the harness has to be able to drive:
//
//  1. a SINGLE-GOROUTINE event loop, so Sync is a real quiescence barrier;
//  2. the FIRST-BUILD SETTLE window on a virtual timer — no tree before either the
//     window expires or every member has reported;
//  3. the THREE TREES of docs/PLAN.md §5.6a kept distinct: `published` (what the
//     fleet realized), `working` (published + ratified promotions; BuildTree's prev)
//     and `next` (this round's output). Conflating them was a critical defect in the
//     v2 contract, so the model must not conflate them either.
//
// The load-bearing detail is in run(): the sync branch DRAINS the settle timer
// before acking. A fire and a sync arriving at one parked select are resolved by
// Go's uniform-random choice, so without the drain the barrier can ack before the
// reaction it exists to wait for — and the whole trace becomes scheduler-dependent.
type modelCoordinator struct {
	net       *Network
	clk       *VirtualClock
	start     time.Time
	cons      overlay.Constraints
	settleFor time.Duration

	events chan modelEvent
	stopCh chan struct{}
	doneCh chan struct{}
	timer  clock.Timer

	// Owned by the run goroutine; never touched from a test goroutine.
	reported  map[string]bool
	settled   bool
	published *overlay.Topology
	working   *overlay.Topology

	// mu guards everything a test goroutine reads. pub mirrors published so a test
	// can inspect the converged tree without racing the run goroutine.
	mu      sync.Mutex
	trace   []modelStep
	pub     *overlay.Topology
	failure error
}

// modelStep is one recorded transition. It marshals to JSON so a whole run can be
// compared byte-for-byte between two replays (property (c)).
type modelStep struct {
	At    time.Duration     `json:"at"`
	Event string            `json:"event"`
	Topo  *overlay.Topology `json:"topo,omitempty"`
}

type modelEvent struct {
	kind string // "report" | "sync"
	name string
	ack  chan struct{}
}

func newModelCoordinator(net *Network, clk *VirtualClock, start time.Time, cons overlay.Constraints, settleFor time.Duration) *modelCoordinator {
	m := &modelCoordinator{
		net:       net,
		clk:       clk,
		start:     start,
		cons:      cons,
		settleFor: settleFor,
		events:    make(chan modelEvent),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		timer:     clk.NewTimer(settleFor),
		reported:  map[string]bool{},
	}
	go m.run()
	return m
}

func (m *modelCoordinator) run() {
	defer close(m.doneCh)
	for {
		select {
		case <-m.timer.C():
			m.onSettle()
		case ev := <-m.events:
			switch ev.kind {
			case "sync":
				m.drain()
				close(ev.ack)
			case "report":
				m.onReport(ev.name)
			}
		case <-m.stopCh:
			return
		}
	}
}

func (m *modelCoordinator) drain() {
	select {
	case <-m.timer.C():
		m.onSettle()
	default:
	}
}

func (m *modelCoordinator) onSettle() {
	if m.settled {
		return
	}
	m.settled = true
	m.rebuild("settle")
}

func (m *modelCoordinator) onReport(name string) {
	if m.reported[name] {
		return
	}
	m.reported[name] = true
	if !m.settled {
		// Early exit: once every member of the fleet has been heard from there is
		// nothing left to wait for, so the window is closed early rather than
		// costing every meet a fixed startup delay.
		for _, n := range m.net.OverlayNodes() {
			if !m.reported[n.Name] {
				return
			}
		}
		m.timer.Stop()
		m.settled = true
	}
	m.rebuild("report " + name)
}

// project is the coordinator's BELIEF about the fleet: ground truth from the
// Network, with every not-yet-heard-from member replaced by a provisional
// placeholder. That is precisely the straggler model — a member the coordinator
// knows exists but has no measurement for.
func (m *modelCoordinator) project() []overlay.Node {
	nodes := m.net.OverlayNodes()
	out := make([]overlay.Node, 0, len(nodes))
	for _, n := range nodes {
		if m.reported[n.Name] {
			out = append(out, n)
			continue
		}
		out = append(out, overlay.Node{Name: n.Name, Provisional: true})
	}
	return out
}

func (m *modelCoordinator) rebuild(event string) {
	at := m.clk.Now().Sub(m.start)
	if !m.settled {
		m.record(modelStep{At: at, Event: event})
		return
	}
	nodes := m.project()
	cons, ok := nextConstraints(m.cons, nodes, m.working)
	if !ok {
		m.record(modelStep{At: at, Event: event})
		return
	}
	next, err := buildTree(nodes, m.working, cons)
	if err != nil {
		// Over-constrained for now: hold the previous tree, exactly as the real
		// coordinator does, and try again on the next event.
		m.record(modelStep{At: at, Event: event})
		return
	}
	if verr := validateTopology(next, nodes, cons); verr != nil {
		m.fail(fmt.Errorf("%s: published tree fails Validate: %w", event, verr))
		return
	}
	m.published, m.working = next, next
	m.record(modelStep{At: at, Event: event, Topo: next})
}

func (m *modelCoordinator) record(s modelStep) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trace = append(m.trace, s)
	if s.Topo != nil {
		m.pub = s.Topo
	}
}

func (m *modelCoordinator) fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure == nil {
		m.failure = err
	}
}

// Report tells the coordinator that name's first telemetry frame arrived.
func (m *modelCoordinator) Report(name string) {
	select {
	case m.events <- modelEvent{kind: "report", name: name}:
	case <-m.doneCh:
	}
}

// Sync is the quiescence barrier: it round-trips a no-op through the single-consumer
// FIFO event channel, so its ack proves every event enqueued before it was fully
// processed. This is the signature Scenario.AddBarrier takes.
func (m *modelCoordinator) Sync(ctx context.Context) error {
	ack := make(chan struct{})
	select {
	case m.events <- modelEvent{kind: "sync", ack: ack}:
	case <-ctx.Done():
		return ctx.Err()
	case <-m.doneCh:
		return errors.New("model coordinator stopped")
	}
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-m.doneCh:
		return errors.New("model coordinator stopped")
	}
}

func (m *modelCoordinator) Stop() {
	close(m.stopCh)
	<-m.doneCh
}

func (m *modelCoordinator) Trace() []modelStep {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]modelStep(nil), m.trace...)
}

func (m *modelCoordinator) Published() *overlay.Topology {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pub
}

func (m *modelCoordinator) Builds() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.trace {
		if s.Topo != nil {
			n++
		}
	}
	return n
}

func (m *modelCoordinator) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failure
}
