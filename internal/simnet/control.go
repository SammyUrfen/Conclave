package simnet

import (
	"sort"
	"sync"

	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// This file is what lets a Scenario drive the REAL internal/coordinator instead of a
// model of it: the two seams the loop needs (a Publisher and a Sender) and the
// projection that turns a simnet fleet member into the telemetry frame the loop
// ingests.
//
// The import direction is deliberate and permitted. docs/PLAN.md §2.3 grants
// `simnet -> overlay, coordinator, metrics, clock`, and simnet is TEST-ONLY —
// production never imports it — so this adds no production edge and cannot create a
// cycle. What WOULD create one is an internal (white-box) test file inside
// internal/coordinator importing simnet; that is why the coordinator's clock-driven
// tests live in the external package coordinator_test.
//
// Both recorders are safe for concurrent use, because they must be: the coordinator
// calls Publish from its Run goroutine and SendTopology from a separate outbound
// goroutine, while the scenario's goroutine reads them between steps.

// Push is one recorded SendTopology call.
type Push struct {
	RoomID string
	PeerID string
	// Topo is the tree as it was handed to the sender. The coordinator never mutates
	// a published topology in place, so holding the pointer is safe — DO NOT MUTATE.
	Topo *overlay.Topology
}

// Capture is a coordinator.Sender that records what the control loop decided to send
// instead of putting it on a wire. It is how a scenario asserts on the loop's
// OUTBOUND surface, which is a different question from what the loop decided: a
// yielded coordinator may still compute a tree, and the thing that must be true is
// that the tree never leaves.
type Capture struct {
	mu     sync.Mutex
	pushes []Push
}

// NewCapture returns an empty Capture.
func NewCapture() *Capture { return &Capture{} }

// SendTopology records the push and returns nil. It never blocks and never does I/O,
// which is what coordinator.Sender requires of every implementation.
func (c *Capture) SendTopology(roomID, peerID string, topo *overlay.Topology) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pushes = append(c.pushes, Push{RoomID: roomID, PeerID: peerID, Topo: topo})
	return nil
}

// Pushes returns every recorded push, in call order.
func (c *Capture) Pushes() []Push {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Push(nil), c.pushes...)
}

// Recipients returns the distinct peer ids pushed to, sorted. Sorted rather than in
// call order because "did every member get the tree" is a set question, and fan-out
// order is an implementation detail no test should pin.
func (c *Capture) Recipients() []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range c.Pushes() {
		if !seen[p.PeerID] {
			seen[p.PeerID] = true
			out = append(out, p.PeerID)
		}
	}
	sort.Strings(out)
	return out
}

// Latest returns the most recently pushed tree, or nil if nothing was pushed.
func (c *Capture) Latest() *overlay.Topology {
	p := c.Pushes()
	for i := len(p) - 1; i >= 0; i-- {
		if p[i].Topo != nil {
			return p[i].Topo
		}
	}
	return nil
}

// Reset discards the recorded pushes, so a scenario can assert about one phase of a
// handover without the previous phase's traffic in the way.
func (c *Capture) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pushes = nil
}

// Recorder is a coordinator.Publisher that records the control loop's observable
// event stream — the dashboard's view of it. Scenarios assert against this rather
// than against internal state, because the contract requires the coordinator to be
// LEGIBLE about what it did, not merely correct.
type Recorder struct {
	mu     sync.Mutex
	events []coordinator.Event
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// Publish records the event. It never blocks, as coordinator.Publisher requires.
func (r *Recorder) Publish(ev coordinator.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

// Events returns every recorded event, in publication order.
func (r *Recorder) Events() []coordinator.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]coordinator.Event(nil), r.events...)
}

// OfKind returns every recorded event of one kind, in order.
func (r *Recorder) OfKind(kind coordinator.EventKind) []coordinator.Event {
	var out []coordinator.Event
	for _, ev := range r.Events() {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

// Last returns the most recent event of a kind and whether one exists.
func (r *Recorder) Last(kind coordinator.EventKind) (coordinator.Event, bool) {
	evs := r.OfKind(kind)
	if len(evs) == 0 {
		return coordinator.Event{}, false
	}
	return evs[len(evs)-1], true
}

// Kinds returns the recorded kinds in order, which is what a failure message should
// print: "the loop published nothing" is far less useful than the sequence it did
// publish.
func (r *Recorder) Kinds() []string {
	var out []string
	for _, ev := range r.Events() {
		out = append(out, string(ev.Kind))
	}
	return out
}

// Reset discards the recorded events.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

// ReportOf projects a fleet member into the telemetry frame the coordinator ingests.
//
// It carries only what a real peer can measure about ITSELF — upload budget, NAT
// class, uplink loss. Notably it drops Node.RTT: pairwise latency is not measured in
// production, so the coordinator's projection has no field for it, and a scenario
// that fed RTT to the real loop would be testing an input the loop cannot receive.
// Node.Impaired is likewise absent by design: impairment is the coordinator's own
// conclusion after a full degradation dwell, not something a peer declares.
func ReportOf(n overlay.Node) metrics.Report {
	return metrics.Report{
		Name:       n.Name,
		UploadKbps: n.UploadKbps,
		NAT:        n.NAT,
		LossPct:    n.LossPct,
	}
}
