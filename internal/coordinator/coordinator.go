package coordinator

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"

	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// Sender is how the coordinator pushes a computed tree to a peer. It is
// consumer-defined here (the coordinator says what it needs, not how signaling
// provides it) so this package never imports signaling — the server supplies an
// adapter over the Hub. A failed send is transient (the peer may have just left)
// and never fatal.
type Sender interface {
	SendTopology(roomID, peerID string, topo *overlay.Topology) error
}

// Config parameterises the tree the coordinator builds. The two constraints flow
// straight into overlay.BuildTree; DefaultUploadKbps is the budget assumed for a
// peer that has joined but not yet reported.
//
// DefaultUploadKbps defaults to 0 — a not-yet-reported peer is treated as a LEAF
// until its first report proves it can relay. That conservative choice is
// load-bearing: promoting an unproven peer to relay could hand it children it
// cannot serve, and — because Phase 4's apply is additive — a transient
// wrong-relay tree cannot be fully torn down once realised. So we wait for the one
// report that costs at most a few seconds. A caller may set a positive value to
// attach newcomers at an assumed budget instead.
type Config struct {
	MaxDepth          int
	StreamKbps        int
	DefaultUploadKbps int
}

// Coordinator ingests room membership and telemetry, computes a forwarding tree on
// every threshold event, and pushes it to the peers. It is the Phase 4 control
// plane; for now it runs inside the central server (a documented stepping stone —
// Phase 6 migrates the role to an elected peer under server arbitration).
//
// Concurrency model — this is the fan-in the metrics package advertises, resolved:
// ALL state lives in one goroutine (Run), and every input (peer joined, peer left,
// a metrics report) is funnelled to it as an event on a channel. That is the "share
// memory by communicating" side of the trade-off, chosen over a mutex-guarded
// shared map because the coordinator is a control loop, not a hot path: one owner
// serialising join/leave/report/recompute means there are no locks to reason about
// and the recompute always sees a consistent snapshot. The alternative — an
// RWMutex around the state read by many hub goroutines — would be faster under
// contention we do not have and harder to keep correct. Named, and chosen.
type Coordinator struct {
	log  *slog.Logger
	cfg  Config
	send Sender

	events chan event
	done   chan struct{} // closed when Run returns; makes enqueue non-blocking after shutdown

	// rooms is owned solely by the Run goroutine — never touched under a lock,
	// because nothing else touches it.
	rooms map[string]*roomState
}

type roomState struct {
	nodes map[string]*nodeState // keyed by server-assigned peer id
	topo  *overlay.Topology     // last tree pushed for this room
}

// nodeState is the coordinator's record of one peer: its identity (id + name) and
// its latest telemetry. reported gates the "first report is a threshold event"
// rule — the first report carries a node's real capacity and warrants a recompute;
// later reports only refresh numbers and must NOT, or a chatty sensor would thrash
// the tree.
type nodeState struct {
	id       string
	name     string
	report   metrics.Report
	reported bool
}

// event kinds funnelled to the Run goroutine.
type eventKind int

const (
	evJoin eventKind = iota
	evLeave
	evReport
)

type event struct {
	kind   eventKind
	roomID string
	peerID string
	name   string
	report metrics.Report
}

// eventBuffer is how many membership/telemetry events may queue before an enqueuing
// hub goroutine blocks. Generous: events are tiny and the Run loop drains them
// quickly, so this is only a cushion for a burst of simultaneous joins.
const eventBuffer = 256

// New builds a Coordinator that pushes through send. Run must be called to start
// the owning goroutine; until then events buffer.
func New(log *slog.Logger, cfg Config, send Sender) *Coordinator {
	if cfg.DefaultUploadKbps < 0 {
		cfg.DefaultUploadKbps = 0 // a not-yet-reported peer is a leaf, never an unproven relay
	}
	return &Coordinator{
		log:    log.With(slog.String("component", "coordinator")),
		cfg:    cfg,
		send:   send,
		events: make(chan event, eventBuffer),
		done:   make(chan struct{}),
		rooms:  make(map[string]*roomState),
	}
}

// Run owns all coordinator state until ctx is cancelled, processing one event at a
// time. Callers run it on its own goroutine; it returns ctx.Err() on shutdown.
func (c *Coordinator) Run(ctx context.Context) error {
	defer close(c.done)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-c.events:
			c.handle(ev)
		}
	}
}

// PeerJoined, PeerLeft, and Metrics are the signaling.Observer surface — called
// from Hub goroutines, they only translate the callback into an event and enqueue
// it, so the Hub is never blocked on control-plane work.
func (c *Coordinator) PeerJoined(roomID, peerID, name string) {
	c.enqueue(event{kind: evJoin, roomID: roomID, peerID: peerID, name: name})
}

func (c *Coordinator) PeerLeft(roomID, peerID string) {
	c.enqueue(event{kind: evLeave, roomID: roomID, peerID: peerID})
}

func (c *Coordinator) Metrics(roomID, peerID string, payload []byte) {
	var rep metrics.Report
	if err := json.Unmarshal(payload, &rep); err != nil {
		c.log.Debug("bad metrics payload", slog.String("peer_id", peerID), slog.Any("error", err))
		return
	}
	c.enqueue(event{kind: evReport, roomID: roomID, peerID: peerID, report: rep})
}

// enqueue hands an event to the Run goroutine, giving up if the coordinator has
// shut down so a late hub callback can never block forever on a drained loop.
func (c *Coordinator) enqueue(ev event) {
	select {
	case c.events <- ev:
	case <-c.done:
	}
}

// handle applies one event to the owned state and recomputes when the event is a
// threshold one (join, leave, or a node's first report).
func (c *Coordinator) handle(ev event) {
	switch ev.kind {
	case evJoin:
		rs := c.room(ev.roomID)
		rs.nodes[ev.peerID] = &nodeState{id: ev.peerID, name: ev.name}
		c.log.Info("member joined", slog.String("room_id", ev.roomID),
			slog.String("peer_id", ev.peerID), slog.String("name", ev.name), slog.Int("members", len(rs.nodes)))
		c.recompute(ev.roomID, rs)
	case evLeave:
		rs := c.rooms[ev.roomID]
		if rs == nil {
			return
		}
		if _, ok := rs.nodes[ev.peerID]; !ok {
			return
		}
		delete(rs.nodes, ev.peerID)
		c.log.Info("member left", slog.String("room_id", ev.roomID),
			slog.String("peer_id", ev.peerID), slog.Int("members", len(rs.nodes)))
		if len(rs.nodes) == 0 {
			delete(c.rooms, ev.roomID) // don't leak empty rooms
			return
		}
		c.recompute(ev.roomID, rs)
	case evReport:
		rs := c.rooms[ev.roomID]
		if rs == nil {
			return
		}
		ns := rs.nodes[ev.peerID]
		if ns == nil {
			return // report from a peer we don't know (already left) — ignore
		}
		first := !ns.reported
		ns.report = ev.report
		ns.reported = true
		// Only a node's FIRST report changes the tree: it is when we learn the node's
		// real capacity. Subsequent reports refresh the stored numbers for whenever
		// the next membership change recomputes, but do not themselves trigger one —
		// that anti-thrash rule is the Phase 4 slice of hysteresis.
		if first {
			c.recompute(ev.roomID, rs)
		}
	}
}

// room returns the room's state, creating it on first sight.
func (c *Coordinator) room(roomID string) *roomState {
	rs := c.rooms[roomID]
	if rs == nil {
		rs = &roomState{nodes: make(map[string]*nodeState)}
		c.rooms[roomID] = rs
	}
	return rs
}

// recompute builds a fresh tree from the room's current membership and telemetry
// and pushes it to every managed (named) member. It is the only place BuildTree is
// called, so the "threshold events only" policy is enforced entirely by its
// callers.
func (c *Coordinator) recompute(roomID string, rs *roomState) {
	nodes := c.overlayNodes(rs)
	if len(nodes) == 0 {
		rs.topo = nil
		return
	}
	root := overlay.PickRoot(nodes)
	if root == "" {
		c.log.Warn("no relay-capable member; cannot build tree", slog.String("room_id", roomID))
		return
	}
	topo, err := overlay.BuildTree(nodes, overlay.Constraints{
		Root: root, MaxDepth: c.cfg.MaxDepth, StreamKbps: c.cfg.StreamKbps,
	})
	if err != nil {
		// Over-constrained (too little aggregate upload for this many peers, or a
		// depth bound too shallow). Keep the last good tree rather than tearing the
		// call down, and say why. Phase 7's simulcast is the real fix for capacity.
		c.log.Warn("build tree failed; keeping previous topology",
			slog.String("room_id", roomID), slog.Any("error", err))
		return
	}
	rs.topo = topo
	c.log.Info("computed topology",
		slog.String("room_id", roomID), slog.String("root", root),
		slog.Int("nodes", len(nodes)), slog.Int("edges", len(topo.Edges)))

	for _, ns := range rs.nodes {
		if ns.name == "" {
			continue // unmanaged peer (joined without -name) — nothing to push
		}
		if err := c.send.SendTopology(roomID, ns.id, topo); err != nil {
			c.log.Debug("push topology failed",
				slog.String("room_id", roomID), slog.String("peer_id", ns.id), slog.Any("error", err))
		}
	}
}

// overlayNodes projects the room's members into BuildTree's input, dropping peers
// that cannot be placed in a name-keyed tree (no name) or that collide on a name.
// A member without a report yet is included at the default capacity so it still
// gets attached immediately; its first report will refine the tree.
func (c *Coordinator) overlayNodes(rs *roomState) []overlay.Node {
	// Deterministic order: sort member ids so the projection (and thus root choice
	// among equals, and BuildTree's own tie-breaks) never depends on map iteration.
	ids := make([]string, 0, len(rs.nodes))
	for id := range rs.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var nodes []overlay.Node
	seenName := make(map[string]bool, len(ids))
	for _, id := range ids {
		ns := rs.nodes[id]
		if ns.name == "" {
			continue
		}
		if seenName[ns.name] {
			c.log.Warn("duplicate member name; skipping",
				slog.String("name", ns.name), slog.String("peer_id", id))
			continue
		}
		seenName[ns.name] = true

		up := c.cfg.DefaultUploadKbps
		nat := overlay.NATDirect
		if ns.reported {
			up = ns.report.UploadKbps
			if ns.report.NAT != "" {
				nat = ns.report.NAT
			}
		}
		nodes = append(nodes, overlay.Node{Name: ns.name, UploadKbps: up, NAT: nat})
	}
	return nodes
}
