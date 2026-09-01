package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/dashboard"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

// The narrow views of the signaling Hub that the adapters below take.
//
// They exist so each adapter names exactly the capability it uses and can be tested
// without a Hub, a listener, or a socket. *signaling.Hub satisfies all three
// structurally — Go's implicit interfaces mean signaling never learns they exist.

// frameBus is the outbound half of the Hub: address one peer, or the whole meet.
type frameBus interface {
	SendTo(roomID, peerID string, msg signaling.Message) bool
	SendRoom(roomID string, msg signaling.Message) int
}

// peerRelay is the single-peer half, used by the telemetry forwarder.
type peerRelay interface {
	SendTo(roomID, peerID string, msg signaling.Message) bool
}

// meetRoster is the Hub's ground truth about who is connected, used to turn a demo
// request's peer NAME into a decision about whether that peer exists at all.
type meetRoster interface {
	Roster(roomID string) []signaling.Peer
}

// livenessObserver is the arbiter's slice of signaling.Observer. It is deliberately
// FOUR methods, not five: the arbiter's liveness view is membership plus telemetry
// plus beats, and a self-promotion tells it nothing about whether a peer can
// coordinate. Widening it to match the Hub's Observer would force the arbiter to grow
// a method that ignores its argument, which is a worse lie than a narrower interface.
type livenessObserver interface {
	PeerJoined(roomID, peerID, name string)
	PeerLeft(roomID, peerID string)
	Metrics(roomID, peerID string, payload []byte)
	Heartbeat(roomID, peerID string, payload []byte)
}

// controlObserver is the coordinator's slice: the full Hub Observer surface.
//
// Both are declared HERE rather than imported from signaling so the fan-out can be
// tested with fakes; *arbiter.Arbiter and *coordinator.Coordinator satisfy them
// structurally, which is what keeps either package free of an import of the other.
type controlObserver interface {
	livenessObserver
	Reparented(roomID, peerID string, payload []byte)
}

// termSetter is the in-process coordinator's epoch surface. The announcer drives it,
// and naming it as an interface is what lets the ordering rule below be tested
// without running a coordinator goroutine.
type termSetter interface {
	SetEpoch(roomID string, epoch uint64)
	Yield(roomID string, newEpoch uint64)
}

// meetRegistry is the arbiter surface the demo controls need.
type meetRegistry interface {
	GetMeet(ctx context.Context, id string) (arbiter.Meet, error)
	ForceElection(ctx context.Context, roomID, name string) error
}

// roleTable remembers which peer id holds the coordinator role in each meet.
//
// It exists because §8 routing rule 2 makes the SERVER responsible for forwarding
// telemetry onward when the coordinator is an elected peer, and the Observer callback
// that must do the forwarding has no way to ask who that is. The arbiter knows, and
// announces it; this is where the announcement is remembered so the data path can
// read it.
//
// A mutex rather than a channel: this is shared state read on every telemetry frame
// and written once per election — the read-mostly case a RWMutex is actually for,
// where a channel would put a serialisation point on the hot path to protect a single
// map lookup.
type roleTable struct {
	mu     sync.RWMutex
	byRoom map[string]string
}

func newRoleTable() *roleTable { return &roleTable{byRoom: map[string]string{}} }

// set records the coordinator id announced for a meet.
func (t *roleTable) set(roomID, coordinatorID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.byRoom[roomID] = coordinatorID
}

// get returns the meet's coordinator id, or "" if none has been announced.
func (t *roleTable) get(roomID string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.byRoom[roomID]
}

// forget drops a meet's entry. It is called when the meet empties, which is what
// bounds this map: without it, every meet id ever announced would be retained
// forever, and meets are created from an unauthenticated surface.
func (t *roleTable) forget(roomID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.byRoom, roomID)
}

func (t *roleTable) len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.byRoom)
}

// hubSender adapts the Hub to coordinator.Sender: it marshals a computed topology
// into a TypeTopology frame and delivers it to one peer by id. This is the glue that
// keeps the coordinator ignorant of the wire (it holds a Sender, not a Hub) and the
// Hub ignorant of the coordinator (it holds an Observer, not a coordinator) — the
// seam lives here, in main, where both are already known.
type hubSender struct {
	bus peerRelay
}

// SendTopology pushes one peer's view of the tree.
func (s hubSender) SendTopology(roomID, peerID string, topo *overlay.Topology) error {
	payload, err := json.Marshal(topo)
	if err != nil {
		return fmt.Errorf("marshal topology: %w", err)
	}
	if !s.bus.SendTo(roomID, peerID, signaling.Message{
		Type: signaling.TypeTopology, To: peerID, Payload: payload,
	}) {
		return fmt.Errorf("peer %q not present in meet %q", peerID, roomID)
	}
	return nil
}

// hubAnnouncer adapts the Hub to arbiter.Announcer, and is also the single point
// where the arbiter's decision reaches the in-process coordinator.
//
// It is the one place in the process that knows an election happened AND holds
// something to tell about it, which is why three consequences converge here: the
// broadcast, the role table the telemetry forwarder reads, and the local
// coordinator's adopt-or-yield.
type hubAnnouncer struct {
	log   *slog.Logger
	bus   frameBus
	roles *roleTable
	// term is the in-process coordinator, nil without -coordinate.
	term termSetter
}

// AnnounceCoordinator broadcasts an announcement to every member of a meet and lets
// the local coordinator act on it.
//
// ORDER IS LOAD-BEARING. The frame is queued on every peer's outbound channel BEFORE
// the local coordinator is told to serve the epoch, because the coordinator's first
// push for that epoch rides the SAME FIFO channel. Queue them the other way round and
// a peer can be handed a topology stamped with an epoch it has not adopted yet, which
// its fence rejects (§6.5) — and nothing retries until the next rebuild. This is the
// same argument the Hub already makes for queueing TypeJoined before PeerJoined.
func (a *hubAnnouncer) AnnounceCoordinator(roomID string, an arbiter.Announcement) error {
	payload, err := json.Marshal(an)
	if err != nil {
		return fmt.Errorf("marshal announcement: %w", err)
	}
	a.roles.set(roomID, an.CoordinatorID)
	// A zero return is not an error: an announcement for a meet whose members all
	// just left has nobody to reach, and the arbiter reaps that meet moments later.
	reached := a.bus.SendRoom(roomID, signaling.Message{
		Type: signaling.TypeCoordinator, Payload: payload,
	})
	a.adopt(roomID, an)
	a.log.Debug("announced coordinator",
		slog.String("room_id", roomID),
		slog.Uint64("epoch", an.Epoch),
		slog.String("coordinator_id", an.CoordinatorID),
		slog.Int("reached", reached))
	return nil
}

// RepairCoordinator re-sends a meet's CURRENT announcement to ONE lagging peer
// (§6.8), verbatim and unicast.
//
// It deliberately does NOT touch the role table or the local coordinator's term: the
// epoch it carries is already in force, so re-adopting it would reset the
// coordinator's rev for a frame that changed nothing.
func (a *hubAnnouncer) RepairCoordinator(roomID, peerID string, an arbiter.Announcement) error {
	payload, err := json.Marshal(an)
	if err != nil {
		return fmt.Errorf("marshal announcement: %w", err)
	}
	if !a.bus.SendTo(roomID, peerID, signaling.Message{
		Type: signaling.TypeCoordinator, To: peerID, Payload: payload,
	}) {
		return fmt.Errorf("peer %q not present in meet %q", peerID, roomID)
	}
	return nil
}

// adopt applies an announcement to the in-process coordinator: serve the term if the
// arbiter itself holds the role, stand down otherwise.
//
// A vacancy (Coordinator "" with CoordinatorID "") yields for the same reason a peer
// promotion does — this process is not the coordinator either way, and the raised
// epoch floor is what stops the announcement that demoted it from also resuming it.
func (a *hubAnnouncer) adopt(roomID string, an arbiter.Announcement) {
	if a.term == nil {
		return
	}
	if an.CoordinatorID == signaling.ServerID {
		a.term.SetEpoch(roomID, an.Epoch)
		return
	}
	a.term.Yield(roomID, an.Epoch)
}

// planeObserver is the Hub's single Observer, fanned out to both control planes.
//
// The fan-out is the mechanism, not a convenience: it gives the arbiter a liveness
// view that does not depend on the coordinator being alive to process it, which is
// what lets the arbiter fail a wedged coordinator over WITHOUT importing coordinator.
// Wiring only one of the two compiles, runs, and looks healthy right up until the
// failover that never happens.
//
// It is also where §8 routing rule 2's forwarding lives: metrics, heartbeats and
// reparents terminate at the server, and when the coordinator is an elected PEER this
// is what carries them the last hop. One WebSocket per peer; the peer never opens a
// second control link.
type planeObserver struct {
	log   *slog.Logger
	arb   livenessObserver
	coord controlObserver // nil without -coordinate
	roles *roleTable
	relay peerRelay // nil in tests that do not exercise forwarding
	// roster reports who is still connected, so an emptied meet can be forgotten
	// from the role table. nil disables the pruning.
	roster meetRoster
}

// PeerJoined fans a join to both planes.
func (o *planeObserver) PeerJoined(roomID, peerID, name string) {
	o.arb.PeerJoined(roomID, peerID, name)
	if o.coord != nil {
		o.coord.PeerJoined(roomID, peerID, name)
	}
}

// PeerLeft fans a departure to both planes and prunes the role table once the meet is
// empty — the event-driven bound on the one map this binary owns.
func (o *planeObserver) PeerLeft(roomID, peerID string) {
	o.arb.PeerLeft(roomID, peerID)
	if o.coord != nil {
		o.coord.PeerLeft(roomID, peerID)
	}
	if o.roster != nil && len(o.roster.Roster(roomID)) == 0 {
		o.roles.forget(roomID)
	}
}

// Metrics fans telemetry to both planes and forwards it to an elected coordinator.
func (o *planeObserver) Metrics(roomID, peerID string, payload []byte) {
	o.arb.Metrics(roomID, peerID, payload)
	if o.coord != nil {
		o.coord.Metrics(roomID, peerID, payload)
	}
	o.forward(roomID, peerID, signaling.TypeMetrics, payload)
}

// Heartbeat fans a beat to both planes and forwards it to an elected coordinator.
func (o *planeObserver) Heartbeat(roomID, peerID string, payload []byte) {
	o.arb.Heartbeat(roomID, peerID, payload)
	if o.coord != nil {
		o.coord.Heartbeat(roomID, peerID, payload)
	}
	o.forward(roomID, peerID, signaling.TypeHeartbeat, payload)
}

// Reparented reports that a peer promoted its own backup parent.
//
// It reaches only the coordinator and the elected coordinator peer: the arbiter's
// liveness view has no Reparented method, because who a peer's parent is says nothing
// about whether that peer could coordinate. See livenessObserver.
func (o *planeObserver) Reparented(roomID, peerID string, payload []byte) {
	if o.coord != nil {
		o.coord.Reparented(roomID, peerID, payload)
	}
	o.forward(roomID, peerID, signaling.TypeReparented, payload)
}

// forward carries a terminated control frame the last hop to an ELECTED coordinator
// peer (§8 rule 2). It is a no-op when nobody has been elected, when the arbiter
// itself coordinates (the frame is already home), and when the sender IS the
// coordinator (echoing its own beat back to it buys nothing and doubles the traffic
// on the one socket that can least afford it).
//
// From stays the ORIGINAL peer's id, deliberately: the Hub preserves an explicit From
// rather than stamping the server's, so the elected coordinator attributes telemetry
// to the peer that produced it instead of fencing it as an arbiter-minted frame.
func (o *planeObserver) forward(roomID, peerID string, kind signaling.Type, payload []byte) {
	if o.relay == nil {
		return
	}
	coordID := o.roles.get(roomID)
	if coordID == "" || coordID == signaling.ServerID || coordID == peerID {
		return
	}
	if !o.relay.SendTo(roomID, coordID, signaling.Message{
		Type: kind, From: peerID, To: coordID, Payload: payload,
	}) {
		// Transient by nature: the coordinator may have just left, and the arbiter's
		// own liveness view will fail it over. Debug, not Warn — during a handover
		// this is the expected shape of the race.
		o.log.Debug("could not forward a control frame to the elected coordinator",
			slog.String("room_id", roomID), slog.String("coordinator_id", coordID),
			slog.String("type", string(kind)))
	}
}

// connEntry is one tracked WebSocket connection's teardown handle.
type connEntry struct{ cancel context.CancelFunc }

// evictor gives the demo surface the ability to close ONE peer's WebSocket.
//
// signaling exports no such method, and it does not need to: ServeWS derives its
// per-connection context from the REQUEST's context, and cmd/server owns the handler
// that supplies that request. So the capability is obtained by owning the context
// rather than by reaching into another package — cancelling it unblocks both pumps
// and ends the read loop, which is exactly what a dead socket does. That the eviction
// path and the real failure path are the same code is the reason -demo is worth
// having at all.
//
// Keyed by (meet, name) because that is what the demo API names, and the Hub already
// rejects a duplicate name within a meet, so the key identifies at most one LIVE
// connection. The value is a set anyway: a rejected duplicate is briefly tracked
// alongside the incumbent, and cancelling an already-finished connection is a no-op.
type evictor struct {
	mu    sync.Mutex
	conns map[evictKey]map[*connEntry]struct{}
}

type evictKey struct{ roomID, name string }

func newEvictor() *evictor {
	return &evictor{conns: map[evictKey]map[*connEntry]struct{}{}}
}

// track registers one connection and returns the context ServeWS must run under plus
// the release function the handler defers. A peer with no -name cannot be addressed
// by the demo API, so it is not tracked; its context is still cancellable so the
// caller's shape does not change.
func (e *evictor) track(roomID, name string, parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	if name == "" {
		return ctx, cancel
	}
	key := evictKey{roomID: roomID, name: name}
	entry := &connEntry{cancel: cancel}

	e.mu.Lock()
	set := e.conns[key]
	if set == nil {
		set = map[*connEntry]struct{}{}
		e.conns[key] = set
	}
	set[entry] = struct{}{}
	e.mu.Unlock()

	return ctx, func() {
		e.mu.Lock()
		if set := e.conns[key]; set != nil {
			// Delete by POINTER identity, never by key: a reconnect under the same
			// name may already have registered its own entry, and removing the key
			// would strand it as untrackable.
			delete(set, entry)
			if len(set) == 0 {
				delete(e.conns, key)
			}
		}
		e.mu.Unlock()
		cancel()
	}
}

// evict cancels every connection tracked under (roomID, name) and reports whether
// there was one.
func (e *evictor) evict(roomID, name string) bool {
	e.mu.Lock()
	set := e.conns[evictKey{roomID: roomID, name: name}]
	entries := make([]*connEntry, 0, len(set))
	for entry := range set {
		entries = append(entries, entry)
	}
	e.mu.Unlock()

	for _, entry := range entries {
		entry.cancel()
	}
	return len(entries) > 0
}

func (e *evictor) len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.conns)
}

// demoControl is dashboard.DemoControl, and §2.4 confirms it can only live here:
// evicting a peer means closing a socket, which needs the Hub, and forcing an
// election needs the arbiter. cmd/server is the only holder of both.
type demoControl struct {
	meets  meetRegistry
	roster meetRoster
	ev     *evictor
}

// Evict closes the named peer's WebSocket, producing exactly the failure the liveness
// path exists to handle.
//
// The two 404s are distinguished on purpose and both are returned as WRAPPED
// SENTINELS, because the dashboard's boundary matches with errors.Is and never on
// message text: a bare fmt.Errorf here would silently become a 500 where §9.4a
// promises a 404.
func (d *demoControl) Evict(ctx context.Context, roomID, name string) error {
	if _, err := d.meets.GetMeet(ctx, roomID); err != nil {
		return fmt.Errorf("evict %q: %w", name, err)
	}
	present := false
	for _, p := range d.roster.Roster(roomID) {
		if p.Name == name {
			present = true
			break
		}
	}
	if !present {
		return fmt.Errorf("evict %q in meet %q: %w", name, roomID, dashboard.ErrMemberNotFound)
	}
	if !d.ev.evict(roomID, name) {
		// In the roster but untracked: the peer is mid-disconnect, or joined without
		// a -name and cannot be addressed. Either way the target is not there to
		// evict, which is a 404 and not a server fault.
		return fmt.Errorf("evict %q in meet %q: no live connection to close: %w",
			name, roomID, dashboard.ErrMemberNotFound)
	}
	return nil
}

// ForceElection forces an arbiter.ReasonManual announcement, bypassing ElectionDwell
// and MinTermDuration. An empty name means "the best candidate".
func (d *demoControl) ForceElection(ctx context.Context, roomID, name string) error {
	return d.meets.ForceElection(ctx, roomID, name)
}

// publisherRef breaks the ONE cycle in the object graph.
//
// The package graph is acyclic — dashboard imports coordinator and arbiter, neither
// imports dashboard — but the OBJECTS are mutually referential: the coordinator needs
// a Publisher to report into, and the dashboard needs the coordinator to snapshot. Go
// has no way to construct both at once, so one side is late-bound.
//
// It is set exactly once, before any goroutine starts and before the listener opens,
// which is the same discipline (and the same safety argument) as signaling's
// SetObserver. A nil target means "publish nothing", which is legal for both
// Publisher interfaces and is what -dashboard=false leaves behind.
type publisherRef struct {
	mu   sync.RWMutex
	dash *dashboard.Server
}

// set binds the dashboard. Called once during wiring.
func (p *publisherRef) set(d *dashboard.Server) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dash = d
}

func (p *publisherRef) target() *dashboard.Server {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dash
}

// Publish satisfies coordinator.Publisher.
func (p *publisherRef) Publish(ev coordinator.Event) {
	if d := p.target(); d != nil {
		d.Publish(ev)
	}
}

// PublishElection satisfies arbiter.Publisher.
func (p *publisherRef) PublishElection(a arbiter.Announcement) {
	if d := p.target(); d != nil {
		d.PublishElection(a)
	}
}

// PublishRepair satisfies arbiter.Publisher.
func (p *publisherRef) PublishRepair(roomID, name string, peerEpoch, meetEpoch uint64, resolved bool) {
	if d := p.target(); d != nil {
		d.PublishRepair(roomID, name, peerEpoch, meetEpoch, resolved)
	}
}
