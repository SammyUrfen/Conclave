package signaling

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/policy"
)

const (
	// defaultRoom is used when a peer dials /ws without a ?room= query.
	defaultRoom = "default"
	// writeTimeout bounds a single frame write. Generous: signaling frames are
	// tiny, so exceeding this means the socket is genuinely stuck.
	writeTimeout = 10 * time.Second
	// outboundBuffer is how many frames may queue for one client before we give
	// up on it. Signaling is bursty-but-tiny (a handful of SDP/ICE frames per
	// join), so a healthy peer never approaches this; a client that lets 32
	// frames pile up is wedged or hostile, and send() drops it.
	outboundBuffer = 32
	// readLimit caps an inbound frame. A full SDP offer with many codecs and
	// candidates can run a few KB; 1 MiB is comfortably above that and well
	// below anything that could exhaust memory.
	readLimit = 1 << 20
)

// The control-link liveness constants. They are a FAMILY, not three independent
// numbers, and the relationship between them is the load-bearing part.
const (
	// WSPingInterval is how often the Hub sends a WebSocket protocol ping.
	//
	// A TCP connection that dies without a FIN — a sleeping laptop, a yanked
	// Ethernet cable — leaves wsjson.Read blocked forever, so without a ping the
	// Hub would carry a ghost member in the roster indefinitely. 5 s is far inside
	// any NAT or proxy idle timeout (typically 60 s+), so the ping doubles as a
	// keepalive, and it is cheap: a ping frame is a handful of bytes next to a
	// 2 Mbit/s video stream.
	//
	// It is deliberately NOT the 15 s a keepalive alone would want. See
	// WSLivenessBudget for the constraint that pins it down.
	WSPingInterval = 5 * time.Second
	// WSPingTimeout is how long the Hub waits for the matching pong. A pong needs
	// no application processing, so a socket that cannot turn one around in 2 s is
	// dead rather than slow. It must stay strictly below WSPingInterval or pings
	// would overlap; NewHubWithConfig enforces that.
	WSPingTimeout = 2 * time.Second
	// WSLivenessBudget is the worst-case time for the Hub to notice a dead socket:
	// the socket can die immediately after a successful ping, so detection takes a
	// full interval plus the pong timeout.
	//
	// REQUIRED RELATIONSHIP, and the reason the two constants above are as tight as
	// they are: this budget must not exceed the control plane's "gone" threshold
	// (metrics.GoneAfter of the peer's declared heartbeat cadence, 8 s at the 1 s
	// default). If the socket detector were the slower of the two, there would be a
	// window in which the coordinator has declared a peer gone and removed it from
	// the tree while its socket is still registered here — and since TypeJoined is
	// only ever sent on a NEW connection, a peer whose socket never closed could
	// never rejoin. It would be permanently ejected from a meet it believes it is
	// still in. cmd/server validates the relationship at startup with
	// metrics.ValidateLivenessBudget; this comment states it where the numbers live.
	WSLivenessBudget = WSPingInterval + WSPingTimeout
)

// RoomIDPattern is the one rule for what a meet id may look like. A meet id is used
// verbatim as a ?room= query value and as an HTTP path segment, so it is constrained
// at the boundary rather than escaped at every use.
//
// The same pattern governs POST /api/meets. Enforcing it here too is what stops a
// meet existing that /ws admitted but the API would reject — a meet the dashboard
// could never render.
//
// RULING A (docs/PLAN.md §2.4): the rule itself lives in internal/policy now, so
// the dashboard's meet-creation endpoint can share it without importing signaling
// (the DAG forbids dashboard → signaling — see §2.3). This is a documented alias
// kept for source compatibility with every Phase 1-4 call site that named
// RoomIDPattern/ValidRoomID directly; it is not a second copy of the rule, and
// TestRoomIDValidation pins that it never becomes one.
const RoomIDPattern = policy.MeetIDPattern

// ValidRoomID reports whether id is an acceptable meet id (RoomIDPattern). See
// RoomIDPattern for why this delegates to internal/policy instead of owning the
// rule.
func ValidRoomID(id string) bool { return policy.ValidMeetID(id) }

// Observer is the control-plane's window into room membership and telemetry. A Hub
// with an observer set (Phase 4's coordinator) calls it when a peer joins or
// leaves and when a metrics frame arrives — the events that let the coordinator
// track who is present and recompute the forwarding tree. It is deliberately
// declared HERE, in the signaling layer, and takes only strings and raw bytes: the
// Hub calls it but never imports the coordinator, so the wire layer stays free of
// control-plane types. The methods are invoked from Hub goroutines and must be
// safe to call concurrently and cheap (the coordinator's implementation just
// enqueues an event and returns).
type Observer interface {
	// PeerJoined reports a new member. name is the peer's self-declared label.
	PeerJoined(roomID, peerID, name string)
	// PeerLeft reports a member whose socket has closed — a graceful quit, a write
	// failure, or a keepalive ping the peer failed to answer.
	PeerLeft(roomID, peerID string)
	// Metrics delivers an opaque metrics.Report body.
	Metrics(roomID, peerID string, payload []byte)
	// Heartbeat delivers an opaque metrics.Heartbeat body.
	//
	// RESURRECTION CONTRACT — the half the control plane must implement. The Hub
	// keeps no health state: it calls this for every LIVE socket, whatever verdict
	// the control plane has reached about that peer. So a heartbeat arriving for a
	// peerID the control plane has already declared gone is PROOF the peer is back,
	// and MUST be treated as a re-join rather than dropped. Everything a re-join
	// needs is present: peerID is server-stamped here, and the payload carries the
	// peer's name. The Hub cannot help by re-sending TypeJoined, because that frame
	// is bound to a new connection and this socket never closed — this callback is
	// the only re-entry point. Hub.Roster is the authoritative cross-check.
	Heartbeat(roomID, peerID string, payload []byte)
	// Reparented delivers an opaque metrics.Reparented body: a peer promoted its
	// own backup parent, or failed to and is stranded.
	Reparented(roomID, peerID string, payload []byte)
}

// Hub is the signaling rendezvous: it multiplexes connected peers into rooms and
// relays frames between them. One Hub is constructed per server process and its
// ServeWS method is mounted as an http.HandlerFunc.
//
// All mutable state lives behind a single mutex. Rooms are independent and the
// critical sections are tiny (map lookups, never I/O), so one lock is simpler
// than per-room locks and shows no contention at this scale — the mutex-vs-
// finer-locking trade-off, resolved toward simplicity until a profile says
// otherwise. Crucially, actual sends happen AFTER the lock is released (see
// register/unregister/relay): we snapshot the target clients under the lock,
// then fan out, so a slow socket can never be written to while the map is held.
// HubConfig parameterises a Hub. Every field has a working zero value except Log,
// so a caller may pass only what it wants to change.
type HubConfig struct {
	// Log is the structured logger. nil falls back to slog.Default().
	Log *slog.Logger
	// Clock drives the keepalive ping. nil ⇒ clock.System().
	Clock clock.Clock
	// AllowedOrigins gates the WebSocket upgrade. Empty means same-origin only,
	// which is the Phase 1-4 behaviour and is correct for a Go peer (it sends no
	// Origin header at all). The browser dashboard lives on a different origin, so
	// it needs its host listed here — the SAME internal/policy.Origins value the
	// REST surface uses for CORS (see policy.ParseOrigins), injected in both
	// places rather than written down twice (docs/PLAN.md §2.4, §9.8). Patterns
	// are matched by path.Match against "scheme://host", so "http://localhost:*"
	// is a port wildcard. Never pass "*".
	AllowedOrigins policy.Origins
	// PingInterval / PingTimeout override the keepalive cadence. 0 ⇒ the WSPing*
	// defaults. They exist so a test can observe a reap without waiting seconds,
	// and so an operator running a non-default -heartbeat can keep the two liveness
	// detectors consistent (see WSLivenessBudget).
	PingInterval time.Duration
	PingTimeout  time.Duration
}

type Hub struct {
	log *slog.Logger
	clk clock.Clock

	// allowedOrigins, pingInterval and pingTimeout are fixed at construction and
	// read-only thereafter, so they need no lock.
	allowedOrigins policy.Origins
	pingInterval   time.Duration
	pingTimeout    time.Duration

	mu    sync.Mutex
	rooms map[string]map[string]*member // roomID → peerID → member
	seq   uint64                        // monotonic id source; guarded by mu

	// obs is the optional control-plane observer (Phase 4 coordinator). nil for a
	// pure signaling relay (Phases 1–3), which keeps that behaviour untouched. It is
	// set once at startup before any connection is served, so it needs no lock.
	obs Observer
}

// NewHub returns a ready Hub with default configuration, logging through the given
// logger. It is the Phase 1-4 constructor and is kept so no existing call site has
// to change; it delegates to NewHubWithConfig.
//
// It cannot fail and so keeps the non-erroring signature: HubConfig{Log: log}'s
// defaults (WSPingInterval, WSPingTimeout) are compile-time constants, known-good,
// and pinned by TestHubConfigDefaults — a constructor that cannot fail should not
// pretend it can (docs/PLAN.md §4.2). If NewHubWithConfig ever rejected them, that
// would mean the defaults themselves regressed, which is a programmer error worth
// a panic naming the bug, not a startup error naming an operator's flag.
func NewHub(log *slog.Logger) *Hub {
	h, err := NewHubWithConfig(HubConfig{Log: log})
	if err != nil {
		panic(fmt.Sprintf("signaling: NewHub's own defaults are self-contradictory: %v", err))
	}
	return h
}

// ErrInvalidPingConfig reports a self-contradictory HubConfig ping configuration:
// PingTimeout is not strictly shorter than PingInterval. Wrapped with %w in the
// error NewHubWithConfig returns, so a caller can match it with errors.Is rather
// than parsing a message.
var ErrInvalidPingConfig = errors.New("signaling: ping timeout must be strictly shorter than ping interval")

// NewHubWithConfig returns a ready Hub built from cfg, or an error if cfg is
// self-contradictory (currently: PingTimeout >= PingInterval, which would let
// pings overlap and make WSLivenessBudget's detection window unbounded).
//
// RULING B (docs/PLAN.md §2.4/§4.2, §15.4): this returns an error rather than
// panicking, because PingInterval/PingTimeout are OPERATOR INPUT — the same
// family as -allowed-origins, destined to be wired from flags — not a programmer
// error. The project's rule is errors-as-values with fail-loud-at-startup,
// translated by run() error into one sentence on stderr (e.g.
// "server: signaling: ..."); a panic there produces a stack trace where the
// operator needs one sentence naming the field they got wrong.
func NewHubWithConfig(cfg HubConfig) (*Hub, error) {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.System()
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = WSPingInterval
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = WSPingTimeout
	}
	if cfg.PingTimeout >= cfg.PingInterval {
		return nil, fmt.Errorf("%w: HubConfig.PingTimeout=%v, HubConfig.PingInterval=%v",
			ErrInvalidPingConfig, cfg.PingTimeout, cfg.PingInterval)
	}
	return &Hub{
		log:            cfg.Log.With(slog.String("component", "signaling")),
		clk:            cfg.Clock,
		allowedOrigins: cfg.AllowedOrigins,
		pingInterval:   cfg.PingInterval,
		pingTimeout:    cfg.PingTimeout,
		rooms:          make(map[string]map[string]*member),
	}, nil
}

// LivenessBudget is the worst case time this Hub takes to notice a dead socket. It
// is exported so the binary that also knows the heartbeat cadence can validate the
// cross-package relationship documented on WSLivenessBudget.
func (h *Hub) LivenessBudget() time.Duration { return h.pingInterval + h.pingTimeout }

// SetObserver attaches a control-plane observer. Call it once, before serving any
// connection (there is no lock guarding obs precisely because it is fixed at
// startup). Passing nil leaves the Hub a plain relay.
func (h *Hub) SetObserver(o Observer) { h.obs = o }

// nextID hands out a fresh, process-unique peer id. A monotonic counter is
// enough here: ids only need to be unique among live connections and are never
// persisted. (A production rendezvous would use an unguessable id — crypto/rand
// or a UUID — so a peer cannot probe or hijack another's id; noted, deferred.)
//
// It takes the lock on its own, then releases it, precisely because Go's
// sync.Mutex is NOT reentrant (unlike Java's ReentrantLock): register() also
// locks, so if register called nextID while holding mu it would deadlock. Keep
// each locked section flat and self-contained.
func (h *Hub) nextID() string {
	h.mu.Lock()
	h.seq++
	n := h.seq
	h.mu.Unlock()
	return fmt.Sprintf("p%d", n)
}

// ServeWS upgrades an HTTP request to a WebSocket and runs that peer's
// connection to completion. It is the http.HandlerFunc mounted at GET /ws.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	// WebSocket upgrades are NOT subject to CORS — the browser sends Origin and it
	// is the server's job to check it. An unlisted origin gets the upgrade refused
	// outright, not a 200 with an empty stream.
	//
	// The check is OURS, and InsecureSkipVerify turns the library's off. That reads
	// backwards, so: websocket.Accept's own check returns ALLOW as soon as the Origin
	// header's host equals the request's Host — before it looks at OriginPatterns at
	// all, and ignoring the scheme. Host is a client-supplied header, so that rule
	// lets a caller authorise itself, which is exactly what DNS rebinding
	// manufactures: an attacker rebinds evil.example to this server's address, the
	// victim's browser sends Host: evil.example with Origin: http://evil.example, the
	// two agree, and the attacker has a socket into a process they could never dial
	// directly. That reachability is the whole reason "it only listens on localhost"
	// is considered safe. Handing the library our patterns therefore admits a
	// strictly larger set than the REST surface does, from the one component whose
	// purpose is to be the single trustworthy matcher.
	//
	// So: skip the library's gate and apply policy.Origins.AllowUpgrade, which the
	// dashboard's event stream applies too. An absent Origin is allowed there —
	// every Go peer sends none, and breaking that breaks the data plane.
	if origin := r.Header.Get("Origin"); !h.allowedOrigins.AllowUpgrade(origin) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		h.log.Warn("rejected websocket upgrade from unlisted origin",
			slog.String("origin", origin), slog.String("host", r.Host))
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Safe ONLY because the gate above has already run. Never set this without
		// an explicit origin check preceding it.
		InsecureSkipVerify: true,
	})
	if err != nil {
		// Accept has already written an error response to w.
		h.log.Warn("websocket accept failed", slog.Any("error", err))
		return
	}
	// Backstop close: CloseNow aborts without a closing handshake. The clean
	// paths below end by returning from readLoop; this defer only matters if we
	// bail early.
	defer conn.CloseNow()
	conn.SetReadLimit(readLimit)

	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		roomID = defaultRoom
	}
	// Reject rather than normalise: a silently rewritten meet id would put a peer in
	// a different meet than the one it asked for, which is far harder to debug than
	// a refusal. The write is safe without a writePump because no other goroutine
	// has this connection yet.
	if !ValidRoomID(roomID) {
		_ = wsjson.Write(r.Context(), conn, Message{Type: TypeError, From: ServerID,
			Error: fmt.Sprintf("meet id %q is invalid; it must match %s", roomID, RoomIDPattern)})
		h.log.Warn("rejected invalid meet id", slog.String("room_id", roomID))
		return
	}
	// name is the peer's self-declared label (optional). It is authoritative only
	// as "what this connection calls itself" — the id remains the routing address.
	//
	// It is bounded and charset-restricted at this boundary for the same reason the
	// meet id is, and with more urgency: a name is not merely stored, it is FANNED
	// OUT — announced to every member on join and on leave, carried in every roster
	// snapshot, used as a map key, adopted as the relay tree's node identity, and
	// re-serialised into every dashboard event. An unbounded name is therefore an
	// amplification primitive, N-subscribers wide, bounded only by MaxHeaderBytes.
	// Reject rather than truncate: a shortened name could collide with a live one,
	// and collisions are exactly what the join path refuses connections to prevent.
	name := r.URL.Query().Get("name")
	if !policy.ValidPeerName(name) {
		_ = wsjson.Write(r.Context(), conn, Message{Type: TypeError, From: ServerID,
			Error: fmt.Sprintf("peer name is invalid; it must match %s", policy.PeerNamePattern)})
		h.log.Warn("rejected invalid peer name",
			slog.Int("name_len", len(name)), slog.String("room_id", roomID))
		return
	}

	// One cancelable context per connection, derived from the request context so
	// it also dies if the HTTP server shuts down. Cancelling it is the single
	// signal that unblocks — and thereby stops — both pumps.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	c := &member{
		id:     h.nextID(),
		name:   name,
		conn:   conn,
		out:    make(chan Message, outboundBuffer),
		cancel: cancel,
		log:    h.log,
	}

	// Register first: it rejects a duplicate name and, on success, announces us to
	// the incumbents. Registration writes only to OTHER members' channels, so it does
	// not need our own writePump — which is why, on a rejection, we can report the
	// error with a single direct write (no concurrent writer to race) and close.
	peers, ok := h.register(roomID, c)
	if !ok {
		_ = wsjson.Write(ctx, conn, Message{Type: TypeError, To: c.id,
			Error: fmt.Sprintf("name %q is already in use in room %q", name, roomID)})
		h.log.Warn("rejected duplicate name",
			slog.String("name", name), slog.String("room_id", roomID))
		return // deferred conn.CloseNow closes the socket
	}

	// Start the two per-connection helper goroutines. Both select on ctx, and both
	// are JOINED below before ServeWS returns: a goroutine that merely gets
	// cancelled is still a goroutine, and one leaked per connection is invisible in
	// a two-peer test and fatal in a long-lived server.
	var pumps sync.WaitGroup
	pumps.Add(2)
	go func() { defer pumps.Done(); c.writePump(ctx) }()
	go func() { defer pumps.Done(); h.pingLoop(ctx, c) }()

	c.send(Message{Type: TypeJoined, To: c.id, Peers: peers})
	// Broadcast the new roster AFTER the newcomer's joined frame is queued on its
	// own FIFO channel, so it learns its id before it is handed a roster naming it.
	h.broadcastMembership(roomID)

	// Only NOW tell the control plane this peer joined — strictly after its joined
	// frame is queued on its own outbound channel. The coordinator reacts by pushing
	// a computed topology to this peer; queueing that push after the joined frame (on
	// the same FIFO channel) guarantees the peer learns its id and roster before it
	// is told a tree that references them. Firing this inside register would race the
	// push ahead of the joined frame and the peer would resolve an empty roster.
	if h.obs != nil {
		h.obs.PeerJoined(roomID, c.id, c.name)
	}

	// readLoop blocks on this goroutine for the whole life of the connection,
	// which is what keeps the handler (and so the accepted conn) alive. When it
	// returns, the peer is gone: leave the room and tell everyone.
	h.readLoop(ctx, roomID, c)
	// Cancel before joining: readLoop returning means the connection is finished, and
	// the two helpers only unblock once their shared context is done.
	cancel()
	pumps.Wait()
	h.unregister(roomID, c)
}

// pingLoop is the control-link liveness detector, and it needs its own goroutine:
// readLoop occupies the handler goroutine for the life of the connection, and it is
// blocked in exactly the read that a dead-but-unclosed socket never satisfies.
//
// It writes a PROTOCOL ping, not a Message, so it cannot be routed through the
// member's outbound channel — that channel carries application frames and is drained
// by writePump. Concurrency is safe anyway: coder/websocket serialises control
// frames against data writes with its own internal write lock, and Conn.Ping is
// documented as requiring a concurrent reader (our readLoop) to consume the pong.
// The alternative — teaching writePump to emit pings — would put liveness detection
// behind the very buffer that a wedged peer fills, so a stuck peer could never be
// detected. Hence: separate goroutine, direct control write.
func (h *Hub) pingLoop(ctx context.Context, c *member) {
	ticker := h.clk.NewTicker(h.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			// Same latent hazard as member.writePump: Ping is a control-frame WRITE,
			// so cancelling pingCtx would hard-close the socket rather than abort the
			// ping. Safe today because this package only ever tears down with
			// CloseNow and sends no graceful close code — but a ping racing a close
			// code would destroy the socket before the code was written. See the full
			// note in member.go's writePump before adding one.
			pingCtx, cancel := context.WithTimeout(ctx, h.pingTimeout)
			err := c.conn.Ping(pingCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return // the connection was already going away
				}
				// A pong that never arrives is a definitive "this socket is dead",
				// which is the one thing a blocked read can never tell us.
				c.log.Info("keepalive ping failed; closing member",
					slog.String("peer_id", c.id), slog.Any("error", err))
				c.cancel()
				return
			}
		}
	}
}

// register adds c to roomID and returns the members already present (id + name),
// after telling those incumbents that c arrived. The bool is false — and nothing is
// added or announced — when c's name collides with a live member (see below).
func (h *Hub) register(roomID string, c *member) ([]Peer, bool) {
	h.mu.Lock()
	room := h.rooms[roomID]
	if room == nil {
		room = make(map[string]*member)
		h.rooms[roomID] = room
	}
	// A non-empty name must be UNIQUE within a room. A tree/coordinator addresses
	// peers by name, so two members sharing one would make name→id resolution
	// ambiguous — a relay could resolve its child to the wrong peer and silently
	// strand the intended one. Reject the newcomer (fail loud) rather than admit an
	// ambiguous roster. Empty names are exempt: mesh peers don't use names, and many
	// may share "".
	if c.name != "" {
		for _, other := range room {
			if other.name == c.name {
				h.mu.Unlock()
				return nil, false
			}
		}
	}
	existing := make([]Peer, 0, len(room))
	incumbents := make([]*member, 0, len(room))
	for id, other := range room {
		existing = append(existing, Peer{ID: id, Name: other.name})
		incumbents = append(incumbents, other)
	}
	room[c.id] = c
	h.mu.Unlock()

	// Fan out AFTER unlocking: send() only touches the target's channel, never
	// the Hub map, so holding mu across the loop would needlessly serialize every
	// room's traffic behind one lock.
	announce := Message{Type: TypePeerJoined, From: c.id, Name: c.name}
	for _, other := range incumbents {
		other.send(announce)
	}
	h.log.Info("peer joined",
		slog.String("peer_id", c.id), slog.String("room_id", roomID),
		slog.Int("room_size", len(existing)+1))
	return existing, true
}

// unregister removes c from roomID and tells the remaining members it left. It
// is safe to call more than once — the identity check makes a second call a
// no-op.
func (h *Hub) unregister(roomID string, c *member) {
	h.mu.Lock()
	room := h.rooms[roomID]
	if room == nil || room[c.id] != c {
		// Already gone, or the slot was reclaimed by another client with a
		// recycled id — either way, nothing of ours to remove.
		h.mu.Unlock()
		return
	}
	delete(room, c.id)
	remaining := make([]*member, 0, len(room))
	for _, other := range room {
		remaining = append(remaining, other)
	}
	if len(room) == 0 {
		delete(h.rooms, roomID) // don't leak empty rooms
	}
	h.mu.Unlock()

	announce := Message{Type: TypePeerLeft, From: c.id, Name: c.name}
	for _, other := range remaining {
		other.send(announce)
	}
	h.log.Info("peer left",
		slog.String("peer_id", c.id), slog.String("room_id", roomID),
		slog.Int("room_size", len(remaining)))
	// Tell the control plane a member left, so it can re-parent the survivors.
	if h.obs != nil {
		h.obs.PeerLeft(roomID, c.id)
	}
	// And tell every peer, so an ELECTED coordinator (which has no Observer) sees
	// the same departure the server-hosted one does. Without this a peer-hosted
	// coordinator would learn a graceful leave only from the multi-second "gone"
	// backstop, i.e. the two coordinator hosts would have different fidelity and the
	// difference would only surface after a handover.
	h.broadcastMembership(roomID)
}

// Roster returns a snapshot of roomID's members, ordered by name then id.
//
// The order is a TOTAL order chosen for replayability, not join order: a coordinator
// that rebuilds state from a roster must get the same sequence every time, or the
// first tree it computes depends on Go's map iteration.
//
// It is derived purely from which sockets are registered, so it is ground truth
// about liveness and carries no health verdict: a peer the control plane has
// declared gone is still here if its socket is. That is what makes it the
// authoritative cross-check for the resurrection contract on Observer.Heartbeat.
func (h *Hub) Roster(roomID string) []Peer {
	h.mu.Lock()
	room := h.rooms[roomID]
	peers := make([]Peer, 0, len(room))
	for id, m := range room {
		peers = append(peers, Peer{ID: id, Name: m.name})
	}
	h.mu.Unlock()

	sort.Slice(peers, func(i, j int) bool {
		if peers[i].Name != peers[j].Name {
			return peers[i].Name < peers[j].Name
		}
		return peers[i].ID < peers[j].ID
	})
	return peers
}

// broadcastMembership pushes the authoritative roster to every member of roomID.
// It is a snapshot, so a peer that missed a delta frame self-corrects on the next
// membership change instead of drifting permanently.
func (h *Hub) broadcastMembership(roomID string) {
	roster := h.Roster(roomID)
	if len(roster) == 0 {
		return // the meet is gone; there is nobody to tell
	}
	h.SendRoom(roomID, Message{Type: TypeMembership, Peers: roster})
}

// readLoop reads frames from c until the connection ends, routing each one. It
// runs on the handler goroutine and returns on any read error (normal close,
// context cancellation, or a real fault).
func (h *Hub) readLoop(ctx context.Context, roomID string, c *member) {
	for {
		var msg Message
		if err := wsjson.Read(ctx, c.conn, &msg); err != nil {
			switch {
			case errors.Is(err, context.Canceled),
				websocket.CloseStatus(err) == websocket.StatusNormalClosure,
				websocket.CloseStatus(err) == websocket.StatusGoingAway:
				c.log.Debug("peer disconnected", slog.String("peer_id", c.id))
			default:
				c.log.Info("read ended",
					slog.String("peer_id", c.id), slog.Any("error", err))
			}
			return
		}
		// Identity is server-authoritative: overwrite whatever From the client
		// put on the wire with the id we assigned. A peer must not be able to
		// forge another's id.
		msg.From = c.id
		h.route(roomID, c, msg)
	}
}

// route dispatches one inbound frame. Peers send media-signaling frames (relayed
// to another peer) and metrics frames (consumed by the control plane, never
// relayed); every other type is server-originated and rejected if received.
func (h *Hub) route(roomID string, sender *member, msg Message) {
	switch msg.Type {
	case TypeOffer, TypeAnswer, TypeCandidate, TypeTopology:
		// TypeTopology is relayed like a media frame when it comes FROM a peer: that
		// is an elected coordinator pushing a tree. The Hub deliberately does not
		// check whether the arbiter actually named that peer coordinator — policing
		// the control plane here would grow a second, divergent copy of the epoch
		// rules. The receiving peer's fence rejects an unauthorised push.
		if msg.To == "" {
			sender.send(Message{Type: TypeError, To: sender.id,
				Error: fmt.Sprintf("%s frame missing 'to'", msg.Type)})
			return
		}
		if !h.relay(roomID, msg) {
			sender.send(Message{Type: TypeError, To: sender.id,
				Error: fmt.Sprintf("unknown peer %q in room", msg.To)})
		}
	case TypeMetrics, TypeHeartbeat, TypeReparented:
		// Control-plane frames are for the coordinator, not another peer: hand the
		// raw payload to the observer and stop. Without a coordinator (Phases 1–3)
		// they are simply dropped — a peer reporting into a relay-only server loses
		// nothing. When the coordinator is an elected PEER, it is the server's
		// Observer implementation that forwards the frame onward, so a peer still
		// needs only one control link.
		if h.obs == nil {
			return
		}
		switch msg.Type {
		case TypeMetrics:
			h.obs.Metrics(roomID, sender.id, msg.Payload)
		case TypeHeartbeat:
			h.obs.Heartbeat(roomID, sender.id, msg.Payload)
		case TypeReparented:
			h.obs.Reparented(roomID, sender.id, msg.Payload)
		}
	default:
		sender.log.Warn("ignoring unexpected frame type from peer",
			slog.String("peer_id", sender.id), slog.String("type", string(msg.Type)))
	}
}

// SendTo delivers a server-originated frame to one peer by id within a room, the
// path the coordinator uses to push a computed topology down. Like relay, the
// lookup happens under the lock and the send after it is released; it returns false
// if no such peer is present (e.g. it left between compute and push).
func (h *Hub) SendTo(roomID, peerID string, msg Message) bool {
	h.mu.Lock()
	dst := h.rooms[roomID][peerID]
	h.mu.Unlock()
	if dst == nil {
		return false
	}
	dst.send(stampServer(msg))
	return true
}

// SendRoom delivers a server-originated frame to EVERY member of roomID and returns
// how many it reached. Like SendTo, the roster snapshot is taken under the lock and
// the sends happen after it is released, so one wedged peer cannot stall the rest.
//
// This is the first genuinely BROADCAST frame in the system — every other fan-out
// carries per-peer content — and it exists for the coordinator announcement and the
// membership snapshot, both of which are the same fact for everyone.
func (h *Hub) SendRoom(roomID string, msg Message) int {
	h.mu.Lock()
	room := h.rooms[roomID]
	members := make([]*member, 0, len(room))
	for _, m := range room {
		members = append(members, m)
	}
	h.mu.Unlock()

	msg = stampServer(msg)
	for _, m := range members {
		m.send(msg)
	}
	return len(members)
}

// stampServer marks a frame as server-originated, but only when no sender is set.
// An explicit From is preserved because the server also FORWARDS frames on a peer's
// behalf — telemetry relayed to an elected coordinator keeps the original peer's id,
// or the receiver would attribute it to the arbiter and fence it wrongly.
func stampServer(msg Message) Message {
	if msg.From == "" {
		msg.From = ServerID
	}
	return msg
}

// relay forwards msg to the peer named by msg.To within roomID. It returns false
// if no such peer exists (so the caller can tell the sender). The target lookup
// happens under the lock; the send happens after it is released.
func (h *Hub) relay(roomID string, msg Message) bool {
	h.mu.Lock()
	dst := h.rooms[roomID][msg.To]
	h.mu.Unlock()
	if dst == nil {
		return false
	}
	dst.send(msg)
	return true
}
