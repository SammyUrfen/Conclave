package signaling

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
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
type Hub struct {
	log *slog.Logger

	mu    sync.Mutex
	rooms map[string]map[string]*member // roomID → peerID → member
	seq   uint64                        // monotonic id source; guarded by mu
}

// NewHub returns a ready Hub that logs through the given logger.
func NewHub(log *slog.Logger) *Hub {
	return &Hub{
		log:   log.With(slog.String("component", "signaling")),
		rooms: make(map[string]map[string]*member),
	}
}

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
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Our Go peer sends no Origin header, so the default same-origin check
		// passes. When a browser dashboard arrives (Phase 7), authorize its host
		// here via OriginPatterns rather than reaching for InsecureSkipVerify.
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

	// One cancelable context per connection, derived from the request context so
	// it also dies if the HTTP server shuts down. Cancelling it is the single
	// signal that unblocks — and thereby stops — both pumps.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	c := &member{
		id:     h.nextID(),
		conn:   conn,
		out:    make(chan Message, outboundBuffer),
		cancel: cancel,
		log:    h.log,
	}

	// Start the write side first so anything the join fans out is deliverable
	// immediately, then register (which notifies incumbents), then greet the
	// newcomer with its id and the current roster.
	go c.writePump(ctx)
	peers := h.register(roomID, c)
	c.send(Message{Type: TypeJoined, To: c.id, Peers: peers})

	// readLoop blocks on this goroutine for the whole life of the connection,
	// which is what keeps the handler (and so the accepted conn) alive. When it
	// returns, the peer is gone: leave the room and tell everyone.
	h.readLoop(ctx, roomID, c)
	h.unregister(roomID, c)
}

// register adds c to roomID and returns the ids already present, after telling
// those incumbents that c arrived.
func (h *Hub) register(roomID string, c *member) []string {
	h.mu.Lock()
	room := h.rooms[roomID]
	if room == nil {
		room = make(map[string]*member)
		h.rooms[roomID] = room
	}
	existing := make([]string, 0, len(room))
	incumbents := make([]*member, 0, len(room))
	for id, other := range room {
		existing = append(existing, id)
		incumbents = append(incumbents, other)
	}
	room[c.id] = c
	h.mu.Unlock()

	// Fan out AFTER unlocking: send() only touches the target's channel, never
	// the Hub map, so holding mu across the loop would needlessly serialize every
	// room's traffic behind one lock.
	announce := Message{Type: TypePeerJoined, From: c.id}
	for _, other := range incumbents {
		other.send(announce)
	}
	h.log.Info("peer joined",
		slog.String("peer_id", c.id), slog.String("room_id", roomID),
		slog.Int("room_size", len(existing)+1))
	return existing
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

	announce := Message{Type: TypePeerLeft, From: c.id}
	for _, other := range remaining {
		other.send(announce)
	}
	h.log.Info("peer left",
		slog.String("peer_id", c.id), slog.String("room_id", roomID),
		slog.Int("room_size", len(remaining)))
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

// route dispatches one inbound frame. Peers may only send media-signaling
// frames; control types are server-originated and rejected if received.
func (h *Hub) route(roomID string, sender *member, msg Message) {
	switch msg.Type {
	case TypeOffer, TypeAnswer, TypeCandidate:
		if msg.To == "" {
			sender.send(Message{Type: TypeError, To: sender.id,
				Error: fmt.Sprintf("%s frame missing 'to'", msg.Type)})
			return
		}
		if !h.relay(roomID, msg) {
			sender.send(Message{Type: TypeError, To: sender.id,
				Error: fmt.Sprintf("unknown peer %q in room", msg.To)})
		}
	default:
		sender.log.Warn("ignoring unexpected frame type from peer",
			slog.String("peer_id", sender.id), slog.String("type", string(msg.Type)))
	}
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
