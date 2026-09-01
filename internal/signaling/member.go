package signaling

import (
	"context"
	"log/slog"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// member is the hub's handle on one connected participant: a WebSocket plus the
// bookkeeping needed to talk to it safely from many goroutines. It is unexported
// — the Hub is the only thing that creates or touches a member. (Distinct from
// the exported Client, which is the peer *side* of a signaling connection.)
//
// The load-bearing rule for any WebSocket library (coder/websocket included): at
// most one goroutine may WRITE to a Conn at a time. Broadcasts, relays, and
// control frames all originate on *other* goroutines, so rather than let them
// call conn.Write directly (a data race, and the kind of bug -race exists to
// catch), they push onto out, and exactly one goroutine — writePump — owns the
// write side. The channel is the serialization: "share the socket by
// communicating," not by wrapping every write in a lock.
type member struct {
	id   string
	name string // peer-declared stable label (may be empty); echoed in rosters
	conn *websocket.Conn
	out  chan Message // buffered; filled by senders, drained by writePump

	// cancel tears down THIS member: it cancels the per-connection context both
	// pumps select on, unblocking Read and Write together. It is idempotent, so
	// any goroutine that spots trouble (read error, write error, full buffer)
	// may call it to bring the whole connection down.
	cancel context.CancelFunc
	log    *slog.Logger
}

// send queues one message for delivery and never blocks. If the outbound buffer
// is full the peer is not keeping up, so we drop the connection (cancel it)
// instead of blocking the caller: a room broadcast must not stall on one wedged
// peer — that would be head-of-line blocking, one dead member freezing everyone.
// Returns false if the message was dropped.
func (m *member) send(msg Message) bool {
	select {
	case m.out <- msg:
		return true
	default:
		m.log.Warn("outbound buffer full; dropping slow member", slog.String("peer_id", m.id))
		m.cancel()
		return false
	}
}

// writePump is the single owner of the write side. It drains out to the socket
// until the context is cancelled — by this member (send/read/write failure), by
// the Hub, or by server shutdown. Each write gets its own short deadline so a
// stuck TCP send cannot wedge the pump forever.
func (m *member) writePump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-m.out:
			// HAZARD, latent — read this before adding a graceful close code.
			//
			// coder/websocket arms a context.AfterFunc for the duration of every
			// frame write, and that hook does not abort the write: it calls the
			// connection's internal close, tearing the TCP socket down with NO close
			// frame. So a write whose context is CANCELLED does not fail politely,
			// it destroys the connection — and writeClose then swallows the
			// resulting net.ErrClosed and reports success.
			//
			// That is harmless here TODAY only because this package never sends a
			// graceful close code: every teardown path ends in CloseNow, so there is
			// no close frame for a cancellation to steal. The trap springs the moment
			// someone adds one — say, mirroring the dashboard's 1013 subscriber cap —
			// because the close path would cancel this very ctx to wake the writer,
			// killing the socket a moment before the code could go out. The peer
			// would see an abrupt EOF instead of the reason it was disconnected.
			//
			// If you add a close code, strip cancellation from the write context
			// (context.WithoutCancel; see dashboard.socketCtx) and keep only the
			// deadline. The timeout is what bounds a stuck send; cancellation was
			// buying nothing else. Reads keep their cancellation — a reader has no
			// close frame to protect.
			writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := wsjson.Write(writeCtx, m.conn, msg)
			cancel()
			if err != nil {
				// Peer gone or too slow. Tear down; readLoop's caller will
				// unregister us from the room on the way out.
				m.log.Debug("write failed; closing member",
					slog.String("peer_id", m.id), slog.Any("error", err))
				m.cancel()
				return
			}
		}
	}
}
