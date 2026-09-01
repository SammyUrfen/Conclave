package signaling

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Client is the peer side of a signaling connection: it dials the Hub's /ws
// endpoint, surfaces inbound frames on Incoming(), and sends outbound frames
// under a single-writer discipline. (Distinct from the hub's unexported member,
// which is the server's handle on a connected peer — the two ends of the same
// wire.)
//
// Lifecycle: Dial starts a read pump and a write pump; Close stops both and
// closes the socket. Incoming() is closed when the read pump exits, so a range
// over it terminates naturally on disconnect.
type Client struct {
	conn *websocket.Conn
	in   chan Message // inbound frames; closed when the read pump exits
	out  chan Message // outbound frames; drained by the write pump
	log  *slog.Logger

	cancel context.CancelFunc
	done   chan struct{} // closed when the read pump exits
}

// Dial connects to the signaling server for the given room. serverURL may be an
// http(s):// or ws(s):// base URL, or a bare host:port; the scheme is normalized
// to ws/wss and /ws?room=<room> is appended. name is the peer's stable label for
// topology resolution (may be empty in mesh mode). The passed ctx bounds only the
// handshake — the connection's lifetime is controlled by Close.
func Dial(ctx context.Context, log *slog.Logger, serverURL, room, name string) (*Client, error) {
	wsURL, err := wsURLFor(serverURL, room, name)
	if err != nil {
		return nil, err
	}
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial signaling %s: %w", wsURL, err)
	}
	conn.SetReadLimit(readLimit)

	// The run context outlives the dial ctx: it is the connection's lifetime,
	// cancelled only by Close (or a read failure).
	runCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:   conn,
		in:     make(chan Message, outboundBuffer),
		out:    make(chan Message, outboundBuffer),
		log:    log.With(slog.String("component", "signaling-client")),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go c.writePump(runCtx)
	go c.readPump(runCtx)
	return c, nil
}

// Send queues a frame for delivery. It blocks only while the outbound buffer is
// full (a healthy 2-peer negotiation never fills it), and returns an error once
// the client is closed — losing an SDP or ICE frame silently would strand the
// connection, so a closed client is reported rather than dropped.
func (c *Client) Send(msg Message) error {
	select {
	case c.out <- msg:
		return nil
	case <-c.done:
		return errors.New("signaling client closed")
	}
}

// Ping measures this peer's round trip to the server on the control link: it writes
// a WebSocket PROTOCOL ping and blocks until the matching pong arrives or ctx
// expires, so a nil return IS the round trip. metrics.RTTProbe times the call.
//
// It writes DIRECTLY to the connection rather than queueing on the out channel, for
// the same reason Hub.pingLoop does: a ping routed through writePump would sit behind
// whatever application frames are already buffered, so the measurement would report
// queueing delay as network latency — and on a wedged peer it would never go out at
// all. Concurrency with writePump is safe because coder/websocket serialises control
// frames against data writes with its own internal write lock.
//
// It also REQUIRES a concurrent reader to consume the pong, which readPump provides
// for the life of the connection. On a closed Client the read pump is gone and Ping
// returns an error rather than blocking — which is the behaviour the probe relies on
// to tell a dead link from a fast one.
func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// Incoming returns the stream of inbound frames. It is closed when the
// connection ends, so ranging over it is a clean way to consume until disconnect.
func (c *Client) Incoming() <-chan Message { return c.in }

// Close tears down the connection and waits for the read pump to exit.
func (c *Client) Close() error {
	c.cancel()
	err := c.conn.CloseNow()
	<-c.done
	return err
}

func (c *Client) readPump(ctx context.Context) {
	defer close(c.done)
	defer close(c.in)
	for {
		var msg Message
		if err := wsjson.Read(ctx, c.conn, &msg); err != nil {
			if ctx.Err() == nil { // not a deliberate Close
				c.log.Debug("signaling read ended", slog.Any("error", err))
			}
			return
		}
		select {
		case c.in <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) writePump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.out:
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
			err := wsjson.Write(writeCtx, c.conn, msg)
			cancel()
			if err != nil {
				c.log.Debug("signaling write failed", slog.Any("error", err))
				return
			}
		}
	}
}

// wsURLFor normalizes a user-supplied server value into a concrete ws(s):// URL
// for the /ws endpoint. It accepts http(s), ws(s), or a bare host:port, and
// rejects hostless or unknown-scheme values (the same fail-loud discipline as
// the peer's healthURLFor).
func wsURLFor(server, room, name string) (string, error) {
	s := strings.TrimSpace(server)
	if s == "" {
		return "", errors.New("empty signaling server URL")
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("parse signaling URL %q: %w", server, err)
	}
	switch u.Scheme {
	case "http", "ws":
		u.Scheme = "ws"
	case "https", "wss":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("invalid signaling URL %q: scheme must be http(s) or ws(s)", server)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("invalid signaling URL %q: missing host", server)
	}
	u.Path = "/ws"
	q := u.Query()
	if room != "" {
		q.Set("room", room)
	}
	if name != "" {
		q.Set("name", name)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
