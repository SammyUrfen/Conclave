package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/policy"
)

const (
	// clientFrameLimit caps an inbound frame. The client sends exactly two shapes,
	// `{"op":"ping"}` and `{"op":"resync"}`, so 4 KiB is far more than any legitimate
	// message and small enough that a hostile page cannot make the server buffer.
	clientFrameLimit = 4 << 10

	// wsWriteTimeout bounds a single frame write, so a stuck TCP send cannot wedge a
	// connection's writer forever. Matches signaling's budget for the same reason.
	wsWriteTimeout = 10 * time.Second

	// wsPingInterval / wsPingTimeout reap a socket whose peer vanished without a
	// close — a laptop lid, a NAT rebind — which TCP alone can hide for minutes.
	//
	// The client also pings at 20 s, but a client-side keepalive only proves the
	// CLIENT is alive; it cannot free a goroutine here when the client is gone. These
	// are compile-time and NOT operator-tunable, for the same reason the Hub's are
	// (§10): what an operator actually wants to tune is how fast a dead PEER is
	// noticed, which is -gone-after, a layer above this socket detail.
	wsPingInterval = 25 * time.Second
	wsPingTimeout  = 10 * time.Second

	// statusMeetNotFound is §9.4a's application close code for "no such meet at
	// upgrade time". It is in the 4000-4999 private range and tells the client to
	// stop and navigate to the meet list rather than retry something that cannot work.
	statusMeetNotFound = websocket.StatusCode(4404)
)

// clientOp is the complete client->server vocabulary (§9.4a): two operations, and no
// subscribe/unsubscribe — the socket is path-scoped to one meet, and a client watching
// two meets opens two sockets.
type clientOp struct {
	Op string `json:"op"`
}

const (
	opPing   = "ping"
	opResync = "resync"
)

// frameItem is one MINTED frame, queued for one connection.
type frameItem struct {
	// seq is this connection's monotonic counter, assigned when the frame is minted
	// (§9.4). Minting-time assignment is the load-bearing choice: a frame dropped for
	// a slow consumer still CONSUMES a seq, so the client sees a hole and resyncs. A
	// write-time counter would renumber the survivors contiguously and hide the loss
	// forever, which would quietly make the entire gap-detection contract a no-op.
	seq uint64
	// snapshot marks a frame the WRITER materialises: it calls SubnetSource at
	// dequeue time. Deferring the read to the writer is what makes ordering exact —
	// the frame's position in the queue is fixed at mint time, so no event published
	// during the read can slip in front of the snapshot it is already reflected in.
	snapshot   bool
	epoch, rev uint64
	kind       string
	// data is shared, unmodified, by every subscriber. Nothing mutates it after
	// fan-out, which is what makes sharing it safe without a copy per socket.
	data any
	at   time.Time
}

// subscription is one live event stream.
type subscription struct {
	meetID string
	ch     chan frameItem
	// first is the seq-1 snapshot request, reserved at registration so the very first
	// frame a socket receives is always a snapshot (§9.3) no matter what is published
	// in between.
	first frameItem
	// produced counts frames minted for this connection, delivered OR dropped.
	// Guarded by Server.mu.
	produced uint64
	// lastResync is when this connection's last resync was honoured, for
	// resyncMinInterval. Guarded by Server.mu.
	lastResync time.Time
	// stop is closed by Server.Close to bring the stream down with code 1000.
	stop chan struct{}
}

// stream is the per-connection glue between the reader goroutine and the single writer
// goroutine. Only the writer ever writes to the socket — the same rule signaling's
// member follows, and for the same reason: coder/websocket permits one writer, and a
// close frame sent from the reader while the writer is mid-write is a data race the
// race detector would find only under load.
type stream struct {
	conn   *websocket.Conn
	sub    *subscription
	cancel context.CancelFunc

	mu     sync.Mutex
	set    bool
	code   websocket.StatusCode
	reason string
}

// fail records the close code the writer should use and stops the connection. First
// caller wins, so the real cause is not overwritten by the teardown it triggers.
func (st *stream) fail(code websocket.StatusCode, reason string) {
	st.mu.Lock()
	if !st.set {
		st.set, st.code, st.reason = true, code, reason
	}
	st.mu.Unlock()
	st.cancel()
}

func (st *stream) closeCode() (websocket.StatusCode, string, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.code, st.reason, st.set
}

// handleEvents serves GET /api/meets/{id}/events.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, r, "GET, OPTIONS")
		return
	}
	// A WebSocket upgrade is NOT subject to CORS — the browser sends Origin and it is
	// the server's job to check it. The check runs HERE, before Accept, and Accept is
	// told to skip its own: see allowUpgradeOrigin for why the library's version is
	// not usable.
	//
	// A rejection is an HTTP 403 on the HANDSHAKE, so NO WebSocket is established and
	// there is NO close code — 1008 is unreachable here and v2.4's table was wrong to
	// list it (corrected in §15.14). Do not "improve" this by accepting the upgrade and
	// then closing with 1008: that would turn a configuration error the client can name
	// (-allowed-origins) into a close event indistinguishable from a policy violation
	// mid-stream.
	if !s.allowUpgradeOrigin(r) {
		s.log.Warn("event stream upgrade refused: origin not allowed",
			slog.String("origin", r.Header.Get("Origin")), slog.String("host", r.Host))
		s.writeError(w, http.StatusForbidden, codeForbiddenOrigin,
			"this origin is not in the server's -allowed-origins list",
			map[string]any{"origin": r.Header.Get("Origin")})
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The origin decision was made above, by the one matcher this process has.
		InsecureSkipVerify: true,
	})
	if err != nil {
		// Accept has already written the rejection.
		s.log.Warn("event stream upgrade failed", slog.Any("error", err))
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(clientFrameLimit)

	// From here on the socket exists, so every refusal is reported as a CLOSE CODE
	// rather than an HTTP status: the client is already past the handshake and can
	// only learn why through the code. Nothing else writes yet, so these direct
	// closes have no concurrent writer to race.
	id := r.PathValue("id")
	if !policy.ValidMeetID(id) {
		_ = conn.Close(statusMeetNotFound, "invalid meet id")
		return
	}
	if _, err := s.cfg.Meets.GetMeet(r.Context(), id); err != nil {
		_ = conn.Close(statusMeetNotFound, "no such meet")
		return
	}

	sub, admit := s.subscribe(id)
	switch admit {
	case admitOK:
	case admitShuttingDown:
		_ = conn.Close(websocket.StatusNormalClosure, "server shutting down")
		return
	case admitAtCapacity:
		// 1013 "try again later" rather than 1008: retrying LATER genuinely can help,
		// and the client's unrecognised-code branch already reconnects with backoff,
		// which is exactly the right behaviour. 1008 would tell it to stop forever.
		s.log.Warn("event stream refused: meet at subscriber capacity",
			slog.String("meet_id", id), slog.Int("cap", maxSubscribersPerMeet))
		_ = conn.Close(websocket.StatusTryAgainLater, "too many viewers for this meet")
		return
	}
	defer s.unsubscribe(sub)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	st := &stream{conn: conn, sub: sub, cancel: cancel}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.writeLoop(ctx, st)
	}()
	s.readLoop(ctx, st)
	cancel()
	<-done
}

// readLoop consumes the client's two operations. It never writes to the socket: it
// mints frames for the writer, or records a close code and returns.
func (s *Server) readLoop(ctx context.Context, st *stream) {
	for {
		_, raw, err := st.conn.Read(ctx)
		if err != nil {
			// Peer gone, context cancelled, or a frame past the read limit. All of
			// them mean this connection is over; the writer closes the socket.
			st.cancel()
			return
		}
		var op clientOp
		if err := json.Unmarshal(raw, &op); err != nil {
			// §9.4a: 1008 tells the client to STOP — retrying cannot fix a client
			// that speaks the wrong protocol, and a reconnect loop on a bug is worse
			// than a visible failure.
			st.fail(websocket.StatusPolicyViolation, "malformed frame")
			return
		}
		switch op.Op {
		case opPing:
			s.mintPong(st.sub)
		case opResync:
			s.mintSnapshot(st.sub)
		default:
			st.fail(websocket.StatusPolicyViolation, "unknown op")
			return
		}
	}
}

// writeLoop is the single owner of the write side.
func (s *Server) writeLoop(ctx context.Context, st *stream) {
	ticker := s.clk.NewTicker(wsPingInterval)
	defer ticker.Stop()

	// The reserved seq-1 snapshot, before anything else this connection ever sees.
	if st.sub.first.seq != 0 {
		if !s.writeItem(ctx, st, st.sub.first) {
			s.finish(st)
			return
		}
	}
	for {
		select {
		case <-ctx.Done():
			s.finish(st)
			return
		case <-st.sub.stop:
			// §9.4a: 1000 means "server shutting down" — reconnect with backoff.
			_ = st.conn.Close(websocket.StatusNormalClosure, "server shutting down")
			return
		case <-ticker.C():
			pctx, cancel := context.WithTimeout(socketCtx(ctx), wsPingTimeout)
			err := st.conn.Ping(pctx)
			cancel()
			if err != nil {
				st.cancel()
				s.finish(st)
				return
			}
		case it := <-st.sub.ch:
			if !s.writeItem(ctx, st, it) {
				s.finish(st)
				return
			}
		}
	}
}

// socketCtx strips cancellation from a context that is about to be handed to a
// coder/websocket WRITE, leaving only the caller's own deadline.
//
// This is not a style choice, it is the close-code contract. coder/websocket arms a
// context.AfterFunc for the duration of every frame write, and that hook does not
// abort the write — it calls the connection's internal close, which tears the TCP
// socket down with no close frame at all. So a write whose context is cancelled does
// not fail politely; it destroys the connection.
//
// The connection ctx is cancelled by exactly the thing that wants a close code:
// st.fail records a code and then cancels so the writer wakes up and sends it. Passing
// that ctx to the write means a cancellation arriving while a frame is still in flight
// kills the socket a moment before finish gets to write the frame — and the library
// reports that stolen close as success, because writeClose swallows net.ErrClosed. The
// client sees an abrupt EOF and a -1 status instead of the frozen §9.4a code it
// branches on, so it reconnects in a loop against a server that just said "stop".
//
// Dropping cancellation costs nothing here: wsWriteTimeout / wsPingTimeout already
// bound a stuck send, which is the only thing cancellation was buying. Reads keep
// their cancellation — the reader has no close frame to protect.
func socketCtx(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

// writeItem materialises and writes one frame. It reports whether the connection is
// still usable.
func (s *Server) writeItem(ctx context.Context, st *stream, it frameItem) bool {
	env, ok := s.materialise(ctx, st, it)
	if !ok {
		return false
	}
	wctx, cancel := context.WithTimeout(socketCtx(ctx), wsWriteTimeout)
	err := wsjson.Write(wctx, st.conn, env)
	cancel()
	if err != nil {
		st.cancel()
		return false
	}
	return true
}

// materialise turns a queued item into a wire envelope, reading the current snapshot
// if the item is a snapshot request.
//
// The read happens HERE, on the connection's own goroutine, and deliberately not under
// Server.mu: Snapshot round-trips the coordinator's Run goroutine, and holding the
// dashboard's lock across that call would let Publish — which runs ON that Run
// goroutine — block behind it. That is a deadlock, and it is the kind that only appears
// once a real fleet is connected.
func (s *Server) materialise(ctx context.Context, st *stream, it frameItem) (envelope, bool) {
	if !it.snapshot {
		return envelope{
			APIVersion: APIVersion, Seq: it.seq, AtUnixMs: unixMs(it.at),
			MeetID: st.sub.meetID, Epoch: it.epoch, Rev: it.rev,
			Kind: it.kind, Data: it.data,
		}, true
	}
	body, err := s.snapshotBody(ctx, st.sub.meetID)
	if err != nil {
		if errors.Is(err, arbiter.ErrMeetNotFound) {
			// §9.4a: 1001 "going away" means the meet is gone. The client stops and
			// navigates to the list rather than reconnecting to something that no
			// longer exists.
			st.fail(websocket.StatusGoingAway, "meet ended")
		} else {
			// 1011 is the code that says "server fault, reconnecting may help".
			s.log.Error("snapshot for an event stream failed",
				slog.String("meet_id", st.sub.meetID), slog.Any("error", err))
			st.fail(websocket.StatusInternalError, "snapshot failed")
		}
		return envelope{}, false
	}
	// The snapshot is the authoritative version for this meet; remember it so frames
	// that carry no version of their own (demo, pong) are stamped with something true.
	s.noteVersion(st.sub.meetID, body.Epoch, body.Rev)
	return envelope{
		APIVersion: APIVersion, Seq: it.seq, AtUnixMs: body.AtUnixMs,
		MeetID: st.sub.meetID, Epoch: body.Epoch, Rev: body.Rev,
		Kind: kindSnapshot, Data: body,
	}, true
}

// finish performs the connection's one and only close, using whatever code the reader
// or the writer recorded.
func (s *Server) finish(st *stream) {
	if code, reason, ok := st.closeCode(); ok {
		_ = st.conn.Close(code, reason)
		return
	}
	// No recorded cause: the peer went away or the request context ended. CloseNow
	// aborts without a handshake, which is the honest thing when there may be nobody
	// left to complete one.
	_ = st.conn.CloseNow()
}

// subscribe registers a connection and reserves seq 1 for its snapshot.
//
// Registration happens BEFORE the snapshot is read (the writer does that), so no event
// can fall into the gap between "the snapshot was taken" and "we started listening".
// The cost of choosing that direction is that an event published in that window may be
// delivered as a delta even though the snapshot already reflects it — a duplicate. Every
// delta is idempotent under that: each carries an ABSOLUTE value (a full tree, a health
// verdict, a cumulative refusal total), never an increment to be applied.
func (s *Server) subscribe(meetID string) (*subscription, admission) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, admitShuttingDown
	}
	ms := s.streamLocked(meetID)
	if len(ms.subs) >= maxSubscribersPerMeet {
		return nil, admitAtCapacity
	}
	sub := &subscription{
		meetID: meetID,
		ch:     make(chan frameItem, eventBuffer),
		stop:   make(chan struct{}),
	}
	sub.produced = 1
	sub.first = frameItem{seq: 1, snapshot: true, at: s.clk.Now()}
	ms.subs[sub] = struct{}{}
	return sub, admitOK
}

// unsubscribe drops a connection. It does NOT close sub.stop: Close owns that channel,
// and both run under mu, so a subscription is either still in the map (Close closes it)
// or already removed (Close never sees it). There is no path to a double close.
func (s *Server) unsubscribe(sub *subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ms := s.meets[sub.meetID]; ms != nil {
		delete(ms.subs, sub)
	}
}

// mintPong answers a client keepalive with a normal envelope (§9.4a), so the client
// needs no special parse path and the frame participates in seq like everything else.
func (s *Server) mintPong(sub *subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := s.streamLocked(sub.meetID)
	s.enqueueLocked(sub, frameItem{
		kind: kindPong, data: emptyData{},
		epoch: ms.epoch, rev: ms.rev, at: s.clk.Now(),
	})
}

// mintSnapshot is the `resync` reply — the recovery path a client takes after seeing a
// seq gap, and the replacement for the SSE Last-Event-ID this transport gave up.
func (s *Server) mintSnapshot(sub *subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clk.Now()
	// Rate limit, and DROP rather than queue: a suppressed request was never a frame,
	// so it burns no sequence number and cannot manufacture a gap that would provoke
	// the very resync it just refused.
	if !sub.lastResync.IsZero() && now.Sub(sub.lastResync) < resyncMinInterval {
		return
	}
	sub.lastResync = now
	s.streamLocked(sub.meetID)
	s.enqueueLocked(sub, frameItem{snapshot: true, at: now})
}

// enqueueLocked mints one frame for one subscription. Caller holds mu.
func (s *Server) enqueueLocked(sub *subscription, it frameItem) {
	sub.produced++
	it.seq = sub.produced
	select {
	case sub.ch <- it:
	default:
		// The consumer is not keeping up. Drop rather than buffer without bound —
		// produced has already advanced, so the gap is visible and the client resyncs.
	}
}

// fanoutLocked mints the same frame for every subscriber of a meet. Caller holds mu.
func (s *Server) fanoutLocked(ms *meetStream, it frameItem) {
	for sub := range ms.subs {
		s.enqueueLocked(sub, it)
	}
}

// Publish implements coordinator.Publisher.
//
// It runs on the coordinator's single Run goroutine and MUST NOT BLOCK: the frame is
// built outside the lock, the lock is held only for the per-meet bookkeeping and a
// non-blocking send per subscriber, and a subscriber that cannot keep up is dropped
// rather than waited on. One wedged browser tab must never stall every meet in the
// process.
func (s *Server) Publish(ev coordinator.Event) {
	if ev.RoomID == "" {
		return
	}
	kind, data := frameForEvent(ev)
	if kind == "" {
		return // an event kind this build does not know: logged nowhere, dropped safely
	}
	at := ev.At
	if at.IsZero() {
		at = s.clk.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	ms := s.streamLocked(ev.RoomID)
	// EventStale is the ONE kind whose Epoch/Rev do not describe the meet: they are the
	// refusing PEER's fence, which coordinator.publish deliberately leaves undefaulted
	// so a peer that adopted nothing reads as epoch 0. Folding those into the meet's
	// tracked version would roll the operator's epoch display BACKWARDS — and keep it
	// there, since every later versionless frame is stamped from this memory. They are
	// carried in `data` instead (see staleData).
	if ev.Kind != coordinator.EventStale {
		if ev.Epoch != 0 {
			ms.epoch = ev.Epoch
		}
		if ev.Rev != 0 {
			ms.rev = ev.Rev
		}
	}
	s.fanoutLocked(ms, frameItem{
		kind: kind, data: data, epoch: ms.epoch, rev: ms.rev, at: at,
	})
}

// PublishElection implements half of arbiter.Publisher. Only a real leadership change
// arrives here — a re-announcement of the same epoch is deliberately not published,
// because an event log that shows one per join is an event log nobody reads.
func (s *Server) PublishElection(a arbiter.Announcement) {
	if a.RoomID == "" {
		return
	}
	at := a.IssuedAt
	if at.IsZero() {
		at = s.clk.Now()
	}
	data := &electionData{
		Epoch: a.Epoch, Coordinator: a.Coordinator, Prev: a.Prev, Reason: string(a.Reason),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := s.streamLocked(a.RoomID)
	if a.Epoch != 0 {
		ms.epoch = a.Epoch
	}
	s.fanoutLocked(ms, frameItem{
		kind: kindElection, data: data, epoch: ms.epoch, rev: ms.rev, at: at,
	})
}

// PublishRepair implements the other half. The arbiter emits it on TRANSITION only —
// entering and leaving the lagging state — so 1 Hz of wire repair produces two events
// per episode rather than drowning the log at exactly the moment an operator is trying
// to read it.
func (s *Server) PublishRepair(roomID, name string, peerEpoch, meetEpoch uint64, resolved bool) {
	if roomID == "" {
		return
	}
	data := &repairData{
		Name: name, PeerEpoch: peerEpoch, MeetEpoch: meetEpoch, Resolved: resolved,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := s.streamLocked(roomID)
	if meetEpoch != 0 {
		ms.epoch = meetEpoch
	}
	s.fanoutLocked(ms, frameItem{
		kind: kindAnnounceRepair, data: data, epoch: ms.epoch, rev: ms.rev, at: s.clk.Now(),
	})
}

// publishDemo emits the §9.6 `demo` event, stamped with the meet's CURRENT epoch/rev.
func (s *Server) publishDemo(meetID, action, target, remoteAddr string) {
	data := &demoData{Action: action, Target: target, ByRemoteAddr: remoteAddr}
	s.mu.Lock()
	defer s.mu.Unlock()
	ms := s.streamLocked(meetID)
	s.fanoutLocked(ms, frameItem{
		kind: kindDemo, data: data, epoch: ms.epoch, rev: ms.rev, at: s.clk.Now(),
	})
}

// frameForEvent is the pure §9.4 fan-in: one coordinator.Event to one kind and one
// data shape. It is a free function and does no I/O so it can run outside the lock,
// which matters because the topology case walks the tree.
//
// A struct-per-kind rather than a map[string]any per kind: the JSON is then produced by
// encoding/json from a fixed field order instead of from a randomised map iteration,
// which is what makes two replays of the same history byte-identical.
func frameForEvent(ev coordinator.Event) (string, any) {
	switch ev.Kind {
	case coordinator.EventMember:
		kind := kindMemberLeft
		if ev.Present {
			kind = kindMemberJoined
		}
		return kind, &memberData{ID: ev.NodeID, Name: ev.Node}
	case coordinator.EventHealth:
		// Both values come straight from the coordinator, which performed the
		// transition. PrevHealth == Health is legal (a sustained-degradation verdict)
		// and is passed through rather than normalised away.
		return kindHealthChanged, &healthData{
			Name: ev.Node, Health: string(ev.Health), PrevHealth: string(ev.PrevHealth),
		}
	case coordinator.EventTopology:
		depth, relays := treeShape(ev.Topo)
		root := ""
		if ev.Topo != nil {
			root = ev.Topo.Root
		}
		return kindTopology, &topologyData{
			Root:    root,
			Edges:   edgesOf(ev.Topo),
			Backups: backupsOf(ev.Topo),
			Depth:   depth,
			Relays:  relays,
			Outcome: string(ev.Outcome),
			Reason:  ev.Reason,
		}
	case coordinator.EventReparent:
		return kindReparent, &reparentData{
			Name: ev.Node, From: ev.PrevParent, To: ev.Parent, Reason: ev.Reason,
		}
	case coordinator.EventFailover:
		return kindFailover, &failoverData{
			Name:    ev.Node,
			Orphans: append(make([]string, 0, len(ev.Orphans)), ev.Orphans...),
			Reroot:  ev.Reroot, Reason: ev.Reason,
		}
	case coordinator.EventStale:
		return kindStaleRejected, &staleData{
			Name: ev.Node, Epoch: ev.Epoch, Rev: ev.Rev, Total: ev.Count, Reason: ev.Reason,
		}
	case coordinator.EventSettling:
		return kindSettling, &settlingData{
			Waiting: append(make([]string, 0, len(ev.Waiting)), ev.Waiting...),
			Reason:  ev.Reason,
		}
	case coordinator.EventUnbuildable:
		return kindUnbuildable, &unbuildableData{Reason: ev.Reason}
	default:
		return "", nil
	}
}

// admission is the outcome of asking to open an event stream. It is an enum rather than
// a bool because the two refusals need DIFFERENT close codes, and a caller handed a bare
// false would have to guess which.
type admission int

const (
	admitOK admission = iota
	admitShuttingDown
	admitAtCapacity
)

// allowUpgradeOrigin is the upgrade-time origin decision, delegated in full to
// policy.Origins.AllowUpgrade — the SAME call internal/signaling's /ws upgrade makes, so
// the two endpoints cannot diverge.
//
// # Why AcceptOptions.OriginPatterns is not used
//
// coder/websocket's authenticateOrigin returns ALLOW when r.Host equals the Origin's
// host, BEFORE consulting OriginPatterns and comparing hosts only, ignoring the scheme.
// That is a second matcher carrying a rule policy.Origins does not have, and it is
// exploitable: an attacker who points a name they control at this server's address gets
// a victim's browser to send Host: evil.com and Origin: http://evil.com, the shortcut
// fires, and the upgrade succeeds against an allow-list naming neither — reaching a
// loopback or LAN server the attacker cannot dial, which is the very reachability
// assumption that makes an unauthenticated dashboard defensible. It also let a plaintext
// origin claim a host the allow-list permits only over https. policy.Origins.Patterns()
// carries a doc block saying not to gate an upgrade with it.
//
// So the library's check is switched off (InsecureSkipVerify) and the decision is made
// here. Note the consequence, which is the hole closing rather than a regression: a page
// served from the ARBITER'S OWN ORIGIN is no longer auto-allowed and must be listed in
// -allowed-origins. Self-origin auto-allow IS the rebinding vector. The default list
// covers localhost and 127.0.0.1, so the intended Pages-plus-local-server setup is
// unaffected; a hosted deployment that serves its own dashboard must list its origin.
func (s *Server) allowUpgradeOrigin(r *http.Request) bool {
	return s.origins.AllowUpgrade(r.Header.Get("Origin"))
}
