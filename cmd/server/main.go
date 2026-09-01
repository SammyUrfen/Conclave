// Command server is conclave's arbiter: the always-up central node that owns meet
// rendezvous, the signaling relay, the coordinator-election arbitration, and the
// browser-facing dashboard surface.
//
// It is deliberately NEVER a media relay. Media flows peer-to-peer over the subnet an
// elected coordinator computes; everything this process carries is control plane.
//
// This file is the ONE place every seam in the system meets, which is why the wiring
// lives in a single readable list (newPlane) rather than being spread across the
// packages it connects. Each package declares the interface it consumes and never the
// one it implements — that is what keeps the dependency graph acyclic — so the
// concrete ends can only be joined here.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/dashboard"
	"github.com/SammyUrfen/conclave/internal/logging"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

func main() {
	// The whole program is one thin main → run(error) hop. main's only jobs are to
	// hand os.Args to run and to translate a returned error into a non-zero exit
	// code. Everything testable lives in run and below.
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "server: "+err.Error())
		os.Exit(1)
	}
}

// run parses and validates the flags, builds the plane, and serves until the process
// is signalled. Every failure before the listener opens is returned, never logged and
// swallowed: a self-contradictory configuration must not produce a running server.
func run(args []string) error {
	f, err := parseFlags(args, nil)
	if err != nil {
		return err
	}
	res, err := f.resolve()
	if err != nil {
		return err
	}

	// .With() returns a child logger that stamps these fields onto every record, so a
	// mixed server+peer log stream stays sortable by origin.
	logger := logging.New(os.Stdout, res.level, res.format).With(
		slog.String("service", "conclave-server"),
	)

	p, err := newPlane(logger, f, res)
	if err != nil {
		return err
	}
	defer p.close()

	// signal.NotifyContext returns a context cancelled on Ctrl-C (SIGINT) or SIGTERM:
	// one cancellation source fanned out to every goroutine, socket, and peer
	// connection at once. stop() releases the signal handler on the way out.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return p.serve(ctx)
}

// plane is every constructed dependency of the arbiter process, wired together.
//
// It holds the two configs it built from as well, and not only for the tests that
// assert on them: they are logged at startup, because "which knobs is this process
// actually running with" is the first question anyone asks of a control plane and the
// flags alone do not answer it (several resolve to package defaults).
type plane struct {
	log      *slog.Logger
	flags    *serverFlags
	hub      *signaling.Hub
	arb      *arbiter.Arbiter
	coord    *coordinator.Coordinator // nil without -coordinate
	dash     *dashboard.Server        // nil without -dashboard
	ev       *evictor
	roles    *roleTable
	arbCfg   arbiter.Config
	coordCfg coordinator.Config
}

// newPlane wires every seam. Read top to bottom, it is the whole system's object
// graph: transport, then the two control planes, then the browser surface.
//
// The construction ORDER is forced by one cycle in the object graph that is not a
// cycle in the package graph: the coordinator publishes into the dashboard, and the
// dashboard snapshots the coordinator. publisherRef is the late binding that resolves
// it, set once here before anything is running.
func newPlane(log *slog.Logger, f *serverFlags, res resolved) (*plane, error) {
	p := &plane{log: log, flags: f, ev: newEvictor(), roles: newRoleTable()}

	// 1. Transport. It owns the origin allow-list for the WebSocket upgrade — the
	//    same policy.Origins value the dashboard's CORS uses, injected in both places
	//    rather than written down twice.
	hub, err := signaling.NewHubWithConfig(f.hubConfig(log, res.origins))
	if err != nil {
		return nil, err
	}
	p.hub = hub

	// 2. The liveness relationship neither package can check alone: the Hub's
	//    socket-death window against the control plane's gone threshold.
	if err := validateLivenessBudget(f.goneAfter, hub.LivenessBudget()); err != nil {
		return nil, err
	}

	// 3. The dashboard is the Publisher for both control planes, but it cannot exist
	//    until they do. Bind late.
	pub := &publisherRef{}

	// 4. The coordinator: telemetry in, a computed subnet out. It knows nothing of
	//    the wire — it holds a Sender it declared, adapted here to the Hub.
	if f.coordinate {
		p.coordCfg = f.coordinatorConfig(hub.LivenessBudget())
		p.coord = coordinator.New(log, p.coordCfg, hubSender{bus: hub}, pub)
	}

	// 5. The arbiter: meet registry, epoch minting, election. It runs unconditionally
	//    because it is the sole minter of epochs, and without an epoch the coordinator
	//    has no term to serve — even in the Phase 5 posture where the arbiter simply
	//    announces itself.
	p.arbCfg = f.arbiterConfig()
	announcer := &hubAnnouncer{log: log, bus: hub, roles: p.roles}
	if p.coord != nil {
		announcer.term = p.coord
	}
	p.arb = arbiter.New(log, p.arbCfg, announcer, pub)

	// 6. One Observer, fanned to both planes. This is what gives the arbiter a
	//    liveness view of the coordinator without importing it.
	obs := &planeObserver{log: log, arb: p.arb, roles: p.roles, relay: hub, roster: hub}
	if p.coord != nil {
		obs.coord = p.coord
	}
	hub.SetObserver(obs)

	// 7. The browser surface. Demo is expressed as an ABSENT DEPENDENCY rather than a
	//    boolean a handler must remember to check: with -demo off the routes are never
	//    registered, so an unregistered route cannot be reached by a bug in a
	//    permission check (§9.6).
	if f.dashboard {
		var subnet dashboard.SubnetSource
		if p.coord != nil {
			// Assigned through a branch, never directly: a nil *Coordinator stored in
			// a non-nil interface is the classic Go typed-nil trap, and the dashboard
			// tests Subnet == nil to decide whether a coordinator exists at all.
			subnet = p.coord
		}
		var demo dashboard.DemoControl
		if f.demo {
			demo = &demoControl{meets: p.arb, roster: hub, ev: p.ev}
		}
		dash, err := dashboard.New(log, dashboard.Config{
			Meets:     p.arb,
			Subnet:    subnet,
			Demo:      demo,
			Origins:   res.origins,
			PublicURL: f.publicURL,
			Addr:      f.addr,
		})
		if err != nil {
			return nil, err
		}
		p.dash = dash
		pub.set(dash)
	}

	return p, nil
}

// demoEnabled reports whether the destructive demo surface exists. It reads the
// FLAG that decided the dependency, which is the same fact the dashboard advertises
// as demo_enabled from Config.Demo != nil — one decision, never two that can drift.
func (p *plane) demoEnabled() bool { return p.flags.dashboard && p.flags.demo }

// mux wires the HTTP routes.
func (p *plane) mux() http.Handler {
	mux := http.NewServeMux()
	// "GET /healthz" is Go 1.22+ method-aware routing: the mux itself rejects a POST
	// with 405, so the handler never has to check r.Method.
	mux.HandleFunc("GET /healthz", healthzHandler(p.log))
	// The WebSocket upgrade handshake is a GET, so the method-aware pattern fits; the
	// hub owns everything past the upgrade. The wrapper exists only to give the demo
	// surface a handle on this connection's lifetime — see serveWS.
	mux.HandleFunc("GET /ws", p.serveWS)
	if p.dash != nil {
		// Mounted so requests keep their FULL path: the dashboard's patterns are
		// absolute ("/api/meets"), so a StripPrefix here would 404 every route while
		// the server looked perfectly healthy.
		mux.Handle("/api/", p.dash.Handler())
	}
	return mux
}

// serveWS is the signaling upgrade, wrapped so this process owns each connection's
// context.
//
// ServeWS derives its per-connection context from the REQUEST's, and cancelling that
// context is exactly what tears a member down — both pumps unblock and the read loop
// returns. Owning the request context is therefore how cmd/server obtains the ability
// to evict a peer WITHOUT signaling exporting a way to close a socket, and it means
// the demo eviction and a real socket death take the identical code path. A demo
// control that exercised a different path would prove nothing.
func (p *plane) serveWS(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		roomID = defaultRoom
	}
	ctx, release := p.ev.track(roomID, r.URL.Query().Get("name"), r.Context())
	defer release()
	p.hub.ServeWS(w, r.WithContext(ctx))
}

// start launches the control-plane goroutines.
//
// They start HERE and not in newPlane for two reasons: nothing may be running while
// the publisher is still being late-bound, and a test that only wants the object
// graph should not have to spawn and reap two loops to get it.
//
// Both loops own all their state and return ctx.Err() on cancellation, so a
// context.Canceled is the expected exit and not a fault worth logging as one.
//
// Starting them BEFORE the listener is not incidental either: every arbiter and
// coordinator query round-trips through its owning goroutine, so a request served
// before Run is up would block on a loop that has not started rather than fail.
func (p *plane) start(ctx context.Context) {
	go func() {
		if err := p.arb.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			p.log.Error("arbiter stopped", slog.Any("error", err))
		}
	}()
	if p.coord != nil {
		go func() {
			if err := p.coord.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				p.log.Error("coordinator stopped", slog.Any("error", err))
			}
		}()
	}
}

// serve starts the control planes and blocks until ctx ends or the listener fails.
func (p *plane) serve(ctx context.Context) error {
	p.start(ctx)
	p.logStartup()

	srv := &http.Server{
		Addr:    p.flags.addr,
		Handler: p.mux(),
		// A minimal slowloris guard: do not let a client dribble headers forever and
		// pin a connection.
		ReadHeaderTimeout: 5 * time.Second,
		// Every handler runs under a context derived from this one, which is what
		// makes a shutdown reach the WebSocket pumps rather than waiting on them.
		BaseContext: func(_ net.Listener) context.Context { return ctx },
	}

	// ListenAndServe blocks, so it runs on its own goroutine. The channel is buffered
	// on purpose: even if we have already returned via the ctx.Done() path and nobody
	// is left to receive, the goroutine can still send its exit error and terminate.
	serveErr := make(chan error, 1)
	go func() {
		p.log.Info("http server listening", slog.String("addr", p.flags.addr))
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		// ListenAndServe always returns a non-nil error. ErrServerClosed is the benign
		// "you asked me to stop" sentinel; anything else (e.g. the port is taken) is a
		// real failure.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		p.log.Info("shutdown signal received, draining connections")
		// Close the event streams first, with close code 1000, so a watching browser
		// is told to reconnect with backoff rather than being cut off mid-frame and
		// left to guess. Shutdown would otherwise wait out every live stream.
		p.close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		p.log.Info("server stopped cleanly")
		return nil
	}
}

// close releases what the plane owns. It is idempotent, because it runs both on the
// shutdown path and from run's defer.
func (p *plane) close() {
	if p.dash != nil {
		p.dash.Close()
	}
}

// logStartup records the effective configuration once, at Info.
//
// Several knobs resolve to package defaults when a flag is zero, so the flag values
// alone do not answer "what is this process actually running with" — and that is the
// first question asked of any control plane whose behaviour looks wrong.
func (p *plane) logStartup() {
	p.log.Info("arbiter starting",
		slog.Bool("coordinate", p.flags.coordinate),
		slog.Bool("elect", p.flags.elect),
		slog.Bool("dashboard", p.flags.dashboard),
		slog.Bool("demo", p.demoEnabled()),
		slog.String("arbiter_id", p.arbCfg.ArbiterID),
		slog.String("allowed_origins", p.flags.allowedOrigins),
		slog.Duration("socket_detection", p.hub.LivenessBudget()),
	)
	if p.coord != nil {
		p.log.Info("coordinator enabled",
			slog.Int("max_depth", p.coordCfg.MaxDepth),
			slog.Int("stream_kbps", p.coordCfg.StreamKbps),
			slog.Float64("stickiness_ms", p.coordCfg.StickinessMs),
			slog.Duration("join_settle", p.coordCfg.JoinSettle),
			slog.Duration("dwell", p.coordCfg.Dwell),
			slog.String("degraded_after", thresholdLabel(p.coordCfg.DegradedAfter)),
			slog.String("gone_after", thresholdLabel(p.coordCfg.GoneAfter)))
	}
	if p.demoEnabled() {
		// Loud on purpose: these routes let an unauthenticated caller terminate a
		// participant's connection and force a control-plane transition.
		p.log.Warn("DESTRUCTIVE demo control routes are registered (-demo): " +
			"an unauthenticated caller can evict a peer and force an election")
	}
}

// thresholdLabel renders a health threshold for the startup log.
//
// A bare "0" is accurate and tells an operator nothing — it reads as "unset" when it
// actually means "derived per peer from the cadence each one declares", which is the
// single most confusing number in this log precisely because it is the DEFAULT. The
// value is a sentinel, so it is logged as what it means rather than as what it is.
func thresholdLabel(d time.Duration) string {
	if d <= 0 {
		return "derived per peer"
	}
	return d.String()
}

// healthResponse is the JSON body returned by /healthz.
type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// healthzHandler returns an http.HandlerFunc closed over logger — how a dependency is
// injected into a handler without a global.
//
// It is the endpoint a hosted platform's health check calls, so it stays cheap and
// dependency-free: it reports that the process is serving, deliberately not that the
// control planes are healthy. A health check that fails when a meet is misconfigured
// gets the container restarted for something a restart cannot fix.
func healthzHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(healthResponse{
			Status:  "ok",
			Service: "conclave-server",
		}); err != nil {
			// The 200 status line is already flushed, so the response cannot change
			// now — the honest move is to record the failure and move on.
			logger.Error("encode healthz response", slog.Any("error", err))
			return
		}
		logger.Debug("served healthz",
			slog.String("remote", r.RemoteAddr),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
		)
	}
}
