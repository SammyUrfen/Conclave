// Command server is conclave's central bootstrap node: the always-up control
// node that will eventually own room rendezvous, signaling relay, and — most
// importantly — the role of election arbiter and source of truth for "who is
// coordinator" (see docs/ROADMAP.md, Phases 1 and 6).
//
// For Phase 0 it does exactly one thing: answer GET /healthz. That is enough to
// prove the module builds, runs, logs in a structured way, shuts down cleanly,
// and is reachable — before a single line of WebRTC exists.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SammyUrfen/conclave/internal/logging"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

func main() {
	// The whole program is one thin main → run(error) hop. main's only jobs are
	// to hand os.Args to run and to translate a returned error into a non-zero
	// exit code. Everything testable lives in run and below; main itself is too
	// trivial to test. This is the idiomatic Go entrypoint shape.
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "server: "+err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	// A dedicated FlagSet (rather than the global flag.CommandLine) keeps flag
	// parsing self-contained and testable, and ContinueOnError returns the
	// error to us instead of calling os.Exit behind our back.
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	addr := fs.String("addr", ":9000", "TCP address to listen on (host:port)")
	logLevel := fs.String("log-level", "info", "log level: debug|info|warn|error")
	logFormat := fs.String("log-format", "text", "log format: json|text")
	if err := fs.Parse(args); err != nil {
		return err
	}

	level, err := logging.ParseLevel(*logLevel)
	if err != nil {
		return err
	}
	// .With() returns a child logger that stamps these fields onto every record.
	// Every line this binary logs now carries service=conclave-server, so a
	// mixed server+peer log stream stays sortable by origin.
	logger := logging.New(os.Stdout, level, logging.Format(*logFormat)).With(
		slog.String("service", "conclave-server"),
	)

	// signal.NotifyContext returns a context that is cancelled on Ctrl-C
	// (SIGINT) or SIGTERM. This is our first taste of context-as-lifecycle: one
	// cancellation source that we will, from Phase 1 on, fan out to tear down
	// goroutines, websockets, and peer connections all at once. stop() releases
	// the signal handler on the way out.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The signaling hub is a process-lived dependency, constructed here at the
	// edge and injected into the mux — no package globals.
	hub := signaling.NewHub(logger)

	srv := &http.Server{
		Addr:    *addr,
		Handler: newMux(logger, hub),
		// A minimal slowloris guard: don't let a client dribble headers forever
		// and pin a connection. Cheap correctness habit, worth it even on a toy.
		ReadHeaderTimeout: 5 * time.Second,
	}

	// ListenAndServe blocks, so we run it on its own goroutine and let main
	// block on the select below instead. The channel is buffered (capacity 1)
	// on purpose: even if we've already returned via the ctx.Done() path and
	// nobody is left to receive, the goroutine can still send its exit error and
	// terminate — no goroutine leak.
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", slog.String("addr", *addr))
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		// ListenAndServe *always* returns a non-nil error. ErrServerClosed is
		// the benign "you asked me to stop" sentinel; anything else (e.g. port
		// already in use) is a real failure. errors.Is is the correct way to
		// test against a sentinel through any wrapping.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining connections")
		// Shutdown stops accepting new connections and waits for in-flight ones,
		// bounded by this timeout so a stuck client can't block exit forever.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		logger.Info("server stopped cleanly")
		return nil
	}
}

// newMux wires the HTTP routes. The logger and hub are injected so handlers can
// share the binary's service-stamped logger and its single signaling hub.
func newMux(logger *slog.Logger, hub *signaling.Hub) http.Handler {
	mux := http.NewServeMux()
	// "GET /healthz" is Go 1.22+ method-aware routing: the mux itself rejects a
	// POST to /healthz with 405, so the handler never has to check r.Method.
	mux.HandleFunc("GET /healthz", healthzHandler(logger))
	// The WebSocket upgrade handshake is a GET, so the method-aware pattern fits;
	// the hub owns everything past the upgrade.
	mux.HandleFunc("GET /ws", hub.ServeWS)
	return mux
}

// healthResponse is the JSON body returned by /healthz. The struct tags decide
// the wire names, which lets the Go fields stay idiomatic (exported, CamelCase)
// while the JSON stays lowercase. This tiny type is the seed of the wire
// protocol we grow in Phase 1.
type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// healthzHandler returns an http.HandlerFunc. Returning a closure over logger
// (instead of a plain function) is how we inject dependencies into a handler
// without a global — the returned function "remembers" logger. Closures like
// this are a workhorse from here on.
func healthzHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(healthResponse{
			Status:  "ok",
			Service: "conclave-server",
		}); err != nil {
			// The 200 status line is already flushed, so we cannot change the
			// response now — the honest move is to record the failure and move
			// on. slog.Any wraps an arbitrary value (here, the error). The key
			// is "error" to match the logging field vocabulary in
			// docs/design-system.md — key names are a contract, not a whim.
			logger.Error("encode healthz response", slog.Any("error", err))
			return
		}
		logger.Info("served healthz",
			slog.String("remote", r.RemoteAddr),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
		)
	}
}
