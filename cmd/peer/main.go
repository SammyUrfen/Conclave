// Command peer is a conclave participant node. It has two modes:
//
//   - probe (default): dial the server's /healthz and log the result — the
//     Phase 0 reachability check.
//   - call (-call): join a signaling room and establish a WebRTC call with the
//     other peer(s) in it, optionally sending a VP8 track and/or recording the
//     received one. This is the Phase 1 capture→signal→connect→media pipeline.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/SammyUrfen/conclave/internal/logging"
	"github.com/SammyUrfen/conclave/internal/media"
	"github.com/SammyUrfen/conclave/internal/signaling"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "peer: "+err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("peer", flag.ContinueOnError)
	server := fs.String("server", "http://localhost:9000", "base URL of the conclave server")
	logLevel := fs.String("log-level", "info", "log level: debug|info|warn|error")
	logFormat := fs.String("log-format", "text", "log format: json|text")
	timeout := fs.Duration("timeout", 5*time.Second, "overall timeout for the health probe (probe mode)")
	call := fs.Bool("call", false, "join a room and establish a WebRTC call instead of probing /healthz")
	room := fs.String("room", "default", "room to join (call mode)")
	send := fs.Bool("send", false, "send a video track to peers (call mode)")
	mediaPath := fs.String("media", "", "VP8 IVF file to send; empty sends synthetic frames (call mode; implies -send)")
	recordPath := fs.String("record", "", "write the first received track to this IVF file; empty just counts (call mode)")
	stun := fs.String("stun", "", "STUN server URL, e.g. stun:stun.l.google.com:19302 (call mode; empty is fine on one host)")
	name := fs.String("name", "", "stable topology name for this peer, e.g. relay|leaf-b (tree mode)")
	topology := fs.String("topology", "", "path to a tree topology JSON file; enables tree mode (empty ⇒ full mesh)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	level, err := logging.ParseLevel(*logLevel)
	if err != nil {
		return err
	}
	logger := logging.New(os.Stdout, level, logging.Format(*logFormat)).With(
		slog.String("service", "conclave-peer"),
	)

	if *call {
		return runCall(logger, callConfig{
			server:       *server,
			room:         *room,
			send:         *send || *mediaPath != "",
			mediaPath:    *mediaPath,
			recordPath:   *recordPath,
			stun:         *stun,
			name:         *name,
			topologyPath: *topology,
		})
	}
	return runProbe(logger, *server, *timeout)
}

// callConfig is the parsed configuration for call mode.
type callConfig struct {
	server, room, mediaPath, recordPath, stun string
	name, topologyPath                        string
	send                                      bool
}

// runCall joins a signaling room and runs WebRTC calls with the peers in it
// until interrupted. Everything — the signaling client, every session, and the
// media pumps — hangs off one context cancelled by SIGINT/SIGTERM, so a single
// Ctrl-C tears the whole thing down cleanly.
func runCall(logger *slog.Logger, cfg callConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Tree mode: load the shared topology and require a name so this peer can find
	// itself in it. Fail loud on bad config rather than silently falling back to mesh.
	var topo *media.Topology
	if cfg.topologyPath != "" {
		if cfg.name == "" {
			return fmt.Errorf("-topology requires -name so this peer can locate itself in the tree")
		}
		var err error
		if topo, err = media.LoadTopology(cfg.topologyPath); err != nil {
			return err
		}
	}

	// The dial timeout bounds only the WebSocket handshake, not the call.
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	client, err := signaling.Dial(dialCtx, logger, cfg.server, cfg.room, cfg.name)
	cancel()
	if err != nil {
		return err
	}
	defer client.Close()
	logger.Info("connected to signaling",
		slog.String("server", cfg.server), slog.String("room", cfg.room), slog.String("name", cfg.name))

	var iceServers []webrtc.ICEServer
	if cfg.stun != "" {
		iceServers = append(iceServers, webrtc.ICEServer{URLs: []string{cfg.stun}})
	}

	router := media.NewRouter(logger, client, media.RouterConfig{
		ICEServers: iceServers,
		SendMedia:  cfg.send,
		MediaPath:  cfg.mediaPath,
		RecordPath: cfg.recordPath,
		Topology:   topo,
		SelfName:   cfg.name,
	})
	logger.Info("running call",
		slog.Bool("send", cfg.send), slog.String("media", cfg.mediaPath),
		slog.String("record", cfg.recordPath), slog.Bool("stun", cfg.stun != ""),
		slog.Bool("tree", topo != nil))

	if err := router.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("call ended")
	return nil
}

// runProbe is the Phase 0 reachability check: GET /healthz and validate it.
func runProbe(logger *slog.Logger, server string, timeout time.Duration) error {
	healthURL, err := healthURLFor(server)
	if err != nil {
		return err
	}

	// One context bounds the whole probe. context.WithTimeout derives a child
	// context that auto-cancels after the deadline; cancel() must always be
	// called (defer) to release the timer even on the happy path. Threading
	// this ctx into the request is what makes the -timeout flag actually bite —
	// a hung server can't wedge the peer forever.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	logger.Info("probing server health", slog.String("url", healthURL))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", healthURL, err)
	}
	// Always close the body, always via defer so it runs on every return path.
	// Leaking response bodies leaks connections — a classic Go footgun.
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server unhealthy: status %d, body %q", resp.StatusCode, string(body))
	}

	// Decode into an anonymous struct: we only need these two fields here, so
	// there's no reason to export a named type. The json tags map wire names to
	// fields exactly as on the server side.
	var health struct {
		Status  string `json:"status"`
		Service string `json:"service"`
	}
	if err := json.Unmarshal(body, &health); err != nil {
		return fmt.Errorf("decode health response: %w", err)
	}

	logger.Info("server is healthy",
		slog.Int("status_code", resp.StatusCode),
		slog.String("server_status", health.Status),
		slog.String("server_service", health.Service),
	)
	return nil
}

// healthURLFor turns a user-supplied -server value into a concrete /healthz URL.
// It accepts a bare "host:port" as shorthand for "http://host:port" and rejects
// anything without a real host. Pulling this out of run keeps it a small, pure,
// obviously-testable function — the same reasoning behind logging.ParseLevel.
//
// It parses first and validates the parsed structure, rather than trimming and
// concatenating strings: string surgery on URLs is how "http://" quietly
// becomes a request to a host literally named "http". Rebuilding from
// scheme+host also normalizes away any trailing slash or stray path.
func healthURLFor(server string) (string, error) {
	s := strings.TrimSpace(server)
	if s == "" {
		return "", errors.New("empty -server URL")
	}
	// No scheme? Treat it as the http shorthand ("localhost:8080").
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("parse -server %q: %w", server, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("invalid -server URL %q: scheme must be http or https", server)
	}
	// Guard on Hostname(), not Host. u.Host *includes* the port, so it is
	// non-empty for ":", ":9000", or "http://:9000" (empty hostname, port set) —
	// exactly the shapes that mirror the server's own "-addr :9000" and are easy
	// to type by mistake. Those would build a hostless URL that only explodes
	// deep in the HTTP client ("no Host in request URL"). u.Hostname() strips the
	// port, so it is the real "is there a host?" check. Fail loud and early, and
	// point at the likely fix.
	if u.Hostname() == "" {
		hint := ""
		if u.Port() != "" {
			hint = fmt.Sprintf(` (did you mean "localhost:%s"?)`, u.Port())
		}
		return "", fmt.Errorf("invalid -server URL %q: missing host%s", server, hint)
	}
	return u.Scheme + "://" + u.Host + "/healthz", nil
}
