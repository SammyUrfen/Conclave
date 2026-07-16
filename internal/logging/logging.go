// Package logging centralizes construction of the *slog.Logger used across
// conclave's binaries.
//
// Keeping logger construction in one place means every process — the central
// server and every peer — emits records in the same shape and honors the same
// -log-level / -log-format flags. That uniformity is what will let us grep and
// correlate telemetry across the whole overlay later, when a single "call"
// spans one server and a tree of peers.
//
// Design note (Go house style): nothing here reads a global. The writer and the
// level are passed in. Construction happens once, at the edge of the program
// (in each cmd's main), and the resulting *slog.Logger is injected downward.
// Globals are convenient and then, three phases later, untestable.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// Format selects how a log record is encoded on the wire.
type Format string

const (
	// FormatJSON emits one JSON object per line: machine-parseable, the right
	// default for anything that will eventually flow into a log aggregator.
	FormatJSON Format = "json"
	// FormatText emits slog's key=value text: easier on human eyes at a
	// terminal, which is why the binaries default to it during development.
	FormatText Format = "text"
)

// ParseLevel maps a case-insensitive level name ("debug", "info", "warn"/
// "warning", "error") to its slog.Level.
//
// It is a pure function: no I/O, no globals, output determined entirely by the
// input. That is exactly why it is the natural first thing to cover with a
// table-driven test — see logging_test.go. An unrecognized name returns an
// error (and a safe LevelInfo fallback) rather than silently defaulting, so a
// typo in a flag fails loudly at startup instead of quietly swallowing logs.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("logging: unknown level %q (want debug|info|warn|error)", s)
	}
}

// New builds a *slog.Logger that writes to w at the given level in the given
// format. Passing the io.Writer in (rather than hard-coding os.Stdout) is what
// lets a test point the logger at a bytes.Buffer and assert on the output.
func New(w io.Writer, level slog.Level, format Format) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	switch format {
	case FormatText:
		h = slog.NewTextHandler(w, opts)
	default:
		// JSON is the safe default: an unknown format string still produces
		// structured, parseable output rather than an error at construction.
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h)
}
