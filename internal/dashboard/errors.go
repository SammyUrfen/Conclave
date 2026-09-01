package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/SammyUrfen/conclave/internal/arbiter"
)

// The frozen §9.4a error codes. They are stable snake_case tokens the UI may branch
// on, which is why they are constants and not literals sprinkled through handlers.
const (
	codeBadRequest     = "bad_request"     // 400, malformed JSON body
	codeInvalidMeetID  = "invalid_meet_id" // 400, fails policy.MeetIDPattern
	codeMeetExists     = "meet_exists"     // 409
	codeMeetNotFound   = "meet_not_found"  // 404
	codeMemberNotFound = "member_not_found"
	codeNoCandidate    = "no_candidate"   // 409
	codeTooManyMeets   = "too_many_meets" // 429
	codeInternal       = "internal"       // 500

	// The two codes §9.4a's table does not cover, added rather than mis-reused.
	//
	// §9.4a froze a set for the API's own failures and never named a ROUTING failure.
	// Answering an unknown /api/ path with meet_not_found would tell a client its meet
	// vanished when in fact the path was wrong, and answering a bad method with
	// bad_request would contradict its own status line. Both are non-breaking for the
	// frontend, which branches only on the codes it knows and otherwise renders
	// `message`. Flagged for the spec owner.
	// codeForbiddenOrigin is returned on a REJECTED WEBSOCKET UPGRADE (403). It is not
	// reachable from the REST surface, which deliberately serves a non-matching origin
	// the normal body without the allow header so a probing page learns nothing
	// (§9.8). An upgrade has no such option — there is no useful "succeed but let the
	// browser block it" for a socket — so it fails loudly and names the flag to change,
	// which v2.6 requires a client to surface as a configuration error.
	codeForbiddenOrigin = "forbidden_origin" // 403, upgrade only

	codeNotFound         = "not_found"          // 404, no such route
	codeMethodNotAllowed = "method_not_allowed" // 405
)

// errorBody is the project's {code, message, details} envelope, carried on EVERY
// non-2xx response (§9.4a) — including a routing miss, and including a response the
// browser will only see cross-origin, which is why the CORS headers are attached
// before the status is written.
type errorBody struct {
	APIVersion int         `json:"api_version"`
	Error      errorDetail `json:"error"`
}

// errorDetail is the envelope's payload. Details is never absent: the client
// dereferences it unconditionally, so an omitted object would be a TypeError in the
// one place the UI is trying to explain a failure.
type errorDetail struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

// writeJSON writes v with the status and the JSON content type. It marshals to a
// buffer first so a mid-encode failure cannot leave a half-written body behind a 200 —
// the failure mode that makes a client's "malformed body" path fire for what was
// really a server bug.
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		s.log.Error("encoding a response body failed", slog.Any("error", err))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"api_version":1,"error":{"code":"internal",` +
			`"message":"the server could not encode the response","details":{}}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

// writeError emits the frozen envelope.
func (s *Server) writeError(w http.ResponseWriter, status int, code, msg string, details map[string]any) {
	if details == nil {
		details = map[string]any{}
	}
	s.writeJSON(w, status, errorBody{
		APIVersion: APIVersion,
		Error:      errorDetail{Code: code, Message: msg, Details: details},
	})
}

// clientGone reports whether err is the ORDINARY END OF A CLIENT CONNECTION rather than
// a fault on this side.
//
// Every read this package performs runs under the requesting connection's context, so a
// browser closing a tab, navigating away, or aborting a fetch cancels a snapshot already
// in flight. That is not a failure of anything: nothing is broken, nobody needs to look,
// and there is no one left to serve. Reporting it as an internal error produces a log of
// faults that are not faults — the specific confusion that made a dozen harmless lines
// read as a dozen defects earlier in this build — and, on the event stream, synthesises
// an internal-error close code for a client that has already gone.
//
// It matches through errors.Is, never on message text, so a wrapped cancellation from
// any depth is recognised and an unrelated error that happens to mention "context" is
// not. It is deliberately NARROW: only context.Canceled. context.DeadlineExceeded stays
// a genuine error because a deadline being exceeded means something was too slow, which
// is exactly the kind of thing an operator should see — and a predicate that swallowed
// both would be the blanket downgrade this split exists to avoid.
func clientGone(err error) bool {
	return errors.Is(err, context.Canceled)
}

// writeInternal is the deliberate narrowing at the boundary: the caller gets a stable
// code and a message that says nothing, while the actual error goes to the log. There
// is no authentication on this surface, so an error string is a free read of the
// server's internals to anyone who can reach it.
func (s *Server) writeInternal(w http.ResponseWriter, what string, err error) {
	if clientGone(err) {
		// The response below still goes out, because net/http requires the handler to
		// finish writing something; it lands on a closed socket and is discarded.
		s.log.Debug("client disconnected before the response was ready",
			slog.String("op", what))
	} else {
		s.log.Error("dashboard request failed", slog.String("op", what), slog.Any("error", err))
	}
	s.writeError(w, http.StatusInternalServerError, codeInternal,
		"the server could not complete the request; see the server log", nil)
}

// writeSourceError translates an error from a control-plane seam into §9.4a's table.
//
// It matches on SENTINELS with errors.Is, never on message text, which is the whole
// reason arbiter exports them as values: a boundary that matches on prose is a
// boundary that breaks the next time someone improves a message.
func (s *Server) writeSourceError(w http.ResponseWriter, what string, err error, details map[string]any) {
	switch {
	case errors.Is(err, arbiter.ErrMeetNotFound):
		s.writeError(w, http.StatusNotFound, codeMeetNotFound, "no such meet", details)
	case errors.Is(err, arbiter.ErrMeetExists):
		s.writeError(w, http.StatusConflict, codeMeetExists, "that meet already exists", details)
	case errors.Is(err, arbiter.ErrInvalidMeetID):
		s.writeError(w, http.StatusBadRequest, codeInvalidMeetID, "that meet id is not allowed", details)
	case errors.Is(err, arbiter.ErrTooManyMeets):
		s.writeError(w, http.StatusTooManyRequests, codeTooManyMeets,
			"the server is at its meet limit; end a meet and try again", details)
	case errors.Is(err, arbiter.ErrNoCandidate):
		s.writeError(w, http.StatusConflict, codeNoCandidate,
			"no peer in this meet is eligible to coordinate", details)
	case errors.Is(err, ErrMemberNotFound):
		s.writeError(w, http.StatusNotFound, codeMemberNotFound, "no such member in this meet", details)
	default:
		// arbiter.ErrNotRunning lands here on purpose: a shutting-down process is an
		// internal condition, not something a client can act on.
		s.writeInternal(w, what, err)
	}
}

// methodNotAllowed answers a wrong verb in the project's envelope, with the Allow
// header HTTP requires.
func (s *Server) methodNotAllowed(w http.ResponseWriter, r *http.Request, allow string) {
	w.Header().Set("Allow", allow)
	s.writeError(w, http.StatusMethodNotAllowed, codeMethodNotAllowed,
		"that method is not allowed on this path",
		map[string]any{"method": r.Method, "allow": allow})
}

// handleUnknown is the /api/ catch-all. It exists so ServeMux never gets to write its
// own plain-text 404, which would break §9.4a's "every non-2xx is the envelope".
func (s *Server) handleUnknown(w http.ResponseWriter, r *http.Request) {
	s.writeError(w, http.StatusNotFound, codeNotFound, "no such endpoint",
		map[string]any{"path": r.URL.Path})
}
