package dashboard

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"

	"github.com/SammyUrfen/conclave/internal/policy"
)

// maxRequestBody caps a REST body. Every body this API accepts is a single short
// field, so 8 KiB is three orders of magnitude of headroom and still small enough that
// an unauthenticated caller cannot make the server allocate anything interesting.
const maxRequestBody = 8 << 10

// handleMeets dispatches GET (list) and POST (create) on /api/meets.
func (s *Server) handleMeets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listMeets(w, r)
	case http.MethodPost:
		s.createMeet(w, r)
	default:
		s.methodNotAllowed(w, r, corsAllowMethods)
	}
}

// listMeets serves GET /api/meets: the REALIZED view plus the tombstone ring.
func (s *Server) listMeets(w http.ResponseWriter, r *http.Request) {
	live, err := s.cfg.Meets.ListMeets(r.Context())
	if err != nil {
		s.writeInternal(w, "list meets", err)
		return
	}
	ended, err := s.cfg.Meets.ListEndedMeets(r.Context())
	if err != nil {
		s.writeInternal(w, "list ended meets", err)
		return
	}

	body := listBody{
		APIVersion: APIVersion,
		// The capability signal: "is the destructive surface registered". It is
		// derived from the same nil check that decides whether the routes exist, so
		// the advertisement cannot drift from the truth.
		DemoEnabled: s.cfg.Demo != nil,
		Meets:       make([]listRow, 0, len(live)),
		Ended:       make([]endedListRow, 0, len(ended)),
	}
	for _, m := range live {
		body.Meets = append(body.Meets, listRow{
			ID:              m.ID,
			CreatedAtUnixMs: unixMs(m.CreatedAt),
			Members:         m.Members,
			Epoch:           m.Epoch,
			Coordinator:     m.Coordinator,
			CoordinatorID:   m.CoordinatorID,
			ArbiterIsCoord:  m.ArbiterIsCoord,
			Relays:          append(make([]string, 0, len(m.Relays)), m.Relays...),
			Depth:           m.Depth,
			Provenance:      provenanceRealized,
		})
	}
	for _, e := range ended {
		body.Ended = append(body.Ended, endedListRow{
			ID:              e.ID,
			CreatedAtUnixMs: unixMs(e.CreatedAt),
			EndedAtUnixMs:   unixMs(e.EndedAt),
			PeakMembers:     e.PeakMembers,
			FinalEpoch:      e.FinalEpoch,
			Elections:       e.Elections,
		})
	}
	// §8.1: meets[] by created_at descending then id ascending; ended[] by ended_at
	// descending then id ascending. The arbiter already returns both in this order —
	// sorting again is O(n log n) on at most MaxMeets rows and makes the ordering a
	// property of THIS boundary rather than of one implementation of one seam.
	sort.SliceStable(body.Meets, func(i, j int) bool {
		if body.Meets[i].CreatedAtUnixMs != body.Meets[j].CreatedAtUnixMs {
			return body.Meets[i].CreatedAtUnixMs > body.Meets[j].CreatedAtUnixMs
		}
		return body.Meets[i].ID < body.Meets[j].ID
	})
	sort.SliceStable(body.Ended, func(i, j int) bool {
		if body.Ended[i].EndedAtUnixMs != body.Ended[j].EndedAtUnixMs {
			return body.Ended[i].EndedAtUnixMs > body.Ended[j].EndedAtUnixMs
		}
		return body.Ended[i].ID < body.Ended[j].ID
	})
	s.writeJSON(w, http.StatusOK, body)
}

// createMeetRequest is POST /api/meets. `id` is optional: absent or empty means the
// arbiter generates one.
type createMeetRequest struct {
	ID string `json:"id"`
}

// createMeet serves POST /api/meets.
func (s *Server) createMeet(w http.ResponseWriter, r *http.Request) {
	var req createMeetRequest
	if !s.decodeBody(w, r, &req) {
		return
	}
	// Validate a CLIENT-SUPPLIED id here, at the boundary, through the same
	// policy.ValidMeetID every other externally-facing surface uses. The arbiter
	// checks it too and that is not redundancy for its own sake: this check is what
	// keeps unvalidated input from reaching a control-plane seam at all, and both
	// call the single implementation, so there is nothing to drift.
	//
	// An EMPTY id is passed straight through — generating one here would defeat the
	// arbiter's crypto/rand ids, whose unpredictability is what stops an
	// unauthenticated caller enumerating every live meet.
	if req.ID != "" && !policy.ValidMeetID(req.ID) {
		s.writeError(w, http.StatusBadRequest, codeInvalidMeetID,
			"a meet id must match "+policy.MeetIDPattern,
			map[string]any{"id": req.ID, "pattern": policy.MeetIDPattern})
		return
	}

	meet, err := s.cfg.Meets.CreateMeet(r.Context(), req.ID)
	if err != nil {
		s.writeSourceError(w, "create meet", err, map[string]any{"id": req.ID})
		return
	}
	// Location alongside the body (§9.4a), so a client that follows headers and one
	// that reads the body land in the same place.
	w.Header().Set("Location", "/api/meets/"+meet.ID)
	s.writeJSON(w, http.StatusCreated, createBody{
		APIVersion:      APIVersion,
		ID:              meet.ID,
		CreatedAtUnixMs: unixMs(meet.CreatedAt),
		Join:            s.joinFor(meet.ID),
	})
}

// handleMeet serves GET /api/meets/{id}: one meet, full INTENDED snapshot — the same
// object the WS stream sends as its first frame.
func (s *Server) handleMeet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, r, "GET, OPTIONS")
		return
	}
	id, ok := s.meetIDParam(w, r)
	if !ok {
		return
	}
	body, err := s.snapshotBody(r.Context(), id, s.staleCount(id))
	if err != nil {
		s.writeSourceError(w, "snapshot", err, map[string]any{"id": id})
		return
	}
	s.writeJSON(w, http.StatusOK, body)
}

// handleDemoEvict serves POST /api/demo/meets/{id}/evict. Registered only when a
// DemoControl was supplied (§9.6).
func (s *Server) handleDemoEvict(w http.ResponseWriter, r *http.Request) {
	s.demoAction(w, r, "evict", true, func(id, name string) error {
		return s.cfg.Demo.Evict(r.Context(), id, name)
	})
}

// handleDemoElect serves POST /api/demo/meets/{id}/elect. `name` is OPTIONAL: empty
// means "the best candidate", which is the arbiter's own default.
func (s *Server) handleDemoElect(w http.ResponseWriter, r *http.Request) {
	s.demoAction(w, r, "elect", false, func(id, name string) error {
		return s.cfg.Demo.ForceElection(r.Context(), id, name)
	})
}

// demoActionRequest is the body of both demo endpoints.
type demoActionRequest struct {
	Name string `json:"name"`
}

// demoAction is the shared shape of the two destructive endpoints: validate, delegate,
// 202, and publish the `demo` event that makes the action visible to everyone watching
// the meet — including the operator's colleague on the same screen, which is the whole
// reason by_remote_addr exists.
func (s *Server) demoAction(w http.ResponseWriter, r *http.Request, action string, nameRequired bool, run func(id, name string) error) {
	if r.Method != http.MethodPost {
		s.methodNotAllowed(w, r, "POST, OPTIONS")
		return
	}
	id, ok := s.meetIDParam(w, r)
	if !ok {
		return
	}
	var req demoActionRequest
	if !s.decodeBody(w, r, &req) {
		return
	}
	if nameRequired && req.Name == "" {
		s.writeError(w, http.StatusBadRequest, codeBadRequest,
			`"name" is required for this action`, map[string]any{"action": action})
		return
	}
	if err := run(id, req.Name); err != nil {
		s.writeSourceError(w, "demo "+action, err,
			map[string]any{"id": id, "name": req.Name, "action": action})
		return
	}
	s.log.Warn("demo control invoked",
		"action", action, "meet_id", id, "target", req.Name, "remote_addr", r.RemoteAddr)
	s.publishDemo(id, action, req.Name, r.RemoteAddr)
	s.writeJSON(w, http.StatusAccepted, demoAckBody{
		APIVersion: APIVersion, OK: true, Action: action, Target: req.Name,
	})
}

// meetIDParam reads and validates the {id} path segment, answering 400 rather than
// letting a malformed id reach a seam. A malformed id is refused, never normalised: a
// silently rewritten id would put a caller in a different meet than the one it asked
// for, which is far harder to debug than a refusal.
func (s *Server) meetIDParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !policy.ValidMeetID(id) {
		s.writeError(w, http.StatusBadRequest, codeInvalidMeetID,
			"a meet id must match "+policy.MeetIDPattern,
			map[string]any{"id": id, "pattern": policy.MeetIDPattern})
		return "", false
	}
	return id, true
}

// decodeBody reads a bounded JSON body into dst. An EMPTY body is legal and leaves dst
// zero — POST /api/meets with no body is a valid "create a meet with a generated id",
// and treating an absent body as malformed would make the simplest call the awkward one.
func (s *Server) decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, codeBadRequest,
			"the request body could not be read", nil)
		return false
	}
	if len(raw) == 0 {
		return true
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		s.writeError(w, http.StatusBadRequest, codeBadRequest,
			"the request body is not valid JSON", nil)
		return false
	}
	return true
}
