package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/policy"
)

// TestListMeets pins GET /api/meets against §9.3 and §9.4b: the row schema, the
// deliberate ABSENCE of rev, the "realized" provenance marker, the ended[] tombstone
// array, and the frozen orderings from §8.1.
func TestListMeets(t *testing.T) {
	t0 := time.UnixMilli(1756684800000).UTC()
	meets := &fakeMeets{
		// Deliberately NOT in wire order: the handler owes the frontend a defined
		// order (§8.1) and must not merely pass the source's order through.
		live: []arbiter.Meet{
			{ID: "beta", CreatedAt: t0, Members: 1, Epoch: 1, Coordinator: "eve", CoordinatorID: "p9"},
			{ID: "old", CreatedAt: t0.Add(-time.Hour), Members: 0, Depth: -1},
			{ID: "alpha", CreatedAt: t0, Members: 4, Epoch: 3, Coordinator: "alice", CoordinatorID: "p2",
				Relays: []string{"alice", "bob"}, Depth: 2},
		},
		ended: []arbiter.EndedMeet{
			{ID: "z-old", CreatedAt: t0.Add(-2 * time.Hour), EndedAt: t0.Add(-time.Hour), PeakMembers: 5, FinalEpoch: 4, Elections: 2},
			{ID: "a-new", CreatedAt: t0.Add(-2 * time.Hour), EndedAt: t0, PeakMembers: 2, FinalEpoch: 1},
			{ID: "b-new", CreatedAt: t0.Add(-2 * time.Hour), EndedAt: t0, PeakMembers: 3, FinalEpoch: 2},
		},
	}
	_, ts := newTestServer(t, Config{Meets: meets})

	res, body := doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if body["api_version"] != float64(APIVersion) {
		t.Errorf("api_version = %v", body["api_version"])
	}

	rows, _ := body["meets"].([]any)
	if len(rows) != 3 {
		t.Fatalf("meets has %d rows, want 3: %v", len(rows), body["meets"])
	}
	// §8.1: created_at_unix_ms descending, then id ascending.
	var gotIDs []string
	for _, r := range rows {
		gotIDs = append(gotIDs, r.(map[string]any)["id"].(string))
	}
	wantIDs := []string{"alpha", "beta", "old"}
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Errorf("meets order = %v, want %v", gotIDs, wantIDs)
	}

	row := rows[0].(map[string]any)
	for k, want := range map[string]any{
		"id": "alpha", "created_at_unix_ms": float64(1756684800000), "members": float64(4),
		"epoch": float64(3), "coordinator": "alice", "coordinator_id": "p2",
		"arbiter_is_coordinator": false, "depth": float64(2), "provenance": "realized",
	} {
		if row[k] != want {
			t.Errorf("meets[0].%s = %#v, want %#v", k, row[k], want)
		}
	}
	if _, present := row["rev"]; present {
		t.Errorf("meets[0] carries rev; §9.3/§9.4b say rev is ABSENT from a list row " +
			"because a heartbeat-derived rev would be a fabricated number wearing an authoritative name")
	}
	if r, ok := row["relays"].([]any); !ok || len(r) != 2 {
		t.Errorf("meets[0].relays = %#v, want a 2-element array", row["relays"])
	}
	// A meet with no relays must serialize as [] and not null: the frontend does
	// Array.isArray()/.map() on it without a null guard.
	if r, ok := rows[2].(map[string]any)["relays"].([]any); !ok || len(r) != 0 {
		t.Errorf("meets[2].relays = %#v, want []", rows[2].(map[string]any)["relays"])
	}

	ended, ok := body["ended"].([]any)
	if !ok {
		t.Fatalf("ended[] is absent; §9.3 says it is ALWAYS present")
	}
	if len(ended) != 3 {
		t.Fatalf("ended has %d rows, want 3", len(ended))
	}
	// §8.1/§15.12: ended_at_unix_ms descending, then id ascending.
	var gotEnded []string
	for _, r := range ended {
		gotEnded = append(gotEnded, r.(map[string]any)["id"].(string))
	}
	if fmt.Sprint(gotEnded) != fmt.Sprint([]string{"a-new", "b-new", "z-old"}) {
		t.Errorf("ended order = %v, want [a-new b-new z-old]", gotEnded)
	}
	e := ended[2].(map[string]any)
	for k, want := range map[string]any{
		"id": "z-old", "ended_at_unix_ms": float64(t0.Add(-time.Hour).UnixMilli()),
		"created_at_unix_ms": float64(t0.Add(-2 * time.Hour).UnixMilli()),
		"peak_members":       float64(5), "final_epoch": float64(4), "elections": float64(2),
	} {
		if e[k] != want {
			t.Errorf("ended[2].%s = %#v, want %#v", k, e[k], want)
		}
	}
}

// TestListMeetsEmpty pins that the two arrays are [] and never null on a fresh server.
func TestListMeetsEmpty(t *testing.T) {
	_, ts := newTestServer(t, Config{})
	res, body := doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	for _, k := range []string{"meets", "ended"} {
		v, ok := body[k].([]any)
		if !ok || v == nil {
			t.Errorf("%s = %#v, want []", k, body[k])
		}
	}
}

// TestDemoCapabilitySignal covers the addition §9 lacks: the frontend must be able to
// learn whether -demo is on WITHOUT issuing a destructive verb speculatively. The
// signal rides GET /api/meets because that is the one call the dashboard already makes
// before it can do anything else.
func TestDemoCapabilitySignal(t *testing.T) {
	for _, tt := range []struct {
		name string
		demo DemoControl
		want bool
	}{
		{name: "off", demo: nil, want: false},
		{name: "on", demo: &fakeDemo{}, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, ts := newTestServer(t, Config{Demo: tt.demo})
			_, body := doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
			got, ok := body["demo_enabled"].(bool)
			if !ok {
				t.Fatalf("demo_enabled is absent from GET /api/meets: %v", body)
			}
			if got != tt.want {
				t.Errorf("demo_enabled = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCreateMeet pins POST /api/meets: 201, the Location header, the join rendezvous
// block, and the frozen error mapping. The error cases matter as much as the happy
// path — a boundary that guesses a status is a boundary the UI cannot branch on.
func TestCreateMeet(t *testing.T) {
	t.Run("created", func(t *testing.T) {
		meets := &fakeMeets{created: arbiter.Meet{ID: "standup", CreatedAt: time.UnixMilli(1756684800000).UTC()}}
		_, ts := newTestServer(t, Config{Meets: meets, Addr: ":9000"})
		res, body := doJSON(t, ts, http.MethodPost, "/api/meets", `{"id":"standup"}`, nil)
		if res.StatusCode != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body %v)", res.StatusCode, body)
		}
		if loc := res.Header.Get("Location"); loc != "/api/meets/standup" {
			t.Errorf("Location = %q, want /api/meets/standup", loc)
		}
		if body["id"] != "standup" || body["created_at_unix_ms"] != float64(1756684800000) {
			t.Errorf("body = %v", body)
		}
		join, ok := body["join"].(map[string]any)
		if !ok {
			t.Fatalf("no join block: %v", body)
		}
		if join["ws_url"] != "ws://localhost:9000/ws?room=standup" {
			t.Errorf("join.ws_url = %v", join["ws_url"])
		}
		if join["peer_command"] != "peer -call -managed -server http://localhost:9000 -room standup -name YOUR_NAME" {
			t.Errorf("join.peer_command = %v", join["peer_command"])
		}
	})

	t.Run("generated id passes empty through", func(t *testing.T) {
		meets := &fakeMeets{created: arbiter.Meet{ID: "m-7fa3k2qd", CreatedAt: testAt}}
		_, ts := newTestServer(t, Config{Meets: meets})
		res, body := doJSON(t, ts, http.MethodPost, "/api/meets", `{}`, nil)
		if res.StatusCode != http.StatusCreated {
			t.Fatalf("status = %d (body %v)", res.StatusCode, body)
		}
		if body["id"] != "m-7fa3k2qd" {
			t.Errorf("id = %v, want the arbiter's generated id", body["id"])
		}
		if len(meets.createdIDs) != 1 || meets.createdIDs[0] != "" {
			t.Errorf("CreateMeet got %q, want the empty id passed straight through so the "+
				"arbiter generates it (the dashboard must not mint ids)", meets.createdIDs)
		}
	})

	t.Run("empty body is a legal create", func(t *testing.T) {
		meets := &fakeMeets{created: arbiter.Meet{ID: "m-abc", CreatedAt: testAt}}
		_, ts := newTestServer(t, Config{Meets: meets})
		res, _ := doJSON(t, ts, http.MethodPost, "/api/meets", "", nil)
		if res.StatusCode != http.StatusCreated {
			t.Errorf("status = %d, want 201", res.StatusCode)
		}
	})

	errCases := []struct {
		name   string
		id     string
		err    error
		status int
		code   string
	}{
		{name: "exists", id: "standup", err: arbiter.ErrMeetExists, status: 409, code: "meet_exists"},
		{name: "too many", id: "standup", err: arbiter.ErrTooManyMeets, status: 429, code: "too_many_meets"},
		{name: "invalid from arbiter", id: "standup", err: arbiter.ErrInvalidMeetID, status: 400, code: "invalid_meet_id"},
		{name: "not running", id: "standup", err: arbiter.ErrNotRunning, status: 500, code: "internal"},
	}
	for _, tc := range errCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ts := newTestServer(t, Config{Meets: &fakeMeets{createErr: fmt.Errorf("wrapped: %w", tc.err)}})
			res, body := doJSON(t, ts, http.MethodPost, "/api/meets", `{"id":"`+tc.id+`"}`, nil)
			wantErr(t, res, body, tc.status, tc.code)
		})
	}

	t.Run("malformed json", func(t *testing.T) {
		_, ts := newTestServer(t, Config{})
		res, body := doJSON(t, ts, http.MethodPost, "/api/meets", `{"id":`, nil)
		wantErr(t, res, body, 400, "bad_request")
	})

	t.Run("invalid id rejected at the boundary", func(t *testing.T) {
		meets := &fakeMeets{}
		_, ts := newTestServer(t, Config{Meets: meets})
		res, body := doJSON(t, ts, http.MethodPost, "/api/meets", `{"id":"Bad Id"}`, nil)
		wantErr(t, res, body, 400, "invalid_meet_id")
		if len(meets.createdIDs) != 0 {
			t.Errorf("a malformed id reached the MeetSource (%v); policy.ValidMeetID must "+
				"reject it at the boundary so nothing downstream sees unvalidated input", meets.createdIDs)
		}
	})
}

// TestJoinURLDerivation pins §9.4a's -public-url rule, including the host-less -addr
// default that makes the out-of-the-box GitHub Pages story work.
func TestJoinURLDerivation(t *testing.T) {
	tests := []struct {
		name      string
		addr      string
		publicURL string
		wantWS    string
		wantHTTP  string
	}{
		{name: "hostless addr defaults to localhost", addr: ":9000",
			wantWS: "ws://localhost:9000/ws?room=m1", wantHTTP: "http://localhost:9000"},
		{name: "addr with host", addr: "example.com:9000",
			wantWS: "ws://example.com:9000/ws?room=m1", wantHTTP: "http://example.com:9000"},
		{name: "public-url wins", addr: ":9000", publicURL: "https://conclave.example.org",
			wantWS: "wss://conclave.example.org/ws?room=m1", wantHTTP: "https://conclave.example.org"},
		{name: "public-url trailing slash trimmed", addr: ":9000", publicURL: "https://x.example/",
			wantWS: "wss://x.example/ws?room=m1", wantHTTP: "https://x.example"},
		{name: "public-url with explicit ws scheme normalises", addr: ":9000", publicURL: "ws://host:1234",
			wantWS: "ws://host:1234/ws?room=m1", wantHTTP: "http://host:1234"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meets := &fakeMeets{created: arbiter.Meet{ID: "m1", CreatedAt: testAt}}
			_, ts := newTestServer(t, Config{Meets: meets, Addr: tt.addr, PublicURL: tt.publicURL})
			_, body := doJSON(t, ts, http.MethodPost, "/api/meets", `{"id":"m1"}`, nil)
			join, _ := body["join"].(map[string]any)
			if join["ws_url"] != tt.wantWS {
				t.Errorf("ws_url = %v, want %v", join["ws_url"], tt.wantWS)
			}
			wantCmd := "peer -call -managed -server " + tt.wantHTTP + " -room m1 -name YOUR_NAME"
			if join["peer_command"] != wantCmd {
				t.Errorf("peer_command = %v, want %v", join["peer_command"], wantCmd)
			}
		})
	}
}

// TestJoinCommandIsValidInput pins that the rendezvous this endpoint hands a human is a
// command that WORKS when pasted unchanged.
//
// joinurl.go interpolates the meet id and the name placeholder into a shell command with
// NO escaping, relying entirely on policy's patterns to make that safe — so every value
// it emits must satisfy the pattern the receiving surface enforces. The name placeholder
// is the one that bit: the hub validates -name against policy.ValidPeerName, which is
// lowercase-only, so an uppercase YOUR_NAME is rejected the moment it is used as
// intended. A copy button that hands out a failing command is worse than no button.
func TestJoinCommandIsValidInput(t *testing.T) {
	meets := &fakeMeets{created: arbiter.Meet{ID: "standup", CreatedAt: testAt}}
	_, ts := newTestServer(t, Config{Meets: meets})
	_, body := doJSON(t, ts, http.MethodPost, "/api/meets", `{"id":"standup"}`, nil)
	join, ok := body["join"].(map[string]any)
	if !ok {
		t.Fatalf("no join block: %v", body)
	}
	cmd, _ := join["peer_command"].(string)
	fields := strings.Fields(cmd)
	got := map[string]string{}
	for i := 0; i+1 < len(fields); i++ {
		if strings.HasPrefix(fields[i], "-") {
			got[fields[i]] = fields[i+1]
		}
	}

	name := got["-name"]
	if name == "" {
		t.Fatalf("peer_command has no -name value: %q", cmd)
	}
	if !policy.ValidPeerName(name) {
		t.Errorf("peer_command -name %q fails policy.ValidPeerName (%s) — pasting this "+
			"command verbatim is rejected by the hub", name, policy.PeerNamePattern)
	}
	if room := got["-room"]; !policy.ValidMeetID(room) {
		t.Errorf("peer_command -room %q fails policy.ValidMeetID", room)
	}
	// Still obviously a placeholder: a valid-but-plausible real name would be worse,
	// because a user would paste it without noticing they had joined as someone else.
	if !strings.Contains(name, "-") && !strings.Contains(name, "_") {
		t.Errorf("-name %q does not read as a placeholder", name)
	}
	// Every interpolated value must be shell-safe on its own terms, since nothing here
	// quotes them.
	for flag, v := range got {
		if strings.ContainsAny(v, " \t\"'$`;&|<>()") {
			t.Errorf("peer_command %s value %q contains shell metacharacters and is "+
				"interpolated unescaped", flag, v)
		}
	}
}

// TestGetMeetNotFound pins the 404 for an unknown meet and the 400 for an id that
// cannot be a meet id at all.
func TestGetMeetNotFound(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{}})
	res, body := doJSON(t, ts, http.MethodGet, "/api/meets/nope", "", nil)
	wantErr(t, res, body, 404, "meet_not_found")

	res, body = doJSON(t, ts, http.MethodGet, "/api/meets/NOPE", "", nil)
	wantErr(t, res, body, 400, "invalid_meet_id")
}

// TestRoutingEnvelope pins that a routing miss still speaks the project's error
// envelope. §9.4a says EVERY non-2xx body is {code,message,details}; Go's ServeMux
// default 404/405 pages are plain text, so a bare mux would silently break that.
func TestRoutingEnvelope(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}})

	t.Run("unknown path", func(t *testing.T) {
		res, body := doJSON(t, ts, http.MethodGet, "/api/nonesuch", "", nil)
		if res.StatusCode != 404 {
			t.Errorf("status = %d, want 404", res.StatusCode)
		}
		if _, ok := body["error"].(map[string]any); !ok {
			t.Errorf("unknown /api/ path returned no error envelope: %v", body)
		}
	})

	t.Run("wrong method", func(t *testing.T) {
		res, body := doJSON(t, ts, http.MethodDelete, "/api/meets", "", nil)
		if res.StatusCode != 405 {
			t.Errorf("status = %d, want 405", res.StatusCode)
		}
		if _, ok := body["error"].(map[string]any); !ok {
			t.Errorf("405 returned no error envelope: %v", body)
		}
		if a := res.Header.Get("Allow"); a == "" {
			t.Errorf("405 has no Allow header")
		}
	})
}

// TestListMeetsSourceFailure pins that a failing seam becomes a 500 envelope rather
// than a panic or a half-written body.
func TestListMeetsSourceFailure(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{listErr: fmt.Errorf("boom")}})
	res, body := doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
	wantErr(t, res, body, 500, "internal")
	// The message must not leak the underlying error text to an unauthenticated
	// caller; it is logged instead.
	e := body["error"].(map[string]any)
	if msg, _ := e["message"].(string); msg == "boom" {
		t.Errorf("500 message echoes the internal error verbatim: %q", msg)
	}
}

// TestListBodyIsStableJSON asserts the list body is byte-identical across repeated
// calls with the same state. Go randomises map iteration, and the frontend replays
// these bodies — a map anywhere in the wire types makes two identical states render
// differently. This has bitten the project before, so it is a test, not a review note.
func TestListBodyIsStableJSON(t *testing.T) {
	meets := &fakeMeets{
		live: []arbiter.Meet{
			sampleMeet(),
			{ID: "retro", CreatedAt: time.UnixMilli(1756680000000).UTC(), Members: 1, Epoch: 1, Coordinator: "eve"},
		},
		ended: []arbiter.EndedMeet{{ID: "gone", EndedAt: testAt}},
	}
	clk := newFixedClock()
	_, ts := newTestServer(t, Config{Meets: meets, Clock: clk})
	var first string
	for i := 0; i < 20; i++ {
		// Past the read bound each time, so these are 20 real assemblies of the body
		// rather than 19 reads of one cached string.
		clk.Advance(snapshotMinInterval)
		_, body := doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = string(raw)
			continue
		}
		if string(raw) != first {
			t.Fatalf("list body differs between identical calls:\n %s\n %s", first, raw)
		}
	}
}
