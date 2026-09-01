package dashboard

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/SammyUrfen/conclave/internal/arbiter"
)

// TestDemoRoutesUnregisteredWhenOff is the structural half of §9.6: with no
// DemoControl the destructive routes DO NOT EXIST. Not 403 — an unregistered route
// cannot be reached by a bug in a permission check, and that is the whole argument.
func TestDemoRoutesUnregisteredWhenOff(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}, Demo: nil})
	for _, path := range []string{
		"/api/demo/meets/standup/evict",
		"/api/demo/meets/standup/elect",
	} {
		res, body := doJSON(t, ts, http.MethodPost, path, `{"name":"bob"}`, nil)
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404 (NOT 403 — the route must not exist)", path, res.StatusCode)
		}
		e, _ := body["error"].(map[string]any)
		if e == nil {
			t.Errorf("%s: no error envelope: %v", path, body)
			continue
		}
		// §9.4a: demo_disabled is listed only so nobody adds it. A distinguishable
		// "the demo surface exists but is off" answer would advertise the surface.
		if e["code"] == "demo_disabled" {
			t.Errorf("%s: code = demo_disabled; §9.4a says it is NEVER returned", path)
		}
	}
}

// TestDemoEvict pins the 202 body, the delegation, and the frozen error mapping.
func TestDemoEvict(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		demo := &fakeDemo{}
		_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}, Demo: demo})
		res, body := doJSON(t, ts, http.MethodPost, "/api/demo/meets/standup/evict", `{"name":"bob"}`, nil)
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 (body %v)", res.StatusCode, body)
		}
		for k, want := range map[string]any{
			"api_version": float64(APIVersion), "ok": true, "action": "evict", "target": "bob",
		} {
			if body[k] != want {
				t.Errorf("%s = %#v, want %#v", k, body[k], want)
			}
		}
		if got := demo.evicted(); fmt.Sprint(got) != "[standup/bob]" {
			t.Errorf("Evict calls = %v, want [standup/bob]", got)
		}
	})

	t.Run("name required", func(t *testing.T) {
		demo := &fakeDemo{}
		_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}, Demo: demo})
		res, body := doJSON(t, ts, http.MethodPost, "/api/demo/meets/standup/evict", `{}`, nil)
		wantErr(t, res, body, 400, "bad_request")
		if len(demo.evicted()) != 0 {
			t.Errorf("an empty name reached Evict; evicting 'nobody' has no meaning")
		}
	})

	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{name: "member gone", err: ErrMemberNotFound, status: 404, code: "member_not_found"},
		{name: "meet gone", err: arbiter.ErrMeetNotFound, status: 404, code: "meet_not_found"},
		{name: "other", err: fmt.Errorf("hub exploded"), status: 500, code: "internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, ts := newTestServer(t, Config{
				Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}},
				Demo:  &fakeDemo{evictErr: fmt.Errorf("wrapped: %w", tc.err)},
			})
			res, body := doJSON(t, ts, http.MethodPost, "/api/demo/meets/standup/evict", `{"name":"bob"}`, nil)
			wantErr(t, res, body, tc.status, tc.code)
		})
	}
}

// TestDemoElect pins that "name" is OPTIONAL (⇒ best candidate) and that a forced
// election with no eligible peer is 409 no_candidate, not a 500.
func TestDemoElect(t *testing.T) {
	t.Run("named", func(t *testing.T) {
		demo := &fakeDemo{}
		_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}, Demo: demo})
		res, body := doJSON(t, ts, http.MethodPost, "/api/demo/meets/standup/elect", `{"name":"carol"}`, nil)
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d (body %v)", res.StatusCode, body)
		}
		if body["action"] != "elect" || body["target"] != "carol" {
			t.Errorf("body = %v", body)
		}
		if got := demo.elected(); fmt.Sprint(got) != "[standup/carol]" {
			t.Errorf("ForceElection calls = %v", got)
		}
	})

	t.Run("unnamed means best candidate", func(t *testing.T) {
		demo := &fakeDemo{}
		_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}, Demo: demo})
		res, body := doJSON(t, ts, http.MethodPost, "/api/demo/meets/standup/elect", `{}`, nil)
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d (body %v)", res.StatusCode, body)
		}
		if body["target"] != "" {
			t.Errorf("target = %#v, want \"\"", body["target"])
		}
		if got := demo.elected(); fmt.Sprint(got) != "[standup/]" {
			t.Errorf("ForceElection calls = %v, want the empty name passed through", got)
		}
	})

	t.Run("no candidate", func(t *testing.T) {
		_, ts := newTestServer(t, Config{
			Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}},
			Demo:  &fakeDemo{electErr: fmt.Errorf("wrapped: %w", arbiter.ErrNoCandidate)},
		})
		res, body := doJSON(t, ts, http.MethodPost, "/api/demo/meets/standup/elect", `{}`, nil)
		wantErr(t, res, body, 409, "no_candidate")
	})
}

// TestDemoInvalidMeetID pins that the destructive surface validates its path segment
// at the boundary too — the demo routes are the LAST place unvalidated input should
// reach a control-plane seam.
func TestDemoInvalidMeetID(t *testing.T) {
	demo := &fakeDemo{}
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{}, Demo: demo})
	res, body := doJSON(t, ts, http.MethodPost, "/api/demo/meets/BAD%20ID/evict", `{"name":"bob"}`, nil)
	wantErr(t, res, body, 400, "invalid_meet_id")
	if len(demo.evicted()) != 0 {
		t.Errorf("a malformed meet id reached Evict: %v", demo.evicted())
	}
}
