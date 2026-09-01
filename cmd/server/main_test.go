package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestPlane builds the real wiring from real flags, so these tests exercise what
// run() exercises rather than a hand-assembled lookalike.
func newTestPlane(t *testing.T, args ...string) *plane {
	t.Helper()
	f, err := parseFlags(args, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags(%v): %v", args, err)
	}
	res, err := f.resolve()
	if err != nil {
		t.Fatalf("resolve(%v): %v", args, err)
	}
	p, err := newPlane(testLogger(), f, res)
	if err != nil {
		t.Fatalf("newPlane(%v): %v", args, err)
	}
	t.Cleanup(p.close)
	return p
}

// errorCodeOf pulls the frozen §9.4a error code out of a response body, because the
// interesting distinction between two 404s is the code and not the status.
func errorCodeOf(t *testing.T, body []byte) string {
	t.Helper()
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("response body is not the error envelope: %v (%s)", err, body)
	}
	return env.Error.Code
}

// TestHealthz keeps the Phase 0 probe green through every rewire. It is what the
// deploy target's health check calls, so a regression here is a failed deploy.
func TestHealthz(t *testing.T) {
	p := newTestPlane(t)
	rec := httptest.NewRecorder()
	p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rec.Code)
	}
	var got healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("healthz body: %v (%s)", err, rec.Body.String())
	}
	if got.Status != "ok" || got.Service != "conclave-server" {
		t.Fatalf("healthz body = %+v", got)
	}
}

// TestDashboardMount pins that the /api/ surface is mounted so handlers keep their
// FULL path. dashboard's patterns are absolute ("/api/meets"), so mounting it under a
// StripPrefix — the reflexive thing to do with a sub-router — makes every route 404
// while the server looks perfectly healthy.
func TestDashboardMount(t *testing.T) {
	p := newTestPlane(t)
	rec := httptest.NewRecorder()
	p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/meets", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/meets = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	var body struct {
		APIVersion  int  `json:"api_version"`
		DemoEnabled bool `json:"demo_enabled"`
		Meets       []struct {
			ID string `json:"id"`
		} `json:"meets"`
		Ended []struct{} `json:"ended"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("meets body: %v (%s)", err, rec.Body.String())
	}
	if body.APIVersion != 1 {
		t.Fatalf("api_version = %d, want 1", body.APIVersion)
	}
	if body.Meets == nil || body.Ended == nil {
		t.Fatalf("meets/ended must be [] and never null: %s", rec.Body.String())
	}
}

// TestDashboardCanBeDisabled: with -dashboard=false the surface must not exist at all.
func TestDashboardCanBeDisabled(t *testing.T) {
	p := newTestPlane(t, "-dashboard=false")
	if p.dash != nil {
		t.Fatal("plane.dash is non-nil with -dashboard=false")
	}
	rec := httptest.NewRecorder()
	p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/meets", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /api/meets with -dashboard=false = %d, want 404", rec.Code)
	}
}

// TestDemoRoutesAreOffByDefault is §9.6's structural guard: the destructive routes
// are NOT REGISTERED unless -demo is set, so they answer with the router's
// "no such endpoint" rather than with a permission decision. An unregistered route
// cannot be reached by a bug in a permission check.
//
// The two cases are distinguished by CODE, not by status: both are 404, and a test
// that only checked the status would pass against a server that had registered the
// routes and merely failed to find the meet.
func TestDemoRoutesAreOffByDefault(t *testing.T) {
	const path = "/api/demo/meets/standup/evict"
	body := strings.NewReader(`{"name":"alice"}`)

	t.Run("off: the route does not exist", func(t *testing.T) {
		p := newTestPlane(t)
		if p.demoEnabled() {
			t.Fatal("demo is enabled with no -demo flag")
		}
		rec := httptest.NewRecorder()
		p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, body))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("POST %s = %d, want 404", path, rec.Code)
		}
		if got := errorCodeOf(t, rec.Body.Bytes()); got != "not_found" {
			t.Fatalf("error code = %q, want %q — the route answered, so it was registered",
				got, "not_found")
		}
	})

	t.Run("on: the route exists and reaches the arbiter", func(t *testing.T) {
		p := newTestPlane(t, "-demo")
		if !p.demoEnabled() {
			t.Fatal("demo is disabled with -demo set")
		}
		rec := httptest.NewRecorder()
		p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path,
			strings.NewReader(`{"name":"alice"}`)))
		if got := errorCodeOf(t, rec.Body.Bytes()); got != "meet_not_found" {
			t.Fatalf("error code = %q, want %q — the request never reached the arbiter", got, "meet_not_found")
		}
	})
}

// TestDemoAdvertisementCannotDrift pins the §9.6 property that the `demo_enabled`
// flag on GET /api/meets is derived from the SAME `cfg.Demo != nil` check that
// decides whether the routes exist. Advertising from a separate boolean is how a
// dashboard ends up offering a button that 404s.
func TestDemoAdvertisementCannotDrift(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want bool
	}{
		{name: "off", want: false},
		{name: "on", args: []string{"-demo"}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlane(t, tc.args...)
			rec := httptest.NewRecorder()
			p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/meets", nil))
			var body struct {
				DemoEnabled bool `json:"demo_enabled"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("meets body: %v", err)
			}
			if body.DemoEnabled != tc.want {
				t.Fatalf("demo_enabled = %v, want %v", body.DemoEnabled, tc.want)
			}
			// The advertisement and the routing must agree, whatever they say.
			rec2 := httptest.NewRecorder()
			p.mux().ServeHTTP(rec2, httptest.NewRequest(http.MethodPost,
				"/api/demo/meets/standup/evict", strings.NewReader(`{"name":"a"}`)))
			routeExists := errorCodeOf(t, rec2.Body.Bytes()) != "not_found"
			if routeExists != body.DemoEnabled {
				t.Fatalf("demo_enabled = %v but the route %s exist",
					body.DemoEnabled, map[bool]string{true: "does", false: "does not"}[routeExists])
			}
		})
	}
}

// TestWSRouteIsMounted keeps the peer-facing upgrade path reachable. A plain GET
// without the upgrade headers is rejected by the WebSocket handshake, which is
// itself proof the route exists — a missing route would 404 instead.
func TestWSRouteIsMounted(t *testing.T) {
	p := newTestPlane(t)
	rec := httptest.NewRecorder()
	p.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ws?room=standup&name=alice", nil))
	if rec.Code == http.StatusNotFound {
		t.Fatal("GET /ws = 404: the signaling upgrade route is not mounted")
	}
}

// TestRunFailsLoudOnBadConfig proves the whole startup gate short-circuits BEFORE a
// listener is opened. Each case is a config that contradicts itself; none of them may
// produce a running server. The test would hang, not fail, if run() started serving —
// which is why every case here is one run() must reject.
func TestRunFailsLoudOnBadConfig(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "unknown log level", args: []string{"-log-level", "verbose"}, wantErr: "unknown level"},
		{name: "unknown log format", args: []string{"-log-format", "yaml"}, wantErr: "-log-format"},
		{name: "unparseable origin", args: []string{"-allowed-origins", "http://*.evil.com"}, wantErr: "-allowed-origins"},
		{name: "empty origins with the dashboard on", args: []string{"-allowed-origins", ""}, wantErr: "-allowed-origins"},
		{name: "impossible tree constraints", args: []string{"-coordinate", "-max-depth", "0"}, wantErr: "-max-depth"},
		{name: "gone before the socket reaper", args: []string{"-gone-after", "5s"}, wantErr: "-gone-after"},
		{name: "degraded after gone", args: []string{"-degraded-after", "20s"}, wantErr: "-degraded-after"},
		{name: "unusable public url", args: []string{"-public-url", "ftp://example.com"}, wantErr: "public-url"},
		{name: "unknown flag", args: []string{"-nonsense"}, wantErr: "nonsense"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Every case binds no port, so -addr is never reached; if one of them
			// ever did start serving, that is the bug this test is here to catch.
			err := run(append([]string{"-addr", "127.0.0.1:0"}, tt.args...))
			if err == nil {
				t.Fatalf("run(%v) started; want a startup error", tt.args)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run(%v) = %v, want an error naming %q", tt.args, err, tt.wantErr)
			}
		})
	}
}
