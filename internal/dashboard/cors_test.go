package dashboard

import (
	"net/http"
	"testing"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/policy"
)

// TestCORSAllowedOrigins is the discriminating half of the origin policy: it must
// fail both if the matcher is replaced by "allow everything" AND if it is replaced by
// "allow nothing". The near-miss origins below are the ones a naive prefix/suffix
// check waves through, which is exactly the silently-permissive failure §2.4 exists
// to prevent.
func TestCORSAllowedOrigins(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{}})

	allowed := []string{
		"http://localhost:5173",
		"http://localhost:9000",
		"http://127.0.0.1:8080",
		"https://sammyurfen.github.io",
	}
	denied := []string{
		// The port wildcard is a PORT wildcard: the host must match exactly.
		"http://localhost.evil.com:5173",
		"http://evil-localhost:5173",
		"http://notlocalhost:5173",
		// Suffix/prefix traps on the Pages origin.
		"https://sammyurfen.github.io.evil.com",
		"https://evil.sammyurfen.github.io",
		"https://sammyurfen.github.io:8443",
		// Scheme must match too — an http:// Pages origin is not the Pages origin.
		"http://sammyurfen.github.io",
		// A wildcard is a value, not a pattern, when it arrives as a request Origin.
		"http://localhost:*",
		"*",
		"null",
	}

	for _, o := range allowed {
		t.Run("allow "+o, func(t *testing.T) {
			res, _ := doJSON(t, ts, http.MethodGet, "/api/meets", "", map[string]string{"Origin": o})
			if got := res.Header.Get("Access-Control-Allow-Origin"); got != o {
				t.Errorf("ACAO = %q, want the request origin %q echoed back", got, o)
			}
			if res.Header.Get("Vary") == "" {
				t.Errorf("Vary is absent; a response that depends on Origin must say so")
			}
			if res.StatusCode != 200 {
				t.Errorf("status = %d, want 200", res.StatusCode)
			}
		})
	}
	for _, o := range denied {
		t.Run("deny "+o, func(t *testing.T) {
			res, body := doJSON(t, ts, http.MethodGet, "/api/meets", "", map[string]string{"Origin": o})
			if got := res.Header.Get("Access-Control-Allow-Origin"); got != "" {
				t.Errorf("ACAO = %q for a non-matching origin; the browser must be the one to block", got)
			}
			// §9.8: a non-matching origin is served the NORMAL response, not a 403 —
			// a distinguishable error would leak the allow-list to a probing page.
			if res.StatusCode != 200 {
				t.Errorf("status = %d, want 200: a mismatched origin must not be "+
					"distinguishable from an allowed one at the HTTP layer", res.StatusCode)
			}
			if _, ok := body["meets"]; !ok {
				t.Errorf("body differs for a denied origin (%v); that difference IS the leak", body)
			}
		})
	}
}

// TestCORSNeverWildcard pins the one rule §9.8 spells out twice: echo the matching
// origin, never "*". It costs one line to do correctly now and becomes a real
// vulnerability the moment anyone adds credentials.
func TestCORSNeverWildcard(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{}})
	for _, path := range []string{"/api/meets", "/api/meets/standup", "/api/nonesuch"} {
		res, _ := doJSON(t, ts, http.MethodGet, path, "", map[string]string{"Origin": "http://localhost:5173"})
		if got := res.Header.Get("Access-Control-Allow-Origin"); got == "*" {
			t.Errorf("%s: ACAO = \"*\"", path)
		}
		if got := res.Header.Get("Access-Control-Allow-Credentials"); got != "" {
			t.Errorf("%s: Access-Control-Allow-Credentials = %q; §9.4a says it is NEVER sent", path, got)
		}
	}
}

// TestCORSNoOriginHeader pins §9.4a: a request with no Origin (curl, a health checker)
// is served normally with NO CORS headers at all. CORS is a browser mechanism and
// there is nothing to answer.
func TestCORSNoOriginHeader(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{}})
	res, _ := doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
	for _, h := range []string{
		"Access-Control-Allow-Origin", "Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers", "Access-Control-Max-Age",
	} {
		if got := res.Header.Get(h); got != "" {
			t.Errorf("%s = %q on an Origin-less request, want absent", h, got)
		}
	}
}

// TestCORSPreflight pins the OPTIONS answer: 204, no body, the same headers, and the
// 600 s Max-Age that is "the largest value Chrome actually honours".
func TestCORSPreflight(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{}})
	req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/api/meets", nil)
	req.Header.Set("Origin", "https://sammyurfen.github.io")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Errorf("preflight status = %d, want 204", res.StatusCode)
	}
	for h, want := range map[string]string{
		"Access-Control-Allow-Origin":  "https://sammyurfen.github.io",
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
		"Access-Control-Allow-Headers": "Content-Type",
		"Access-Control-Max-Age":       "600",
	} {
		if got := res.Header.Get(h); got != want {
			t.Errorf("preflight %s = %q, want %q", h, got, want)
		}
	}

	// A preflight from a rejected origin gets no allow header, so the browser blocks
	// the real request before it is ever sent.
	req2, _ := http.NewRequest(http.MethodOptions, ts.URL+"/api/meets", nil)
	req2.Header.Set("Origin", "https://evil.example")
	res2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer res2.Body.Close()
	if got := res2.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("rejected preflight ACAO = %q, want absent", got)
	}
}

// TestCORSEmptyAllowList pins the narrow-but-legal same-origin-only deployment:
// policy.ParseOrigins("") yields nil, and nil must mean "match nothing", never
// "match everything". Getting that polarity backwards is precisely the silent
// failure this package's origin handling exists to avoid.
func TestCORSEmptyAllowList(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{}, Origins: policy.Origins{}})
	res, _ := doJSON(t, ts, http.MethodGet, "/api/meets", "", map[string]string{"Origin": "http://localhost:5173"})
	if got := res.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q with an EMPTY allow-list; empty must deny, not allow", got)
	}
}

// TestCORSHeadersOnErrorResponses pins that the envelope is reachable cross-origin.
// Without ACAO on a 4xx the browser blocks the body, and the UI shows "network
// error" for every one of §9.4a's frozen codes — the error contract would be
// unobservable from the only client that exists.
func TestCORSHeadersOnErrorResponses(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}})
	hdr := map[string]string{"Origin": "https://sammyurfen.github.io"}
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/api/meets/nope", ""},
		{http.MethodGet, "/api/meets/NOPE", ""},
		{http.MethodPost, "/api/meets", `{"id":`},
		{http.MethodGet, "/api/nonesuch", ""},
		{http.MethodPost, "/api/demo/meets/standup/evict", `{"name":"bob"}`},
	} {
		res, _ := doJSON(t, ts, tc.method, tc.path, tc.body, hdr)
		if res.StatusCode < 400 {
			t.Fatalf("%s %s returned %d; this table needs failures", tc.method, tc.path, res.StatusCode)
		}
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "https://sammyurfen.github.io" {
			t.Errorf("%s %s (%d): ACAO = %q, want the error envelope readable cross-origin",
				tc.method, tc.path, res.StatusCode, got)
		}
	}
}
