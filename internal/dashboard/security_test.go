package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
)

// rawUpgrade issues a WebSocket handshake with a caller-chosen Host header and returns
// the response status.
//
// It is hand-rolled rather than driven through websocket.Dial because the whole point is
// to control Host INDEPENDENTLY of the dial address — Go's client always sets Host from
// the URL, and a DNS-rebinding attacker does exactly the opposite: resolves a name they
// control to an address they cannot otherwise reach, so the victim's browser sends that
// name as BOTH Host and Origin while the packets arrive at the server's real address.
func rawUpgrade(t *testing.T, serverURL, host, origin, path string) int {
	t.Helper()
	addr := strings.TrimPrefix(serverURL, "http://")
	conn, err := net.DialTimeout("tcp", addr, wsTimeout)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(wsTimeout)); err != nil {
		t.Fatal(err)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&b, "Host: %s\r\n", host)
	b.WriteString("Connection: Upgrade\r\n")
	b.WriteString("Upgrade: websocket\r\n")
	b.WriteString("Sec-WebSocket-Version: 13\r\n")
	// A fixed 16-byte base64 key; the value is never checked for randomness.
	b.WriteString("Sec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==\r\n")
	if origin != "" {
		fmt.Fprintf(&b, "Origin: %s\r\n", origin)
	}
	b.WriteString("\r\n")
	if _, err := conn.Write([]byte(b.String())); err != nil {
		t.Fatalf("write handshake: %v", err)
	}

	req, _ := http.NewRequest(http.MethodGet, "http://"+host+path, nil)
	res, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	defer res.Body.Close()
	return res.StatusCode
}

// TestUpgradeRejectsDNSRebinding is the CRITICAL regression.
//
// coder/websocket's authenticateOrigin returns ALLOW when r.Host equals the Origin's
// host — before it ever consults OriginPatterns, and ignoring the scheme entirely. So an
// attacker who rebinds a name they control to the server's address gets a browser to
// send Host: evil.com AND Origin: http://evil.com, and the upgrade succeeds against an
// allow-list that names neither. That reaches a loopback or LAN server the attacker
// cannot dial directly, which is precisely the reachability assumption that makes an
// unauthenticated localhost dashboard defensible at all.
//
// policy.Origins.Match has no such rule, so the library was accepting origins this
// service's own CORS surface rejects. The fix is to stop letting the library decide:
// InsecureSkipVerify plus one explicit gate through the single matcher.
func TestUpgradeRejectsDNSRebinding(t *testing.T) {
	_, url := standupServer(t, Config{})

	tests := []struct {
		name   string
		host   string
		origin string
	}{
		{name: "attacker name as both Host and Origin", host: "evil.com", origin: "http://evil.com"},
		{name: "with a port", host: "evil.com:9000", origin: "http://evil.com:9000"},
		{name: "case-folded, as EqualFold accepts", host: "EVIL.com", origin: "http://evil.COM"},
		// The library compares HOSTS ONLY, so a plaintext origin claiming a host the
		// allow-list permits only over https slipped through too. (The loopback host
		// cannot show this: the shipped list allows http://127.0.0.1:* outright.)
		{name: "scheme downgrade on an https-only host",
			host: "sammyurfen.github.io", origin: "http://sammyurfen.github.io"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rawUpgrade(t, url, tt.host, tt.origin, "/api/meets/standup/events")
			if got == http.StatusSwitchingProtocols {
				t.Fatalf("upgrade ACCEPTED (101) for Host=%q Origin=%q — a rebinding page "+
					"can now open this stream against a server it cannot dial", tt.host, tt.origin)
			}
			if got != http.StatusForbidden {
				t.Errorf("status = %d, want 403 so the client can name -allowed-origins "+
					"as the cause (§9.4a, v2.6)", got)
			}
		})
	}
}

// TestUpgradeOriginGateMatchesREST is the "one matcher" property, asserted as parity
// rather than by inspection: for every origin, the WS upgrade decision must equal the
// REST CORS decision. §2.4 extracted internal/policy because two copies of a
// security-relevant matcher drift, and the drift's failure mode is a silently permissive
// policy that each surface's own tests pass against. This is the test that would catch
// that drift, and it is what the DNS-rebinding hole actually was: the WS side was using
// a second matcher (the library's) all along.
func TestUpgradeOriginGateMatchesREST(t *testing.T) {
	_, url := standupServer(t, Config{})
	ts := &httptest.Server{URL: url}
	host := strings.TrimPrefix(url, "http://")

	origins := []struct {
		origin string
		allow  bool
	}{
		{"http://localhost:5173", true},
		{"http://127.0.0.1:8080", true},
		{"https://sammyurfen.github.io", true},
		{"http://localhost.evil.com:5173", false},
		{"https://sammyurfen.github.io.evil.com", false},
		{"https://evil.sammyurfen.github.io", false},
		{"http://sammyurfen.github.io", false},
		{"https://evil.example", false},
		{"null", false},
		{"*", false},
	}
	for _, o := range origins {
		t.Run(o.origin, func(t *testing.T) {
			res, _ := doJSON(t, ts, http.MethodGet, "/api/meets", "", map[string]string{"Origin": o.origin})
			restAllowed := res.Header.Get("Access-Control-Allow-Origin") != ""
			// The upgrade is dialled with the REAL host, so this isolates the origin
			// decision from the rebinding case above.
			wsAllowed := rawUpgrade(t, url, host, o.origin, "/api/meets/standup/events") == http.StatusSwitchingProtocols
			if restAllowed != wsAllowed {
				t.Errorf("origin %q: REST allows=%v but WS upgrade allows=%v — two matchers "+
					"have diverged", o.origin, restAllowed, wsAllowed)
			}
			if wsAllowed != o.allow {
				t.Errorf("origin %q: WS allows=%v, want %v", o.origin, wsAllowed, o.allow)
			}
		})
	}
}

// TestUpgradeWithoutOriginIsAllowed pins that a NON-BROWSER client still connects. Origin
// is a browser-supplied header; a Go peer, curl, or a health checker sends none, and
// there is nothing to authenticate. Rejecting those would break every non-browser
// consumer while stopping no attack, because a page cannot suppress its own Origin.
func TestUpgradeWithoutOriginIsAllowed(t *testing.T) {
	_, url := standupServer(t, Config{})
	host := strings.TrimPrefix(url, "http://")
	if got := rawUpgrade(t, url, host, "", "/api/meets/standup/events"); got != http.StatusSwitchingProtocols {
		t.Errorf("status = %d, want 101 for an Origin-less upgrade", got)
	}
}

// TestUpgradeRejectionCarriesTheErrorEnvelope pins that the 403 is the project's
// {code,message,details} body naming the flag to change, not the library's plain-text
// "Forbidden". §9.4a requires the envelope on every non-2xx, and v2.6 requires a client
// to surface an upgrade 403 as a CONFIGURATION error — which it can only do if the body
// says which configuration.
func TestUpgradeRejectionCarriesTheErrorEnvelope(t *testing.T) {
	_, url := standupServer(t, Config{})
	req, _ := http.NewRequest(http.MethodGet, url+"/api/meets/standup/events", nil)
	req.Header.Set("Origin", "https://evil.example")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "AAAAAAAAAAAAAAAAAAAAAA==")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", res.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("403 body is not JSON: %v", err)
	}
	e, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error envelope: %v", body)
	}
	if msg, _ := e["message"].(string); !strings.Contains(msg, "allowed-origins") {
		t.Errorf("message = %q; it must name -allowed-origins so the operator knows what "+
			"to change", msg)
	}
}

// TestSnapshotReadsAreRateBounded is the MAJOR fix.
//
// Every snapshot round-trips coordinator.Snapshot through the SINGLE Run goroutine that
// also drives heartbeats, dwell timers and rebuilds — so an unauthenticated caller
// hammering GET /api/meets/{id} (no socket required) could stall the control plane for
// every meet in the process. The coordinator deliberately moved its outbound sends OFF
// that goroutine so one slow peer could not do this; an unbounded read amplifier
// reintroduces the same hazard from outside.
//
// The bound is a short-lived cached snapshot: at most one round-trip per meet per
// snapshotMinInterval, no matter the request rate.
func TestSnapshotReadsAreRateBounded(t *testing.T) {
	clk := newFixedClock()
	sub := &fakeSubnet{}
	sub.set(sampleSnapshot())
	_, ts := newTestServer(t, Config{
		Meets:  &fakeMeets{live: []arbiter.Meet{sampleMeet()}},
		Subnet: sub,
		Clock:  clk,
	})

	for i := 0; i < 50; i++ {
		res, _ := doJSON(t, ts, http.MethodGet, "/api/meets/standup", "", nil)
		if res.StatusCode != 200 {
			t.Fatalf("request %d: status %d", i, res.StatusCode)
		}
	}
	sub.mu.Lock()
	calls := sub.calls
	sub.mu.Unlock()
	if calls != 1 {
		t.Errorf("50 requests caused %d coordinator round-trips, want 1 — an "+
			"unauthenticated caller must not be able to drive the control-plane "+
			"goroutine at request rate", calls)
	}

	// Past the bound, the next read refreshes: the cache is a rate limit, not a freeze.
	clk.Advance(snapshotMinInterval)
	doJSON(t, ts, http.MethodGet, "/api/meets/standup", "", nil)
	sub.mu.Lock()
	calls = sub.calls
	sub.mu.Unlock()
	if calls != 2 {
		t.Errorf("after snapshotMinInterval the read count is %d, want 2 — a cache that "+
			"never refreshes is a frozen dashboard, not a rate limit", calls)
	}
}

// TestListReadsAreRateBounded covers the same amplifier on the ARBITER's Run goroutine.
// GET /api/meets round-trips it twice per request (live + tombstones) and is the very
// first call any client makes, so it is the easiest one to point a loop at.
func TestListReadsAreRateBounded(t *testing.T) {
	clk := newFixedClock()
	meets := &countingMeets{fakeMeets: fakeMeets{live: []arbiter.Meet{sampleMeet()}}}
	_, ts := newTestServer(t, Config{Meets: meets, Clock: clk})

	for i := 0; i < 50; i++ {
		doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
	}
	if n := meets.listCalls(); n != 1 {
		t.Errorf("50 list requests caused %d arbiter round-trips, want 1", n)
	}
	clk.Advance(snapshotMinInterval)
	doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
	if n := meets.listCalls(); n != 2 {
		t.Errorf("after the interval the list read count is %d, want 2", n)
	}
}

// TestCreateInvalidatesTheListCache pins that the one flow a human drives — create a
// meet, then look at the list — is never served a body that predates their own write.
// A cache that made your own creation invisible for a quarter second would be read as a
// failed create, and the retry would 409.
func TestCreateInvalidatesTheListCache(t *testing.T) {
	clk := newFixedClock()
	meets := &countingMeets{fakeMeets: fakeMeets{}}
	_, ts := newTestServer(t, Config{Meets: meets, Clock: clk})

	doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
	meets.mu.Lock()
	meets.live = []arbiter.Meet{sampleMeet()}
	meets.created = arbiter.Meet{ID: "standup", CreatedAt: testAt}
	meets.mu.Unlock()

	doJSON(t, ts, http.MethodPost, "/api/meets", `{"id":"standup"}`, nil)
	_, body := doJSON(t, ts, http.MethodGet, "/api/meets", "", nil)
	rows, _ := body["meets"].([]any)
	if len(rows) != 1 {
		t.Errorf("the list served %d rows after a create at the same instant, want 1 — "+
			"a create must invalidate the cache", len(rows))
	}
}

// TestResyncIsRateLimited bounds the client->server verb. A resync is the recovery path
// for a seq gap and a legitimate client sends one per gap episode; a loop of them costs
// a frame, an encode and a seq each. The limit coalesces the flood without killing the
// connection, because a client hitting genuine repeated gaps is not misbehaving.
func TestResyncIsRateLimited(t *testing.T) {
	clk := newFixedClock()
	sub := &fakeSubnet{}
	sub.set(sampleSnapshot())
	s, ts := newTestServer(t, Config{
		Meets:  &fakeMeets{live: []arbiter.Meet{sampleMeet()}},
		Subnet: sub,
		Clock:  clk,
	})
	conn := dialEvents(t, ts.URL, "standup", "")
	if f := readFrame(t, conn); f.Seq != 1 {
		t.Fatalf("first seq = %d", f.Seq)
	}

	for i := 0; i < 20; i++ {
		writeOp(t, conn, "resync")
	}
	// Exactly one of the twenty is honoured; the rest are dropped, not queued, so they
	// do not burn seq either — a suppressed request was never a frame.
	f := readFrame(t, conn)
	if f.Kind != "snapshot" || f.Seq != 2 {
		t.Fatalf("first resync reply = kind %q seq %d, want snapshot seq 2", f.Kind, f.Seq)
	}
	// Prove the rest were suppressed rather than merely slow: a publish now must be the
	// very next frame, at seq 3.
	s.Publish(coordinator.Event{Kind: coordinator.EventUnbuildable, RoomID: "standup", Reason: "after"})
	g := readFrame(t, conn)
	if g.Kind != "unbuildable" || g.Seq != 3 {
		t.Errorf("next frame = kind %q seq %d, want unbuildable seq 3 — %d extra resync "+
			"replies were queued instead of suppressed", g.Kind, g.Seq, 19)
	}

	clk.Advance(resyncMinInterval)
	writeOp(t, conn, "resync")
	h := readFrame(t, conn)
	if h.Kind != "snapshot" {
		t.Errorf("after resyncMinInterval kind = %q, want snapshot — the limit must let a "+
			"client that genuinely keeps hitting gaps recover", h.Kind)
	}
}

// TestSubscriberCapPerMeet bounds fan-out. Publish walks every subscriber of a meet
// UNDER THE DASHBOARD'S LOCK, on the coordinator's Run goroutine — so subscriber count
// is per-event work on the control plane, and unbounded subscribers is unbounded work
// there. The cap is what keeps that O(1)-ish rather than attacker-chosen.
func TestSubscriberCapPerMeet(t *testing.T) {
	_, url := standupServer(t, Config{})
	var open []*websocket.Conn
	t.Cleanup(func() {
		for _, c := range open {
			c.CloseNow()
		}
	})
	for i := 0; i < maxSubscribersPerMeet; i++ {
		c := dialEvents(t, url, "standup", "")
		readFrame(t, c)
		open = append(open, c)
	}

	over, _, err := dialEventsErr(t, url, "standup", "")
	if err != nil {
		t.Fatalf("the over-cap dial failed at the handshake (%v); it must upgrade and "+
			"then close with a code, so the client knows to back off rather than treating "+
			"it as an origin rejection", err)
	}
	defer over.CloseNow()
	// StatusTryAgainLater: the client's unrecognised-code branch reconnects with
	// backoff, which is exactly right — unlike 1008, which tells it to stop forever.
	if got := readUntilClose(t, over); got != websocket.StatusTryAgainLater {
		t.Errorf("over-cap close status = %d, want 1013 (try again later)", got)
	}

	// A departure frees a slot: the cap is a limit, not a permanent ceiling.
	open[0].CloseNow()
	open = open[1:]
	waitFor(t, func() bool {
		c, _, err := dialEventsErr(t, url, "standup", "")
		if err != nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, _, rerr := c.Read(ctx)
		c.CloseNow()
		return rerr == nil
	}, "a freed slot to admit a new subscriber")
}

// TestNosniffHeader pins X-Content-Type-Options on every response. Error bodies reflect
// the requested path, and without nosniff a browser may content-sniff a response into a
// type it was not sent as — turning a reflected path into a script execution vector on
// this origin.
func TestNosniffHeader(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}})
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/api/meets", ""},
		{http.MethodGet, "/api/meets/standup", ""},
		{http.MethodGet, "/api/nonesuch", ""},
		{http.MethodGet, "/api/meets/%3Cscript%3E", ""},
		{http.MethodPost, "/api/meets", `{"id":`},
		{http.MethodOptions, "/api/meets", ""},
	} {
		res, _ := doJSON(t, ts, tc.method, tc.path, tc.body, nil)
		if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s %s (%d): X-Content-Type-Options = %q, want nosniff",
				tc.method, tc.path, res.StatusCode, got)
		}
	}
}

// countingMeets counts ListMeets calls so a test can assert the arbiter round-trip bound.
type countingMeets struct {
	fakeMeets
	lists int
}

func (c *countingMeets) ListMeets(ctx context.Context) ([]arbiter.Meet, error) {
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return c.fakeMeets.ListMeets(ctx)
}

func (c *countingMeets) listCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists
}
