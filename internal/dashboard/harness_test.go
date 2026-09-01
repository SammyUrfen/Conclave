package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/clock"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
	"github.com/SammyUrfen/conclave/internal/policy"
)

// testAt is the frozen instant every fixture in this package is stamped with, so an
// assertion may compare *_unix_ms literally instead of accepting any number. It is a
// round value with a non-zero millisecond component, which is what catches a
// second-truncating conversion.
var testAt = time.UnixMilli(1756684812345).UTC()

// fixedClock is the whole clock seam this package needs: dashboard timestamps come
// from Now, and the only ticker it creates (the WebSocket keepalive) must NEVER fire
// during a test — a keepalive firing mid-assertion would inject a frame the test did
// not ask for and make seq assertions flaky. So NewTicker returns a channel nobody
// ever writes to.
type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFixedClock() *fixedClock { return &fixedClock{now: testAt} }

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fixedClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *fixedClock) NewTimer(time.Duration) clock.Timer   { return deadTimer{} }
func (c *fixedClock) NewTicker(time.Duration) clock.Ticker { return deadTicker{} }
func (c *fixedClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

type deadTimer struct{}

func (deadTimer) C() <-chan time.Time      { return make(chan time.Time) }
func (deadTimer) Reset(time.Duration) bool { return false }
func (deadTimer) Stop() bool               { return false }

type deadTicker struct{}

func (deadTicker) C() <-chan time.Time { return make(chan time.Time) }
func (deadTicker) Stop()               {}

// fakeMeets is a MeetSource with no arbiter behind it. Every method may be made to
// fail, because the handler's error mapping is as much of the contract as its happy
// path (§9.4a's frozen code table).
type fakeMeets struct {
	mu      sync.Mutex
	live    []arbiter.Meet
	ended   []arbiter.EndedMeet
	created arbiter.Meet
	// createErr/getErr/listErr are returned verbatim so a test can hand in the exact
	// sentinel arbiter would and assert on the status it maps to.
	createErr, getErr, listErr, endedErr error
	createdIDs                           []string
}

func (f *fakeMeets) ListMeets(context.Context) ([]arbiter.Meet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]arbiter.Meet(nil), f.live...), nil
}

func (f *fakeMeets) ListEndedMeets(context.Context) ([]arbiter.EndedMeet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.endedErr != nil {
		return nil, f.endedErr
	}
	return append([]arbiter.EndedMeet(nil), f.ended...), nil
}

func (f *fakeMeets) CreateMeet(_ context.Context, id string) (arbiter.Meet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createdIDs = append(f.createdIDs, id)
	if f.createErr != nil {
		return arbiter.Meet{}, f.createErr
	}
	m := f.created
	if m.ID == "" {
		m.ID = id
	}
	return m, nil
}

func (f *fakeMeets) GetMeet(_ context.Context, id string) (arbiter.Meet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return arbiter.Meet{}, f.getErr
	}
	for _, m := range f.live {
		if m.ID == id {
			return m, nil
		}
	}
	return arbiter.Meet{}, arbiter.ErrMeetNotFound
}

// fakeSubnet is a SubnetSource. block, when non-nil, holds Snapshot until it is
// closed — the lever the "a handler must never wedge the control plane" tests pull.
type fakeSubnet struct {
	mu    sync.Mutex
	snaps map[string]coordinator.RoomSnapshot
	err   error
	block chan struct{}
	calls int
}

func (f *fakeSubnet) set(s coordinator.RoomSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snaps == nil {
		f.snaps = map[string]coordinator.RoomSnapshot{}
	}
	f.snaps[s.RoomID] = s
}

func (f *fakeSubnet) Snapshot(ctx context.Context, roomID string) (coordinator.RoomSnapshot, error) {
	f.mu.Lock()
	blk, err := f.block, f.err
	f.calls++
	snap := f.snaps[roomID]
	f.mu.Unlock()
	if blk != nil {
		select {
		case <-blk:
		case <-ctx.Done():
			return coordinator.RoomSnapshot{}, ctx.Err()
		}
	}
	return snap, err
}

// fakeDemo records what it was asked to do and returns whatever a test wants.
type fakeDemo struct {
	mu       sync.Mutex
	evictErr error
	electErr error
	evictLog []string
	electLog []string
}

func (f *fakeDemo) Evict(_ context.Context, roomID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictLog = append(f.evictLog, roomID+"/"+name)
	return f.evictErr
}

func (f *fakeDemo) ForceElection(_ context.Context, roomID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.electLog = append(f.electLog, roomID+"/"+name)
	return f.electErr
}

func (f *fakeDemo) evicted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.evictLog...)
}

func (f *fakeDemo) elected() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.electLog...)
}

// testOrigins is the shipped default allow-list, parsed through the real policy
// package. Tests assert against THIS, never against a second copy of the matcher —
// that duplication is the exact hazard §2.4 extracted policy to prevent.
func testOrigins(t *testing.T) policy.Origins {
	t.Helper()
	o, err := policy.ParseOrigins("http://localhost:*,http://127.0.0.1:*,https://sammyurfen.github.io")
	if err != nil {
		t.Fatalf("ParseOrigins: %v", err)
	}
	return o
}

// newTestServer builds a Server over the given seams and returns it with a live
// httptest server in front of its Handler.
func newTestServer(t *testing.T, cfg Config) (*Server, *httptest.Server) {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = newFixedClock()
	}
	if cfg.Meets == nil {
		cfg.Meets = &fakeMeets{}
	}
	if cfg.Addr == "" {
		cfg.Addr = ":9000"
	}
	if cfg.Origins == nil {
		cfg.Origins = testOrigins(t)
	}
	s, err := New(slog.New(slog.DiscardHandler), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		s.Close()
		ts.Close()
	})
	return s, ts
}

// doJSON issues one request and decodes the body into a generic map, returning the
// response so header and status assertions stay available.
func doJSON(t *testing.T, ts *httptest.Server, method, path, body string, hdr map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.URL+path, rdr)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	raw, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: body is not JSON (%v): %s", method, path, err, raw)
		}
	}
	return res, out
}

// wantErr asserts the frozen {code, message, details} envelope (§9.4a): status, code,
// a non-empty message, and details PRESENT (possibly empty) — never absent, because
// the client dereferences it unconditionally.
func wantErr(t *testing.T, res *http.Response, body map[string]any, status int, code string) {
	t.Helper()
	if res.StatusCode != status {
		t.Errorf("status = %d, want %d (body %v)", res.StatusCode, status, body)
	}
	if got := body["api_version"]; got != float64(APIVersion) {
		t.Errorf("api_version = %v, want %d", got, APIVersion)
	}
	e, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("body has no error object: %v", body)
	}
	if e["code"] != code {
		t.Errorf("error.code = %v, want %q", e["code"], code)
	}
	if msg, _ := e["message"].(string); msg == "" {
		t.Errorf("error.message is empty; it must be human-readable")
	}
	if _, ok := e["details"].(map[string]any); !ok {
		t.Errorf("error.details is absent or not an object: %v", e)
	}
}

// sampleSnapshot is the coordinator state the §9.3 example body describes, built with
// the members deliberately OUT of name order so an ordering assertion cannot pass by
// accident on the input's own order.
func sampleSnapshot() coordinator.RoomSnapshot {
	topo := &overlay.Topology{
		Epoch: 3,
		Rev:   11,
		Root:  "alice",
		Edges: []overlay.Edge{
			{Parent: "alice", Child: "bob"},
			{Parent: "alice", Child: "carol"},
			{Parent: "bob", Child: "dave"},
		},
		Backups: []overlay.Backup{{Node: "dave", Parent: "carol"}},
	}
	return coordinator.RoomSnapshot{
		RoomID: "standup",
		Epoch:  3,
		Rev:    11,
		Topo:   topo,
		At:     testAt,
		Members: []coordinator.MemberSnapshot{
			{
				ID: "p5", Name: "dave", Health: coordinator.HealthHealthy,
				Report:   metrics.Report{Name: "dave", UploadKbps: 500, NAT: overlay.NATDirect, RTTServerMs: 44, LossPct: 1.2, CPUPct: 8, Coordinatable: true},
				Reported: true, Parent: "bob", Children: nil, Backup: "carol",
				LastBeatSeq: 401, LastBeatAt: testAt.Add(-445 * time.Millisecond),
			},
			{
				ID: "p2", Name: "alice", Health: coordinator.HealthHealthy,
				Report:   metrics.Report{Name: "alice", UploadKbps: 8000, NAT: overlay.NATDirect, RTTServerMs: 12.5, LossPct: 0.2, CPUPct: 31, Coordinatable: true},
				Reported: true, Parent: "", Children: []string{"bob", "carol"},
				LastBeatSeq: 412, LastBeatAt: testAt.Add(-245 * time.Millisecond),
			},
			{
				ID: "p3", Name: "bob", Health: coordinator.HealthDegraded,
				Report:   metrics.Report{Name: "bob", UploadKbps: 3000, NAT: overlay.NATDirect, RTTServerMs: 240, LossPct: 6.1, CPUPct: 55, Coordinatable: true},
				Reported: true, Parent: "alice", Children: []string{"dave"},
				LastBeatSeq: 407, LastBeatAt: testAt.Add(-3445 * time.Millisecond),
			},
			{
				ID: "p4", Name: "carol", Health: coordinator.HealthHealthy,
				Report:   metrics.Report{Name: "carol", UploadKbps: 1000, NAT: overlay.NATDirect, RTTServerMs: 18.4, CPUPct: 12, Coordinatable: true},
				Reported: true, Parent: "alice", Children: nil,
				LastBeatSeq: 415, LastBeatAt: testAt.Add(-45 * time.Millisecond),
			},
		},
	}
}

// sampleMeet is the arbiter's view of the same meet.
func sampleMeet() arbiter.Meet {
	return arbiter.Meet{
		ID: "standup", CreatedAt: time.UnixMilli(1756684800000).UTC(), Members: 4,
		Epoch: 3, Coordinator: "alice", CoordinatorID: "p2",
		Relays: []string{"alice", "bob"}, Depth: 2,
	}
}
