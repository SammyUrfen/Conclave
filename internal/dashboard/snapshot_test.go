package dashboard

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/SammyUrfen/conclave/internal/arbiter"
	"github.com/SammyUrfen/conclave/internal/coordinator"
	"github.com/SammyUrfen/conclave/internal/metrics"
	"github.com/SammyUrfen/conclave/internal/overlay"
)

// TestGetMeetSnapshot pins the whole §9.3 detail body: the envelope, the INTENDED
// topology fields, the REALIZED per-node fields, every frozen ordering, and the
// §9.4b convergence pair.
func TestGetMeetSnapshot(t *testing.T) {
	subnet := &fakeSubnet{}
	subnet.set(sampleSnapshot())
	_, ts := newTestServer(t, Config{
		Meets:  &fakeMeets{live: []arbiter.Meet{sampleMeet()}},
		Subnet: subnet,
	})

	res, body := doJSON(t, ts, http.MethodGet, "/api/meets/standup", "", nil)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %v)", res.StatusCode, body)
	}
	for k, want := range map[string]any{
		"api_version": float64(APIVersion), "id": "standup", "epoch": float64(3), "rev": float64(11),
		"coordinator": "alice", "coordinator_id": "p2", "arbiter_is_coordinator": false,
		"root": "alice", "at_unix_ms": float64(testAt.UnixMilli()),
		"stale_rejected": float64(0), "converged": true, "provenance": "intended",
	} {
		if body[k] != want {
			t.Errorf("%s = %#v, want %#v", k, body[k], want)
		}
	}
	if d, ok := body["diverged"].([]any); !ok || len(d) != 0 {
		t.Errorf("diverged = %#v, want []", body["diverged"])
	}

	nodes, _ := body["nodes"].([]any)
	if len(nodes) != 4 {
		t.Fatalf("nodes has %d entries, want 4", len(nodes))
	}
	var names []string
	for _, n := range nodes {
		names = append(names, n.(map[string]any)["name"].(string))
	}
	// §8.1: dashboard nodes[] ascending by name. sampleSnapshot supplies them out of
	// order on purpose, so passing here cannot be an accident of the input.
	if fmt.Sprint(names) != fmt.Sprint([]string{"alice", "bob", "carol", "dave"}) {
		t.Errorf("nodes order = %v, want [alice bob carol dave]", names)
	}

	byName := map[string]map[string]any{}
	for _, n := range nodes {
		m := n.(map[string]any)
		byName[m["name"].(string)] = m
	}

	// Expected fitness values are computed BY HAND from arbiter.Score's frozen
	// weights, never by calling Score in the test — a test that re-derives its
	// expectation from the code under test proves only that the code is consistent
	// with itself. Uptime is 0 for every node: coordinator.MemberSnapshot carries no
	// join time, so the dashboard cannot supply that term. §15.14 ruled that the wire
	// key is therefore `fitness_lower_bound` and NOT `fitness`: a value quietly short
	// by up to the 0.15 uptime weight is invisibly wrong, and the name is what makes
	// it announce itself. The absence assertion below is the load-bearing half —
	// emitting BOTH keys would satisfy a rename test that only checked the new one.
	//   alice: .30*.69 + .35*(1-12.5/300) + .20*(1-0.2/10) = 0.738
	//   bob:   .30*.45 + .35*(1-240/300)  + .20*(1-6.1/10) = 0.283
	//   carol: .30*.88 + .35*(1-18.4/300) + .20*1          = 0.793
	//   dave:  .30*.92 + .35*(1-44/300)   + .20*(1-1.2/10) = 0.751
	tests := []struct {
		name    string
		want    map[string]any
		roles   []string
		kids    []string
		fitness float64
	}{
		{
			name: "alice", roles: []string{"coordinator", "relay"}, kids: []string{"bob", "carol"},
			fitness: 0.738,
			want: map[string]any{
				"id": "p2", "health": "healthy", "parent": "", "backup": "", "depth": float64(0),
				"upload_kbps": float64(8000), "nat": "direct", "rtt_server_ms": 12.5,
				"loss_pct": 0.2, "cpu_pct": 31.0, "last_beat_seq": float64(412),
				"last_beat_unix_ms": float64(testAt.Add(-245 * time.Millisecond).UnixMilli()),
			},
		},
		{
			name: "bob", roles: []string{"relay"}, kids: []string{"dave"}, fitness: 0.283,
			want: map[string]any{
				"id": "p3", "health": "degraded", "parent": "alice", "backup": "", "depth": float64(1),
				"upload_kbps": float64(3000), "nat": "direct", "rtt_server_ms": 240.0,
				"loss_pct": 6.1, "cpu_pct": 55.0, "last_beat_seq": float64(407),
			},
		},
		{
			name: "carol", roles: []string{"leaf"}, kids: []string{}, fitness: 0.793,
			want: map[string]any{
				"id": "p4", "health": "healthy", "parent": "alice", "backup": "", "depth": float64(1),
				"upload_kbps": float64(1000), "nat": "direct", "loss_pct": 0.0, "cpu_pct": 12.0,
			},
		},
		{
			name: "dave", roles: []string{"leaf"}, kids: []string{}, fitness: 0.751,
			want: map[string]any{
				"id": "p5", "health": "healthy", "parent": "bob", "backup": "carol", "depth": float64(2),
				"upload_kbps": float64(500), "nat": "direct", "loss_pct": 1.2, "cpu_pct": 8.0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := byName[tt.name]
			if n == nil {
				t.Fatalf("node %q missing", tt.name)
			}
			for k, want := range tt.want {
				if n[k] != want {
					t.Errorf("%s.%s = %#v, want %#v", tt.name, k, n[k], want)
				}
			}
			var roles []string
			rr, ok := n["roles"].([]any)
			if !ok {
				t.Fatalf("%s.roles is not an array: %#v", tt.name, n["roles"])
			}
			for _, r := range rr {
				roles = append(roles, r.(string))
			}
			if fmt.Sprint(roles) != fmt.Sprint(tt.roles) {
				t.Errorf("%s.roles = %v, want %v", tt.name, roles, tt.roles)
			}
			kids, ok := n["children"].([]any)
			if !ok {
				t.Fatalf("%s.children is not an array (must be [] not null): %#v", tt.name, n["children"])
			}
			var got []string
			for _, k := range kids {
				got = append(got, k.(string))
			}
			if fmt.Sprint(got) != fmt.Sprint(tt.kids) && !(len(got) == 0 && len(tt.kids) == 0) {
				t.Errorf("%s.children = %v, want %v", tt.name, got, tt.kids)
			}
			f, ok := n["fitness_lower_bound"].(float64)
			if !ok {
				t.Fatalf("%s has no fitness_lower_bound: %#v", tt.name, n["fitness_lower_bound"])
			}
			if math.Abs(f-tt.fitness) > 1e-9 {
				t.Errorf("%s.fitness_lower_bound = %v, want %v (3 decimals, §9.4a)", tt.name, f, tt.fitness)
			}
			if _, present := n["fitness"]; present {
				t.Errorf("%s still carries the bare `fitness` key; §15.14 renamed it to "+
					"fitness_lower_bound and emitting both would let the UI keep reading "+
					"the unlabelled one", tt.name)
			}
		})
	}

	// §8.1: edges[] mirror Topology.Edges in TOPOLOGICAL order — not sorted. Sorting
	// them would destroy the invariant a rebuild depends on.
	edges, _ := body["edges"].([]any)
	var flat []string
	for _, e := range edges {
		m := e.(map[string]any)
		flat = append(flat, m["parent"].(string)+">"+m["child"].(string))
	}
	if fmt.Sprint(flat) != fmt.Sprint([]string{"alice>bob", "alice>carol", "bob>dave"}) {
		t.Errorf("edges = %v", flat)
	}
	backups, _ := body["backups"].([]any)
	if len(backups) != 1 {
		t.Fatalf("backups = %#v, want one entry", body["backups"])
	}
	b := backups[0].(map[string]any)
	if b["node"] != "dave" || b["parent"] != "carol" {
		t.Errorf("backups[0] = %v", b)
	}
}

// TestSnapshotDivergence pins §9.4b: converged/diverged expose the gap between what
// the coordinator INTENDED and what the peers actually did. That gap is the most
// useful diagnostic in the system, and the UI can only show it if the schema does.
func TestSnapshotDivergence(t *testing.T) {
	snap := sampleSnapshot()
	// dave's heartbeat still says bob, carol's still says alice — but make carol claim
	// a parent the tree never assigned, and dave claim none at all.
	for i := range snap.Members {
		switch snap.Members[i].Name {
		case "carol":
			snap.Members[i].Parent = "bob"
		case "dave":
			snap.Members[i].Parent = ""
		}
	}
	subnet := &fakeSubnet{}
	subnet.set(snap)
	_, ts := newTestServer(t, Config{
		Meets:  &fakeMeets{live: []arbiter.Meet{sampleMeet()}},
		Subnet: subnet,
	})
	_, body := doJSON(t, ts, http.MethodGet, "/api/meets/standup", "", nil)
	if body["converged"] != false {
		t.Errorf("converged = %v, want false", body["converged"])
	}
	var got []string
	dv, ok := body["diverged"].([]any)
	if !ok {
		t.Fatalf("diverged = %#v, want an array of names", body["diverged"])
	}
	for _, d := range dv {
		got = append(got, d.(string))
	}
	// Ascending, so a replay of the same history renders identically.
	if fmt.Sprint(got) != fmt.Sprint([]string{"carol", "dave"}) {
		t.Errorf("diverged = %v, want [carol dave]", got)
	}
}

// TestSnapshotWithoutCoordinator pins the -coordinate-off posture: the endpoint still
// answers, with an empty INTENDED tree, rather than 404ing or panicking on a nil seam.
func TestSnapshotWithoutCoordinator(t *testing.T) {
	_, ts := newTestServer(t, Config{Meets: &fakeMeets{live: []arbiter.Meet{sampleMeet()}}})
	res, body := doJSON(t, ts, http.MethodGet, "/api/meets/standup", "", nil)
	if res.StatusCode != 200 {
		t.Fatalf("status = %d (body %v)", res.StatusCode, body)
	}
	for _, k := range []string{"nodes", "edges", "backups", "diverged"} {
		if v, ok := body[k].([]any); !ok || v == nil {
			t.Errorf("%s = %#v, want []", k, body[k])
		}
	}
	if body["root"] != "" || body["rev"] != float64(0) {
		t.Errorf("root/rev = %v/%v, want \"\"/0", body["root"], body["rev"])
	}
	// The arbiter still knows the epoch, and it MINTS it, so it stays authoritative
	// even with no coordinator snapshot (§9.4b).
	if body["epoch"] != float64(3) {
		t.Errorf("epoch = %v, want 3 from the arbiter", body["epoch"])
	}
}

// TestNodeValueSanitising pins the frozen units/ranges and the defensive clamps: a
// non-finite sensor value must never reach json.Marshal (which fails outright on NaN,
// taking the whole response with it), and an unset enum must render as a legal one.
func TestNodeValueSanitising(t *testing.T) {
	snap := coordinator.RoomSnapshot{
		RoomID: "m", Epoch: 1, Rev: 1, At: testAt,
		Members: []coordinator.MemberSnapshot{
			{ID: "p1", Name: "nan", Report: metrics.Report{
				RTTServerMs: math.NaN(), LossPct: math.Inf(1), CPUPct: math.NaN(),
			}},
			{ID: "p2", Name: "turnpeer", Health: coordinator.HealthHealthy,
				Report: metrics.Report{NAT: overlay.NATRelayed, CPUPct: 1, Coordinatable: true}},
			{ID: "p3", Name: "gonepeer", Health: coordinator.HealthGone,
				Report: metrics.Report{NAT: overlay.NATDirect, CPUPct: 1, Coordinatable: true}},
			{ID: "p4", Name: "rounding", Health: coordinator.HealthHealthy,
				Report: metrics.Report{NAT: overlay.NATDirect, CPUPct: 31.26666, LossPct: 0.24999, Coordinatable: true}},
		},
	}
	subnet := &fakeSubnet{}
	subnet.set(snap)
	_, ts := newTestServer(t, Config{
		Meets:  &fakeMeets{live: []arbiter.Meet{{ID: "m"}}},
		Subnet: subnet,
	})
	res, body := doJSON(t, ts, http.MethodGet, "/api/meets/m", "", nil)
	if res.StatusCode != 200 {
		t.Fatalf("status = %d — a NaN in a Report must not take the response down", res.StatusCode)
	}
	nodes, ok := body["nodes"].([]any)
	if !ok {
		t.Fatalf("nodes = %#v, want an array", body["nodes"])
	}
	byName := map[string]map[string]any{}
	for _, n := range nodes {
		m := n.(map[string]any)
		byName[m["name"].(string)] = m
	}
	for _, k := range []string{"rtt_server_ms", "loss_pct", "cpu_pct", "fitness_lower_bound"} {
		if v := byName["nan"][k]; v != float64(0) {
			t.Errorf("nan.%s = %#v, want 0 (non-finite clamped)", k, v)
		}
	}
	// health has a frozen three-value enum; a zero Health must render as one of them.
	if h := byName["nan"]["health"]; h != "healthy" && h != "degraded" && h != "gone" {
		t.Errorf("nan.health = %#v, not in the frozen enum", h)
	}
	if n := byName["nan"]["nat"]; n != "direct" && n != "turn" {
		t.Errorf("nan.nat = %#v, not in the frozen enum", n)
	}
	// arbiter.Score's hard disqualifiers must survive the trip to the wire: a
	// TURN-bound or non-live peer is INELIGIBLE to coordinate, not merely worse.
	if f := byName["turnpeer"]["fitness_lower_bound"]; f != float64(0) {
		t.Errorf("turnpeer.fitness_lower_bound = %#v, want 0 (NATRelayed is a hard disqualifier)", f)
	}
	if f := byName["gonepeer"]["fitness_lower_bound"]; f != float64(0) {
		t.Errorf("gonepeer.fitness_lower_bound = %#v, want 0 (health gone ⇒ not Live)", f)
	}
	if v := byName["rounding"]["cpu_pct"]; v != 31.3 {
		t.Errorf("rounding.cpu_pct = %#v, want 31.3 (1 decimal, §9.4a)", v)
	}
	if v := byName["rounding"]["loss_pct"]; v != 0.2 {
		t.Errorf("rounding.loss_pct = %#v, want 0.2 (1 decimal, §9.4a)", v)
	}
	// depth is -1 ("unattached") for a node with no tree, never 0.
	if d := byName["nan"]["depth"]; d != float64(-1) {
		t.Errorf("nan.depth = %#v, want -1 for a node not in the tree", d)
	}
}

// TestSnapshotBodyIsStableJSON is the replayability guard for the detail body: 20
// identical requests must produce byte-identical JSON. A map anywhere in the wire
// types would fail this because Go randomises map iteration.
func TestSnapshotBodyIsStableJSON(t *testing.T) {
	subnet := &fakeSubnet{}
	subnet.set(sampleSnapshot())
	_, ts := newTestServer(t, Config{
		Meets:  &fakeMeets{live: []arbiter.Meet{sampleMeet()}},
		Subnet: subnet,
	})
	var first string
	for i := 0; i < 20; i++ {
		_, body := doJSON(t, ts, http.MethodGet, "/api/meets/standup", "", nil)
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = string(raw)
		} else if string(raw) != first {
			t.Fatalf("snapshot body differs between identical calls:\n %s\n %s", first, raw)
		}
	}
}

// TestBigIntSafeFields pins that every uint64 the frontend parses as a BigInt is
// emitted as a bare JSON NUMBER of digits — web/js/format.js quotes those digits with
// a regex BEFORE JSON.parse runs, so emitting them as strings (or in exponent form)
// would silently break the BigInt path on values past 2^53.
func TestBigIntSafeFields(t *testing.T) {
	const huge = uint64(1) << 60 // 1152921504606846976 — far past Number.MAX_SAFE_INTEGER
	snap := sampleSnapshot()
	snap.Epoch, snap.Rev = huge, huge+1
	snap.Members[0].LastBeatSeq = huge + 2
	subnet := &fakeSubnet{}
	subnet.set(snap)
	_, ts := newTestServer(t, Config{
		Meets:  &fakeMeets{live: []arbiter.Meet{sampleMeet()}},
		Subnet: subnet,
	})
	res, err := http.Get(ts.URL + "/api/meets/standup")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	buf := make([]byte, 1<<16)
	n, _ := res.Body.Read(buf)
	raw := string(buf[:n])
	for _, want := range []string{
		`"epoch":1152921504606846976`,
		`"rev":1152921504606846977`,
		`"last_beat_seq":1152921504606846978`,
	} {
		if !contains(raw, want) {
			t.Errorf("body does not contain %s — web/js/format.js's BIGINT_FIELD_RE only "+
				"matches bare digits, so this field would lose precision.\nbody: %s", want, raw)
		}
	}
}

func contains(hay, needle string) bool {
	for i := 0; i+len(needle) <= len(hay); i++ {
		if hay[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
