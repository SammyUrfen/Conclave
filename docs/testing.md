docs/testing.md

# Testing

> Living doc — kept in sync with the code. Reflects the tree at the end of Phase 4 (coordinator computes the tree + the `simnet` deterministic harness).

How conclave is tested, why it's tested that way, and where the coverage honestly stops today. This is also a Go-testing primer: the two tests that exist are the reference implementations of the idiom you'll copy for the rest of the project.

---

## Philosophy

**Prefer pure functions; push side effects to the edges.** A pure function's output depends only on its inputs — no I/O, no globals, no clock, no network. That makes it *trivially* testable: feed it a value, assert on the return. The two things worth testing in Phase 0 are pure by design, and that is not an accident:

| Function | Package | Why it's pure |
|---|---|---|
| `ParseLevel(s string) (slog.Level, error)` | `internal/logging` | `string` in, `(level, error)` out. No reads, no writes. |
| `healthURLFor(server string) (string, error)` | `cmd/peer` | URL normalization only. The actual HTTP request lives elsewhere. |

Everything that *can't* be pure — opening a socket, reading `os.Stdin`, exiting the process — is deliberately shoved to the program's edge. `main()` is a three-line `run(os.Args[1:]) error` hop precisely so that the logic under it never has to touch `os.Exit`. `logging.New` takes an `io.Writer` instead of hard-coding `os.Stdout`, so a test can point it at a `bytes.Buffer`. The logger is constructed once and injected downward; nothing reads a global. This is the same discipline as separating orchestration from mechanism — the mechanism stays testable, the orchestration stays thin.

This matters more, not less, as the project grows. The overlay `BuildTree` (Phase 4, done) and the Phase 6 election logic are the crown jewels, and they are the *hardest* things to test if they're entangled with pion, cameras, and real sockets. Phase 4 made good on the plan: `overlay` is pure (graph + policy in, tree + decisions out), so `BuildTree`/`Validate` are unit-tested in microseconds and the `simnet` harness drives them under hundreds of seeded fleets and long churn scenarios — **with zero media stack**. The same purity is the foundation the Phase 5–6 logic will be built and tested on.

**Priority order** (the owner's, applied to tests): Correctness > Reliability > UX > Maintainability > Performance. A test exists to catch a *correctness* regression first. We do not write tests for coverage theater, and we do not claim "done" without running them — `make check` is the gate.

---

## Running the tests

| Command | What it does |
|---|---|
| `go test ./...` | Run every test in every package. Fast inner loop. |
| `make test` | `go test -race ./...` — the same, **plus the race detector**. |
| `go test -run TestParseLevel ./internal/logging` | Run one test (or a regexp of tests) in one package. |
| `go test -run 'TestParseLevel/mixed' ./internal/logging` | Run one **subtest**. `/` separates test from subtest; spaces in a subtest name become `_`. |
| `go test -v ./...` | Verbose: print every test and subtest name as it runs (`--- PASS: TestParseLevel/warning_alias`). |
| `make cover` | `go test -coverprofile=coverage.out` then `go tool cover -func` — a per-function coverage report in the terminal. |
| `make check` | `fmt` + `vet` + `test`. The pre-commit gate. |

### Why `-race` matters, and why `make test` is the real command

conclave is a concurrency project. Even Phase 0 already has a goroutine running `ListenAndServe` and handing its error back over a buffered channel, and from Phase 1 on there will be goroutines per peer connection, per websocket, and per metrics stream. The race detector instruments memory accesses at runtime and reports any two goroutines touching the same location without synchronization, where at least one is a write. It finds the bugs that pass every time on your machine and then corrupt state in the demo.

Two things to know:

- **It needs a C compiler.** `-race` links a C runtime (TSan). `gcc` is present on this box, so `make test` works. On a machine without a C toolchain, `-race` fails to build; plain `go test ./...` still runs.
- **It only catches races that actually execute.** It is a *dynamic* detector, not a proof. A race on a code path no test exercises is invisible. That is one more reason the simnet harness (below) — which can drive thousands of scheduled interleavings deterministically — is on the roadmap: it turns "a race that happens 1-in-10000 in the wild" into "a race the harness reproduces on every run with the same seed."

Default to `make test`. Reach for bare `go test` only for a quick single-package loop.

### Coverage

`make cover` prints a function-by-function table. It measures *which lines ran*, which is a floor on confidence, not a ceiling — 100% line coverage of `ParseLevel` still wouldn't prove the error *message* is right. For a browsable HTML view, the stdlib tool does it directly (and `make clean` already knows to delete the output):

```
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

---

## The idiom: table-driven tests + `t.Run` subtests

Both existing tests are written the same way on purpose. This is *the* Go testing pattern; learn it once here and reuse it everywhere.

A table-driven test is: **a slice of anonymous structs, one struct per case, looped over with `t.Run` turning each row into its own named subtest.** Worked example, `TestParseLevel` in `internal/logging/logging_test.go`:

```go
func TestParseLevel(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    slog.Level
		wantErr bool
	}{
		{name: "debug", in: "debug", want: slog.LevelDebug},
		{name: "warning alias", in: "warning", want: slog.LevelWarn},
		{name: "mixed case", in: "InFo", want: slog.LevelInfo},
		{name: "surrounding whitespace", in: "  debug  ", want: slog.LevelDebug},
		{name: "empty is an error", in: "", wantErr: true},
		{name: "garbage is an error", in: "loud", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseLevel(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseLevel(%q): expected an error, got nil (level=%v)", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseLevel(%q): unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
```

`TestHealthURLFor` in `cmd/peer/main_test.go` is structurally identical — the same `{name, in, want, wantErr}` struct, the same loop, the same `t.Run` — over the URL-normalization cases (`bare host:port gets http`, `trailing slash trimmed`, `scheme only is error`, …). That sameness is the point: once you recognize the shape, every new test is a shape you already know.

### Why this shape

| Element | What it buys you |
|---|---|
| **Slice of anonymous structs** | The test *is* a data table. Adding a case — a new alias, a new malformed URL — is **one line**, not a copy-pasted function. |
| **`name` field** | A human label per row. `-v` prints it; failures name it. |
| **`t.Run(tt.name, …)`** | Each row is an isolated **subtest** (`TestParseLevel/mixed_case`). One row failing doesn't stop the others, and the failure line tells you *exactly which case* broke. You can rerun just that one with `-run TestParseLevel/mixed_case`. |
| **`want` / `wantErr`** | Expected value and expected-error-ness live *next to* the input, so a case reads as one self-contained fact. |

### `t.Fatalf` vs `t.Errorf` — a real distinction, used deliberately

Both mark the test failed. The difference is what happens next:

- **`t.Fatalf`** logs and **stops this subtest immediately** (it calls `runtime.Goexit`). Use it when continuing is pointless or unsafe — if `ParseLevel` returned an error you didn't expect, `got` is meaningless, so there's no reason to go on and compare it. Both tests use `Fatalf` for the two error-handling branches (expected an error but got none; got an error but wanted none).
- **`t.Errorf`** logs, **marks failed, and keeps going.** Use it for the final value comparison, so a single run can report *all* the value mismatches at once instead of dying on the first. Both tests use `Errorf` for the closing `got != want` check.

Rule of thumb: `Fatalf` when the *rest of the subtest can't run correctly*, `Errorf` when you just want to record a wrong value and move on. `t.Fatal*` must be called from the test's own goroutine, never from a spawned one — relevant once tests start goroutines.

---

## What is actually covered today (honest)

Beyond the pure unit tests, Phase 1 adds two **integration** tests. They use real loopback sockets — and, for media, real pion `PeerConnection`s — but no external network, no server process, and no cameras, and both run under `-race`. They're deterministic and fast (tens of milliseconds to connect).

| Unit under test | Package | Automated test? | Notes |
|---|---|---|---|
| `ParseLevel` | `internal/logging` | ✅ `TestParseLevel` | Pure. Aliases, case, whitespace, empty, garbage. |
| `healthURLFor` | `cmd/peer` | ✅ `TestHealthURLFor` | Pure. Scheme prepend, trailing slash, hostless/`:port` rejection. |
| `wsURLFor` | `internal/signaling` | ✅ `TestWSURLFor` | Pure. ws/wss normalization, room query, bad-scheme/hostless rejection. |
| Signaling hub (join/roster/relay/spoof/leave) | `internal/signaling` | ✅ `TestHubRelaysBetweenPeers`, `TestHubRelayToUnknownPeerErrors` | **Integration:** two real ws clients over `httptest`. Proves server-stamped identity and routing. |
| Media session (connect + forward a track) | `internal/media` | ✅ `TestSessionConnectsAndForwardsTrack` | **Integration:** two pion sessions, in-memory signaling, real loopback ICE+DTLS, synthetic VP8. Proves single-offerer negotiation + the media path. |
| Full mesh (N-peer, bidirectional media, clean shutdown) | `internal/media` | ✅ `TestMeshThreePeersFullyConnect` | **Integration:** 3 peers over the *real* signaling Hub (`httptest`), all sending. Asserts full-mesh formation (each peer `connected` to both others), **media flowing both ways** (each receives a track from every other), and that a ctx-cancel joins every Router goroutine (WaitGroup returns — a leak would hang the test). Under `-race`. |
| Topology queries + loader | `internal/overlay` | ✅ `TestTopology`, `TestLoadTopology` | Pure. Parent/Children/Neighbors/IsRelay/Nodes + the offerer rule, and fail-loud file validation (empty/self-parent/two-parents/bad-json). |
| `BuildTree` + `Validate` + `PickRoot` | `internal/overlay` | ✅ `TestBuildTree`, `TestBuildTreeDeterministic`, `TestValidateCatches` | Pure, table-driven: star, capacity-forced depth-2, TURN-forced-leaf, RTT tiebreak, over-constrained error, unknown/TURN root. Determinism across 50 runs (guards against map-order dependence). `Validate` is exercised as an independent oracle *and* proven to reject broken trees. |
| Relay PLI SSRC translation | `internal/media` | ✅ `TestForwarderTranslatesPLISSRC` | **Deterministic unit** on the #1 SFU footgun: an upstream keyframe request carries the *source* SSRC (not a downstream one), `SenderSSRC=0`, the throttle drops an immediate second request, and a source with no media (ssrc 0) emits none. No timing. |
| Tree relay (forward through a peer) | `internal/media` | ✅ `TestRelayForwardsThroughTree` | **Integration:** 3 peers over the real Hub with a hardcoded tree (relay → leaf-b, leaf-d). Proves a leaf receives another leaf's media **forwarded by the relay** — the proof is *topological*: leaf-d holds a track while having no session to leaf-b, so the bytes transited the relay. Also asserts an upstream PLI fired and that shutdown joins every forwarder goroutine. Under `-race`. |
| Coordinator (fan-in → compute → push) | `internal/coordinator` | ✅ `TestCoordinatorComputesTree`, `…AntiThrash`, `…LeaveRecomputes`, `…SkipsUnnamed` | **Async, `-race`:** drives the single-goroutine event loop through a fake `Sender`. Proves it elects the highest-upload root, pushes each leaf its tree, does **not** re-root on a subsequent metric (anti-thrash) but *does* use the stored value at the next join, recomputes on leave, and skips a nameless peer. |
| Metrics reporter | `internal/metrics` | ✅ `TestReporterEmitsImmediatelyThenTicks`, `…SurvivesSendError`, `…DefaultsInterval` | Emits once immediately then ticks; a send error is logged, never fatal (best-effort telemetry); a zero interval falls back to the default. |
| `overlay.BuildTree` under churn (the crown jewel) | `internal/simnet` | ✅ `TestBuildTreeProperty`, `TestChurnKeepsInvariants`, `TestLatencyAttachment`, `TestScenarioDeterministic` | **Deterministic simulation:** 300 seeded random fleets each either error honestly or pass `Validate`; 200 churn steps (join/leave) never break an invariant; injected latency steers attachment; a seed replays the exact same tree. No pion, no clock — the FoundationDB/TigerBeetle idea in miniature. |
| `logging.New` | `internal/logging` | ❌ | Low risk, currently untested. |
| `healthzHandler` / `newMux` / `run()` | `cmd/*` | ❌ (manual only) | Verified by hand + the live `-call` demo (both peers `connected`; `ffprobe` confirms decodable VP8 output). |
| `media.Router` demux + lifecycle | `internal/media` | ✅ `TestMeshThreePeersFullyConnect` (mesh) | The Router's per-peer demux and `joined`/`peer-joined` handling are now exercised by the 3-peer mesh test; `peer-left` churn under load still awaits a `simnet`-style Phase-4 test. |

**What this coverage does *not* prove:** that the healthz JSON body is exactly `{"status":"ok","service":"conclave-server"}` or that `POST /healthz` really 405s (still manual — the `httptest` handler test below is the next add), that graceful shutdown drains in-flight requests, or that the `Router`'s lifecycle demux is correct under *churn* (rapid `peer-left`/rejoin storms — the mesh test covers steady-state join + teardown, not chaos). The *hard* core — single-offerer negotiation, the media path, N-peer full-mesh formation, and now **tree forwarding (a leaf's media relayed through a third peer) with SSRC-translated upstream PLI** — **is** pinned by automated `-race` tests. What the relay tests do *not* prove: **keyframe *response*** — our file/synthetic sources have no live encoder, so a forwarded PLI can't actually produce a fresh I-frame; the PLI *plumbing* (right SSRC, throttled, reaches upstream) is proven, but end-to-end "recover on demand" needs a real browser sender (Phase 7). Nor do they cover mid-call child churn (Phase 5) or deep (≥3) trees.

### On integration tests with real sockets (why they're worth it here)

The two new tests deliberately are *not* pure. The signaling hub's whole job is concurrency (goroutines, channels, a mutex-guarded map) and the media session's is a callback-driven state machine over real ICE/DTLS — neither has a meaningful "pure core" to unit-test in isolation, and mocking pion would test the mock, not the code. So they stand up the real thing on loopback and assert observable outcomes (a relayed frame arrives with the right `From`; the receiver's `OnTrack` fires and packets flow), all under `-race` to catch the data races that callback-heavy code invites. The trick that keeps them fast and hermetic is substituting only the *transport*: `httptest` for the server, an in-memory `Transport` for signaling — never the media stack itself.

---

## Forward-looking plan

### Next add: a table-driven `httptest` test for `/healthz`

The lowest-hanging, highest-value gap. `net/http/httptest` gives an in-memory server and recorder with no real socket, so the handler can be exercised directly. `newMux(logger)` already returns an `http.Handler` and `healthzHandler` already closes over an injected logger — the code was written to be testable this way. The shape is the same table-driven idiom, with cases like `{name, method, path, wantStatus, wantBody}`:

- `GET /healthz` → `200`, body decodes to `healthResponse{Status:"ok", Service:"conclave-server"}`.
- `POST /healthz` → `405` (proves the Go 1.22+ method-aware routing, without trusting it by eye).
- an unknown path → `404`.

Drive it with `httptest.NewRecorder()` + `mux.ServeHTTP(rec, req)` (or `httptest.NewServer` for a full round-trip), assert on `rec.Code` and the decoded body. This pins the wire contract that Phase 1 grows.

### `internal/simnet` — deterministic simulation (built in Phase 4)

The centerpiece of the testing strategy, and the reason the hard logic is kept pure. `internal/simnet` is an **in-memory simulated network**: a fleet of nodes with injectable upload budgets, NAT classes, and pairwise latencies, plus scriptable churn (joins, leaves) via `Add`/`Remove`/`RemoveRandom`. Phase 4 drives the overlay's `BuildTree` against it — hundreds of seeded random fleets and a 200-step churn scenario, each asserted against the independent `overlay.Validate` oracle — **with no pion, no UDP, no cameras.** As the coordinator's control loop, the failover/backup-parent logic, and eventually the Phase 6 election + migration grow, they will run against this same harness (a seeded clock and scripted degradation are the next additions).

This is the [FoundationDB](https://apple.github.io/foundationdb/testing.html) / [TigerBeetle](https://tigerbeetle.com/) **deterministic-simulation** idea, in miniature. The pitch: replace every source of nondeterminism (wall clock, real network, real scheduler) with a seeded, controllable one, so an entire distributed scenario is a *pure function of its seed*. Then:

- A failing run is **reproducible** — same seed, same interleaving, every time. No more "it only fails in CI."
- You can fast-forward simulated time, so a "30-second sustained-degradation triggers re-optimization" scenario runs in microseconds.
- You can enumerate nasty orderings on purpose — a stale coordinator acting after a new one is elected — and assert the epoch/term fencing actually rejects the stale actor. That is exactly the class of bug that is near-impossible to hit reliably against real machines and near-trivial to hit against a scheduler you control.

We are **not** building full FoundationDB-grade simulation; the honest framing is "the same idea, scoped to a learning project." But the architectural commitment it demands — pure decision logic, side effects at the edges, injectable clock and transport — is being made *now*, in Phase 0, so it's cheap later instead of a rewrite.

### Rough coverage roadmap

| Phase | Testable surface | Approach |
|---|---|---|
| 0 (done) | `ParseLevel`, `healthURLFor` | Table-driven unit tests. |
| 1 (done) | `wsURLFor`; signaling hub; media session (connect + track) | Table-driven unit + `httptest`/loopback integration under `-race`. |
| 1 (gap) | `/healthz` handler body/405; `Router` lifecycle demux | `httptest` handler test; `Router` unit test with a fake client. |
| 3–4 (done) | `BuildTree` (degree-bounded, depth-limited, min-latency) | Pure-function table tests + `simnet` property checks (connected, no cycle, depth/degree bounds, TURN-leaf) against the `Validate` oracle. |
| 4 (done) | metrics fan-in, overlay under churn | `-race` coordinator test (fake `Sender`) + `simnet` deterministic churn scenarios. |
| 5–6 | failover, backup parents, **election + migration** | `simnet` with adversarial interleavings; assert epoch/term fencing. |

---

## Non-goals (for now)

- **No test of `main()` itself.** It's the three-line `os.Exit` shim; the logic lives in `run`, which is where tests point.
- ~~No integration test that boots a real pion peer.~~ **Done in Phase 1:** `TestSessionConnectsAndForwardsTrack` stands up two real pion `PeerConnection`s over loopback and asserts a track is forwarded. Full *media-fidelity* checks (decodable output, keyframe timing) remain manual + the live demo; the automated test proves the transport and negotiation, not codec quality.
- **No benchmarks / performance assertions yet.** Performance is last in the priority order. When latency-per-hop and upload-ceiling claims get made (Phases 3–4), they'll be measured and quoted as numbers, not asserted in unit tests.
- **Coverage percentage is not a target.** It's a floor-of-confidence signal, read from `make cover`, not a number to game.
