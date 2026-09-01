# Conclave — Tech Stack

> **Living doc.** Tracks every library and standard-library package Conclave depends on, why it was chosen over the alternatives, and where it sits on the phase roadmap. Kept in sync with the code; if the code and this table disagree, the code wins and this doc is the bug.
>
> **Module:** `github.com/SammyUrfen/conclave` · **Go:** 1.26.4 · **Last reviewed:** post–**Phase 6** (churn/handover, election + migration, and the dashboard). **Phases 5 and 6 added exactly one direct dependency** — `pion/rtp`, needed to rewrite RTP sequence/timestamp continuity across a re-parent. Everything else those phases introduced (the fence, the arbiter, hysteresis, the virtual clock, the whole `/api` surface, and the frontend) is stdlib or hand-written.

---

## Selection principles

The dependency list is short on purpose. Three rules decide what gets pulled in:

1. **Stdlib first.** Go's standard library covers the whole control plane (HTTP, JSON, signals, timeouts, structured logging) with no third-party code. We lean on it until it genuinely can't do the job.
2. **From-scratch where the mechanism *is* the point; a library where it isn't.** The learning targets here are the *overlay topology, election/migration, and metrics-driven graph rebuilds* — not the SRTP/ICE/DTLS state machines underneath WebRTC. So the graph builder, election arbiter, and hysteresis logic are hand-written, while WebRTC itself comes from `pion`. This split is deliberate and is called out per-row below.
3. **Control plane and data plane stay separate, and so do their deps.** Signaling/bootstrap is HTTP + WebSocket to the central server. Media is WebRTC (SRTP/UDP) peer-to-peer. No dependency is allowed to blur that line.

---

## Standard library — live now

Everything in this table is imported and exercised by Phase 0 code (`cmd/server`, `cmd/peer`, `internal/logging`), verified green under `go test -race ./...`.

| Component | Role | Status | Why this one |
|---|---|---|---|
| `net/http` | Server: `http.Server` + `http.NewServeMux` for `GET /healthz`. Peer: `http.NewRequestWithContext` + `http.DefaultClient` to probe the server. | Live now | Stdlib HTTP is production-grade; no framework needed for a health endpoint and a JSON probe. Method-aware routing (`"GET /healthz"`, Go 1.22+) gives us automatic `405` on `POST` with zero handler code — a framework would add surface area for nothing. |
| `log/slog` | Structured logging. `internal/logging` wraps handler construction (`New`), level parsing (`ParseLevel`), and a `Format` type (`json`/`text`). Logger built once in `main`, injected downward — **no globals**. | Live now | Structured logging is now stdlib (Go 1.21+), so no `zap`/`logrus` dependency. JSON logs are machine-parseable for later metrics/aggregation across peers; text logs are readable during local dev. The `-log-format` flag chooses per-run. Trade-off: `slog` is slightly slower than `zap`, but log volume here is trivial and one fewer dep wins. |
| `encoding/json` | `/healthz` marshalling; peer `Unmarshal`; **and now the signaling wire** — `signaling.Message` struct tags, with SDP/ICE carried as opaque `json.RawMessage` the server relays without decoding. | Live now | Struct-tag (de)serialization with no codegen. `json.RawMessage` is the idiom for "route this blob without understanding it" — it keeps the control-plane server pion-free (the plane split, enforced at the type level). |
| `context` | Request timeouts (peer `context.WithTimeout`) and shutdown propagation (server derives `ctx` from signal handling). | Live now | The idiomatic Go carrier for deadlines and cancellation. Frames a problem the owner already knows — cooperative cancellation — as new syntax. Becomes load-bearing once goroutines fan out (metrics, per-peer connections). |
| `os/signal` | Graceful shutdown: `signal.NotifyContext(SIGINT, SIGTERM)` yields the ctx that triggers `srv.Shutdown`. | Live now | Clean lifecycle without a supervisor. `NotifyContext` ties signal handling directly to `context`, so the same cancellation path drains in-flight requests (10s timeout) and logs `server stopped cleanly`. |
| `net/url` | Peer `healthURLFor`: parse/normalize a `-server` value (bare `host:port` → `http://…`, reject hostless/non-http(s), rebuild `scheme://host/healthz`). | Live now | Correct URL handling is fiddly; the stdlib parser handles the edge cases (trailing slashes, missing scheme) that hand-rolled string surgery gets wrong. |
| `flag` | Per-binary `flag.FlagSet` with `flag.ContinueOnError` (not the global `flag.CommandLine`). Server: `-addr`, `-log-level`, `-log-format`. Peer: `-server`, `-log-level`, `-log-format`, `-timeout`. | Live now | A dedicated `FlagSet` keeps flag state out of package globals, so the `run(args) error` helper is testable and re-runnable. `ContinueOnError` returns a parse error instead of calling `os.Exit`, which the thin `main → run() error` pattern depends on. |
| `testing` | Table-driven tests (`cmd/peer/main_test.go` `TestHealthURLFor`; `internal/logging` level/format tests). Run via `go test -race ./...`. | Live now | Stdlib testing + `-race` is the whole verification story — no assertion library. Table-driven tests match the owner's preference for deterministic, enumerated cases. |

---

## Standard library — concurrency & timing

**Live now (Phases 1–4):** the signaling hub put the core concurrency model into service — `sync.Mutex` guarding the room registry, and per connection a reader goroutine + a writer goroutine draining a **buffered channel**, coordinated with `select` and torn down together by one `context` cancellation. Phase 2 added `sync/atomic` (the lock-free upload meter) and `sync.WaitGroup` (the Router's leak-free goroutine joins); Phase 4 added `time.Ticker` (the metrics reporter) and — the notable call — resolved the coordinator's shared-state trade-off **toward a channel-owned single goroutine, not an `RWMutex`**.

The `RWMutex` row below is therefore the road *not* taken for the coordinator (kept as the alternative, still apt for genuinely read-mostly state); `time.Timer` remains ahead for Phase 5–6. Framed for a systems engineer: this is **new Go syntax for concurrency problems already solved carefully elsewhere** (ReentrantLock + `@Version`, wait-for-graph deadlock detection), not a new discipline.

| Component | Role | Status | Why this one |
|---|---|---|---|
| `time` (`Ticker`) | The metrics `Reporter` emits telemetry on a fixed cadence: `select` over the ticker and `ctx.Done()`, first report hoisted out of the loop so there's no initial silence. | **Live (Phase 4)** | `Ticker` is the natural clock for a heartbeat. It feeds the anti-thrash rule: metrics arrive continuously, but the tree only recomputes on a threshold event (join/leave/first report), never on every tick. |
| `sync/atomic` + `sync.WaitGroup` | Lock-free upload counter (`meter`); leak-free goroutine joins (Router pumps + forwarder drains; the peer's reporter goroutine). | **Live (Phase 2–4)** | `atomic` for a monotonic counter on the hot path (cheaper than a mutex, no invariant spanning fields); `WaitGroup` for "spawn N, join all N" barriers, explicit over a hand-rolled counter. |
| `sync` (`RWMutex`) | *Considered* for the coordinator's overlay/metrics state (many readers, rare writers). | Not used (Phase 4 chose channels) | The coordinator instead **owns all state in one goroutine and funnels every input over a channel** ("share memory by communicating") — no locks to reason about, and every recompute sees a consistent snapshot. `RWMutex` stays the right tool for genuinely read-mostly *shared* state if a future surface needs it; the call is made per-structure, not globally. |
| `time` (`Timer`) | One-shot deadlines: the degradation dwell, gone/degraded thresholds, the rebuild window, the join settle, the recompute cooldown, re-parent and negotiation timeouts. | **Live (Phase 5–6)** — but **never called directly in the control plane.** | Every one of these goes through `clock.Clock`, and `make check-determinism` fails the build if a control-plane package touches `time.Now`/`NewTimer`/`NewTicker`/`After` at all. Two shipped refinements are worth naming: the coordinator **multiplexes every deadline in the process onto ONE timer** — six classes ordered by `(time, class, room, peer)` — because a `select` cannot watch a dynamic set of channels, and because one wake channel is what makes the `Sync` barrier a *complete* drain. And the **arbiter arms no timer at all**: every action it can take requires a live peer, and a live peer is by definition heartbeating. Its test harness makes that structural — its fake clock's `NewTimer` **panics**, so re-introducing a timer is a failing test rather than a review note. |
| `time` (types only) | `time.Time`, `time.Duration`, durations as flag values. | **Live** | The types are fine everywhere; it is the *clock reads and waits* that are gated. |

---

## Third-party — live now

**Five direct `go.mod` requires**, in two ecosystems: `coder/websocket` for signaling (control
plane) and pion — `webrtc/v4`, `rtp`, `rtcp`, `interceptor` — for WebRTC media (data plane). All
are exercised under `go test -race ./...`.

| Component | Role | Status | Why this one |
|---|---|---|---|
| `github.com/coder/websocket` v1.8.15 | The **signaling** transport. `internal/signaling` accepts peers at `GET /ws?room=<id>` (`websocket.Accept`), reads/writes JSON frames (`wsjson.Read`/`Write`), and relays `signaling.Message` between peers in a room. Control plane only — **never media**. | **Live (Phase 1)** | Chosen over the classic `github.com/gorilla/websocket` because it is **`context`-native**: `Read`/`Write` take a `context.Context`, so per-frame timeouts and connection-lifecycle cancellation compose directly with the `context` plumbing already in place (peer probe, server shutdown) instead of `gorilla`'s separate `SetReadDeadline` model. Smaller, modern API with first-class `net/http` integration. `gorilla/websocket` remains the more battle-tested fallback if this hits a wall — a preference for API fit, not a claim `gorilla` is worse. |
| `github.com/pion/webrtc/v4` v4.2.16 (+ `pion/interceptor`, `pkg/media` ivf reader/writer) | **The centerpiece.** Pure-Go WebRTC: `internal/media` builds `PeerConnection`s with VP8 pinned in the `MediaEngine`, runs single-offerer negotiation over the signaling `Transport` (pion can't roll back a local offer, so glare is avoided rather than reconciled), trickles ICE, sends a `TrackLocalStaticSample`, and records a remote `TrackRemote` to IVF. `RegisterDefaultInterceptors` adds NACK/RTCP/TWCC. Phase 2 adds the full mesh + a `sync/atomic` upload meter; Phase 3 adds the tree relay — `TrackRemote.ReadRTP` → `TrackLocalStaticRTP.WriteRTP` fan-out with no re-encode, and SSRC-translated upstream PLI via `pion/rtcp`. | **Live (Phase 3)** | **Deliberately a library, not from-scratch.** The WebRTC stack (ICE, DTLS-SRTP, congestion control) is thousands of lines of protocol state machine and is *not* the learning target — the overlay, election, and metrics logic on top of it are. `pion` is chosen over CGo bindings to libwebrtc because it's **pure Go**: trivial cross-compile, no C build chain, readable source when a behavior needs understanding all the way down. Trade-off named: pure-Go WebRTC can trail Chromium's libwebrtc on bleeding-edge codec/congestion features — acceptable for a learning/portfolio SFU. |

---

### `pion/rtp` — the Phase 5 addition, and why it was unavoidable

| Component | Role | Status | Why this one |
|---|---|---|---|
| `github.com/pion/rtp` v1.10.2 | Direct access to RTP header fields so `internal/media/rewrite.go` can hold a per-leg `(sequence, timestamp)` **offset** and rewrite each forwarded packet across an upstream switch. | **Live (Phase 5)** | `TrackLocalStaticRTP.WriteRTP` rewrites SSRC and payload type per binding but passes **sequence number and timestamp through untouched**. That is exactly right while one source feeds one leg forever, and exactly wrong the instant a *second, unrelated* upstream feeds the same leg after a re-parent: the child sees a stream that jumps backwards and forwards in sequence space, which pion's own NACK and jitter-buffer interceptors read as catastrophic loss. **A keyframe cannot fix this** — a keyframe repairs reference state, and the damage here is to transport ordering, one layer below. Offsets rather than a counter, because gaps *within* an upstream must survive to the child (a lost packet has to stay visible or loss reporting silently stops working) while the discontinuity *between* upstreams must not. |

---

## Third-party — planned

One external dependency remains, and **it was not built**.

| Component | Role | Status | Why this one |
|---|---|---|---|
| `coturn` (external service) | **TURN relay** server for peers behind symmetric NAT / CGNAT that STUN can't traverse. Such peers are forced to be **leaves**, never relays. | **Not built (Phase 7).** `overlay.NATRelayed` exists and the builder honours it, but it is *declared* by the peer's `-nat turn` flag; there is no NAT classification, no coturn, and no TURN credential in `ICEServers`. | Not a Go import — a standalone TURN daemon Conclave connects to. `coturn` is the de-facto standard, well-hardened TURN/STUN implementation; reimplementing TURN is squarely outside the learning target. STUN covers most peers; TURN is the fallback of last resort because relayed media costs bandwidth on infrastructure we run. |

---

## The frontend — deliberately zero dependencies

`web/` is the dashboard UI: **vanilla ES modules and hand-written CSS. No npm, no bundler, no
transpile, no framework, no `package.json`.** The directory that is committed is byte-for-byte
the directory that is served by GitHub Pages, so what you debug against a local `file://` tree is
what is live.

This is the same rule as the Go side, applied to the browser: a build step here would buy
nothing (the app is a few hundred lines of DOM construction and a WebSocket client) and would put
a toolchain — and a supply chain — between the source and the artefact. The one place it costs
something is testing: there is a single hand-run harness,
`web/tests/reconnect-backoff.test.html`, and nothing else is automated.

The single external asset reference is a font *stack* (`Fira Code`, `JetBrains Mono`, then system
monospace fallbacks) — a preference, not a download.

---

## Deployment — not a Go dependency, but part of the product

`deploy/` carries a multi-stage **distroless, non-root Dockerfile**, plus a
`docker-compose.yml` + `Caddyfile` pair that issues a locally-trusted certificate so `wss://` can
be rehearsed without a public hostname. **Caddy is a rehearsal tool, not a runtime dependency** —
the binary speaks plain HTTP and expects a platform (Render, Fly, Spaces) to terminate TLS in
front of it. That is why `-public-url` is mandatory behind a terminator: the server cannot know
whether something in front of it terminates TLS and **will not guess**, because guessing
`https://` would break every plain deployment instead.

---

## Go toolchain

The Makefile wires these into `make fmt / vet / test / lint / check`. Some ship with the Go distribution; others the owner must install (see status).

| Tool | Role | Status | Notes |
|---|---|---|---|
| `gofmt` (via `go fmt`) | Canonical formatting. | **Installed** (part of Go) | `make fmt`. Verified clean at Phase 0. Nudge: prefer `:=` over `var x T = …` and drop trailing semicolons — `gofmt` won't rewrite those, so they're on us. |
| `goimports` | Formatting **plus** automatic import add/remove/grouping. | **Must be installed** (`go install golang.org/x/tools/cmd/goimports@latest`) | Superset of `gofmt` for import hygiene. Worth getting into the edit loop early; not yet a Makefile target. |
| `go vet` | Suspicious-construct static checks (printf verbs, lost cancels, shadowing basics). | **Installed** (part of Go) | `make vet`. Verified clean at Phase 0. Also the fallback for `make lint` when `golangci-lint` is absent. |
| `staticcheck` | Deeper static analysis (dead code, misused stdlib, simplifications) beyond `go vet`. | **Must be installed** (`go install honnef.co/go/tools/cmd/staticcheck@latest`) | Highest-signal single linter after `go vet`. Not yet a Makefile target. |
| `golangci-lint` | Meta-linter running many linters (incl. `staticcheck`, `govet`) in one pass. | **Must be installed** | `make lint` runs it **if present, else falls back to `go vet`**. Installing it upgrades `make lint` in place with no Makefile change. |
| `go test -race` | Unit/table tests under the race detector. | **Installed** (part of Go) | `make test` and `make check`. The core verification gate — green at Phase 0. `-race` needs the C toolchain; `gcc 16` is present, so it works here. |

**One project-specific gate joins them:** `make check-determinism` is a `grep` in the Makefile,
not a tool — it fails the build if `internal/{overlay,simnet,coordinator,arbiter}` (test files
included) reaches for the wall clock. It is a build gate rather than a review note because the
failure mode it catches — a silently un-replayable control plane — is invisible in a diff.
`make check` = `fmt` + `vet` + `check-determinism` + `test -race`, and CI runs the same four
commands spelled out separately so a red job names which one failed.

**Toolchain status summary:** installed and in use today — `gofmt`, `go vet`, `go test -race`, plus the determinism grep. Must be installed by the owner to reach full lint coverage — `goimports`, `staticcheck`, `golangci-lint`.

---

## Non-goals & limitations (calibrated)

- **No consensus library.** The central server is the election **arbiter** and single source of truth for who is coordinator. This sidesteps split-brain **by construction** — it does **not** make the system fault-tolerant against the central server itself. Raft/Paxos among home PCs was explicitly rejected; a single arbiter is the deliberate trade (simplicity and no split-brain, at the cost of a control-plane single point of failure).
- **No web framework, ORM, or DI container.** Nothing here needs one, and each would be dependency surface working against the "explain the primitive" ethos.
- **Dependency count is a feature.** As of Phase 6: **two ecosystems, five direct `go.mod` entries** — `coder/websocket` for signaling, and pion for media (`webrtc/v4` + `interceptor` + `rtcp` + `rtp`). Phase 4 added none; Phases 5–6 added exactly one (`pion/rtp`, for a reason no amount of hand-writing avoids — see above). Everything those two phases introduced that *is* the learning target — the epoch fence, the arbiter, the health FSM, the sticky builder and its oracles, the virtual clock, the whole REST/WS surface and the frontend — is stdlib or hand-written. Every future addition should have to justify itself against this table.
- **`internal/overlay` goes further than the module boundary.** It imports exactly five standard-library packages — `encoding/json`, `fmt`, `math`, `os`, `sort` — and **nothing else at all**: no pion, no `clock`, no other `internal/` package. That is a hard requirement, not an accident, and it is what lets the whole control algorithm be tested in milliseconds and lets `simnet` drive the real control plane without a media stack.
- **These are choices, not verdicts.** `gorilla/websocket` vs `coder/websocket`, `Mutex` vs channel, `slog` vs `zap` — each row states a preference with a reason, not a claim that the alternative is bad. Any of them can be revisited if a phase surfaces a concrete wall.
