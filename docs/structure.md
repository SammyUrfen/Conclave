# conclave — Repository Structure

> **Status:** living document. Kept in sync with the tree as phases land. Last aligned with the code at **Phase 4 complete (metrics plane + coordinator computes the tree + simnet harness)**.
>
> This is the map: every directory, what it owns, when it comes alive, and which way its dependencies are *allowed* to point. If you're about to add a file and you're not sure where it goes, the answer is here — or this doc needs an edit.

---

## The tree (annotated)

Every `internal/` package now carries real code: Phase 4 populated the last four placeholders (`overlay`, `metrics`, `coordinator`, `simnet`), each keeping a lean `doc.go` as its package-doc home — see [The `doc.go` convention](#the-docgo-convention-empty-packages-on-purpose).

```
conclave/
├── go.mod                       module github.com/SammyUrfen/conclave · go 1.26.4 · deps: coder/websocket (signaling), pion/webrtc + pion/rtcp (media)
├── Makefile                     help·build·run-server·run-peer·test(-race)·cover·fmt·vet·tidy·lint·check·clean
├── .gitignore                   ignores ./bin/ and build artifacts (do not commit compiled binaries)
├── README.md                    the pitch + why-it's-interesting
├── ARCHITECTURE.md              conceptual narrative: elected-SFU, plane split, election/migration
├── CLAUDE.md                    session index for Claude Code
│
├── cmd/                         ← entrypoints only. One dir per binary. Thin main(); no business logic.
│   ├── server/
│   │   └── main.go              central bootstrap / signaling rendezvous / election ARBITER.
│   │                             Serves /healthz + /ws; -coordinate hosts the Phase-4 coordinator (hubSender adapter).
│   └── peer/
│       ├── main.go              a participant node. Modes: probe /healthz (P0), -call (WebRTC, P1), -managed (P4 telemetry+pushed tree).
│       └── main_test.go         table-driven TestHealthURLFor (pure helper, no network).
│
├── internal/                    ← ALL library code. Import-fenced by the compiler (see below).
│   │                              package-by-FEATURE, not by-layer.
│   ├── logging/                 LIVE. slog construction + level/format parsing. Leaf dependency.
│   │   ├── logging.go           New(w, level, Format) *slog.Logger · ParseLevel · Format{JSON,Text}
│   │   └── logging_test.go
│   ├── signaling/               LIVE. control-plane transport: WebSocket rendezvous, rooms, message relay.
│   │   ├── message.go           the wire protocol: Type constants (+ metrics/topology) + Message (SDP/ICE/Payload opaque json.RawMessage).
│   │   ├── hub.go               server Hub: room registry (sync.Mutex), ServeWS, register/unregister/route/relay; Observer hooks + SendTo (P4).
│   │   ├── member.go            hub's per-connection handle: buffered out-channel + single writePump goroutine.
│   │   ├── client.go            peer-side Client: dial /ws, read/write pumps, Incoming()/Send(); + wsURLFor.
│   │   ├── hub_test.go          integration test: two real ws clients join/relay/leave, under -race.
│   │   ├── client_test.go       table-driven wsURLFor test.
│   │   └── doc.go               package doc (control/data-plane split, server-authoritative identity).
│   ├── media/                   LIVE. data plane: pion/webrtc sessions, full mesh + tree relay (peer-SFU).
│   │   ├── session.go           Session: PeerConnection (VP8 pinned) + single-offerer negotiation (pion can't rollback); AddForwardTrack/WriteRTCP for relaying.
│   │   ├── transport.go         Transport interface (consumer-defined): Send + Incoming signaling frames.
│   │   ├── relay.go             forwarder: per-source read→fan-out (TrackRemote→TrackLocalStaticRTP) + SSRC-translated, throttled upstream PLI.
│   │   ├── router.go            Router: demux into per-peer Sessions; mesh / static tree / MANAGED (applyTopology from pushed frames); name↔id; Stats(). Consumes overlay.Topology.
│   │   ├── source.go            outbound: PlayIVF (file) / SendSynthetic → a sampleWriter (interface over the track).
│   │   ├── sink.go              inbound: RecordVP8 (→ IVF file) / DrainAndCount over a TrackRemote.
│   │   ├── meter.go             upload meter: sync/atomic byte counter + 1s sampler logging aggregate/per-peer kbit/s.
│   │   ├── session_test.go      integration test: two sessions connect over loopback + forward a track, -race.
│   │   ├── mesh_test.go         integration test: 3 peers full-mesh over the real Hub + bidirectional media, -race.
│   │   ├── relay_test.go        deterministic PLI-SSRC-translation unit + 3-peer tree forwarding integration, -race.
│   │   └── doc.go               package doc (data plane; mesh + the elected-relay forwarding core).
│   ├── overlay/                 LIVE. the tree model + graph builder (PURE: no sockets, no clock, pion-free).
│   │   ├── topology.go          Topology (edges in names) + Parent/Children/Neighbors/IsRelay/Offers/Nodes queries + fail-loud LoadTopology.
│   │   ├── buildtree.go         BuildTree (greedy degree/depth-bounded min-latency) + Node/Constraints/NATType + PickRoot policy.
│   │   ├── validate.go          Validate: the independent invariant oracle (single-root, tree, depth, capacity, TURN-leaf).
│   │   ├── topology_test.go     tree queries + LoadTopology fail-loud validation.
│   │   ├── buildtree_test.go    table-driven BuildTree (star/depth/TURN/RTT/over-constrained) + determinism + Validate-catches.
│   │   └── doc.go               package doc.
│   ├── metrics/                 LIVE. telemetry Report {upload, nat, rtt, loss, cpu} + peer-side Reporter (ticker, best-effort).
│   │   ├── report.go            Report (wire type) + Reporter (immediate-then-tick, injected sample/send, never faults the peer).
│   │   ├── report_test.go       reporter emits-immediately-then-ticks, survives-send-error, default-interval.
│   │   └── doc.go               package doc.
│   ├── coordinator/             LIVE. control-plane brain: single-goroutine event loop; metrics fan-in → BuildTree → push.
│   │   ├── coordinator.go       Coordinator (owned state, Observer surface, Sender seam); recompute on membership change / first report.
│   │   ├── coordinator_test.go  -race: computes tree, anti-thrash, leave-recompute, skips-unnamed (fake Sender, async).
│   │   └── doc.go               package doc (Phase 4 on server; election/migration is Phase 6).
│   └── simnet/                  LIVE (test harness). deterministic media-free network driving the REAL overlay.BuildTree.
│       ├── network.go           Network: Add/Remove/SetRTT/RemoveRandom + Build + Random generator (seeded, reproducible).
│       ├── network_test.go      property (300 seeds), churn (200 steps), latency-attachment, scenario-determinism.
│       └── doc.go               package doc.
│
└── docs/
    ├── ROADMAP.md               source of truth: architecture + phased build order.
    ├── structure.md             ← you are here.
    ├── tech-stack.md            every dependency + why-chosen, tracked by phase.
    ├── setup.md                 prereqs + tool install (goimports, staticcheck, golangci-lint).
    ├── usage.md                 flags, example sessions, endpoint contracts, exit codes.
    ├── testing.md               table-driven idiom + the simnet plan.
    ├── troubleshooting.md       symptom → cause → fix, per phase.
    └── design-system.md         log field vocabulary (live) + reserved dashboard theme.
```

---

## Why `internal/` exists (it's a compiler rule, not a convention)

`internal/` is one of the very few directory names the **Go toolchain treats specially**. The rule: a package under `.../internal/` may be imported **only** by code rooted in the parent of that `internal/` directory. For this repo, the parent is the module root, so:

- ✅ `cmd/server` and `cmd/peer` may import `github.com/SammyUrfen/conclave/internal/...`
- ✅ any `internal/*` package may import any other `internal/*` package (subject to the direction rules below)
- ❌ **anyone outside this module cannot import our `internal/...` at all** — the build fails, it's not a lint warning

Coming from C++/Java: this is the *enforced* version of "these headers are private to the library" or a `package-private` boundary — except no visibility keyword and no discipline required. It's structural. That buys us freedom to rename, split, and reshape every `internal/` package without a single external consumer to break, which matters a lot for a project that will be under heavy churn through Phase 6. When something genuinely needs to be public API later, it graduates *out* of `internal/`; until proven, everything stays in.

---

## Why entrypoints live in `cmd/`

`cmd/<name>/` is the Go community convention for "this directory is `package main` and compiles to the binary `<name>`." `go build ./cmd/server` → `server`; the Makefile emits `./bin/server` and `./bin/peer`.

The discipline that matters more than the directory name: **`main()` is thin.** Each binary's `main()` parses flags into a `FlagSet`, builds the logger, calls a testable `run(args) error`, and translates the error into `os.Exit(1)` + a stderr line. That's the whole `main`. Everything worth testing lives either in `run` or — as it grows — down in `internal/`. Two consequences:

1. **`main` isn't unit-testable and shouldn't need to be.** `os.Exit` can't be asserted on cleanly; keeping `main` trivial means there's nothing there worth a test. The logic that *is* worth testing (`healthURLFor`, later the signaling/overlay code) sits behind `run` or in packages, where `go test` can reach it. `cmd/peer/main_test.go` already exercises this seam.
2. **Binaries are compositions, not libraries.** `cmd/*` is where concrete `internal/*` pieces get wired together and injected (the logger is built once in `main` and passed downward — no globals). Nothing imports `cmd/`; `cmd/` is always a leaf at the top of the graph.

---

## Package-by-feature, not package-by-layer

There are two ways to cut the `internal/` tree:

| | Package-by-layer (**not** used) | Package-by-feature (**used here**) |
|---|---|---|
| Group by | technical role | domain capability |
| Looks like | `internal/handlers/`, `internal/services/`, `internal/models/` | `internal/signaling/`, `internal/overlay/`, `internal/coordinator/` |
| A change to "how election works" touches | 3+ packages (a handler, a service, a model) | mostly 1 package (`coordinator`) |
| Import graph | tends toward everything-imports-everything | narrow, directional edges between features |

This repo is package-by-feature on purpose. Each `internal/` package owns one **capability** end to end — its types, its logic, and (eventually) its transport wiring — the same way the owner's Spring projects give `auctions/` and `disputes/` their own `dto/` instead of one global `controllers/` + `services/` split. The payoff for *this* system specifically: the control plane and data plane are supposed to stay separate (different resources, different failure modes), and feature packages make that separation something the compiler can see. `signaling`/`coordinator`/`overlay`/`metrics` are control-plane; `media` is data-plane; they meet only at explicit seams, not in a shared `services/` blob.

---

## Per-package responsibilities & dependency direction

The golden rule: **imports point one way and never cycle.** Read the "May import" column as the *only* internal edges each package is allowed to add. `logging` and `overlay` are **leaves** (no internal deps) so they can be imported freely; `metrics` and `coordinator` sit above them; `cmd/*` sits at the very top.

| Package | Owns | Comes alive | May import (internal) | Imported by |
|---|---|---|---|---|
| `logging` | slog handler construction, level/format parsing | **Live (Phase 0)** | — (leaf) | everything (`cmd/*`, most `internal/*`) |
| `signaling` | WS transport, rooms, message relay (SDP/ICE opaque) + control-plane Observer/SendTo | **Live (Phase 1)** | `logging` | `cmd/*`, `media` |
| `overlay` | the tree model + pure `BuildTree` heuristic + `Validate` oracle | **Live (Phase 4)** | — (leaf, pure) | `media`, `metrics`, `coordinator`, `simnet` |
| `media` | pion sessions; full mesh + tree relay (peer-SFU RTP forwarding, upstream PLI); single-offerer negotiation; upload meter; consumes `overlay.Topology` | **Live (Phase 3)** | `logging`, `signaling`, `overlay` | `cmd/peer` |
| `metrics` | telemetry `Report` + peer-side `Reporter` | **Live (Phase 4)** | `overlay` | `coordinator`, `cmd/peer` (producer) |
| `coordinator` | metrics fan-in → `BuildTree` → push; hysteresis; failover; migration | **Live (Phase 4)** (election Phase 6) | `overlay`, `metrics` | `cmd/server` |
| `simnet` | fake network (latencies, upload caps, churn) driving the *real* `overlay.BuildTree` | **Live (Phase 4)** | `overlay` | **tests only** |
| `cmd/server` | bootstrap, signaling rendezvous, election arbiter, coordinator host | Live (healthz + /ws + -coordinate) | any `internal/*` | — (binary, top of graph) |
| `cmd/peer` | participant: probe + WebRTC call + managed telemetry | Live (probe + call + managed) | any `internal/*` | — (binary, top of graph) |

Three directional rules worth stating out loud, because they're the ones that are easy to violate:

- **`overlay` stays pure — no sockets, no clock, no `internal` imports.** Given nodes + constraints, `BuildTree` returns a tree and `Validate` checks one. Purity is what lets the topology logic be unit- and simulation-tested in milliseconds instead of behind real WebRTC — and, crucially, it is why `simnet` (which imports `overlay`) never drags in pion. Don't let a "convenient" import of `media` or `metrics` runtime state leak in; pass data *in* as plain values. (`media` and `metrics` both import `overlay` for the shared `Topology`/`NATType` types — one-way, downward.)
- **`coordinator` depends only on `overlay` + `metrics` — it imports NEITHER `signaling` NOR `media`.** The two cross-plane seams are pure interface: it *satisfies* `signaling.Observer` structurally (the Hub calls it; no import needed), and it *defines* its own `Sender` interface that `cmd/server` adapts to `hub.SendTo`. See the interface note below.
- **`simnet` is test-only and imports the real logic, never the reverse.** Production code must not import `simnet`. In Phase 4 it drives `overlay.BuildTree` directly against synthetic fleets; as the coordinator's control loop grows testable behind interfaces, the same harness will drive it too.

### The interface seam that keeps control logic testable

The task constraint "control-logic tests need no pion" is bought with one Go idiom: **the consumer defines the interface, at the point of use.** Phase 4 realised this with two seams that keep `coordinator` free of both `signaling` and `media`:

- **Inbound (events):** `signaling` declares `Observer` (`PeerJoined`/`PeerLeft`/`Metrics`, all in strings + raw bytes), the Hub calls it, and `coordinator` *satisfies it structurally* — Go's implicit interfaces mean no import is needed at all. A test (or `simnet`) can call the same methods directly.
- **Outbound (pushes):** `coordinator` declares its own `Sender` (`SendTopology(room, peer, *overlay.Topology)`) and accepts a value of it. `cmd/server` supplies a tiny `hubSender` adapter over `hub.SendTo`; a test supplies a fake that records pushes. `coordinator` imports neither `signaling` nor `media`.

This is also precisely how import cycles are avoided. A cycle would appear the moment `coordinator` imported `signaling` *and* `signaling` called back into `coordinator`. By having each side own the interface at its point of use — `signaling` owns `Observer`, `coordinator` owns `Sender`, and `cmd/server` wires the two concrete ends together at the edge — there is no mutual edge anywhere. "Accept interfaces, return structs" isn't decoration here; it's the mechanism that makes the dependency graph a DAG.

---

## The `doc.go` convention (empty packages, on purpose)

Through Phase 3 this repo carried several placeholder packages: a single `doc.go` holding only a package doc comment and the `package <name>` clause, no types or functions. Phase 4 graduated the last four (`overlay`, `metrics`, `coordinator`, `simnet`) — each kept a lean `doc.go` as its package-doc home and grew real files alongside. The convention is documented here because it is how *every* future package (Phase 5's failover types, Phase 7's simulcast) should start. It did three things while a package was still a placeholder:

1. **Gives the package a compile target *now*.** An empty directory isn't a Go package and won't build; `package overlay` in `doc.go` means `go build ./...` and `go vet ./...` already cover it. The skeleton is green from day one, so when real code lands there's no "does this package even exist" step.
2. **Documents the responsibility before the code exists.** Each `doc.go` states, in prose, what the package will own and which phase populates it (e.g. `overlay`'s doc pins down "pure `BuildTree`, degree-bounded, depth-limited, min-latency"). It's a contract-with-future-self, and it renders in `go doc` / pkg docs today. The intended boundaries in the table above are *already written down at the source*.
3. **Makes the intended architecture reviewable ahead of implementation.** You can read the whole system's shape — control vs. data plane, what's pure, what's test-only — by reading six short comments, before a line of pion is imported.

When a package graduates from placeholder to real, the rule is: **delete `doc.go`'s placeholder body and move the package comment onto the file that best represents the package** (or keep a lean `doc.go` if the package comment is substantial). `logging` shows the end state — no placeholder, real code, doc comments on exported identifiers.

---

## How the layout prevents import cycles (the short version)

Go **forbids import cycles at compile time** — two packages that import each other simply won't build. That's a hard constraint, and it's easiest to satisfy by construction:

- **Leaves at the bottom:** `logging`, `overlay`, `metrics` import no other `internal` package. Anyone can depend on them; they depend on nobody, so they can't be *in* a cycle.
- **One layer up:** `coordinator` depends downward on `overlay` + `metrics`. Downward-only edges can't cycle.
- **Binaries at the top:** `cmd/*` may import anything in `internal/`; nothing imports `cmd/*`. Top of the DAG.
- **Cross-plane calls go through interfaces the caller owns**, so the concrete data-plane (`media`) and test harness (`simnet`) point *up* at `coordinator`'s interfaces rather than creating a back-edge.
- **`simnet` is test-only and one-directional:** it imports the real logic; production never imports it.

If you ever find yourself adding an import that makes an edge point "backward" (a leaf reaching up into `coordinator`, or `overlay` importing `media`), stop — that's the shape of a future cycle, and the fix is almost always to invert it with an interface defined at the point of use.
