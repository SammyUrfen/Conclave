# conclave — Repository Structure

> **Status:** living document. Kept in sync with the tree as phases land. Last aligned with the code at **Phase 6 complete** (churn/handover, backup parents, coordinator election + migration, and the browser-facing dashboard surface).
>
> This is the map: every directory, what it owns, and which way its dependencies are *allowed* to point. If you're about to add a file and you're not sure where it goes, the answer is here — or this doc needs an edit. For the *reasoning* behind the boundaries, read [`DESIGN.md`](./DESIGN.md) §3.

---

## The tree (annotated)

Thirteen Go packages, plus three non-Go directories that are part of the product: `web/`
(the dashboard frontend), `deploy/` (container + TLS recipes), and `.github/workflows/`
(the gate and the Pages publish).

```
conclave/
├── go.mod / go.sum              module github.com/SammyUrfen/conclave · go 1.26.4
│                                 5 direct deps: pion/webrtc/v4 · pion/rtp · pion/rtcp
│                                 · pion/interceptor · coder/websocket
├── Makefile                     help·build·run-server·run-peer·test(-race)·cover·fmt·vet
│                                 ·tidy·lint·check-determinism·check·clean
├── .gitignore                   ignores ./bin/ and build artifacts (never commit binaries)
├── README.md                    the pitch + quickstart
├── CLAUDE.md                    session index for Claude Code
│
├── cmd/                         ← entrypoints only. One dir per binary. Thin main(); no logic.
│   ├── server/                  the ARBITER binary.
│   │   ├── main.go              thin main → run(); newPlane wires hub ↔ coordinator ↔
│   │   │                         arbiter ↔ dashboard; /healthz, /ws, /api/; graceful drain.
│   │   ├── flags.go             the whole flag surface + fail-loud validation, incl.
│   │   │                         validateLivenessBudget (the check no single package can do).
│   │   ├── wiring.go            the adapters between packages: hubSender, planeObserver
│   │   │                         (fans every observer callback to BOTH coordinator and
│   │   │                         arbiter), the dashboard's Meets/Demo sources.
│   │   └── *_test.go            flag validation, wiring, and the ServerID agreement test.
│   └── peer/                    the PARTICIPANT binary.
│       ├── main.go              modes: probe /healthz · -call (mesh) · -topology (static
│       │                         tree) · -managed (telemetry + pushed tree + failover).
│       └── main_test.go         URL normalization, flag gating, inert-flag warnings.
│
├── internal/                    ← ALL library code. Import-fenced by the compiler.
│   │                              package-by-FEATURE, not by-layer.
│   ├── logging/                 slog construction + level/format parsing. LEAF.
│   ├── clock/                   the injectable time seam: Clock, Timer, Ticker. LEAF.
│   │   ├── clock.go             the interface + System(); Timer/Ticker expose their
│   │   │                         channel through a C() METHOD, because a Go interface
│   │   │                         can declare methods but not fields.
│   │   └── doc.go
│   ├── policy/                  boundary rules for UNTRUSTED INPUT. LEAF.
│   │   ├── origins.go           Origins: the parsed -allowed-origins list. AllowUpgrade
│   │   │                         (WS) and Match (CORS) must accept the same set.
│   │   ├── meetid.go            MeetIDPattern `^[a-z0-9][a-z0-9_-]{0,63}$` — reject,
│   │   │                         never truncate.
│   │   └── peername.go          PeerNamePattern, identical in shape and for the same
│   │                             reasons; "" is legal (an unnamed mesh peer).
│   ├── overlay/                 the subnet model + the builder + the oracles. LEAF, PURE:
│   │   │                         5 stdlib imports and nothing else. No clock, no pion,
│   │   │                         no map range in non-test code.
│   │   ├── topology.go          Topology (edges in names, stamped with Epoch/Rev) +
│   │   │                         Parent/Children/Neighbors/IsRelay/Offers/BackupOf/
│   │   │                         Supersedes (ORDERING only) + fail-loud LoadTopology.
│   │   ├── buildtree.go         BuildTree: greedy, five-rank lexicographic comparator,
│   │   │                         degree/depth-bounded, sticky. Node/Constraints/NATType,
│   │   │                         PickRoot (sticky root policy).
│   │   ├── backup.go            assignBackups: warm secondary parents under
│   │   │                         B ∉ Subtree(ParentOf(u)) and a fan-in cap.
│   │   ├── fence.go             Fence: a peer's view of AUTHORIZATION. Accept/Applied/
│   │   │                         AdoptAnnouncement/Reset. Exact-epoch match.
│   │   └── validate.go          Validate (invariant oracle) + ValidateLocalRepair
│   │                             (transition oracle). Independent of the builder.
│   ├── metrics/                 the peer → control-plane wire payloads + the peer-side
│   │   │                         Reporter + the cadence constants.
│   │   └── report.go            Report, Heartbeat (realized state), Reparented; the
│   │                             Reporter; DegradedAfter/GoneAfter derived from the
│   │                             cadence the PEER declares; ValidateLivenessBudget.
│   ├── signaling/               control-plane transport: the WebSocket hub + peer client.
│   │   ├── message.go           the wire protocol: Type constants + Message (SDP/ICE/
│   │   │                         Payload opaque json.RawMessage). ServerID = "_server".
│   │   ├── hub.go               Hub: meet registry, ServeWS (origin gate BEFORE the
│   │   │                         upgrade, 403 on a miss), register/route/relay,
│   │   │                         server-stamped identity, WS keepalive, Observer seam.
│   │   ├── member.go            one connection: a bounded out-channel + a writePump that
│   │   │                         is the single owner of the write side + a separate
│   │   │                         pingLoop (liveness must not queue behind a wedged peer).
│   │   └── client.go            peer-side Client: dial /ws, read/write pumps, Incoming().
│   ├── coordinator/             the control-plane BRAIN. Single-goroutine event loop.
│   │   │                         Imports NEITHER signaling NOR media — that is what let
│   │   │                         the role move to a peer as a wiring change.
│   │   ├── coordinator.go       Coordinator: Run's three-case select, the ten event
│   │   │                         kinds, the Observer surface it structurally satisfies,
│   │   │                         the Sender/Publisher consumer-defined seams, Sync.
│   │   ├── config.go            every named constant with its justification: dwells,
│   │   │                         cooldowns, thresholds, the socket-detection floor.
│   │   ├── event.go             Health (3 states), BuildOutcome (built/settling/relaxed/
│   │   │                         unbuildable), EventKind, the dashboard Event.
│   │   ├── recompute.go         the ONLY caller of the build path: eligibility → PickRoot
│   │   │                         → deriveWorking → two-attempt BuildTree → both oracles
│   │   │                         → publish. Also epoch adoption + rebuild-from-peers.
│   │   └── snapshot.go          RoomSnapshot: a deep-copied point-in-time view, produced
│   │                             ON the Run goroutine so a dashboard read cannot tear.
│   ├── arbiter/                 the central server's authority over WHO COORDINATES.
│   │   ├── arbiter.go           meet registry + lifecycle, its own liveness view, the
│   │   │                         SINGLE writer of the epoch, election policy, the
│   │   │                         announcement + repair + vacancy paths. No timer at all.
│   │   ├── fitness.go           Fitness (control-plane evidence only — no UploadKbps)
│   │   │                         and Score, pure and total, NaN-safe.
│   │   ├── announcement.go      Announcement + the frozen Reason enum.
│   │   └── meet.go              Meet: the arbiter-owned description of one meet and the
│   │                             payload of /api/meets.
│   ├── media/                   the DATA PLANE. The only package that imports pion.
│   │   ├── session.go           Session: one PeerConnection (VP8 pinned) + the
│   │   │                         single-offerer negotiation serializer + the retry
│   │   │                         ladders + pendingLocalChange (pion won't renegotiate a
│   │   │                         RemoveTrack on its own).
│   │   ├── transport.go         Transport: the consumer-defined signaling seam.
│   │   ├── router.go            Router: demux into per-peer Sessions; mesh / static tree
│   │   │                         / MANAGED; applyTopology (Fence.Accept is its first act
│   │   │                         and the fence's ONLY call site); name↔id; Stats().
│   │   ├── diff.go              diffTopology: a PURE diff of (self, wanted, REALITY) into
│   │   │                         seven ordered buckets. Baseline is reality, not the last
│   │   │                         push, so a partial state converges.
│   │   ├── relay.go             the forwarder: per-(source, LAYER) read → per-child
│   │   │                         fan-out, no re-encode; SSRC-translated upstream PLI,
│   │   │                         throttled per (SOURCE, LAYER). Two axes, one invariant:
│   │   │                         exactly one (generation, layer) per leg may write.
│   │   ├── layerpin_test.go     pion pins: an ANSWERER fills every offered recvonly m-line,
│   │   │                         and the relay's receive-slot rule is right on BOTH sides
│   │   │                         of the edge (asymmetric on purpose). See DESIGN §7.5.
│   │   ├── layers.go            the quality ladder (q/h/f), the `<base>.<rung>` track-name
│   │   │                         split, and selectLayer — a PURE, clock-free per-child
│   │   │                         choice with asymmetric hysteresis. Data plane on purpose.
│   │   ├── reparent.go          the async re-parent state machine + backup promotion +
│   │   │                         the promotionPending exemption + ladder-exhaustion
│   │   │                         recovery. Nothing blocks the Run goroutine.
│   │   ├── rewrite.go           rtpRewriter: per-leg (sequence, timestamp) OFFSETS,
│   │   │                         recomputed at Switch(); drop until a keyframe.
│   │   ├── source.go / sink.go  outbound IVF/synthetic, one pump per published layer;
│   │   │                         inbound record/count.
│   │   └── meter.go             the upload meter: lock-free atomic counter + 1s sampler.
│   ├── dashboard/               the browser-facing surface. Read-mostly, eventually
│   │   │                         consistent. NOT a control surface.
│   │   ├── dashboard.go         Server + the mux. Patterns are registered WITHOUT a
│   │   │                         method and dispatch on r.Method, so every non-2xx keeps
│   │   │                         the {code, message, details} envelope.
│   │   ├── rest.go              GET/POST /api/meets · GET /api/meets/{id} · the two
│   │   │                         demo actions. Bounded bodies, boundary validation.
│   │   ├── events.go            GET /api/meets/{id}/events — one WS per open meet view.
│   │   ├── snapshot.go/wire.go  the JSON bodies (unexported: the contract is the wire
│   │   │                         schema, not a set of Go types) + the two truths,
│   │   │                         REALIZED vs INTENDED, labelled rather than averaged.
│   │   ├── cors.go              echo the origin, never "*"; a non-matching origin gets a
│   │   │                         normal response minus the header, not a 403.
│   │   ├── errors.go            the centralized error → HTTP status/code mapping.
│   │   └── joinurl.go           derives the join command from -public-url / -addr.
│   └── simnet/                  TEST-ONLY. Deterministic, media-free; drives the REAL
│       │                         overlay and coordinator. Production never imports it.
│       ├── virtualclock.go      VirtualClock: total order (deadline, seq), one deadline
│       │                         per iteration, cap-1 non-blocking channels, panics
│       │                         rather than hangs.
│       ├── network.go           the fleet model: upload caps, NAT classes, pairwise RTT.
│       ├── scenario.go          Scenario + Settle (the quiescence barrier).
│       ├── control.go           the seams that let a Scenario drive the real coordinator.
│       ├── overlaycalls.go      the SINGLE point at which simnet calls into overlay.
│       └── injection.go         failure injection as separate verbs, not one Fault().
│
├── web/                         the dashboard FRONTEND. Vanilla ES modules + hand-written
│   │                             CSS. NO BUILD STEP — what is committed is what is served.
│   ├── index.html
│   ├── css/tokens.css           the design tokens (dark-only; see design-system.md).
│   ├── css/layout.css · components.css
│   ├── js/api.js                the ONLY module that touches the network: REST client,
│   │                             EventSocket + its reconnect ladder, server-URL parsing.
│   ├── js/state.js · router.js · dom.js · format.js · main.js
│   ├── js/mockApi.js            drives the UI from web/fixtures/ with no server.
│   ├── js/views/                serverBar · meetsList · meetDetail · subnetTree ·
│   │                             eventLog · demoControls
│   ├── fixtures/                meets.json · meet-standup.json · events-standup.json
│   └── tests/                   reconnect-backoff.test.html (open it in a browser)
│
├── scripts/
│   └── make-layers.sh           generates the three VP8 IVF quality layers an origin can
│                                 publish (320x180/640x360/1280x720 @ 150/500/1500 kbit).
│                                 The SCRIPT is committed, the .ivf files are not — they
│                                 are megabytes of build artifact and *.ivf is gitignored.
│
├── deploy/                      running the arbiter somewhere other than your shell.
│   ├── Dockerfile               multi-stage, distroless, non-root. Build from repo ROOT.
│   ├── docker-compose.yml       + Caddyfile: a locally-trusted wss:// rehearsal.
│   ├── Caddyfile
│   └── README.md                the ws:// vs wss:// decision, platform notes, and the
│                                 three flags a hosted deployment cannot omit.
│
├── .github/workflows/
│   ├── ci.yml                   the gate, spelled out as the same four commands make
│   │                             check runs, so a red job names which one failed.
│   └── pages.yml                publishes web/ to GitHub Pages on a web/-touching push.
│
└── docs/
    ├── DESIGN.md                the single best explanation of the finished system.
    ├── ROADMAP.md               phased build order + learning goals.
    ├── ARCHITECTURE.md          the original conceptual narrative + trade-off record.
    ├── PLAN.md                  the frozen Phase 5–6 contract (HISTORICAL — amended six
    │                             times; the code is the truth).
    ├── structure.md             ← you are here.
    ├── tech-stack.md            every dependency + why-chosen.
    ├── setup.md                 prereqs + tool install.
    ├── usage.md                 flags, sessions, endpoint contracts, exit codes.
    ├── testing.md               the strategy: virtual clock, oracles, mutation discipline.
    ├── troubleshooting.md       symptom → cause → fix.
    └── design-system.md         log field vocabulary + the dashboard design system.
```

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

The golden rule: **imports point one way and never cycle.** The "May import" column is the
*complete* set of internal edges each package is allowed to have — treat it as the edge list
you check a new import against.

| Package | Owns | Src / test LOC | May import (internal) |
|---|---|---:|---|
| `logging` | slog construction, level/format parsing | 73 / 48 | — (leaf) |
| `clock` | the injectable time seam: `Clock`, `Timer`, `Ticker` | 106 / 117 | — (leaf) |
| `policy` | boundary rules for untrusted input: origins, meet ids, peer names | 267 / 346 | — (leaf) |
| `overlay` | the subnet model, the pure `BuildTree`, `PickRoot`, backup assignment, the `Fence`, and the two oracles `Validate` / `ValidateLocalRepair` | 1,678 / 2,892 | — (leaf, and **stdlib-only**) |
| `metrics` | the peer → control-plane payloads (`Report`, `Heartbeat`, `Reparented`), the peer-side `Reporter`, the cadence constants, `ValidateLivenessBudget` | 380 / 606 | `overlay`, `clock`, `logging` |
| `signaling` | the WebSocket hub + peer client; meets, per-client goroutines, frame relay with server-stamped identity, the origin gate, WS keepalive, the `Observer` seam | 1,113 / 1,208 | `logging`, `clock`, `policy` |
| `coordinator` | the control brain: single-goroutine loop, health FSM, hysteresis timers, the three trees, the two-attempt build, ratification, epoch adoption, rebuild-from-peers, the `Publisher` seam | 2,544 / 3,714 | `overlay`, `metrics`, `clock`, `logging` |
| `arbiter` | meet registry + lifecycle, its own liveness view, **epoch minting**, `Score`, election policy, announcement + repair + vacancy | 1,690 / 2,421 | `overlay`, `metrics`, `clock`, `policy`, `logging` |
| `media` | the data plane: pion sessions, the negotiation serializer, RTP forwarding, upstream PLI with SSRC translation, diff-and-apply, the async re-parent machine, the RTP rewriter, backup promotion, the upload meter | 3,973 / 3,514 | `overlay`, `signaling`, `metrics`, `clock`, `logging` |
| `dashboard` | the browser-facing REST + WebSocket surface, the event envelope, CORS/origin policy, the gated demo surface | 2,319 / 2,889 | `overlay`, `coordinator`, `arbiter`, `clock`, `policy`, `logging` |
| `simnet` | the deterministic media-free harness: virtual clock, event queue, failure injection, the `Settle` barrier | 1,240 / 2,700 | `overlay`, `coordinator`, `metrics`, `clock` — **test-only** |
| `cmd/server` | the arbiter binary; wires every seam | 1,252 / 1,139 | anything under `internal/` |
| `cmd/peer` | the participant binary: probe / call / static-tree / managed modes | 870 / 1,157 | anything under `internal/` |

Rendered as a DAG:

```
   binaries (may import anything)
        ┌──────────────┐        ┌──────────────┐
        │  cmd/server  │        │   cmd/peer   │
        └──────┬───────┘        └──────┬───────┘
     ┌─────────┼──────────┐            │
     ▼         ▼          ▼            ▼
┌──────────┐ ┌─────────┐ ┌──────────────────┐
│dashboard │ │ arbiter │ │      media       │  ← the only package that imports pion
└────┬─────┘ └────┬────┘ └────────┬─────────┘
     │            │               │
     ▼            │        ┌──────┴──────┐
┌─────────────┐   │        ▼             ▼
│ coordinator │◄──┘  ┌───────────┐  ┌─────────┐
└──────┬──────┘      │ signaling │  │ overlay │
       │             └─────┬─────┘  └────▲────┘
       ▼                   │             │
  ┌──────────┐             │             │
  │ metrics  │─────────────┼─────────────┘
  └────┬─────┘             │
       ▼                   ▼
┌─────────┐ ┌───────┐ ┌────────┐ ┌─────────┐
│ overlay │ │ clock │ │ policy │ │ logging │   leaves: no internal imports, ever
└─────────┘ └───────┘ └────────┘ └─────────┘

   simnet (test-only) → overlay, coordinator, metrics, clock
```

Five directional rules worth stating out loud, because they are the ones that are easy to
violate:

- **`overlay` stays pure.** Five stdlib imports (`encoding/json`, `fmt`, `math`, `os`, `sort`) and **nothing else at all** — no pion, no `clock`, no other `internal/` package. Purity is what makes `BuildTree` a function of `(nodes, prev, constraints)` alone, lets the whole control algorithm be tested in milliseconds, and keeps `simnet` from dragging in a media stack. It also contains **no `range` over a map** outside tests: maps are O(1) lookup tables, and every ordered decision carries a parallel slice.
- **`coordinator` imports neither `signaling` nor `media`.** Every mention of `signaling` in the package is in a comment. This is not tidiness — it is why moving the coordinator role onto a peer in Phase 6 was a *wiring* change rather than a rewrite. Both cross-plane seams are consumer-defined interfaces (below).
- **`coordinator → arbiter` is a forbidden edge**, in both directions of intent. It is what makes "just read the fitness score in the tree builder" a compile error instead of a code-review conversation, and it is the mechanical form of the control/data-plane split.
- **`dashboard → signaling` is forbidden.** Not because it would cycle (it would not) but because of what it enables next: it would hand the unauthenticated browser surface a compile-time handle on the peer hub, and the very next convenience ("we already import signaling — just call `hub.SendRoom` from the demo endpoint") puts control-plane authority behind an HTTP handler with no auth in front of it. `internal/policy` exists precisely so the shared origin matcher can be applied by both surfaces without that edge.
- **`simnet` is test-only and one-directional.** Production must never import it. It drives the *real* `overlay.BuildTree` and the *real* `coordinator.Coordinator`; where it keeps a simplified model, a differential test pins the model to the shipped loop (`docs/testing.md`).

Two of these are enforced mechanically rather than by review: `make check-determinism` fails
the build if `overlay`, `simnet`, `coordinator` or `arbiter` reaches for the wall clock, and
`internal/dashboard/zz_seamcheck_test.go` is a *compile-time* file (no runtime test at all)
whose four `var _ Iface = (*Impl)(nil)` lines fail to build the moment a producer stops
satisfying a consumer-defined interface.

### The interface seam that keeps control logic testable

"Control-plane tests need no pion" is bought with one Go idiom: **the consumer defines the
interface, at the point of use.** Go interfaces are structural and implicit — a type satisfies
one by having the methods, never by naming it — so the dependency arrow points *from the
consumer to the data*, not from the producer to a shared `interfaces` package. Coming from
Java the instinct is to declare `Publisher` next to its implementation and have the consumer
import it; that is exactly the back-edge that makes the DAG cyclic.

Applied here in three directions:

- **Inbound (hub → coordinator).** `signaling` declares `Observer`; `*coordinator.Coordinator` *structurally satisfies* it — five methods over strings and `[]byte`. The coordinator never names the interface and never imports `signaling`. `cmd/server` passes the concrete coordinator where an `Observer` is wanted and the compiler checks the shape. In `cmd/server`, `planeObserver` fans every callback to **both** the coordinator and the arbiter, so the arbiter keeps its own liveness view — which matters, because the thing it must eventually detect is the coordinator's own death.
- **Outbound (coordinator → hub).** `coordinator` *declares* `Sender { SendTopology(roomID, peerID string, topo *overlay.Topology) error }`. Note the method set mentions only stdlib types and `overlay` types — types the *consumer* already imports. That is the precise condition under which a consumer-defined interface avoids an import. `cmd/server` supplies a `hubSender` adapter; a test supplies a fake that records pushes.
- **Outbound (coordinator → dashboard).** `coordinator` declares `Publisher { Publish(Event) }` with `Event` owned by `coordinator`; `dashboard` imports `coordinator` and implements it. The arrow points the right way.

**The rule, stated precisely enough to check:** a consumer-defined interface avoids the import
only when its signatures name types the **implementer** owns, or stdlib types. The contract
originally claimed `dashboard` declared consumer interfaces and therefore did not import
`arbiter`; that was false in both directions at once (one interface returned a
`dashboard.Meet`, which would have forced `arbiter → dashboard`; another named
`arbiter.Announcement`). The fix was to state the rule, accept `dashboard → arbiter` as a legal
downward edge, and move `Meet` to its producer.

### The two deliberate exceptions

Two shared leaf packages break the consumer-defines-it rule. Naming the *kind* of exception is
what keeps them from becoming a junk drawer: both are **shared vocabularies that several
packages must agree on exactly, where the agreement is the point and divergence is silent.**

- **`internal/clock` — forced by Go's type system.** A consumer-defined `interface{ Now() time.Time }` works fine. The moment it says `NewTimer(d) Timer`, the *return type* is part of the signature, and Go compares **named** types in return position — so a method returning `coordinator.Timer` does not satisfy an interface requiring `simnet.Timer`, even if the two declarations are character-identical. Every consumer would need its own fake. One leaf package is the only workable shape. (`Timer`/`Ticker` also expose their channel through a `C()` **method** rather than `time.Timer`'s `C` **field**, because a Go interface can declare methods but not fields — which is why the "just use `time.Timer`" shortcut does not exist.)
- **`internal/policy` — forced by a security-relevant agreement.** The origin allow-list must be applied identically by the peer WebSocket upgrade (`signaling`) and by the dashboard's CORS surface (`dashboard`), and the DAG forbids `dashboard → signaling`. Two copies of a security matcher drift, and **the drift's failure mode is a silently permissive CORS policy that no test notices**, because each surface's tests pass against its own copy. *Accepting the duplication with a cross-checking test was considered and loses arithmetically:* a test asserting two implementations agree is more code than one implementation, and it only catches drift on the cases it enumerates — the wrong shape for a matcher whose dangerous inputs are the ones nobody thought of.

A third candidate was tested against that rule and split both ways, which is the useful
precedent: `RebuildWindow` was declared in `arbiter` and consumed only by `coordinator`, across
a forbidden edge — it **moved to `metrics`**, which both already import, because it is
literally "how long until every peer has reported at least once", a cadence sitting beside
`HeartbeatInterval` and `GoneAfter`. It explicitly did *not* go to `policy`, whose charter is
untrusted boundary input. Meanwhile `signaling.ServerID` vs `arbiter.DefaultArbiterID` — the
same string `"_server"` — **kept its duplication**, because it is a *wire protocol* constant
and moving it into `policy` would make signaling's wire contract depend on a package with
nothing to do with the wire. Two tests enforce that agreement instead: one asserting the
constants are equal, and one in `cmd/server` asserting the wiring actually passes it.

## The `doc.go` convention (empty packages, on purpose)

Through Phase 3 this repo carried several placeholder packages: a single `doc.go` holding only
a package doc comment and the `package <name>` clause, no types or functions. Phase 4 graduated
the last four (`overlay`, `metrics`, `coordinator`, `simnet`), and Phases 5–6 added four more
packages (`clock`, `policy`, `arbiter`, `dashboard`) that started the same way. Most kept a lean
`doc.go` as the package-doc home; a few (`media`, `overlay`, `signaling`) moved the package
comment onto the file that best represents the package. Both endings are fine — what matters is
that `go doc` has somewhere to land.

The convention is documented here because it is how *every* future package should start. It did
three things while a package was still a placeholder:

1. **Gives the package a compile target *now*.** An empty directory isn't a Go package and won't build; `package overlay` in `doc.go` means `go build ./...` and `go vet ./...` already cover it.
2. **Documents the responsibility before the code exists.** Each `doc.go` states, in prose, what the package will own and which phase populates it. It's a contract-with-future-self, and it renders in `go doc` today.
3. **Makes the intended architecture reviewable ahead of implementation.** You can read the whole system's shape — control vs. data plane, what's pure, what's test-only — from a handful of short comments, before a line of pion is imported.

The surviving `doc.go` files are worth reading in this order for a five-minute orientation:
`overlay` (what the graph layer promises), `coordinator` (what the brain does), `arbiter` (who
owns the epoch), `media` (the data plane), `policy` (why untrusted input is one package), and
`simnet` (the two non-negotiable determinism rules).

---

## How the layout prevents import cycles (the short version)

Go **forbids import cycles at compile time** — two packages that import each other simply won't build. That's a hard constraint, and it's easiest to satisfy by construction:

- **Leaves at the bottom:** `logging`, `clock`, `policy`, `overlay` import no other `internal` package. Anyone can depend on them; they depend on nobody, so they can't be *in* a cycle. (`metrics` was a leaf through Phase 4 and no longer is — it imports `overlay`, `clock` and `logging`.)
- **One layer up:** `metrics` above the leaves; `signaling`, `coordinator` and `arbiter` above that; `media` and `dashboard` above those. Downward-only edges can't cycle.
- **Binaries at the top:** `cmd/*` may import anything in `internal/`; nothing imports `cmd/*`. Top of the DAG.
- **Cross-plane calls go through interfaces the caller owns**, so the concrete data-plane (`media`) and test harness (`simnet`) point *up* at `coordinator`'s interfaces rather than creating a back-edge.
- **`simnet` is test-only and one-directional:** it imports the real logic; production never imports it.

If you ever find yourself adding an import that makes an edge point "backward" (a leaf reaching up into `coordinator`, or `overlay` importing `media`), stop — that's the shape of a future cycle, and the fix is almost always to invert it with an interface defined at the point of use.
