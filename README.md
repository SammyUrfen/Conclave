# conclave

> A decentralized, peer-assisted video-meeting platform in Go.
>
> **One-line framing:** an **SFU (Selective Forwarding Unit) that is *elected*
> from among the participants and can *migrate*.** Instead of renting a cloud SFU
> to forward everyone's media, conclave elects a *coordinator peer* that computes
> a forwarding graph, and strong peers act as *relays* that forward media on
> behalf of others. The graph re-optimizes and the coordinator role hands over as
> people join, leave, and their machines get overloaded.

This is a **learning / portfolio** project — a guided tour through idiomatic Go
and real distributed-systems problems (leader election, failover, congestion
control), not a product. Every phase de-risks one genuinely hard part and teaches
one slice of Go. See [`docs/ROADMAP.md`](docs/ROADMAP.md) for the full plan.

**Module:** `github.com/SammyUrfen/conclave` · **Go:** 1.26 · **License:** TBD

---

## Why this is interesting

A naive mesh call dies at 4–5 participants because **upload bandwidth is the
scarce resource**: every peer must upload its own stream `N−1` times. A cloud SFU
fixes this by uploading once to a rented server that fans out — but now you pay
for and depend on that server.

conclave's bet: **elect the SFU from the participants themselves.** A capable peer
becomes a relay and forwards media for its neighbors; a coordinator peer computes
a shallow, latency-bounded relay tree from live telemetry; and a small central
server acts only as **bootstrap + election arbiter** (the one reliable node, so we
never have to run consensus among flaky home PCs).

- **Data plane** = WebRTC (SRTP/UDP) between overlay neighbors. Decentralized.
- **Control plane** = WebSocket/HTTP to the central server (signaling + metrics +
  "who is coordinator"). Centralized, because a single source of truth defeats
  split-brain.

**Slogan:** *decentralized data plane, centralized control/bootstrap.*

```
        ┌──────────────────────────────┐
        │   Central bootstrap server    │  control plane (always up)
        │   • room rendezvous           │
        │   • signaling relay (WS)      │
        │   • ELECTION ARBITER          │
        │   • source of truth: coord id │
        └──────────────────────────────┘
          ▲ WS/HTTP: signaling + metrics ▲
    ┌─────┴────┐   ┌──────────┐    ┌─────┴────┐
    │ Peer C   │   │ COORD /  │    │ Peer E   │
    │ (relay)  │   │ Peer A   │    │ (leaf,   │
    └──┬────┬──┘   │ (relay)  │    │  TURN)   │
   WebRTC media    └──┬────┬──┘    └──────────┘
   (SRTP/UDP)   ┌─────┘    └─────┐
            ┌───┴───┐        ┌───┴───┐
            │Peer F │        │Peer B │   every leaf ≤ 1 hop from a relay;
            │(leaf) │        │(leaf) │   TURN-bound peers are forced leaves.
            └───────┘        └───────┘
```

**The single best explanation of the system — architecture, end-to-end traces, every
load-bearing trade-off with its rejected alternative, and an honest Limitations section — is
[`docs/DESIGN.md`](docs/DESIGN.md).** The original conceptual narrative is
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Status

| Phase | Title | Status |
|------:|-------|--------|
| **0** | **Repo bootstrap & Go foundations** | ✅ **done** |
| **1** | **Signaling server + 2-peer WebRTC call** | ✅ **done** |
| 2 | Full mesh up to ~4 peers (feel the ceiling) | ✅ **done** |
| 3 | Static relay tree — *the peer SFU* ⭐ novel core | ✅ **done** |
| 4 | Metrics plane + coordinator computes the tree | ✅ **done** |
| 5 | Join/leave handover with backup parents | ✅ **done** |
| 6 | Coordinator election + migration | ✅ **done** |
| 7 | Simulcast/SVC, TURN fallback, polish & demo | ⬜ **not built** |

Phase 7's **observability** slice was pulled forward into Phases 5–6 and reshaped: the planned
`/debug` page shipped instead as the arbiter's `/api` REST + WebSocket surface plus a separate
zero-build static dashboard on GitHub Pages. **The rest of Phase 7 — simulcast/SVC and
TURN/coturn — was not built.**

**40,256 lines of Go** (17,505 non-test, 22,751 test), 372 test functions, 13 packages, 5 direct
third-party dependencies. `make check` — `fmt` + `vet` + `check-determinism` + `go test -race
./...` — is green.

**Phase 0 delivers:** a clean module layout, two runnable binaries (`server`,
`peer`), structured `log/slog` logging, graceful shutdown, a `Makefile`, and the
first table-driven tests — all passing under `go test -race`.

**Phase 1 delivers:** a WebSocket signaling hub (rooms, message relay,
server-stamped identity) and a real 2-peer WebRTC call — pion `PeerConnection`s
exchanging SDP/ICE through the server with a **single-offerer** negotiation scheme
(one deterministic offerer per pair — glare-free, because pion can't roll back a
local offer), carrying a VP8 track peer-to-peer over SRTP/UDP. Verified two ways: an
in-process `-race` integration test (two sessions connect + forward a track) and a
live two-`peer` demo whose received file `ffprobe` confirms is decodable VP8.

**Phase 2 delivers:** the naive full mesh — every peer holds a `PeerConnection` to
every other, and an **upload meter** (lock-free `sync/atomic` counter + 1 s sampler)
logs the aggregate send rate so the `O(N)` upload cost is *visible*. Per-peer
goroutine lifecycle is managed with `context` + `sync.WaitGroup` (no leaks), and
`Router.Stats()` snapshots the mesh (seed of the Phase-4 metrics plane). Verified by
a 3-peer `-race` integration test (full mesh + bidirectional media over the real
Hub) and a live 4-peer demo. **The measured ceiling:** real 720p VP8 ≈1.5 Mbit/s per
stream, so each peer uploads ≈4.6 Mbit/s at 4 peers and ≈6 Mbit/s at 5 — enough to
saturate a typical home uplink. That linear cost is the whole reason for the elected
SFU that Phase 3 begins.

**Phase 3 delivers:** the novel core — a **participant that forwards other
participants' media**. A hardcoded tree (`-topology tree.json`, `-name`) makes one
peer a *relay*: it reads each source's RTP (`TrackRemote.ReadRTP`) and fans it out to
the other children (`TrackLocalStaticRTP.WriteRTP`) with **no re-encode**, plumbing
keyframe requests (PLI) back upstream to the original sender with the SSRC translated
(the classic SFU footgun). Signaling now carries a stable peer name so a tree authored
in names resolves to runtime ids. Verified by a 3-peer `-race` test (a leaf receives
another leaf's media *through the relay*, with no direct session to the source — a
topological proof — plus a deterministic SSRC-translation test and clean-shutdown
join) and a live demo: a relay forwarded a leaf's real 720p VP8 to another leaf, which
recorded **124 decodable frames** it could only have received via the relay. Honest
limit: file/synthetic sources have no live encoder, so PLI *plumbing* is proven but
keyframe *response* awaits a browser sender (Phase 7).

**Phase 4 delivers:** the tree stops being hardcoded — a **coordinator computes it
from live telemetry**. Peers report an upload budget + NAT class (`internal/metrics`);
a coordinator running in the server fans those reports in on a single-goroutine event
loop and runs a pure greedy **`overlay.BuildTree`** (degree-bounded by upload,
depth-limited, min-latency, TURN→forced-leaf) on every membership change, pushing the
result down a new `topology` frame that peers realise with the Phase-3 relay machinery
(`-managed`, no `-topology` file). The star of the phase is the **`simnet`** harness:
a deterministic, media-free network that drives the *real* `BuildTree` under hundreds
of seeded random fleets and a long churn scenario, asserting an independent
`overlay.Validate` oracle every step — the FoundationDB/TigerBeetle "deterministic
simulation" idea in miniature. Verified three ways: the property/churn `simnet` tests,
a `-race` coordinator test (async fan-in, root election, anti-thrash, leave-recompute)
and a **live managed-room demo** whose leaf recorded **33 decodable VP8 frames**
forwarded through a relay the *coordinator elected from telemetry* — the Phase-3
topology, now computed. Honest limits: apply is **additive** (a new peer attaches; mid-call
re-parent/teardown is Phase 5); upload/NAT are *declared* (real probing is Phase 7);
recompute fires only on membership change (full hysteresis is Phase 5).

**Phase 5 delivers:** the tree survives churn. The coordinator precomputes a **warm backup
parent** per node under the invariant `B ∉ Subtree(ParentOf(u))` — stronger than the obvious
rule, and the one that rules out the sibling trap — with a hard fan-in cap so a failover
oversubscribes a relay by at most one child. On a parent failure a peer **promotes that backup
itself, without asking anyone**: make-before-break (the old parent keeps feeding it until the new
one carries media), asynchronous (waiting for `Connected` on the goroutine that also routes the
answer times out 100% of the time), and it reports success only when **media** arrives, not
merely ICE. Each downstream leg carries an RTP rewriter holding a `(sequence, timestamp)` offset,
because a keyframe repairs reference state and the damage from a second upstream is to transport
ordering, one layer below. Phase 4's *additive* apply was replaced by **diff-and-apply** whose
baseline is *reality*, not the last push, so a partially realized state converges. Hysteresis
landed as three health states with thresholds derived from the cadence each **peer declares**,
floored at the Hub's socket-detection window — and a resurrection rule, because a frame arriving
is itself proof the socket is live. Local repair needed **no bespoke `Repair()`**: stickiness
makes it fall out of the general builder, so there is one algorithm and one oracle. The honest
consequence, measured rather than hidden: a stability-preserving builder is **path-dependent by
construction** — 120 first-report permutations of one fleet converge to **9 distinct valid trees
with 1 identical root**, and path-independence and minimal-disruption rebuilds are mutually
exclusive.

**Phase 6 delivers:** the control plane survives its owner. `internal/arbiter` is the **single
writer of the epoch** (`ms.epoch++` appears in exactly one place) and its `Run` loop has **no
timer at all** — every action it can take requires a live peer, and a live peer is by definition
heartbeating. Authority is a two-field token `(Epoch, Rev)`, Raft's `(term, index)` split for the
same reason, and the two questions it answers are deliberately **two different functions**:
`Supersedes` orders trees and is never on the apply path, while `Fence.Accept` authorizes and
demands the epoch match **exactly** — *a higher epoch is rejected*, because a peer may learn who
is in charge only from the arbiter, never from the node claiming the job. (The frozen contract
mandated otherwise, and as literally written any peer stamping `MaxUint64−1` would have been
universally obeyed.) State handover is **rebuild-from-peers, and only that** — snapshot-and-ship
was rejected outright, not on the 3 seconds but because *a fallback path that only runs on
crashes is a path that is never tested and is therefore broken when you need it.* Three failure
modes around the window are closed: a lost announcement (repaired verbatim, unicast, uncapped),
a demoted-but-live coordinator, and no eligible successor (a **vacancy at a bumped epoch** —
the epoch is a fencing token, not a term counter). Watching it happen is the other half of the
phase: `internal/dashboard` plus a zero-build static frontend that renders **realized vs
intended** as two labelled truths rather than averaging them.

> **What Phase 6 does *not* demonstrate live, stated up front.** `metrics.Report`'s `CPUPct`,
> `LossPct` and `RTTServerMs` are **never populated in production** — `cmd/peer` fills in only
> name, upload budget, NAT class and willingness, all from flags. With those three structurally
> zero, every eligible peer scores **0.85–1.0** in `arbiter.Score`, so `DemoteBelowScore` can
> never be crossed and `PromoteMarginScore` can never be met. **Voluntary promotion and demotion
> are unreachable live; only bootstrap and failover elections fire.** The same gap makes the
> degradation dwell inert and leaves `BuildTree`'s min-latency rank without data. All of it is
> real, tested, and exercised **only in `simnet`**, where the values are injected. The logic is
> built; the sensors are not. Do not demo this as "elects the fittest machine" — it elects on
> the inputs it has. ([`DESIGN.md`](docs/DESIGN.md) §8.1.)

---

## Quickstart

Requires **Go 1.26+** (and a C compiler — `gcc`/`clang` — for `-race`).

```sh
# 1. Start the central server (serves GET /healthz on :9000).
make run-server
#   → text logs, "http server listening" addr=:9000

# 2. In another terminal, run a peer that probes the server.
make run-peer
#   → "server is healthy" status_code=200 server_status=ok

# 3. Run the tests (race detector on).
make test
```

Useful variations:

```sh
make run-server ARGS="-addr :9000 -log-format json"   # JSON logs on a custom port
make run-peer   ARGS="-server http://localhost:9000"  # point the peer elsewhere
make build                                            # → ./bin/server, ./bin/peer
make check                                            # fmt + vet + check-determinism + test -race
make help                                             # list all targets
```

**Make a real 2-peer call (Phase 1):**

```sh
make run-server                                              # signaling hub on :9000

# receiver: join room "demo", record what it receives
go run ./cmd/peer -call -room demo -record out.ivf

# sender: join the same room, stream a VP8 IVF file (make one with:
#   ffmpeg -f lavfi -i testsrc=duration=5:size=320x240:rate=30 -c:v libvpx -f ivf sample.ivf)
go run ./cmd/peer -call -room demo -send -media sample.ivf
```

Both peers' logs climb to `peer connection state … connected`; the receiver
writes `out.ivf`, playable in any VP8 player. Omit `-media` to send synthetic
frames (proves transport without a file); add `-stun stun:stun.l.google.com:19302`
for two machines behind NAT.

**Let the coordinator compute the tree (Phase 4):**

```sh
make run-server ARGS="-coordinate -stream-kbps 2000 -max-depth 2"   # coordinator ON

# a strong relay advertises upload budget; leaves advertise little/none.
go run ./cmd/peer -call -managed -name relay  -upload-kbps 8000 -room mgmt
go run ./cmd/peer -call -managed -name leaf-b -upload-kbps 0 -media sample.ivf -room mgmt
go run ./cmd/peer -call -managed -name leaf-d -upload-kbps 0 -record out.ivf   -room mgmt
```

No `-topology` file: each peer reports telemetry, the server elects the highest-upload
peer as the root relay, computes `relay → {leaf-b, leaf-d}`, and pushes it. `leaf-d`
records media forwarded *through the computed relay* (`ffprobe out.ivf` → `vp8`). A
TURN-bound peer (`-nat turn`) is forced to a leaf.

**Watch it happen (the dashboard):**

```sh
make run-server ARGS="-coordinate -elect"     # coordinator + election arbitration
xdg-open web/index.html                       # type localhost:9000 in the server field
```

No build step, no npm — `web/` is vanilla ES modules served as-is. The published Pages site can
point at `localhost:9000` too: browsers exempt `localhost` from mixed-content blocking, so an
`https://` page may open `ws://localhost`. Any *other* host needs real TLS — see
[`deploy/README.md`](deploy/README.md).

Every flag: [`docs/usage.md`](docs/usage.md). Tooling install: [`docs/setup.md`](docs/setup.md).

---

## Repository layout

```
conclave/
  cmd/
    server/main.go      # central bootstrap / signaling / election arbiter
    peer/main.go        # a participant node
  internal/             # compiler-enforced private packages (package-by-feature)
    logging/            # slog construction + level parsing                        (leaf)
    clock/              # the injectable time seam: Clock, Timer, Ticker           (leaf)
    policy/             # boundary rules for untrusted input: origins, ids, names  (leaf)
    overlay/            # tree model, pure BuildTree, backups, Fence, two oracles  (leaf, stdlib-only)
    metrics/            # Report/Heartbeat/Reparented + peer-side Reporter + cadences
    signaling/          # WS hub + peer client; SDP/ICE relayed opaquely
    coordinator/        # single-goroutine brain: fan-in → health FSM → BuildTree → push
    arbiter/            # meet registry, epoch minting, election, announcements
    media/              # pion sessions, relay fan-out, diff-and-apply, re-parent, RTP rewrite
    dashboard/          # the browser-facing REST + WS surface
    simnet/             # deterministic media-free harness (TEST-ONLY)
  web/                  # the dashboard frontend — vanilla ES modules, NO build step
  deploy/               # distroless Dockerfile + a wss:// rehearsal (compose + Caddy)
  .github/workflows/    # ci.yml (the gate) · pages.yml (publish web/)
  docs/                 # DESIGN + ROADMAP + living docs
  Makefile
```

Why `internal/` and `cmd/`, and where each package's boundary is:
[`docs/structure.md`](docs/structure.md).

---

## Documentation

| Doc | What's in it |
|-----|--------------|
| [`docs/DESIGN.md`](docs/DESIGN.md) | **Start here.** The whole system end to end: architecture, traces, every trade-off with what it rejected, where the contract was wrong, and a calibrated Limitations section |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | Phased build order + per-phase Go/systems learning goals, with what each phase actually shipped |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | The original conceptual narrative + trade-off record |
| [`docs/PLAN.md`](docs/PLAN.md) | The frozen Phase 5–6 architecture contract. **Historical** — amended six times during the build; where it and the code disagree, the code wins |
| [`deploy/README.md`](deploy/README.md) | Running the arbiter somewhere other than your shell; the `ws://` vs `wss://` decision |
| [`docs/structure.md`](docs/structure.md) | File tree → responsibility, package boundaries |
| [`docs/tech-stack.md`](docs/tech-stack.md) | Libraries & stdlib pieces, why each was chosen |
| [`docs/setup.md`](docs/setup.md) | Prerequisites, tooling install, editor setup |
| [`docs/usage.md`](docs/usage.md) | Running the binaries, every flag, make targets |
| [`docs/testing.md`](docs/testing.md) | The strategy: virtual clock, independent oracles, `check-determinism`, the mutation discipline |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | Symptom → cause → fix, including the startup refusals and the build outcomes |
| [`docs/design-system.md`](docs/design-system.md) | Log field vocabulary + the (live, dark-only) dashboard design system |

---

## Non-goals (honest limitations)

- **Not production software.** No auth, no persistence, no horizontal scale of the
  central server. It optimizes for *understanding*, not uptime.
- **No custom consensus.** Election is arbitrated by the central server on purpose;
  we explicitly do **not** run Raft/Paxos among home PCs.
- **Small scale.** The target is tens of peers in a shallow tree, not thousands.
- **No authentication, anywhere.** No token, password, credential, JWT or TLS termination in
  any non-test file. What exists instead: server-stamped identity, the epoch fence, an origin
  allow-list on both the upgrade and CORS, unguessable `crypto/rand` meet ids, and bounded
  state. The destructive demo routes are **not registered at all** unless `-demo` is passed.
- **The telemetry is mostly declared, not measured.** Upload budget and NAT class are operator
  claims; CPU, loss and server RTT are never populated. See the Phase 6 box above for what
  that costs.
- **No simulcast, no SVC, no TURN.** Phase 7 was not built.
- **Every live run so far has been single-host.** Multiple processes on one machine over
  loopback — no cross-machine result, no real NAT traversal, no real congestion control. The
  "≈6 Mbit/s at 5 peers" ceiling is arithmetic on a *measured* per-stream bitrate, not an
  observed collapse. And Phases 5–6 have **no recorded live run at all**: they are verified by
  the automated suite.
- **No benchmarks and no fuzz targets** exist in the repository.
- Correctness and reliability are prioritized over performance and polish, in that
  order.
