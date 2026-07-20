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

Full narrative and trade-offs: [`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## Status

| Phase | Title | Status |
|------:|-------|--------|
| **0** | **Repo bootstrap & Go foundations** | ✅ **done** |
| **1** | **Signaling server + 2-peer WebRTC call** | ✅ **done** |
| 2 | Full mesh up to ~4 peers (feel the ceiling) | ✅ **done** |
| 3 | Static relay tree — *the peer SFU* ⭐ novel core | ✅ **done** |
| 4 | Metrics plane + coordinator computes the tree | ⬜ |
| 5 | Join/leave handover with backup parents | ⬜ |
| 6 | Coordinator election + migration | ⬜ |
| 7 | Simulcast/SVC, TURN fallback, polish & demo | ⬜ |

Stopping after Phase 3–4 already yields a defensible portfolio piece: an elected
peer-SFU with a metrics-driven, simulation-tested graph builder. Phases 5–7 are
"hard mode."

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
make check                                            # fmt + vet + test (pre-commit gate)
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

Every flag: [`docs/usage.md`](docs/usage.md). Tooling install: [`docs/setup.md`](docs/setup.md).

---

## Repository layout

```
conclave/
  cmd/
    server/main.go      # central bootstrap / signaling / election arbiter
    peer/main.go        # a participant node
  internal/             # compiler-enforced private packages (package-by-feature)
    logging/            # slog construction + level parsing  (live)
    signaling/          # WS hub + peer client, SDP/ICE relay (live)
    media/              # pion sessions, mesh + tree relay (peer-SFU), upload meter (live)
    overlay/            # graph model + greedy tree builder   (Phase 4)
    metrics/            # telemetry types, collection, fan-in (Phase 4)
    coordinator/        # election + graph orchestration      (Phase 4+)
    simnet/             # in-memory simulated network          (Phase 4)
  docs/                 # ROADMAP + living design docs
  Makefile
```

Why `internal/` and `cmd/`, and where each package's boundary is:
[`docs/structure.md`](docs/structure.md).

---

## Documentation

| Doc | What's in it |
|-----|--------------|
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | Phased build order + per-phase Go/systems learning goals (source of truth) |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Conceptual narrative + trade-offs |
| [`docs/structure.md`](docs/structure.md) | File tree → responsibility, package boundaries |
| [`docs/tech-stack.md`](docs/tech-stack.md) | Libraries & stdlib pieces, why each was chosen |
| [`docs/setup.md`](docs/setup.md) | Prerequisites, tooling install, editor setup |
| [`docs/usage.md`](docs/usage.md) | Running the binaries, every flag, make targets |
| [`docs/testing.md`](docs/testing.md) | Testing philosophy, table-driven idiom, simnet plan |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | Common failure modes, per phase |
| [`docs/design-system.md`](docs/design-system.md) | Logging/observability conventions + dashboard theme |

---

## Non-goals (honest limitations)

- **Not production software.** No auth, no persistence, no horizontal scale of the
  central server. It optimizes for *understanding*, not uptime.
- **No custom consensus.** Election is arbitrated by the central server on purpose;
  we explicitly do **not** run Raft/Paxos among home PCs.
- **Small scale.** The target is tens of peers in a shallow tree, not thousands.
- Correctness and reliability are prioritized over performance and polish, in that
  order.
