# conclave — Roadmap

> A decentralized, peer-assisted video-meeting platform in Go.
>
> **One-line framing:** an **SFU (Selective Forwarding Unit) that is *elected* from among the participants and can *migrate***. Instead of a rented cloud SFU forwarding everyone's media, we elect a coordinator peer that computes a forwarding graph, and strong peers act as relays that forward media on behalf of others. The graph re-optimizes and the coordinator role hands over as people join, leave, and their machines get overloaded.

This is a **learning / portfolio** project. It is not shipping to real users. Every phase is chosen to (a) teach a slice of idiomatic Go and (b) de-risk one genuinely hard part of the system. Optimize for understanding, not for a product.

**Module:** `github.com/SammyUrfen/conclave` · **Dir:** `/home/SammyUrfen/Codes/go/conclave`

---

## The core architecture (agreed, encoded here so future-you doesn't relitigate it)

- **Media transport is WebRTC, not WebSockets.** Media flows over `RTCPeerConnection`s (SRTP over UDP/ICE). One peer connection per neighbor in the overlay carries audio + video + a data channel, multiplexed. WebSockets/HTTP exist *only* for **signaling** (SDP offer/answer + ICE candidates) and for the **metrics/telemetry** channel. (This corrects the earlier "4 websockets, 3 for media" mental model — there are no media websockets.)

- **Control plane ≠ data plane. Keep them separate.**
  - **Coordinator = control plane.** Collects metrics, computes the forwarding graph, drives signaling orchestration, monitors health. Wants **compute + a reliable, low-jitter network**.
  - **Relays = data plane.** Forward other people's media. Want **upload bandwidth**.
  - These are different resources. A node can be a heavy relay without being coordinator, and can be coordinator without relaying much. Do **not** merge the two roles into one "super node" abstraction.

- **Hybrid, not fully decentralized.** A small central Go server handles bootstrap / onboarding / signaling rendezvous, **and acts as the election arbiter / source of truth for "who is coordinator."** This is what avoids split-brain. We are explicitly **not** running Raft/Paxos among flaky home PCs — the consensus problem is delegated to the one reliable node we already have. Slogan: **decentralized data plane, centralized control/bootstrap.**

- **Keep the relay tree shallow.** Latency accumulates per hop (~150 ms one-way is good, ~400 ms is the tolerable ceiling). Deep `A→B→C→D` chains are the **enemy**, not the goal. Target: a few strong relays with most peers exactly **1 hop** from a relay.

- **Upload bandwidth is the scarce resource.** This is *why* naive mesh dies at 4–5 participants: each peer uploads its stream N−1 times. Mitigation (later phase): **simulcast / SVC** (VP8/VP9/AV1) — a sender emits multiple quality layers, and a relay forwards only the layer each downstream actually needs.

- **NAT traversal:** STUN gets most peers a direct path; **TURN (coturn)** is the fallback for symmetric-NAT / CGNAT peers. Peers that can only reach others via TURN must be **leaves, never relays** (they're already bandwidth- and path-constrained).

- **Dynamic graph with hysteresis.** Re-optimize only on **threshold events**: join, leave, or *sustained* degradation over N seconds — **never** on every metric wiggle. Thrashing = constant stream interruptions. Keep changes **local** (move one subtree, not the world). Keep a **warm backup parent** per node for fast failover. The optimization target is a *degree-bounded, depth-limited, min-latency tree* — optimal is NP-hard, but a **greedy heuristic** ("attach to the lowest-latency relay that has spare upload and won't exceed max depth") is plenty and runs in milliseconds.

- **Coordinator election & migration.** The creator is coordinator #1. A peer gets promoted based on metrics as the current coordinator nears capacity; a weak coordinator gets demoted; if the coordinator leaves, the next-best candidate takes over — **the central server arbitrates** each transition. Churn and handover is the **hardest engineering problem** in this whole project. Budget accordingly.

### Overlay topology

```
                        ┌───────────────────────────────┐
                        │   Central bootstrap server     │   control/bootstrap (always up)
                        │   (cmd/server)                 │
                        │   • room rendezvous            │
                        │   • signaling relay (WS)       │
                        │   • ELECTION ARBITER           │
                        │   • source of truth: coord id  │
                        └───────────────────────────────┘
                         ▲        ▲            ▲        ▲
              signaling  │        │ metrics    │        │  (WebSocket / HTTP only)
              + metrics   │        │            │        │
                         │        │            │        │
                 ┌───────┴──┐  ┌──┴───────┐    │    ┌───┴──────┐
                 │  Peer C  │  │ COORD /  │    │    │  Peer E  │
                 │ (relay)  │  │ Peer A   │────┼────│  (leaf,  │
                 │          │  │ (relay)  │  control │  TURN)   │
                 └───┬───┬──┘  └──┬───┬───┘    plane └──────────┘
        WebRTC media │   │        │   │  WebRTC media (SRTP/UDP)
        (SRTP/UDP)   │   │        │   │
              ┌──────┘   └───┐ ┌──┘   └──────┐
          ┌───┴───┐     ┌────┴─┴──┐     ┌────┴───┐
          │Peer F │     │ Peer B  │     │ Peer D │
          │(leaf) │     │ (leaf)  │     │ (leaf) │
          └───────┘     └─────────┘     └────────┘

  ── control plane ──►  WS/HTTP to central server (signaling + telemetry)
  ══ data plane   ══►  WebRTC peer connections between overlay neighbors
                       (audio + video + data, multiplexed; 1 conn per edge)
  Note: A is BOTH coordinator (control) and a relay (data) here — allowed,
        but they are distinct hats. C is a relay but not coordinator.
        E and D are TURN-bound → forced leaves, never relays. Tree is shallow:
        every leaf is 1 hop from a relay.
```

---

## How to read the phases

Each phase is a **runnable milestone**. Build order below. Each ships with:
- **Goal / risk retired** — the one thing this de-risks.
- **Go tasks** — concrete, with the actual libraries and APIs.
- **What you'll learn (Go + systems)** — the callout that makes this a learning project.
- **Done when** — acceptance criteria you can demo.

It is **completely fine to stop after Phase 3 or 4.** A working "elected peer SFU with a metrics-driven graph builder and a simulation harness" is already an impressive, unusual portfolio piece. Phases 5–7 are where the difficulty (and the interview stories) really live, but they are optional.

### Status

| Phase | Title | Status |
|------:|-------|--------|
| 0 | Repo bootstrap & Go foundations | ✅ done |
| 1 | Signaling + 2-peer WebRTC call | ✅ done |
| 2 | Full mesh to ~4 peers (feel the ceiling) | ✅ done |
| 3 | Static relay tree — the peer SFU ⭐ | ✅ done |
| 4 | Metrics plane + coordinator computes the tree | ✅ done |
| 5 | Join/leave handover + backup parents | ✅ done |
| 6 | Coordinator election + migration | ✅ done |
| 7 | Simulcast/SVC, TURN fallback, polish & demo | ⬜ **not built** |

**Phase 7's scope moved, and only partly.** The *observability* task ("a small `/debug`
dashboard … showing the live graph, per-node upload, coordinator identity/epoch, and
failover events") was **brought forward into Phases 5–6 and reshaped**: it shipped not as
a `/debug` page rendered by the server but as the arbiter's **`/api` REST + WebSocket
surface** (`internal/dashboard`) read by a **separate zero-build static frontend** (`web/`)
published to GitHub Pages (`.github/workflows/pages.yml`), with container/TLS recipes in
`deploy/`. Splitting them was deliberate: the frontend is not a Go concern, and a page
served by the arbiter would have to be rebuilt and redeployed with it.

**The rest of Phase 7 was NOT built.** There is no simulcast, no SVC, and no TURN/coturn
infrastructure. A relay forwards one quality layer to every downstream, and
`overlay.NATRelayed` is a *modelled* constraint declared by the peer's `-nat turn` flag,
not a measured NAT classification. Nothing in the repository probes bandwidth, RTT, loss,
or CPU either — see the limitation box under Phase 6.

The single-doc synthesis of what actually shipped, including every trade-off with its
rejected alternative, is **[`DESIGN.md`](./DESIGN.md)**.

---

## Phase 0 — Repo bootstrap & Go foundations

**Goal / risk retired:** a clean, idiomatic Go layout you won't fight later; muscle memory for modules, packages, and tooling.

**Go tasks**
- `go mod init github.com/SammyUrfen/conclave`. Understand `go.mod` / `go.sum`, semantic import paths, and why the module path matches the repo URL.
- Lay out the tree:
  ```
  conclave/
    cmd/
      server/main.go      # central bootstrap/signaling/arbiter
      peer/main.go        # a participant node
    internal/
      signaling/          # WS signaling, SDP/ICE plumbing
      overlay/            # graph model + greedy tree builder
      metrics/            # telemetry types, collection, fan-in
      coordinator/        # election + graph orchestration
      media/              # pion wiring, tracks, relays
      simnet/             # in-memory simulated network (Phase 4)
    go.mod
    Makefile              # or justfile
  ```
  Learn *why* `internal/` exists (compiler-enforced import boundary) and why entrypoints live under `cmd/`.
- Trivial `cmd/server` that serves `GET /healthz` via `net/http`, and `cmd/peer` that dials it and logs the response.
- Structured logging with **`log/slog`** (stdlib): a `slog.Logger` with a JSON handler, log levels, `With()` for contextual fields. Wire one logger per binary, pass it down (don't use a global).
- `Makefile`/`justfile`: `build`, `run-server`, `run-peer`, `test`, `lint` (`go vet`, and install `golangci-lint`).
- First **table-driven test** on any pure helper so the pattern is in your fingers.

**What you'll learn (Go + systems)**
- Modules, packages, `internal/`, `cmd/` convention, import cycles and how to avoid them.
- `log/slog` structured logging; the "pass dependencies explicitly, avoid globals" Go style.
- `go build` / `go test` / `go vet`, and the table-driven test idiom.

**Done when:** `make run-server` serves `/healthz`; `make run-peer` prints a successful response; `make test` runs a table-driven test that passes.

---

## Phase 1 — Signaling server + a 2-peer WebRTC call

**Goal / risk retired:** the entire capture → signal → connect → media pipeline works end to end. This is the single biggest "does WebRTC even work for me" unknown — kill it early.

**Go tasks**
- Signaling over WebSocket with **`coder/websocket`** (recommended: modern, `context`-aware; `gorilla/websocket` is the classic alternative). Server accepts two peers into a "room" and relays JSON messages between them.
- Define signaling message types in `internal/signaling`: `{Type: "offer"|"answer"|"candidate", SDP, Candidate, From, To}`. Marshal with `encoding/json` and struct tags.
- Media with **`github.com/pion/webrtc/v4`**:
  - Create an `webrtc.PeerConnection` with a STUN server in `ICEServers`.
  - `pc.OnICECandidate` → send trickle candidates over signaling; `pc.AddICECandidate` on receipt.
  - `CreateOffer` / `SetLocalDescription` / `SetRemoteDescription` / `CreateAnswer`.
  - For a first pass, skip a real camera: use pion's sample media (e.g. an IVF/Ogg file via `mediadevices` is heavier — instead start from pion's `save-to-disk` / `play-from-disk` examples) so you prove transport without fighting codecs and cameras.
  - `pc.OnTrack` on the receiver → write frames to disk (proof of media) or to a naive viewer.
- Avoid offer/answer glare with a **single-offerer scheme**: pick one deterministic offerer per pair (e.g. the higher peer id) and let the other only answer. (The textbook "perfect negotiation" pattern relies on rolling a local offer back on glare — **pion v4 cannot do that**: its signaling state machine has no `SetLocal`+rollback transition from `have-local-offer`. So single-offerer, which never produces glare, is the pion-correct choice. Both directions still ride one sendrecv m-line: the answerer adds its track before answering, so pion folds it into the offered m-line.)

**What you'll learn (Go + systems)**
- Goroutines + channels for the read/write pumps of a websocket connection; `select` on "message in / context done".
- `context.Context` for connection lifecycle and cancellation from the very first networked code you write.
- The WebRTC state machine (ICE gathering, DTLS, connection states) and *why signaling is out-of-band*.
- JSON (de)serialization with struct tags; designing a small wire protocol.

**Done when:** two `cmd/peer` processes on the same LAN establish a WebRTC connection through the signaling server and one receives the other's media track (frames written to disk or rendered). ICE completes via STUN.

**Where you'll get stuck:** ICE/DTLS states that never reach `connected` (usually a candidate not being relayed, or firewall). Log every state transition. Glare/negotiation bugs (and the pion-can't-rollback trap above — single-offerer sidesteps it). Codec negotiation mismatches — pin codecs explicitly in the `MediaEngine`.

---

## Phase 2 — Full mesh up to ~4 peers (feel the ceiling)

**Goal / risk retired:** *viscerally* understand why mesh doesn't scale — you'll build the thing you're about to replace, and measure exactly where it dies.

**Go tasks**
- Room now admits N peers. Each peer opens a `PeerConnection` to **every** other peer (N−1 connections). Manage them in a `map[peerID]*webrtc.PeerConnection` guarded by a `sync.RWMutex`.
- One goroutine per peer connection running its own send loop; a per-peer `context` for teardown. Learn to shut goroutines down cleanly (no leaks) using `context` + a `sync.WaitGroup`.
- Instrument **upload**: count bytes/sec sent across all your outbound tracks. Log it via `slog`. Watch it scale linearly with N.
- Deliberately push to 4–5 peers on real home upload and record where frame rate / quality collapses.

**What you'll learn (Go + systems)**
- `sync.RWMutex` guarding shared connection state; the "mutex vs channel for shared state" trade-off (name it explicitly).
- Goroutine lifecycle management: spawning per-connection workers, cancelling them, `WaitGroup` joins, avoiding goroutine leaks.
- Firsthand: the `O(N)` per-peer upload cost — the core motivation for the whole project.

**Done when:** 4 peers see each other in full mesh; you have a logged measurement of aggregate upload per peer and a note on the N where it broke. You can articulate the ceiling in numbers.

---

## Phase 3 — Static relay tree: "the peer SFU"  ⭐ the novel core

**Goal / risk retired:** prove that a **participant** can forward other participants' media. This is the heart of the idea. If this works, the concept is real; everything after is optimization and automation. **Hardcode the topology** — no election, no metrics, no dynamism yet.

**Go tasks**
- Pick a topology by hand, e.g. `Relay R` with children `B, C, D`, plus `R` connected to coordinator `A`. Config it via flags/JSON, don't compute it.
- On the relay, forward media without re-encoding: use pion's `TrackLocalStaticRTP` and pump incoming RTP straight out.
  - Receiver side: `pc.OnTrack` gives you a `*webrtc.TrackRemote`; read packets with `track.ReadRTP()`.
  - Create one `TrackLocalStaticRTP` per (source, downstream) and `pc.AddTrack` it on the relay's outbound connections; copy packets from the remote track to the local tracks.
  - Handle **RTCP** (PLI / keyframe requests) — a newly attached downstream needs a keyframe; forward PLIs upstream toward the original sender. Getting this wrong = black video until the next keyframe.
- Study **`pion/ion-sfu`** as the reference implementation of exactly this forwarding logic. You are building a tiny, tree-shaped ion-sfu.
- Keep the tree **shallow** (depth ≤ 2 for now) and prove a leaf receives another leaf's media *through* the relay.

**What you'll learn (Go + systems)**
- Interfaces as the extension point: pion's `TrackLocal` / `TrackRemote`; writing code against interfaces, not concrete types (your first real taste of Go's implicit-interface design).
- Fan-out with goroutines: one reader → many writers, and the backpressure/error handling when a downstream is slow or gone.
- SFU mechanics: RTP forwarding vs. transcoding, RTCP feedback (PLI/NACK), keyframe dynamics.

**Done when:** a leaf attached to a relay receives another peer's video *forwarded by the relay* (not a direct connection), video recovers within one keyframe interval on attach, and the relay's own upload is what scales — not every peer's. **This is a legitimate stopping point for a portfolio.**

**Where you'll get stuck:** keyframe/PLI handling (black frames), RTP timestamp/SSRC handling across the forward, and cleanly tearing down a forwarded track when a downstream leaves.

---

## Phase 4 — Metrics plane + coordinator computes the tree (still no migration)

**Goal / risk retired:** the graph stops being hardcoded — a coordinator ingests telemetry and *computes* the forwarding tree. Crucially, build the **simulation harness** here so you can test all of this without cameras.

**Go tasks**
- **Telemetry** in `internal/metrics`: each peer periodically reports `{peerID, uploadEstimate, rtt[neighbor], loss, cpu, natType}` to the coordinator over the control channel (WebSocket to server, or a data channel — start with WS via the server to keep it simple).
- **Fan-in** on the coordinator: many peer report streams → one metrics view. Classic Go pattern: N goroutines writing to one channel, a single consumer goroutine owning the aggregated state (or a `sync.RWMutex`-guarded snapshot). Name the trade-off: *serialize via channel* (share by communicating) vs *shared map + RWMutex*.
- **Greedy tree builder** in `internal/overlay`: degree-bounded (max children = spare upload / stream cost), depth-limited (≤ maxDepth), min-latency. Algorithm: sort candidate parents by RTT; attach each peer to the lowest-latency relay with spare upload capacity that keeps it within the depth bound; TURN-bound peers are forced leaves. Pure function: `BuildTree(nodes, constraints) → Graph`. This makes it trivially unit-testable.
- **The simulation harness** in `internal/simnet` — *emphasize this, it's how you'll actually make progress*:
  - A fake network of nodes with injectable latencies, upload caps, and churn events. No pion, no media.
  - Feed synthetic metrics into the real `BuildTree` and real coordinator logic; assert properties: depth ≤ bound, no relay over-subscribed, tree connected, TURN nodes are leaves.
  - This lets you unit-test the graph/election/failover logic **deterministically**, which is the only sane way to develop Phases 5–6.
- Coordinator pushes the computed graph out as signaling instructions ("connect to parent P"), reusing Phase 1–3 machinery to realize edges.

**What you'll learn (Go + systems)**
- Channel fan-in / worker patterns; `select` with a `time.Ticker` for periodic reporting and a `ctx.Done()` case.
- Designing pure, testable functions vs. stateful services; dependency injection for the network so tests don't need real sockets.
- Property-based / table-driven testing of a graph algorithm; deterministic simulation testing (the FoundationDB/TigerBeetle idea, in miniature).
- Modeling a degree/depth-constrained optimization and accepting a greedy heuristic over an optimal NP-hard solve — with the trade-off stated.

**Done when:** with the relay tree from Phase 3 now *computed* from live metrics, adding a peer causes the coordinator to compute a valid attachment and the peer connects to the chosen parent — and the whole builder + a churn scenario are covered by `simnet` tests that run in milliseconds with no media.

---

## Phase 5 — Join/leave handover with backup parents (the churn problem) — ✅ done

**Goal / risk retired:** the graph survives peers coming and going *without* everyone's stream freezing. Fast failover.

**Go tasks**
- **Backup parent:** for each node, the coordinator precomputes a warm secondary parent. On primary-parent failure, the node fails over to the backup while the coordinator recomputes — media gap measured in a keyframe, not a reconnection storm.
- **Hysteresis:** re-optimize only on threshold events (join, leave, sustained degradation over N seconds via a debounce/dwell timer). Implement the dwell with a `time.Timer` you reset on each good sample. Explicitly **do not** rebuild on every metric tick.
- **Local repair:** when a relay dies, re-parent *only its subtree*, not the whole graph. Keep changes minimal to avoid mass interruption.
- **Health/liveness:** heartbeat + timeout detection (`context.WithTimeout`, missed-heartbeat counter). Distinguish "degraded" from "gone."
- Drive all of this first in `simnet` with injected failures; only then wire to real connections.

**What you'll learn (Go + systems)**
- `context` deadlines/timeouts for failure detection; `time.Timer`/`time.Ticker` for dwell and heartbeat.
- Designing for **partial failure** and idempotent reconfiguration — the essence of distributed systems, now in Go.
- Debouncing/hysteresis as a first-class control-loop concern (anti-thrash).

**Done when:** in `simnet`, killing a relay re-parents only its subtree and downstream nodes fail over to their backup within your target gap; hysteresis provably suppresses re-optimization under noisy-but-stable metrics. Then reproduce a single real relay kill with fast recovery.

**Where you'll get stuck:** distinguishing transient blips from real failures without either flapping or reacting too slowly; making reconfiguration idempotent so a late/duplicate instruction doesn't corrupt the graph.

### ✅ What shipped

- **Backup parents.** `overlay.assignBackups` computes a warm secondary parent per node under the invariant `B ∉ Subtree(ParentOf(u))` (which also rules out the sibling trap the weaker rule allows) and a hard fan-in cap, `BackupOvershootAllowance = 1`. Backup capacity is *not* reserved; the cap is what makes "a relay may serve at most one child over capacity at the instant of a failover" literally true. The root's direct children get **no** backup by construction — losing the root re-roots the whole subnet.
- **Peer-local failover, no coordinator round trip.** A child that sees its parent's `PeerConnection` reach `failed` — or sit `disconnected` for `ParentDisconnectGrace = 2s` — promotes its precomputed backup itself. It is **make-before-break** (the old parent stays open and receiving until the new one carries media) and asynchronous: `internal/media/reparent.go` is a state machine over an internal channel, because waiting for `Connected` on the goroutine that also routes the offer/answer would time out every time. `OK: true` requires **media**, not just ICE.
- **RTP continuity across the switch.** `TrackLocalStaticRTP` passes sequence and timestamp through untouched, so a second upstream on the same leg reads to pion's NACK/jitter interceptors as catastrophic loss. `internal/media/rewrite.go` holds a per-leg `(sequence, timestamp)` offset recomputed only at a `Switch()`, and drops packets until a VP8 keyframe lands.
- **Diff-and-apply replaced Phase 4's additive apply.** `diffTopology` is a pure diff of *(self, wanted tree, current reality)* — the baseline is **reality**, so a partially realized state converges. Seven buckets applied in a fixed order, legs added before removed. Three things force a full session re-create: the offerer role inverted, *our* relay-ness changed, and the *peer's* relay-ness changed.
- **Hysteresis.** Three health states (`healthy → degraded → gone`), thresholds derived from the cadence the *peer declares* (`Heartbeat.IntervalMs`) rather than a server constant, floored at the socket-detection window; a `DegradationDwell`; a `RecomputeCooldown`; and a `JoinSettle` that re-arms on every join. A `gone` node is never deleted — a later frame **resurrects** it, because the frame arriving is itself proof the socket is live.
- **Local repair without a bespoke repair function.** Stickiness (rank 1 of the builder's comparator) makes local repair fall out of the general `BuildTree`: survivors keep their incumbent parent, so only orphans move. One algorithm, one oracle.
- **Honest consequence, recorded rather than patched:** a stability-preserving builder is **path-dependent by construction**. Over 120 first-report permutations of one fleet the code converges to **9 distinct valid trees with an identical root**. The equality property the contract originally asserted is false and was replaced by three that are true — legality, a churn-bounded edge delta, and determinism given the same event *sequence*. See `DESIGN.md` §5.2.

---

## Phase 6 — Coordinator election + migration (server as arbiter) — ✅ done

**Goal / risk retired:** the control plane itself survives its owner leaving or weakening. This is the **hardest** part — the coordinator holds authoritative state and now that role must move.

**Go tasks**
- **Election, arbitrated by the central server** (no peer-to-peer consensus): peers report coordinator-fitness metrics (compute headroom, network reliability); the server picks and *announces* the coordinator. The server is the single source of truth for "who is coordinator," which is what prevents split-brain.
- **Promotion / demotion triggers:** current coordinator nearing capacity → promote a fitter peer; coordinator degraded → demote. All transitions go through the server.
- **State handover:** the outgoing coordinator's authoritative state (current graph, metric history, backup assignments) must transfer to the incoming one. Options to weigh: snapshot-and-ship vs. rebuild-from-peers-reporting-in. Handle the in-flight window where old and new both think they might be in charge — the server's announcement (a monotonically increasing **term/epoch** number) is the tiebreaker; peers ignore instructions from a stale epoch.
- **Coordinator-leaves case:** server detects loss, runs election, promotes next-best, new coordinator rebuilds/receives state, resumes the control loop.

**What you'll learn (Go + systems)**
- Leader election *without* rolling your own consensus — leveraging a trusted arbiter, and why that's the right call for flaky home nodes (name the trade-off vs. Raft explicitly).
- Epoch/term fencing to defeat split-brain and stale actors — the same idea real systems use, implemented small.
- State-transfer / handover design; reasoning about the unavoidable window of ambiguity.
- Advanced `context` propagation to cancel an entire in-flight control loop on role change.

**Done when:** in `simnet`, forcibly removing the coordinator triggers a server-arbitrated election, the successor takes over with correct graph state, stale-epoch instructions are rejected, and the media tree keeps functioning throughout. Then demo a real coordinator handover.

**Where you'll get stuck:** this is *the* budget-eater. The ambiguity window (two would-be coordinators), making handover atomic-enough, and testing all the interleavings. Lean hard on `simnet` and deterministic replay.

### ✅ What shipped

- **`internal/arbiter` is the single writer of the epoch.** `ms.epoch++` appears in exactly one place. Its `Run` loop has *no timer at all* — every action it can take needs a live peer, and a live peer is by definition heartbeating — and reads go through a closure executed on that goroutine, which is why no field in the package needs a mutex.
- **Two counters, two writers.** `Epoch` is the arbiter-minted coordinator *term*; `Rev` is the sitting coordinator's revision within it, resetting to 0 when the epoch advances. This is Raft's `(term, index)` split for the same reason: one counter cannot fence a handover *and* order one coordinator's trees without letting a writer forge the other's authority.
- **Authorization and ordering are separate functions.** `Topology.Supersedes` answers "which is more recent" and is **never** on the peer's apply path. `overlay.Fence.Accept(from, topo)` answers "may I apply this" and requires `t.Epoch == f.Epoch` — **exact match; a higher epoch is rejected**, because a peer may learn who is in charge only from the arbiter. The zero `Fence` obeys nobody. `AdoptAnnouncement` is the only method that may raise the epoch. (The contract's mandate that `Supersedes` be "the ONE place the fencing comparison is written" was a privilege-escalation bug in the *specification*: as literally written, a peer stamping `MaxUint64−1` on a self-computed tree would have been universally obeyed.)
- **State handover is rebuild-from-peers, and only that** — see §8 of `ARCHITECTURE.md` for the decision and its rejected alternative.
- **Three failure modes around the handover, each closed.** A lost announcement is repaired by a verbatim, unicast, uncapped re-announce on any heartbeat whose epoch lags (the condition is indefinite, so the repair must be). A demoted-but-live coordinator is cancelled by the same repair; a *partitioned* one stops its own loop after `GoneAfter` of arbiter silence. No eligible successor ⇒ the arbiter announces a **vacancy at a bumped epoch** and retains the role — the epoch is a fencing token, not a term counter, and bumping is the only way to tell a live-but-demoted coordinator to stop.
- **The observability surface** (`internal/dashboard` + `web/`) landed here rather than in Phase 7, because a handover you cannot watch is a handover you cannot demo. It is read-mostly and eventually consistent, and it labels **realized** (reconstructed from heartbeats) versus **intended** (the coordinator's last published tree) rather than averaging them — they genuinely disagree during convergence, and that gap is diagnostic.

> ### ⚠️ What Phase 6 does NOT demonstrate live
>
> `metrics.Report`'s `CPUPct`, `LossPct` and `RTTServerMs` are **never populated anywhere
> in production code** — `cmd/peer.sampleReport` fills in only `Name`, `UploadKbps`, `NAT`
> and `Coordinatable`, all from flags. There is no CPU sampler, no loss counter, and no
> RTT probe.
>
> The consequence is not cosmetic. With those three terms structurally zero, every
> eligible peer scores between **0.85 and 1.0** in `arbiter.Score` (the only varying term
> is uptime, worth at most 0.15). So `DemoteBelowScore = 0.35` can never be crossed and
> `PromoteMarginScore = 0.20` can never be met:
>
> **Voluntary promotion and demotion are unreachable in production. Live, the election
> reduces to bootstrap (a meet with members and no coordinator) and failover (the
> incumbent is not live), tie-broken by uptime then name.**
>
> The same gap makes the degradation machinery inert live — the dwell arms only on
> `LossPct ≥ 5`, `RTTServerMs ≥ 400` or `CPUPct ≥ 90` — and leaves `BuildTree`'s
> min-latency rank with no data, so attachment falls through to fewest-children then name.
> All of it is real, and all of it is exercised **only in `simnet`**, where the values are
> injected. This is a limitation of what the phase demonstrates, not a bug. Do not demo it
> as "elects the fittest machine"; it elects on the inputs it has. (`DESIGN.md` §8.1.)

---

## Phase 7 — Simulcast/SVC, TURN fallback, polish & demo — ⬜ not built

> **Read this before the task list below.** One of the four tasks — observability — was
> pulled forward into Phases 5–6 and reshaped; the other three were not started. Stated
> plainly:
>
> | Phase 7 task | Status |
> |---|---|
> | Simulcast / SVC (multi-layer send, per-downstream layer selection) | **not built.** A relay forwards one layer to every downstream. |
> | Adaptive per-edge layer selection under congestion | **not built.** Depends on simulcast. |
> | TURN via coturn; detect symmetric NAT and force those peers to leaves | **not built.** `overlay.NATRelayed` exists and is honoured by the builder, but it is a *declared* constraint (`peer -nat turn`), not a detection. No coturn, no TURN credentials in `ICEServers`. |
> | Observability: a `/debug` dashboard showing the live graph, coordinator identity/epoch, and failover events | **shipped in Phases 5–6, in a different shape** — the arbiter's `/api` REST + WS surface (`internal/dashboard`) plus a separate zero-build static frontend (`web/`) on GitHub Pages, with `deploy/` recipes for `wss://`. Not a server-rendered `/debug` page. |
> | Demo script (join to 6, kill a relay, kill the coordinator, watch recovery) | **not built** as a script. The behaviours are covered by the automated suite; a live multi-process run of Phases 5–6 has not been recorded (`DESIGN.md` §9.5). |
>
> The remaining text is the original plan, kept as the plan.


**Goal / risk retired:** address the real scarce resource (upload) and the real network (NAT), then make it demoable.

**Go tasks**
- **Simulcast / SVC:** senders emit multiple quality layers (VP8/VP9 simulcast, or SVC with VP9/AV1). Relays forward only the layer each downstream needs (adaptive layer selection based on downstream RTT/loss/requested resolution). Reference pion's **simulcast** example. This is what lets one relay serve heterogeneous downstreams cheaply.
- **Adaptive selection:** feed downstream metrics into per-edge layer decisions; drop to a lower layer on congestion, promote on recovery (with hysteresis, again).
- **TURN via coturn:** stand up `coturn`; add TURN creds to `ICEServers`; detect symmetric-NAT/CGNAT peers and force them to be **leaves**. Verify a TURN-only peer still participates as a leaf.
- **Observability:** a small `/debug` dashboard (or just structured `slog` → a viewer) showing the live graph, per-node upload, coordinator identity/epoch, and failover events. This is what sells the portfolio piece.
- **Demo script:** scripted scenario (join to 6, kill a relay, kill the coordinator, watch recovery) that shows the system self-healing.

**What you'll learn (Go + systems)**
- Layered media / SVC mechanics and per-consumer forwarding decisions.
- NAT traversal realities: STUN vs TURN, symmetric NAT, why some peers *cannot* relay.
- Building lightweight observability into a distributed Go service; turning a systems project into a *legible demo*.

**Done when:** a heterogeneous set of downstreams each receive an appropriate quality layer from a single relay; a TURN-only peer participates as a leaf; and the demo script visibly self-heals through relay and coordinator loss.

---

## Concepts ladder — what unlocks when

**Go idioms, progressively**

| Phase | Go concepts introduced |
|-------|------------------------|
| 0 | modules, packages, `internal/`/`cmd/` layout, `log/slog`, table-driven tests, tooling (`vet`, `golangci-lint`) |
| 1 | goroutines + channels + `select`, `context` lifecycle, `encoding/json` + struct tags, interfaces (pion types) in anger |
| 2 | `sync.RWMutex`, goroutine lifecycle & leak avoidance, `sync.WaitGroup`, mutex-vs-channel trade-off |
| 3 | implicit interfaces as extension points, fan-out concurrency, backpressure & error propagation |
| 4 | channel fan-in / worker pools, `time.Ticker`, pure-function design for testability, deterministic simulation testing |
| 5 | `context` timeouts/deadlines, `time.Timer` debouncing, idempotent reconfiguration, partial-failure design |
| 6 | epoch/term fencing, leader election via arbiter, state handover, cancelling in-flight control loops via `context` |
| 7 | layered media decisioning, integrating external infra (coturn), lightweight observability |

**Distributed-systems themes** (you already know these — mapped so you can point at where each shows up): control/data-plane split (0–4), greedy vs optimal under NP-hardness (4), hysteresis/anti-thrash control loops (5), partial failure & failover (5), leader election & split-brain avoidance via a fencing epoch (6), congestion-adaptive forwarding (7).

---

## Known hard parts / where you'll get stuck (be honest)

1. **ICE/DTLS not reaching `connected` (Phase 1).** Almost always a candidate not relayed or a firewall. Log every state transition on both peers before doing anything else.
2. **Keyframes/PLI in the relay (Phase 3).** New downstream = black video until a keyframe; you must forward PLIs upstream. This is the classic SFU footgun.
3. **Goroutine leaks (Phases 2–5).** Every per-connection goroutine needs a `context` and a join. Run with the race detector (`go test -race`) and watch goroutine counts.
4. **Anti-thrash tuning (Phase 5).** Too sensitive → constant re-optimization and stream interruptions; too sluggish → slow failover. There's no perfect constant; make the dwell configurable and test both extremes in `simnet`.
5. **Coordinator handover ambiguity (Phase 6).** The window where two nodes might think they're coordinator. This is the single biggest time sink. The epoch/fencing number is your friend; the deterministic simulator is your only sane test harness.
6. **Testing anything media-real is slow and flaky.** That's *why* `simnet` (Phase 4) exists — push every control-plane decision behind an interface so it can be tested without pion. If you skip the harness, Phases 5–6 will be miserable.

---

## References

- **pion/webrtc** — the pure-Go WebRTC stack; the centerpiece. Read the `examples/` directory: `save-to-disk`, `play-from-disk`, `data-channels`, and especially **`simulcast`**. https://github.com/pion/webrtc
- **pion/ion-sfu** — a real SFU built on pion; the reference for Phase 3's forwarding logic. https://github.com/pion/ion-sfu
- **Perfect negotiation pattern** — the polite/impolite peer approach to offer/answer glare (MDN WebRTC guide). Instructive background, but note it needs offer *rollback*, which pion v4 lacks — Phase 1 uses a glare-free single-offerer scheme instead.
- **coder/websocket** — modern, context-aware WebSocket library for signaling (formerly `nhooyr.io/websocket`). `gorilla/websocket` is the classic alternative. https://github.com/coder/websocket
- **coturn** — TURN/STUN server for the Phase 7 NAT fallback. https://github.com/coturn/coturn
- **Prior art to study for framing:** mediasoup, LiveKit, Janus (SFU designs); "SFU cascading" for multi-SFU forwarding; application-layer / overlay multicast literature for the tree-building intuition.
- **`log/slog`, `context`, `sync`, `time`** — read the stdlib docs directly; you'll use all four constantly.

---

*Stopping after Phase 3–4 already yields a novel, defensible portfolio project: an elected peer-SFU with a metrics-driven, simulation-tested graph builder. Phases 5–7 are the "hard mode" that turn it into a genuinely self-healing distributed system — and the best interview stories. Pick your depth deliberately.*
