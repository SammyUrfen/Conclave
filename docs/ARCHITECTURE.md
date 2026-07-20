# conclave — Architecture

> **What this document is.** The conceptual narrative and the trade-off record for conclave — *why* the system is shaped the way it is, not *how* to run it. If you want commands and milestones, read [`ROADMAP.md`](./ROADMAP.md); if you want package responsibilities, read the code. This file exists so that a future maintainer (probably future-me) can reconstruct the reasoning behind every load-bearing decision without re-deriving it.
>
> **Status:** living document, kept in sync with the roadmap. Ground truth as of **Phase 4 complete** (elected peer-SFU + coordinator computes the tree from telemetry + `simnet` harness). Everything past Phase 4 (hysteresis/failover, election/migration, simulcast/TURN) is design intent, labelled by phase.
>
> **One-line framing:** an **SFU (Selective Forwarding Unit) that is *elected* from among the participants and can *migrate***.

---

## Contents

1. [The problem: why the obvious design dies at 4–5 peers](#1-the-problem-why-the-obvious-design-dies-at-45-peers)
2. [The idea: an elected, migratable peer-SFU](#2-the-idea-an-elected-migratable-peer-sfu)
3. [Control plane vs. data plane](#3-control-plane-vs-data-plane)
4. [Why hybrid: a central arbiter instead of consensus among home PCs](#4-why-hybrid-a-central-arbiter-instead-of-consensus-among-home-pcs)
5. [The overlay topology](#5-the-overlay-topology)
6. [Building the tree: degree-bounded, depth-limited, min-latency](#6-building-the-tree-degree-bounded-depth-limited-min-latency)
7. [The dynamic graph and hysteresis](#7-the-dynamic-graph-and-hysteresis)
8. [Coordinator election and migration](#8-coordinator-election-and-migration)
9. [Where the phases realize each idea](#9-where-the-phases-realize-each-idea)
10. [Non-goals and honest limitations](#10-non-goals-and-honest-limitations)

---

## 1. The problem: why the obvious design dies at 4–5 peers

The naive way to build a group video call is a **full mesh**: every participant opens a direct WebRTC connection to every other participant and uploads their own camera stream to each of them.

This is correct, has the lowest possible latency (one hop, peer to peer), and needs no server in the media path. It also collapses almost immediately. The reason is a single, unforgiving constraint.

**Upload bandwidth is the scarce resource.** Residential internet is asymmetric — a connection that downloads at 100 Mbps often uploads at 5–15 Mbps. Video, meanwhile, is expensive to *send* and cheap to *receive relative to what you're already receiving from everyone else*. So the binding constraint on every home participant is: **how many copies of my own stream can I afford to upload?**

In a full mesh with `N` participants, each peer uploads its stream to the other `N−1`:

```
per-peer upload      = (N − 1) × stream_bitrate
total room upload    = N × (N − 1) × stream_bitrate   → O(N²)
```

The per-peer cost grows **linearly** with the number of participants, and the room-wide cost grows **quadratically**. Neither is survivable on home upload.

Worked illustration (numbers below are *illustrative assumptions*, not measurements — the measurement is what Phase 2 exists to produce):

| Participants `N` | Copies uploaded `N−1` | Per-peer upload @ 2 Mbps/stream |
|---|---|---|
| 2 | 1 | 2 Mbps |
| 3 | 2 | 4 Mbps |
| 4 | 3 | 6 Mbps |
| 5 | 4 | 8 Mbps |
| 6 | 5 | 10 Mbps |

A peer on a ~6 Mbps upload link runs out of headroom at **`N−1 = 3`, i.e. around 4 participants** — beyond that, congestion control starves the encoder, frame rate drops, and quality visibly collapses. That's the "dies at 4–5 peers" ceiling, and it's not a tuning problem: it's `O(N)` per-peer upload meeting a fixed upload budget.

The classic fix is a **cloud SFU** (Selective Forwarding Unit): every peer uploads *one* copy to a well-provisioned server, and the server fans each stream out to everyone else. Per-peer upload drops to `1 × stream_bitrate`, independent of `N`. The server pays the `O(N²)` fan-out cost on a fat, symmetric pipe where bandwidth is cheap. This works — it's what mediasoup, LiveKit, Janus, and pion's own ion-sfu do — but it means **renting a server that sits in the media path for the whole call**.

conclave's wager is that we can get the SFU's upload economics *without* renting the SFU.

---

## 2. The idea: an elected, migratable peer-SFU

> Instead of renting a cloud SFU, **elect one from among the participants**, and let it **move** when the elected machine can no longer carry the load.

The insight from §1 is that the SFU's job is mostly **fan-out on a fat upload pipe**. Some participants *have* a fat-enough upload pipe — a wired desktop on a good connection can forward several streams. So:

- A few **strong peers** act as **relays**: they receive an origin's stream once and forward it to several downstream peers, exactly like a mini-SFU. The origin uploads *one* copy (to its relay); the relay pays the fan-out.
- One peer acts as the **coordinator**: it collects telemetry from everyone, **computes** the forwarding graph, and drives the signaling that realizes it.
- Because peers are flaky — they join, leave, sleep, get overloaded — the graph **re-optimizes** on churn, and the **coordinator role itself hands over** when the elected machine weakens or leaves.

That last clause is what makes this different from "just run ion-sfu on your desktop." The SFU is not a fixed piece of infrastructure; it is a **role assigned to a participant**, and both the *shape of the forwarding tree* and the *identity of the coordinator* are dynamic. The novel, defensible core of the project is precisely: **a participant can forward other participants' media (Phase 3), a coordinator can compute who forwards to whom from live metrics (Phase 4), and that coordinator role can migrate without split-brain (Phase 6).**

What this buys, and what it costs, is the rest of this document.

---

## 3. Control plane vs. data plane

The single most important structural decision: **the coordinator and the relays are different roles, wanting different resources, and must not be merged into one "super-node" abstraction.**

| | Control plane — **Coordinator** | Data plane — **Relays** |
|---|---|---|
| **Job** | Collect metrics, compute the forwarding graph, orchestrate signaling, watch health, arbitrate handover with the server | Receive an origin's media once, forward RTP to several downstream peers |
| **Scarce resource it needs** | Compute + a **reliable, low-jitter** control link | Raw **upload bandwidth** |
| **What kills it** | CPU starvation, unreliable/high-jitter network, going offline mid-decision | Saturated upload, forcing frame drops on everything it forwards |
| **Traffic profile** | Small, bursty JSON (metrics + signaling) | Large, sustained SRTP media |

These are **different resources**. A wired desktop with mediocre CPU but a fat symmetric pipe is a great relay and a poor coordinator. A quiet laptop with spare CPU and a rock-solid low-jitter connection but modest upload is a great coordinator and a poor relay. If you collapse both into one "super-node" score, you will keep electing the wrong machine for one of the two jobs.

Keeping them separate also **decouples their failure modes**:

- A relay that saturates its upload degrades *the streams it carries* — it must **not** be allowed to drag down graph-computation or election decisions. Those are control-plane concerns and should be insulated from media congestion.
- A coordinator that is busy thinking should **not** be conscripted into carrying a large share of the media just because it happens to be the coordinator.

A single node *may* wear both hats at once — in the diagram below, peer **A** is coordinator **and** a relay — but they remain **distinct hats with distinct promotion/demotion criteria**. The transport reinforces the split: **media is WebRTC (SRTP over UDP/ICE) between overlay neighbors; control and bootstrap are WebSocket/HTTP to the central server.** There is no media over WebSockets, and no control decisions riding inside the media path.

---

## 4. Why hybrid: a central arbiter instead of consensus among home PCs

conclave is **decentralized in the data plane, centralized in the control/bootstrap plane.** A small, always-up central Go server (`cmd/server`) handles room rendezvous, relays signaling, ingests metrics, and — crucially — **acts as the election arbiter and single source of truth for "who is the coordinator."**

The obvious alternative is to run a real consensus protocol (Raft, Paxos) *among the peers* so there's no central dependency at all. We deliberately do **not** do that. Here is the trade-off named in both directions.

**Consensus among home PCs (Raft/Paxos) — rejected.**

- ✅ Fully decentralized; no central node to depend on or pay for.
- ❌ Home PCs are the **worst possible** quorum members: they churn constantly, sit behind NATs, have asymmetric links, and suspend/sleep without warning. Consensus assumes a stable-ish membership set; ours is defined by instability.
- ❌ It solves a problem we don't have. Raft's entire purpose is agreeing on a leader *without a trusted node*. But we already operate **one reliable node** — the bootstrap server that peers must contact anyway to join. Paying full consensus overhead to avoid using a node we're already depending on is wasted complexity.
- ❌ It's a large, subtle, time-devouring implementation that is **orthogonal to the learning goals** of this project. The interesting problems here are media forwarding and graph dynamics, not re-deriving Raft.

**Central server as arbiter — chosen.**

- ✅ **Delegates the consensus problem to the one node that is actually reliable.** With a single writer for "who is coordinator," there is no distributed agreement to reach — the server *decides* and *announces*. This is what structurally prevents **split-brain**.
- ✅ The fencing mechanism (§8) becomes trivial: a single writer can hand out a **monotonically increasing epoch/term** with no coordination.
- ✅ It's cheap. The server carries only signaling + telemetry (small JSON), never media, so it stays up on modest hardware and never becomes the `O(N²)` bottleneck a cloud SFU would.
- ❌ It is a **single point of failure for the *control* plane.** If the server is down, no new peers can join, no re-optimization or election can happen, and no handover can complete.
- ⚠️ **But — and this is the point — it is not a single point of failure for the *media* plane.** The data plane is decentralized: an already-established relay tree keeps forwarding media even while the server is unreachable, because media flows peer-to-peer over WebRTC and never touches the server. The server is a **coordination** SPOF, not a **conversation** SPOF. For a learning/portfolio system, an always-up cheap coordinator we can reason about beats a fragile home-grown consensus we can't.

Slogan, worth internalizing: **decentralized data plane, centralized control/bootstrap.**

---

## 5. The overlay topology

The overlay is a **shallow tree** (more precisely a forest of shallow relay trees) rooted near the coordinator, with the central server sitting *outside* the media path as the control/bootstrap hub.

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

Three properties are load-bearing:

**1. The tree is deliberately shallow.** Latency accumulates *per hop*: an `A→B→C→D` chain adds every intermediate leg's delay end to end. Rough budget — **~150 ms one-way is good, ~400 ms is the tolerable ceiling** for interactive conversation. So deep chains are the *enemy*, not a scaling win. The target shape is **a few strong relays, with most peers exactly one hop from a relay** (initial depth bound ≤ 2). We trade some relays' upload load for shallow depth; we do *not* trade latency for a prettier fan-out.

**2. TURN-bound peers are forced to be leaves.** A peer behind symmetric NAT or CGNAT can only be reached via a **TURN relay** (see NAT note below). Such a peer is already path- and bandwidth-constrained — every packet to and from it detours through the TURN server. Making it a *relay* would compound that penalty on everyone downstream of it, so a TURN-only peer is **always a leaf, never a relay.** This is a hard constraint the tree builder must respect, not a preference.

**3. Each overlay edge is one WebRTC peer connection.** Audio, video, and a data channel are multiplexed over a single `RTCPeerConnection` per neighbor. The central server never appears on an `══` edge.

> **NAT, briefly.** STUN gives most peers a directly reachable candidate. Peers that STUN can't punch through fall back to **TURN (coturn)**, which relays their media through a rendezvous server. STUN peers can be relays; TURN-bound peers cannot (see property 2). TURN is Phase 7 work.

---

## 6. Building the tree: degree-bounded, depth-limited, min-latency

The coordinator's core computation (`internal/overlay`, Phase 4) is a **pure function** — `BuildTree(nodes, constraints) → Graph` — with no I/O, so it is trivially unit-testable in the simulation harness. It optimizes three constraints simultaneously:

| Constraint | Meaning | Why |
|---|---|---|
| **Degree-bounded** | A relay's max children = `spare_upload ÷ stream_cost` | A relay can only forward as many copies as its upload budget allows — the §1 constraint, applied per relay |
| **Depth-limited** | Every peer within `≤ maxDepth` hops of the root | Latency accumulates per hop (§5); bound the depth, bound the worst-case delay |
| **Min-latency** | Prefer attaching a peer to the lowest-RTT parent | Minimize the delay each peer actually experiences |

Plus the hard constraint from §5: **TURN-bound peers are forced leaves.**

**Why greedy, and what we give up.** The exact version of this — build a spanning/Steiner tree that is simultaneously degree-bounded, depth-limited, and latency-minimal — is **NP-hard**. An optimal solve would need ILP or branch-and-bound, which does not belong in a real-time control loop that must re-run on every join, leave, and degradation event.

So we use a **greedy heuristic**: *sort candidate parents by RTT; attach each peer to the lowest-latency relay that has spare upload capacity and keeps it within the depth bound; TURN peers go straight to leaves.* It runs in **milliseconds**.

Naming the trade-off honestly:

- **What greedy costs us:** the resulting tree can be *suboptimal* — a peer might land a hop deeper, or on a slightly-higher-RTT parent, than a globally optimal placement would give it. Greedy is order-dependent and can paint itself into a corner (fill a nearby relay, then force a later peer onto a distant one).
- **Why that's the right call anyway:** (a) it's fast enough to run inside a control loop reacting to churn; (b) it's deterministic and easy to reason about and test; (c) — the honest kicker — **the inputs are noisy.** RTT and upload estimates jitter constantly. Optimizing hard against noisy measurements is *false precision*: you'd burn compute finding the "optimal" tree for numbers that are wrong by the time you finish. A fast, good-enough tree that we re-run on real changes beats a slow, "optimal" tree fit to noise.

This is the same instinct as any greedy-vs-exact call in scheduling or routing: when the objective is fuzzy and the input churns, a cheap heuristic you can re-run beats an expensive optimum you can't.

**Realized (Phase 4).** `overlay.BuildTree` is exactly this greedy heuristic (attach strongest-upload nodes first so they become the relays; each remaining node to the eligible parent with min RTT, then fewest children, then name); `overlay.Validate` is an independent oracle the tests assert against, and `overlay.PickRoot` is the default root policy (highest-upload non-TURN node). Three honest details worth pinning down:

- **The coordinator runs *inside the central server* in Phase 4** — a documented stepping stone. It is already the always-up fan-in point, so hosting the role there defers coordinator-handover to Phase 6, where the role migrates to an elected peer and the server becomes pure arbiter (§8).
- **RTT is unpopulated live** (measuring pairwise RTT before peers connect is a chicken-and-egg the coordinate-system/probe work of Phase 7 solves). `BuildTree` tolerates missing RTT — an unknown parent scores worst, so with no RTT the tie-break becomes *fewest children*, i.e. **load-balancing** attachment, which is the right default on a LAN. `simnet` injects real latency matrices to exercise the min-latency path.
- **A not-yet-reported peer is treated as a leaf** (default upload 0) until its first report proves capacity — conservative, because Phase 4's apply is *additive* (it connects new neighbours but does not tear down a dropped one), so a transient wrong-relay tree could not be fully undone. Recompute fires only on a threshold event (join/leave/first report); full hysteresis on degradation is §7 / Phase 5.

---

## 7. The dynamic graph and hysteresis

The tree from §6 is not computed once — the world changes. But it must **not** be recomputed on every metric wiggle, because rebuilding an edge means tearing down and re-establishing a WebRTC connection, which **interrupts the stream**. Constant re-optimization = constant stream interruptions. That failure mode is called **thrashing**, and avoiding it is a first-class design concern.

**Re-optimize only on threshold events.** There are exactly three:

1. A peer **joins**.
2. A peer **leaves**.
3. **Sustained** degradation of a link over `N` seconds — *not* a momentary spike.

The mechanism that enforces "sustained" is **hysteresis** (a dwell/debounce): a degradation must persist past a **dwell timer** before it counts as real. In Go this is a `time.Timer` that gets **reset on every good sample** — only if the timer actually fires (the badness held long enough) does re-optimization trigger. A single bad RTT sample resets nothing; a *run* of bad samples does. (This is the same debouncing you'd reach for in any control loop or input handler — Go just spells it with `time.Timer` and a `select`.)

**The tuning is a genuine trade-off with no perfect constant:**

- Dwell **too short** → the system reacts to transient blips, thrashes, and interrupts streams for no reason.
- Dwell **too long** → real failures take too long to notice, and failover is sluggish.

The answer is not a magic number; it's making the dwell **configurable** and testing both extremes deterministically in `simnet` (Phase 5).

Two more principles keep churn cheap:

**Local repair — move one subtree, not the world.** When a relay dies, re-parent **only its orphaned subtree**, not the entire graph. A global rebuild would interrupt streams for peers who had nothing to do with the failure. Reconfiguration must also be **idempotent**, so that a late or duplicate "connect to parent P" instruction — always possible in a distributed control loop — can't corrupt the graph.

**Warm backup parents — failover in a keyframe, not a reconnection storm.** For each node, the coordinator precomputes a **secondary parent** ahead of time. When the primary parent fails, the node fails over to the already-decided backup *while* the coordinator recomputes the tree in the background. The media gap is bounded by roughly one keyframe interval instead of a full detect-recompute-signal-reconnect round trip. The cost is modest: a little extra precompute and a designated (kept-warm) standby edge per node — cheap insurance against the churn that home networks guarantee.

---

## 8. Coordinator election and migration

> This is the **hardest engineering problem in the project** (Phase 6). The coordinator holds authoritative state — the current graph, metric history, backup assignments — and that authority now has to *move* between machines while the call keeps running. Budget for it accordingly.

**Lifecycle.** The room creator is coordinator #1. From there the central server, as arbiter, drives every transition:

- **Promotion:** as the current coordinator nears capacity (compute/reliability headroom shrinking), a fitter peer is promoted.
- **Demotion:** a degraded coordinator is demoted before it fails.
- **Failure:** if the coordinator leaves outright, the server detects the loss, runs an election, and promotes the next-best candidate.

Election fitness is scored on **control-plane** qualities (compute headroom, network reliability), *not* upload — per §3, the coordinator and relay roles are scored separately. **The server picks and announces; peers do not vote among themselves.** That's the §4 decision cashed out: delegating agreement to the one reliable node is exactly what removes split-brain from the election.

**The ambiguity window is the real enemy.** Handover is not instantaneous. There is unavoidably a window where the *outgoing* coordinator hasn't fully stopped and the *incoming* one hasn't fully started — and during it, **two nodes might each believe they are in charge.** If both issue graph instructions, peers get contradictory orders and the tree corrupts. You cannot make this window zero; you can only make it *safe*.

**Epoch/term fencing makes it safe.** Every coordinator announcement from the server carries a **monotonically increasing epoch (term) number.** The rule peers enforce:

> **A peer obeys instructions only from the current epoch, and ignores anything stamped with a stale (lower) epoch.**

So when the server promotes a new coordinator, it bumps the epoch. The new coordinator's instructions carry the new, higher epoch; any straggling instruction from the old coordinator still carries the old, lower one and is **rejected on arrival.** A stale actor cannot do damage no matter how confused it is about its own status — the fence, not perfect timing, is what guarantees correctness. This is the same **fencing-token** idea used by distributed locks and the *term* in Raft; here it's cheap because a single writer (the server, §4) hands out the epochs with no coordination needed.

**State handover — two options, both on the table:**

| Approach | How | Trade-off |
|---|---|---|
| **Snapshot-and-ship** | Outgoing coordinator serializes its authoritative state and transfers it to the incoming one | Faster warm-up, but relies on the outgoing node being alive and cooperative — useless if it *crashed* |
| **Rebuild-from-peers** | New coordinator reconstructs state from peers re-reporting their metrics | Survives a hard crash of the old coordinator, but there's a rebuild window before the new one has a full picture |

The pragmatic answer is likely **both**: snapshot-and-ship on a graceful, planned handover; rebuild-from-peers as the fallback when the coordinator died without warning.

On role change, the outgoing coordinator must also **cancel its entire in-flight control loop** — in Go, `context` cancellation propagated through the coordinator's goroutines, so a demoted node stops emitting instructions promptly rather than racing the new one. (Framed for a systems engineer: this is leader failover with fencing — a problem you've solved before; Go just gives you `context.Context` and a monotonic epoch as the primitives.)

Because every interleaving of this is hard to reproduce with real media, **all of it is developed against `simnet`** (Phase 4's deterministic in-memory network) with injected failures and replay, *then* demonstrated once on a real handover. Testing election on live cameras would be slow and flaky; the deterministic harness is the only sane way to cover the interleavings.

---

## 9. Where the phases realize each idea

Each concept above is de-risked by a specific, runnable phase. Full detail lives in [`ROADMAP.md`](./ROADMAP.md); this is the concept → phase index.

| Concept (this doc) | Realized in | What that phase proves |
|---|---|---|
| Signaling + the media pipeline works at all | **Phase 1** | Two peers establish a WebRTC call through the signaling server |
| The mesh ceiling (§1), upload as `O(N)` per peer | **Phase 2** | Build the mesh, *measure* where it breaks, quantify the ceiling |
| A participant can forward others' media — the peer-SFU (§2) | **Phase 3** ⭐ | A leaf receives another peer's media *forwarded by a relay*; only the relay's upload scales |
| Control/data split + coordinator computes the tree (§3, §6) | **Phase 4** | Metrics fan-in; greedy `BuildTree` runs from live telemetry; `simnet` harness exists |
| Greedy tree builder, testable pure function (§6) | **Phase 4** | Property tests: depth ≤ bound, no relay over-subscribed, TURN nodes are leaves |
| Hysteresis, backup parents, local repair (§7) | **Phase 5** | Killing a relay re-parents only its subtree; failover within a keyframe; thrash suppressed |
| Coordinator election, migration, epoch fencing (§8) | **Phase 6** | Server-arbitrated handover; stale-epoch instructions rejected; tree survives coordinator loss |
| Simulcast/SVC (the real upload fix), TURN, demo | **Phase 7** | One relay serves heterogeneous downstreams cheaply; TURN-only peer participates as a leaf |

Stopping after **Phase 3 or 4** already yields a coherent, unusual portfolio piece: *an elected peer-SFU with a metrics-driven, simulation-tested graph builder.* Phases 5–7 are hard mode.

---

## 10. Non-goals and honest limitations

This is a **learning / portfolio** project, not a product. Being explicit about what it is *not* is part of the design.

**Non-goals (deliberately out of scope):**

- **Not shipping to real users.** No accounts, no persistence, no billing, no SLA. Optimize for understanding, not uptime.
- **Not peer-to-peer consensus.** We will *not* run Raft/Paxos among peers; the central server is the arbiter, on purpose (§4).
- **Not a fully decentralized system.** Bootstrap, signaling, and election are centralized. Only the *media/data plane* is decentralized.
- **Not media-layer security beyond WebRTC's defaults.** SRTP/DTLS encrypt media in transit; end-to-end encryption *through relays* (so a relay can't observe the media it forwards) is out of scope. **A relay can see what it forwards.**
- **Not optimal graphs.** The tree is a greedy heuristic, knowingly suboptimal (§6).
- **Not codec/transcoding work.** Relays forward RTP without re-encoding; there is no server-side transcoding.

**Honest limitations (true even where in scope):**

- **The central server is a control-plane SPOF.** If it's down, no joins, no re-optimization, no election, no handover. Existing media keeps flowing (§4), but the room can't adapt. We accept this trade for the split-brain safety it buys.
- **Trust model.** A relay is a *participant's machine* forwarding other participants' media. Participants must trust relays not to be malicious. There is no Byzantine-fault tolerance — a lying or hostile peer (fabricated metrics, dropped media) is **not** defended against. The epoch fence (§8) defeats *stale* actors, not *dishonest* ones.
- **Bandwidth heterogeneity is only partly solved before Phase 7.** Until simulcast/SVC lands, a relay forwards a single quality layer to all downstreams; a slow downstream and a fast one get the same stream. Adaptive per-consumer layers are the Phase 7 fix.
- **The mesh-ceiling and latency numbers are targets/illustrations, not measurements** — the `2 Mbps/stream` figure in §1 and the `150/400 ms` budget in §5 are working assumptions. The *real* numbers are what Phase 2 (mesh ceiling) and later phases exist to produce. **This document proves a design is coherent; it does not prove the system performs.** Only a run does that.
- **Everything past Phase 0 is design intent.** As of this writing the codebase serves `/healthz` and structured logs and nothing else. Treat phase-labelled sections as *plans*, and this file as the record of *why* — to be reconciled against reality as each phase actually lands.

*This is a living document. When a phase changes a decision recorded here, update the section and note what changed — the value of this file is that it stays true.*
