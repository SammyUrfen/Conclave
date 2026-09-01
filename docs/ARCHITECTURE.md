# conclave — Architecture

> **What this document is.** The conceptual narrative and the trade-off record for conclave — *why* the system is shaped the way it is, not *how* to run it. If you want commands and milestones, read [`ROADMAP.md`](./ROADMAP.md); if you want package responsibilities, read the code. This file exists so that a future maintainer (probably future-me) can reconstruct the reasoning behind every load-bearing decision without re-deriving it.
>
> **Status:** living document, kept in sync with the roadmap. Ground truth as of **Phase 6 complete** — elected peer-SFU, coordinator computes the tree from telemetry, hysteresis + backup parents + peer-local failover, and coordinator election/migration under an arbiter-minted epoch fence. **Phase 7 (simulcast/SVC, TURN/coturn) is not built**; only its observability slice shipped, reshaped as the arbiter's `/api` surface plus a static Pages frontend.
>
> **Where this document sits.** It is the *original conceptual narrative* — the why, written before and during the build. [`DESIGN.md`](./DESIGN.md) is the post-build synthesis and is the better single explanation of the finished system; where the two differ in detail, `DESIGN.md` and the code win. This file has been reconciled against the code, and the places where the build superseded the plan are marked **Shipped** or struck through in prose.
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

> **NAT, briefly.** STUN gives most peers a directly reachable candidate. Peers that STUN can't punch through fall back to **TURN (coturn)**, which relays their media through a rendezvous server. STUN peers can be relays; TURN-bound peers cannot (see property 2).
>
> **Shipped status:** TURN was Phase 7 work and **Phase 7 was not built.** `overlay.NATRelayed` exists, is honoured by the builder (capacity 0 ⇒ forced leaf) and is exercised in `simnet` — but it is *declared* by the peer's `-nat turn` flag. There is no NAT classification, no coturn, and no TURN credential in `ICEServers`. Property 2 is enforced against an operator's claim, not a measurement.

---

## 6. Building the tree: degree-bounded, depth-limited, min-latency

The coordinator's core computation (`internal/overlay`) is a **pure function** — `BuildTree(nodes, prev, constraints) → Topology` — with no I/O, no clock and no randomness, so it is trivially unit-testable in the simulation harness. (`prev` is the stickiness baseline; it arrived in Phase 5 and is what makes local repair fall out of the general builder.) The package imports exactly five standard-library packages and nothing else at all — no pion, no `clock`, no other `internal/` package — and contains **no `range` over a map** in non-test code, so no ordering decision can depend on Go's randomised map iteration. It optimizes three constraints simultaneously:

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

**Shipped.** `overlay.BuildTree` is exactly this greedy heuristic, and the choice among candidate parents is a **frozen five-rank lexicographic comparator** rather than a weighted score:

```
rank 0   hard filters — attached, able to parent, within depth, spare capacity
rank 0b  impairment (SOFT: impaired candidates are re-admitted if no healthy one survives)
rank 1   INCUMBENCY — u's parent in prev wins unless a challenger beats it by > StickinessMs
rank 2   minimum RTT (unknown scores worst)
rank 3   fewest children (load balance)
rank 4   name ascending (determinism; no tie survives)
```

*A scored objective (`cost = α·rtt + β·churn + γ·load`) was rejected on explainability:* a lexicographic ordering answers "why is B parented to R?" by walking four rules, where a weighted sum needs three floats and their weights reconstructed — and with noisy inputs, the extra expressiveness is false precision.

`overlay.Validate` is an independent oracle the tests assert against, `overlay.ValidateLocalRepair` is a second oracle over *transitions*, and `overlay.PickRoot` is the root policy (sticky: an incumbent keeps the root unless a challenger beats it by `RootChangeMarginKbps = 2000`, one stream's worth). Capacity is integer children, `floor(effectiveUpload / StreamKbps)` where `effectiveUpload = UploadKbps × (1 − min(LossPct, 50)/100)`; a TURN-relayed node has capacity 0 and is a forced leaf.

Four honest details worth pinning down:

- **Where the coordinator runs.** Phase 4 hosted it in the central server as a documented stepping stone. Phase 6 kept that as a *mode* (`server -coordinate`) and added the migration (`server -elect`): the role can now be held by the arbiter process or by an elected peer, and it moves between them. What made that a wiring change rather than a rewrite is that `internal/coordinator` imports neither `signaling` nor `media` — every seam is a consumer-defined interface.
- **RTT is unpopulated live**, and so are loss and CPU (§10). `BuildTree` tolerates missing RTT — an unknown parent scores worst — so live, rank 2 has no data and attachment falls through to *fewest children*, i.e. load-balancing. `simnet` injects real latency matrices to exercise the min-latency path. A precise consequence: with no RTT, no challenger can beat an incumbent, so the **stickiness margin is inert live and the anti-thrash property is stronger live than in simulation**, while the min-latency property is only exercised in simulation.
- **A not-yet-reported peer is treated as a leaf** (`-default-upload-kbps 0`) until its first report proves capacity. It is also structurally barred from being rooted: `Node.Provisional` makes a provisional root *impossible*, not merely unlikely. The join settle is a composite — that invariant, plus a `JoinSettle = 1500ms` window that re-arms on every join, plus "everyone reported" as the fast path, plus `MinBuildableMembers = 2`.
- **~~Apply is additive~~ — superseded.** Phase 4's apply only connected new neighbours and never tore down a dropped one, which is why an unreported peer had to be treated conservatively. Phase 5 replaced it with **diff-and-apply**: `diffTopology` is a pure diff of *(self, wanted tree, current reality)* whose baseline is **reality**, not the previously pushed tree, so a partially realized state converges rather than diverging. Legs are added before they are removed ("a child must never lose a source it is about to regain"), and three conditions force a full session re-create rather than a patch — the offerer role inverted, *our* relay-ness changed, and the *peer's* relay-ness changed.
- **Stickiness makes the builder path-dependent, deliberately.** A builder whose output depends only on the current node set is a *memoryless* builder — that was Phase 4, and its memorylessness is the defect stickiness exists to fix. So two identical fleets reached by different histories can hold different, equally valid trees. Measured: 120 first-report permutations of one fleet converge to **9 distinct valid trees and 1 identical root**. Path-independence and minimal-disruption rebuilds are mutually exclusive; this project chose the second (`DESIGN.md` §5.2).

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

The answer is not a magic number; it's making the dwell **configurable** and testing both extremes deterministically in `simnet`.

**Shipped.** Three health states, not two: `healthy → degraded → gone`. Collapsing `degraded` into `gone` would make every GC pause a re-parenting event; collapsing it into `healthy` would leave the operator blind to a node about to fail. **Degraded is visible but not actionable** — it colours the dashboard and arms `DegradationDwell`, and never re-parents anything by itself.

Two details are load-bearing and were not obvious from the plan:

- **Thresholds multiply the cadence the *peer* declared, not a server constant.** `metrics.DegradedAfter(interval) = 3×interval` and `GoneAfter(interval) = 8×interval`, read from `Heartbeat.IntervalMs`. Hardcoding `3 × HeartbeatInterval` would declare a perfectly healthy peer running `-heartbeat 2s` dead every time.
- **…which opens a hole that a floor closes.** A peer running `-heartbeat 500ms` shrinks its own `GoneAfter` to 4 s, below the 7 s worst case for detecting a dead socket (`WSPingInterval + WSPingTimeout`). The *peer* breaks the invariant, so no flag validation can catch it; every per-node threshold is therefore floored at the socket-detection window. The inequality must run that way round — the health FSM must be the **slower** detector — or a peer gets ejected from the tree while the Hub still considers it present, with no event able to bring it back. The complementary rule: **`gone` never deletes a node's record**, and a heartbeat from a `gone` peer *resurrects* it, because the frame arriving is itself proof the socket is live.

**Backup parents, shipped, with the honest caveats.** Backup capacity is *not* reserved — at 4–8 peers on residential upload, halving usable fan-out to insure one recompute is a bad trade — but backup **fan-in is capped** at `BackupOvershootAllowance = 1`. Without that cap, one relay's death promotes its whole subtree onto whichever node they all named, so the overshoot would be the size of the failed subtree exactly when the survivors can least absorb it. The invariant is `B ∉ Subtree(ParentOf(u))`, which is stronger than the obvious `B ∉ Subtree(u)` and is what rules out the sibling trap (a sibling passes the weaker rule and is orphaned by the very same failure). Cost, stated plainly: in a tight fleet some nodes get **no backup at all**, and **the root's direct children never have one by construction** — with `-max-depth 2` that is a large fraction of the meet.

**Ratification, not correction.** When a peer reports a successful self-promotion, the coordinator patches its *working copy* so the chosen backup becomes the incumbent parent and *then* rebuilds. Rebuilding from the pre-failure tree would make stickiness see the *dead* parent as incumbent, find it ineligible, and re-choose freely — two interruptions where one was needed. It ratifies only a promotion reported against the currently published `(Epoch, Rev)`.

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

**Shipped — and the rule above is only half of it.** As stated, "ignore a *lower* epoch" is a privilege-escalation bug waiting to happen: it says nothing about a *higher* one, so any peer could promote itself by stamping a bigger number on a self-computed tree. (The frozen contract really did mandate exactly that, and the review caught it.) The code splits the two questions into two functions:

- **`Topology.Supersedes`** answers *"which of these two trees is more recent"* — a lexicographic `(Epoch, Rev)` comparison. Its call sites are enumerated in its doc comment and the enumeration *is* the safety property: the coordinator ordering its own successive trees, and the dashboard detecting a stale snapshot. **Never the peer's apply path.**
- **`overlay.Fence.Accept(from, topo)`** answers *"may I apply this"*, and accepts only if **all** of: the peer has been told who is in charge (`f.Epoch != 0`); the sender *is* that node (`from == f.CoordinatorID`, and `From` is server-stamped, so a peer cannot forge it); `topo.Epoch == f.Epoch` — **exact equality, a higher epoch is rejected**; and `topo.Rev > f.Rev`.

The third clause being equality rather than `>=` is the whole design: **a peer may learn about a new coordinator only from the arbiter, never from the node claiming the job.** The cost is one extra round trip on handover, bounded and well inside the rebuild window. Supporting rules: the **zero `Fence` is the unauthorised state** (a peer that has been told nothing obeys nobody); `AdoptAnnouncement` is the *only* method that may raise the epoch; `Applied` is separate from `Accept`, so a failed apply is retried by the next push rather than silently skipped; and `Reset()` — called on `TypeJoined` and nowhere else — is what closes the arbiter-restart hole, since a restarted arbiter has no meets, so every peer must rejoin, and rejoining drops its authority to zero.

Two counters, not one. **`Epoch`** is the arbiter-minted coordinator *term*; **`Rev`** is the sitting coordinator's revision within that term, starting at 1 and resetting to 0 when the epoch advances. A single counter cannot both fence handovers and order one coordinator's successive trees without letting one writer forge the other's authority — a coordinator able to mint the number could fence the arbiter's own announcement. This is **Raft's `(term, index)` split, for the same reason.**

**State handover — the decision, and what was rejected.**

| | **Snapshot-and-ship** | **Rebuild-from-peers** |
|---|---|---|
| How | The outgoing coordinator serializes {topology, per-node reports, dwell state, backups} and ships it via the arbiter | The new coordinator starts empty. Every peer, on adopting the new epoch, immediately sends an out-of-cycle heartbeat + report carrying its **realized** parent/children |
| Warm-up | Instant | ≤ `RebuildWindow` (3 s) |
| Works on a crash | **No.** Needs the outgoing node alive and cooperative — the case that matters least | **Yes.** Identical code path either way |
| New wire surface | A versioned snapshot message kept in sync with coordinator internals | **None.** Reuses `Heartbeat` + `Report`, already exercised every second |
| Fidelity | The old coordinator's *beliefs*, which may already disagree with what peers realized | Peers' *ground truth* |
| Failure mode | A path exercised only on graceful handover — i.e. almost never — silently rots | The only path, exercised on every handover including every test |

> **Shipped: rebuild-from-peers, and only that.** Snapshot-and-ship is not implemented, not even as a fast path. An earlier revision of this document said the pragmatic answer was "likely both"; that was wrong and the build rejected it.

The deciding argument is **not** the 3 seconds. It is that **a fallback path that only runs on crashes is a path that is never tested, and is therefore broken when you need it.** Building both means the graceful path gets all the exercise and the crash path gets all the bugs — in the phase whose entire purpose is surviving a crash.

The 3-second cost is also smaller than it looks, and this is the honest framing: **the data plane does not need the coordinator.** During the rebuild window every relay keeps forwarding, every peer keeps receiving, and every peer still holds the backup parent from the last published tree — so even a *parent failure during the rebuild window* is handled locally. What is suspended for ≤3 s is re-optimization, not the call.

One refinement matters enough to record, because it changed a stated limitation into the real one: the reconstruction is validated over the **reduced heard-from node set**, not the full roster. Validating over the full roster meant one member whose heartbeat was lost made the reconstruction "disconnected", failed `Validate`, and dropped to `prev = nil` — **a global re-parent caused by one dropped frame**, while the limitations section claimed the trigger was "heavy churn". Reduced, an unheard-from member is simply *absent from `prev`*, which the builder already handles: absent means "newcomer", and newcomers attach after incumbents. One lost heartbeat now moves one node instead of all of them.

On role change, the outgoing coordinator **cancels its entire in-flight control loop** — `context` cancellation through the coordinator's goroutines, plus dropping in-flight pushes — so a demoted node stops emitting promptly rather than racing the new one.

Because every interleaving of this is hard to reproduce with real media, **all of it is developed against `simnet`** — now over a virtual clock, driving the real `coordinator` loop — with injected failures and replay. Testing election on live cameras would be slow and flaky; the deterministic harness is the only sane way to cover the interleavings. **Honest status:** the handover is covered by the automated suite; a live multi-process demonstration of Phases 5–6 has not been recorded (`DESIGN.md` §9.5).

> **And a limit on what the election can actually decide live.** The promotion/demotion triggers described above — "nearing capacity", "degraded" — read `CPUPct`, `LossPct` and `RTTServerMs` from `metrics.Report`, and **none of those three is ever populated in production code**. Every eligible peer therefore scores 0.85–1.0 in `arbiter.Score`, so `DemoteBelowScore` cannot be crossed and `PromoteMarginScore` cannot be met. Live, the election reduces to **bootstrap** (a meet with members and no coordinator) and **failover** (the incumbent is not live), tie-broken by uptime then name. Voluntary handover is implemented and simnet-tested; it is unreachable without sensors. See §10.

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
| Hysteresis, backup parents, local repair (§7) | **Phase 5** ✅ | Backup assignment under `B ∉ Subtree(ParentOf(u))` with a fan-in cap; peer-local make-before-break failover; diff-and-apply; three health states with peer-declared, floored thresholds. Local repair falls out of stickiness rather than a bespoke `Repair()`. |
| Coordinator election, migration, epoch fencing (§8) | **Phase 6** ✅ | Single-writer epoch minting; `Fence.Accept` (exact-epoch) split from `Supersedes` (ordering); rebuild-from-peers handover; lost-announcement repair; vacancy at a bumped epoch. Verified by the automated suite, **not** by a live multi-process run. |
| Observability (the graph, epoch, failover events) | **pulled forward into 5–6** ✅ | Not the planned server-rendered `/debug` page: the arbiter's `/api` REST + WS surface (`internal/dashboard`) plus a separate zero-build static frontend (`web/`) on GitHub Pages. Read-mostly, eventually consistent, and it labels *realized* vs *intended* rather than averaging them. |
| Simulcast/SVC (the real upload fix), TURN | **Phase 7** ⬜ | **Not built.** A relay forwards one layer to every downstream; `NATRelayed` is a flag-declared constraint, not a detection. |

Stopping after **Phase 3 or 4** already yields a coherent, unusual portfolio piece: *an elected peer-SFU with a metrics-driven, simulation-tested graph builder.* Phases 5–6 were built; Phase 7 was not.

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
- **Bandwidth heterogeneity is not solved.** Phase 7 was not built: a relay forwards a single quality layer to all downstreams, so a slow downstream and a fast one get the same stream. Adaptive per-consumer layers remain the fix and remain unwritten.
- **The telemetry is mostly declared, not measured — and this has teeth.** In a live run, `cmd/peer.sampleReport` populates four of `metrics.Report`'s seven fields, all from flags: `Name`, `UploadKbps` (`-upload-kbps`, default 3000 — **there is no bandwidth probe**), `NAT` (`-nat` — **there is no NAT detection**), and `Coordinatable`. `RTTServerMs`, `LossPct` and `CPUPct` are **never populated anywhere in production code.** Three consequences, each bigger than "some fields are TODO":
  - *The degradation machinery cannot fire live.* The dwell arms only on `LossPct ≥ 5`, `RTTServerMs ≥ 400` or `CPUPct ≥ 90` — all structurally zero — so `DegradationDwell`, `Node.Impaired`, rank 0b's impairment filter and the loss-derated capacity are exercised **only in `simnet`**, where the values are injected.
  - *Voluntary election cannot fire live.* Every eligible peer scores `0.30 + 0.35 + 0.20 + 0.15·min(uptime/120, 1)` = **0.85 to 1.0**, so `DemoteBelowScore = 0.35` is uncrossable and `PromoteMarginScore = 0.20` unmeetable. The election reduces to bootstrap and failover, tie-broken by uptime then name. **Do not demo this as "elects the fittest machine."**
  - *Pairwise RTT is unmeasured*, so `BuildTree`'s min-latency rank has no live data and the stickiness margin is inert live (§6).
  The logic is real and tested; the sensors are not built. The genuinely measured signals are the heartbeat's realized state (parent, children, each edge's pion `PeerConnectionState`), the fence `(Epoch, Rev)`, the stale-rejection counter, and the upload meter's byte count.
- **No authentication, anywhere.** No token, password, credential, JWT or TLS termination in any non-test file. Anyone who can reach `/ws` can join any meet under any unused name; anyone who can reach `/api` can create meets and read every meet's telemetry. What exists instead: server-stamped identity, the epoch fence, an origin allow-list on both the WS upgrade and CORS, unguessable `crypto/rand` meet ids, and bounded state (`MaxMeets`, `MaxEndedMeets`, `maxSubscribersPerMeet`, a snapshot min-interval). The `-demo` routes — evict a peer, force an election — are a DoS primitive and are therefore **not registered at all** unless the flag is set: `404`, not `403`, because an unregistered route cannot be reached by a bug in a permission check.
- **The arbiter's state is entirely in memory.** Meets, the epoch counter, and the 20-entry tombstone ring do not survive a restart, and epochs restart at 1. The hole is closed by *behaviour* rather than persistence: a restarted arbiter has no meets, so every peer must rejoin, and `TypeJoined` resets each peer's fence to the unauthorised zero state.
- **Structural limits of the tree design.** The root's direct children have no backup, by construction, and with `-max-depth 2` that is a large fraction of the meet. Backup capacity is not reserved, so a failover can transiently oversubscribe a relay by one child. Trees are path-dependent, so two identical fleets reached by different histories may hold different valid trees — anyone comparing two runs and expecting identical topologies is applying the wrong invariant. And `PickRoot`'s `RootChangeMarginKbps = 2000` means a meet whose strongest machine arrives *second* runs permanently on its second-best relay; that is intended (re-rooting re-parents everyone) and it is a real cost.
- **The measurements that exist, and what they do not prove.** Phase 2's meter measured a per-stream bitrate; the "≈4.6 Mbit/s at 4 peers, ≈6 Mbit/s at 5" ceiling is **arithmetic on it**, not an observed cross-machine collapse — every live run so far has been multiple processes on one host over loopback, so there is no real NAT traversal, no real packet loss, and no real congestion control in any result. PLI *plumbing* is proven; keyframe *response* is not, because file and synthetic sources have no live encoder. There are **no benchmarks and no fuzz targets** in the repository. **This document proves a design is coherent; only a run proves the system performs.**
- **Where the truth lives.** Each phase-labelled section above has been reconciled against the code as of Phase 6. `docs/PLAN.md` is the frozen Phase 5–6 contract and is **historical** — it was amended six times during the build and several sections describe things that later changed. Where a doc, the contract, and the code disagree, **the code is the truth**; `DESIGN.md` §7 lists the places the contract was wrong.

*This is a living document. When a phase changes a decision recorded here, update the section and note what changed — the value of this file is that it stays true.*
