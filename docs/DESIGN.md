# conclave — Design Document

> **What this is.** The single document that explains conclave end to end: what it does, how
> it is built, and *why* each load-bearing decision went the way it did — including the ones
> that were wrong the first time. It is written for an engineer who has never seen the code.
>
> **Companions.** [`ROADMAP.md`](./ROADMAP.md) is the phased build order and the learning
> goals. [`ARCHITECTURE.md`](./ARCHITECTURE.md) is the original conceptual narrative.
> [`PLAN.md`](./PLAN.md) is the ~5,000-line Phase 5–6 architecture contract that the
> implementation was built against, with a §15 amendment log recording every ruling. This
> document supersedes none of them; it is the synthesis, and where it disagrees with the
> contract it is because **the code disagrees with the contract**, which is called out
> explicitly in §7.
>
> **Honesty rule.** Every claim here is checkable in the repository. Where something is
> designed but unmeasured, unbuilt, or unproven, §8 says so.
>
> **Status.** Phases 0–6 of 7 implemented, plus the telemetry sensors that Phase 6's
> election logic was written against (§8.1). Phase 7 (simulcast/SVC, TURN) untouched.
> 48,126 lines of Go: 20,061 non-test, 28,065 test. Numbers in §9 predating the sensor
> work were taken at `8bacee6` on `integration/phase5`.

---

## Contents

1. [What this is](#1-what-this-is)
2. [The architecture](#2-the-architecture)
3. [File and package responsibilities](#3-file-and-package-responsibilities)
4. [How it works, end to end](#4-how-it-works-end-to-end)
5. [The design trade-offs](#5-the-design-trade-offs)
6. [What testing this actually required](#6-what-testing-this-actually-required)
7. [Where the contract was wrong](#7-where-the-contract-was-wrong)
8. [Limitations](#8-limitations)
9. [Numbers](#9-numbers)

---

## 1. What this is

conclave is a group video-calling system whose **SFU is elected from among the participants
and can migrate to another participant mid-call**. There is a central server, but it never
carries a byte of media: it is a rendezvous point, a signaling relay, and an election
arbiter. All video flows peer-to-peer over WebRTC.

### 1.1 Why the obvious design dies at 4–5 peers

The naive group call is a **full mesh**: every participant opens a direct WebRTC connection
to every other and uploads their own camera stream to each. It is correct, it is
lowest-latency (one hop), and it needs no media server. It also collapses almost immediately,
for one reason: **residential upload is the scarce resource.** A home connection that
downloads at 100 Mbit/s often uploads at 5–15 Mbit/s, and video is expensive to *send*.

In a mesh of `N` participants each peer uploads `N−1` copies of its own stream:

```
per-peer upload   = (N − 1) × stream_bitrate      → O(N)   per peer
room-wide upload  = N × (N − 1) × stream_bitrate  → O(N²)  in aggregate
```

Phase 2 built the mesh and instrumented it rather than arguing about it. `internal/media`
carries an **upload meter** — a lock-free `sync/atomic` byte counter sampled once a second,
counting only bytes that actually reached the wire — so the aggregate send rate per peer is a
logged number rather than an assumption. On a live 4-peer demo it logged
`peers=3 kbit_per_sec=184.3 kbit_per_sec_per_peer=61.4` with synthetic frames.

**Be precise about what that measures.** The per-stream bitrate is measured; the collapse
point is *arithmetic on it*. Real 720p VP8 runs **≈1.5 Mbit/s per stream**, so per-peer upload
is ≈**4.6 Mbit/s at 4 participants** and ≈**6 Mbit/s at 5** — a typical home uplink, entirely
spent, before anything else on the machine sends a packet. That is `(N−1) × measured
per-stream rate`, not an observed cross-machine failure: the demo ran four processes on one
host over loopback, where there is no uplink to saturate. The honest claim is *"the linear
cost is measured and the ceiling follows from it"*, not *"we watched a 5-peer mesh fall
over."*

This is not a tuning problem either way. It is `O(N)` per-peer upload meeting a fixed upload
budget, and the only fixes are structural.

### 1.2 What an SFU changes, and what an *elected* SFU changes

The industry fix is a **cloud SFU** (Selective Forwarding Unit): every peer uploads *one*
copy to a well-provisioned server, and the server fans each stream out to everyone else.
Per-peer upload becomes `1 × stream_bitrate`, independent of `N`. The server pays the `O(N²)`
fan-out on a fat symmetric pipe where bandwidth is cheap. This is what mediasoup, LiveKit,
Janus, and pion's own ion-sfu do — and it means renting a machine that sits in the media path
for the whole call.

conclave's wager: **get the SFU's upload economics without renting the SFU.**

The observation is that an SFU's job is mostly fan-out on a fat upload pipe, and *some
participants already have one*. A wired desktop can forward several streams. So:

- A few strong peers become **relays**: they receive an origin's stream once and forward its
  RTP to several downstream peers, with no re-encode. The origin uploads one copy; the relay
  pays the fan-out.
- One peer becomes the **coordinator**: it ingests everyone's telemetry, computes the
  forwarding tree, and pushes it out.
- Because peers are flaky — they join, leave, sleep, saturate — the tree **re-optimizes on
  churn**, and the coordinator role itself **hands over** when the elected machine weakens or
  disappears.

That last clause is what makes this different from "run ion-sfu on your desktop." The SFU is
not infrastructure; it is a **role assigned to a participant**, and both the shape of the
forwarding tree and the identity of the coordinator are dynamic. The defensible core is
precisely: a participant forwards other participants' media; a coordinator computes who
forwards to whom from live metrics; and that coordinator role migrates without split-brain.

### 1.3 What it costs

Stated up front, because the rest of the document is about paying these bills:

| Cost | Where it is paid |
|---|---|
| Latency accumulates per hop | Depth bound (`-max-depth`, default 2); ~150 ms one-way is good, ~400 ms is the ceiling |
| A relay's failure orphans its whole subtree | Precomputed backup parents + peer-local failover (§4.2, §5.9) |
| Re-optimizing the tree interrupts streams | Stickiness in the builder + a cooldown + a settle window (§5.2) |
| A migrating coordinator could split-brain | Arbiter-minted epochs and a peer-side fence (§5.4, §5.11) |
| Home peers are unreliable quorum members | The arbiter is a single writer instead of Raft (§5.7) |

---

## 2. The architecture

### 2.1 Control plane vs data plane

The single most important structural decision: **the coordinator and the relays are
different roles, wanting different resources, and they are never merged into one
"super-node" score.**

| | Control plane — **coordinator** | Data plane — **relay** |
|---|---|---|
| Job | Ingest telemetry, compute the tree, push it, watch health | Receive one copy of a source's media, forward RTP to several children |
| Scarce resource | CPU + a reliable, low-jitter control link | Raw upload bandwidth |
| What kills it | CPU starvation, jitter, going offline mid-decision | Saturated uplink, dropping frames on everything it carries |
| Traffic | Small bursty JSON over WebSocket | Large sustained SRTP over UDP |

A wired desktop with mediocre CPU and a fat pipe is a great relay and a poor coordinator. A
quiet laptop with spare CPU and a rock-solid link is the reverse. Collapse them into one
score and you will keep electing the wrong machine for one of the two jobs.

This is enforced in three places, not merely asserted:

1. `arbiter.Score` consumes **only** control-plane inputs — CPU-free %, RTT to server, loss
   %, uptime, NAT class, and the peer's own willingness. `UploadKbps` does not appear in the
   formula. The test that enforces this is worth noting for its shape: it reflects over
   `arbiter.Fitness`'s **fields** and fails if any is named `*upload*` or `*bandwidth*`,
   because — as its comment says — *"asserting 'a large UploadKbps changes nothing' is
   impossible to express as a value test precisely because the field must not exist, so the
   assertion is made against the type instead."*
2. `overlay.BuildTree` consumes **only** data-plane inputs — upload budget, pairwise RTT, NAT
   class, loss, impairment. It does not take "who is the coordinator" as a parameter and has
   no way to learn it.
3. A peer's role is a **set**, not a scalar. A node may be coordinator *and* relay; the
   dashboard renders two independent badges.

The transport reinforces the split: **media is WebRTC (SRTP over UDP/ICE) between overlay
neighbours; control is WebSocket/HTTP to the central server.** No media over WebSockets, no
control decisions riding inside the media path.

### 2.2 The four roles

| Role | Who | What it owns |
|---|---|---|
| **arbiter** | the central server, `cmd/server` | Meet registry, its own liveness view, **epoch minting**, coordinator election, the announcement. Never a relay. |
| **coordinator** | the arbiter process (Phase 5) or an elected peer (Phase 6) | Telemetry fan-in, the health FSM, tree computation, the topology push. Mints `Rev`. |
| **relay** | any peer with ≥1 child | Forwards other peers' RTP, translates PLI upstream, rewrites RTP continuity across an upstream switch. |
| **leaf** | any peer with 0 children | Sends its own stream to its parent; receives everything else from it. |

Vocabulary note: a **meet** is a room (`roomID` in Go, `meet_id` on the HTTP surface — the
same string, character for character), and a meet's relay tree is its **subnet**.

### 2.3 The picture

```
                        ┌────────────────────────────────────┐
                        │  ARBITER  (cmd/server)             │   control/bootstrap only
                        │  • meet registry + rendezvous      │   never in the media path
                        │  • WS signaling relay              │
                        │  • liveness view of its own        │
                        │  • MINTS THE EPOCH                 │
                        │  • elects the coordinator          │
                        │  • dashboard HTTP + WS  ───────────┼──▶ browser (read-mostly UI,
                        └────────────────────────────────────┘     never carries media)
                           ▲          ▲            ▲       ▲
        WebSocket:         │          │            │       │
        signaling,         │          │            │       │
        metrics,           │          │            │       │
        heartbeats,        │          │            │       │
        announcements      │          │            │       │
                           │          │            │       │
                    ┌──────┴───┐  ┌───┴────┐  ┌────┴───┐  ┌┴───────┐
                    │    A     │  │   B    │  │   C    │  │   D    │
                    │coordinator│ │        │  │        │  │        │
                    │ + relay  │  │ relay  │  │  leaf  │  │  leaf  │
                    └──────┬───┘  └───┬────┘  └────────┘  └────────┘
                           │          │
             ══════════════╪══════════╪═══════════════════════════════
              MEDIA        │          │      WebRTC / SRTP over UDP
              (never       │          │      never touches the server
               touches     ▼          ▼
               the      ┌─────┐   ┌───────┐
               server)  │  B  │   │ C   D │      A is root: it forwards its own
                        └─────┘   └───────┘      stream and everyone else's down
                                                  the tree; B re-fans to C and D
```

Two things to read off this diagram:

- **A wears two hats.** It is the coordinator (control) and the root relay (data). Those are
  independent facts with independent promotion and demotion criteria; A can lose either one
  without losing the other.
- **The double rule is the SPOF boundary.** If the arbiter dies, no new peer can join, no
  re-optimization happens, and no handover can complete — but every established relay keeps
  forwarding and the call keeps working. The arbiter is a **coordination** single point of
  failure, not a **conversation** one.

### 2.4 Why control and data roles are deliberately not merged

Beyond the resource argument, merging them couples failure modes that must stay decoupled:

- A relay saturating its uplink degrades *the streams it carries*. It must not be allowed to
  drag down graph computation or election decisions.
- A coordinator busy thinking must not be conscripted into carrying media just because it
  happens to hold the role.

The mechanism that keeps this honest is that the two decisions read **disjoint input sets**
(`arbiter.Fitness` vs `overlay.Node`) and live in packages that cannot import each other.
`coordinator → arbiter` is a forbidden edge in the dependency DAG, so "just read the fitness
score in the tree builder" is a compile error, not a code-review conversation.

---

## 3. File and package responsibilities

### 3.1 The dependency DAG

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

Exhaustive edge list, which is what you actually check against:

| Package | May import (internal) |
|---|---|
| `logging`, `clock`, `policy`, `overlay` | — (leaves) |
| `metrics` | `overlay`, `clock`, `logging` |
| `signaling` | `logging`, `clock`, `policy` |
| `coordinator` | `overlay`, `metrics`, `clock`, `logging` |
| `arbiter` | `overlay`, `metrics`, `clock`, `policy`, `logging` |
| `media` | `overlay`, `signaling`, `metrics`, `clock`, `logging` |
| `dashboard` | `overlay`, `coordinator`, `arbiter`, `clock`, `policy`, `logging` |
| `simnet` (test-only) | `overlay`, `coordinator`, `metrics`, `clock` |
| `cmd/*` | anything under `internal/` |

### 3.2 What each package owns

| Package | Owns | Src / test LOC |
|---|---|---|
| `internal/overlay` | The subnet model: `Topology`, `Node`, `Constraints`; the pure `BuildTree`; `PickRoot`; backup assignment; the `Fence`; and the two independent oracles `Validate` / `ValidateLocalRepair`. | 1,678 / 2,892 |
| `internal/coordinator` | The control brain: single-goroutine event loop, health FSM, hysteresis timers, the three trees, the two-attempt build, ratification, epoch adoption, rebuild-from-peers, the dashboard `Publisher` seam. | 2,544 / 3,714 |
| `internal/arbiter` | Meet registry + lifecycle, its own liveness view, **epoch minting**, `Score`, election policy, announcement + repair + vacancy. | 1,690 / 2,421 |
| `internal/media` | The data plane: pion sessions, the negotiation serializer, RTP forwarding, upstream PLI with SSRC translation, diff-and-apply of a pushed topology, the async re-parent state machine, the RTP rewriter, backup promotion, the upload meter. | 3,973 / 3,514 |
| `internal/signaling` | The WebSocket hub: meets, per-client goroutines, frame relay with server-stamped identity, the origin gate, WS keepalive ping, and the `Observer` seam. Treats SDP/ICE/payloads as opaque `json.RawMessage`. | 1,113 / 1,208 |
| `internal/metrics` | The peer → control-plane wire payloads (`Report`, `Heartbeat`, `Reparented`) and the peer-side `Reporter`, plus the cadence constants and `ValidateLivenessBudget`. | 380 / 606 |
| `internal/dashboard` | The browser-facing REST + WebSocket surface, the event envelope, CORS/origin policy, the gated demo control surface. | 2,319 / 2,889 |
| `internal/simnet` | The deterministic, media-free harness: virtual clock, event queue, failure injection, the `Settle` barrier. **Test-only; production never imports it.** | 1,240 / 2,700 |
| `internal/policy` | Boundary rules for untrusted input: which origins may connect, what a meet id may look like. Leaf. | 267 / 346 |
| `internal/clock` | The injectable time seam: `Clock`, `Timer`, `Ticker`. Leaf. | 106 / 117 |
| `internal/logging` | `slog` construction, level/format parsing. Leaf. | 73 / 48 |
| `cmd/server` | The arbiter binary. Wires every seam: hub ↔ coordinator ↔ arbiter ↔ dashboard. | 1,252 / 1,139 |
| `cmd/peer` | The participant binary: probe / call / static-tree / managed modes. | 870 / 1,157 |
| `web/` | Zero-build static dashboard frontend (vanilla ES modules, no bundler, no framework). Not a Go package. | 2,526 (JS+CSS+HTML) |

Packaging is **by feature, not by layer** — there is no `handlers/`, `models/`, `util/`. Each
package is named for the concept it owns, and its test suite lives beside it.

### 3.3 Why `overlay` and `simnet` are pion-free

`internal/overlay` imports exactly five standard-library packages —
`encoding/json`, `fmt`, `math`, `os`, `sort` — and **nothing else at all**. No pion, no
`clock`, no `policy`, no other `internal/` package. That is a hard contract requirement, and
it buys three things:

1. **`BuildTree` is a pure function.** Same `(nodes, prev, constraints)` always yields the
   same tree. Everything temporal is passed in as a value; there is no `time.Now()` and no
   randomness anywhere in the package.
2. **The whole control algorithm is testable in milliseconds** with no sockets, no ICE, no
   codecs, no ports. A 200-step churn scenario runs faster than one WebRTC handshake.
3. **`simnet` can drive the real control plane** without importing a byte of media code. The
   harness constructs `overlay.Node` values, feeds the real `coordinator`, and asserts the
   real `overlay.Validate` — it is exercising production logic, not a model of it.

The purity is enforced mechanically, not by discipline. `make check-determinism` greps
`overlay`, `simnet`, `coordinator`, and `arbiter` for every entry point into wall-clock time
(`time.Now|Since|Until|Sleep|After|AfterFunc|Tick|NewTimer|NewTicker`) and fails the build on
a hit. The comment on that Makefile variable is worth quoting, because it explains why the
list is exhaustive rather than representative:

> `WALL_CLOCK_CALLS` is every entry point into package time that reads or waits on real time.
> The list is deliberately exhaustive: a guard that catches `time.Now` but misses
> `time.NewTicker` is worse than no guard, because it reads as coverage.

Determinism inside `overlay` goes further than avoiding the clock. **No `range` over a map
exists anywhere in the non-test code.** Maps are used only as O(1) lookup tables; every
ordered decision carries a parallel slice — `attachedOrder` for candidate parents, the attach
order threaded into backup assignment, `Nodes()` for root derivation, BFS order for `Height`.
Every comparator ends in a name tiebreak, so no tie ever survives to be broken by Go's
randomised map iteration.

### 3.4 Why `coordinator` imports neither `signaling` nor `media`

`internal/coordinator`'s complete non-test import set is: stdlib (`context`, `encoding/json`,
`errors`, `fmt`, `log/slog`, `sort`, `sync/atomic`, `time`) plus `internal/clock`,
`internal/metrics`, `internal/overlay`. Every mention of `signaling` in the package is in a
comment.

This matters because the coordinator is the thing that will later run **on a peer**. If it
had a compile-time dependency on the server's hub, moving it to a peer in Phase 6 would have
been a rewrite instead of a wiring change. It was a wiring change.

The mechanism is **Go's consumer-defined interfaces**, and it is worth spelling out because
it is the load-bearing Go idiom in the whole codebase.

> **The idiom.** Go interfaces are *structural* and *implicit*: a type satisfies an interface
> by having the methods, and never by naming it. So the dependency arrow points **from the
> consumer to the data**, not from the producer to a shared "interfaces" package. Coming from
> Java, the instinct is to define `Publisher` next to its implementation in `dashboard` and
> have `coordinator` import it — that creates the back-edge that would make the DAG cyclic.
> Declare it where it is *used*.

Applied here in three directions:

- **Inbound (hub → coordinator).** `*coordinator.Coordinator` *structurally satisfies*
  `signaling.Observer` — five methods taking strings and `[]byte`. The coordinator never
  names the interface and never imports `signaling`; `cmd/server` passes the concrete
  coordinator where an `Observer` is wanted and the compiler checks the shape.
- **Outbound (coordinator → hub).** `coordinator` *declares* what it needs:
  ```go
  type Sender interface {
      SendTopology(roomID, peerID string, topo *overlay.Topology) error
  }
  ```
  Note the method set mentions only stdlib types and `overlay` types — types the *consumer*
  already imports. That is the precise condition under which a consumer-defined interface
  avoids an import.
- **Outbound (coordinator → dashboard).** `type Publisher interface { Publish(Event) }`, with
  `Event` owned by `coordinator`. `dashboard` imports `coordinator` and implements it; the
  arrow points the right way.

The same rule explains the one place the project **stopped** pretending. The contract
originally claimed `dashboard` declared consumer interfaces and therefore did not import
`arbiter`. That was false in both directions at once: one interface returned a
`dashboard.Meet` (which would have forced `arbiter` to import `dashboard`), and another named
`arbiter.Announcement` (so `dashboard` imported `arbiter` regardless of the claim). The fix
was to state the rule precisely —

> A consumer-defined interface avoids the import only when its signatures name types the
> **implementer** owns, or stdlib types.

— accept `dashboard → arbiter` as a legal downward edge, and move `Meet` to its producer.

### 3.5 The two deliberate exceptions to consumer-defined interfaces

Two shared leaf packages break the rule. Both are the same *kind* of exception, and naming
the kind is what keeps it from becoming a junk drawer.

**`internal/clock` — forced by Go's type system.** A consumer-defined
`interface{ Now() time.Time }` works fine; every clock satisfies it structurally. But the
moment the interface has `NewTimer(d) Timer`, the **return type becomes part of the
signature**, and Go compares *named types* in a return position, not their shapes. A method
returning `coordinator.Timer` does not satisfy an interface requiring `simnet.Timer`, even if
the two interface declarations are character-identical. So every consumer duplicating the
interface would need a distinct fake per consumer. One leaf package is the only workable
shape. (`Timer` and `Ticker` also expose their channel through a `C()` *method* rather than
the `C` *field* that `time.Timer` uses, because a Go interface can declare methods but not
fields — so the real implementation is a one-line wrapper. This is why the "just use
`time.Timer`" shortcut does not exist.)

**`internal/policy` — forced by a security-relevant agreement.** The origin allow-list must
be applied identically by the peer WebSocket upgrade (in `signaling`) and by the dashboard's
CORS surface (in `dashboard`), and the DAG forbids `dashboard → signaling`. Two copies of a
security matcher will drift, and **the drift's failure mode is a silently permissive CORS
policy that no test notices**, because each surface's tests pass against its own copy.

Two alternatives were considered and are recorded as rejected:

- *Amend the DAG to allow `dashboard → signaling`.* Cheapest, creates no cycle. It lost on
  what it enables next: it hands the unauthenticated browser surface a compile-time handle on
  the peer hub, and the very next convenience ("we already import signaling — just call
  `hub.SendRoom` from the demo endpoint") puts control-plane authority behind an HTTP handler
  with no auth in front of it. The forbidden edge is what prevents that.
- *Accept the duplication with a cross-checking test.* It loses arithmetically: a test
  asserting two implementations agree is **more code than one implementation**, and it only
  catches drift on the cases the test enumerates — exactly the wrong shape for a matcher
  whose dangerous inputs are the ones nobody thought of.

The generalisation, stated so a third candidate gets tested rather than waved through: these
are **shared vocabularies that multiple packages must agree on exactly, where the agreement
is the point and divergence is silent.** That is a different thing from a service seam, where
each consumer's needs differ and an interface is right.

A third candidate was in fact tested against that rule and split both ways. `RebuildWindow`
was declared in `arbiter` but consumed only by `coordinator`, across a forbidden edge — it
**moved to `metrics`**, which both already import, because it is literally "how long until
every peer has reported at least once", a metrics-plane cadence sitting beside
`HeartbeatInterval` and `GoneAfter`. It explicitly did *not* go to `policy`, whose charter is
untrusted boundary input. Meanwhile `signaling.ServerID` vs `arbiter.DefaultArbiterID` —
the same string `"_server"` — **kept its duplication**, because it is a *wire protocol*
constant and moving it into `policy` would make signaling's wire contract depend on a package
that has nothing to do with the wire. Two tests enforce the agreement instead: one in an
external `arbiter_test` package (a test-only `signaling` import that links into no binary)
asserting the constants are equal, and one in `cmd/server` asserting the wiring actually
passes it. Constants agreeing while `main` forgets to pass them is exactly what a single test
would miss.

---

## 4. How it works, end to end

Three traces. Each names the file the work happens in, so the code is findable.

### 4.1 A peer joins and starts receiving media

```
peer                     arbiter (cmd/server)                     other peers
 │                            │                                        │
 │ 1 WS dial /ws?room&name    │                                        │
 ├───────────────────────────▶│ origin gate → policy.Origins           │
 │                            │ ValidMeetID / ValidPeerName            │
 │ 2 ◀── TypeJoined (your id, roster)                                  │
 │                            │ 3 ──▶ TypePeerJoined / TypeMembership ─┤
 │                            │ 4 Observer.PeerJoined → coordinator    │
 │                            │      + arbiter (fanned to BOTH)        │
 │                            │ 5 ──▶ TypeCoordinator (announcement) ──┤
 │ ◀──────────────────────────┤        re-broadcast on every           │
 │   Fence.AdoptAnnouncement  │        membership change               │
 │                            │                                        │
 │ 6 ──▶ TypeMetrics (Report) │  ──▶ coordinator: first report =       │
 │ 6'──▶ TypeHeartbeat (1 Hz) │      threshold event; join arms the    │
 │                            │      1.5 s settle                      │
 │                            │ 7 recompute: PickRoot → BuildTree      │
 │                            │      → Validate → ValidateLocalRepair  │
 │ 8 ◀── TypeTopology ────────┤ 8' ──▶ TypeTopology ───────────────────┤
 │   Fence.Accept             │                                        │
 │   diffTopology → apply     │                                        │
 │ 9 ══ SDP offer/answer/ICE ═╪═══════════════════════════════════════▶│
 │ 10 ══════ SRTP/UDP media, peer-to-peer, server not involved ═══════▶│
```

**1–2 — dial and identity.** `signaling.Hub.ServeWS` (`internal/signaling/hub.go`) checks the
`Origin` header against `policy.Origins.AllowUpgrade` **before** the upgrade and returns 403
on a miss (§7.3 explains why the library's own check is disabled). It validates the room id
and the peer name — rejecting, never truncating — and assigns a server-side id (`p1`, `p2`,
…). The joiner receives `TypeJoined` carrying its new id and the current roster.

Three goroutines now exist per connection: the handler goroutine running `readLoop`, a
`writePump` that is the **single owner of the write side** (*"share the socket by
communicating, not by wrapping every write in a lock"*), and a `pingLoop` writing WebSocket
*protocol* pings. The ping is separate on purpose: teaching `writePump` to emit pings would
put liveness detection behind the very buffer that a wedged peer fills, so a stuck peer could
never be detected. `member.send` never blocks — a full 32-deep buffer cancels the member,
because a room broadcast must not stall on one wedged peer.

**3–4 — membership, and the ordering that matters.** `TypeJoined` is queued on the newcomer's
own outbound channel *inside* `register`, then the membership snapshot goes out, and **only
then** does `obs.PeerJoined` fire. Firing the observer earlier would race a topology push
ahead of the `joined` frame, and the peer would resolve the tree against an empty roster.
`cmd/server`'s `planeObserver` fans every observer callback to **both** the coordinator and
the arbiter, so the arbiter keeps its own liveness view rather than asking the coordinator —
which matters because the thing it must eventually detect is the coordinator's own death.

**5 — the announcement, and the quietest bug in the system.** On *every* membership change in
a meet that has a coordinator, the arbiter re-broadcasts the current announcement verbatim,
same epoch. Without this, a peer joining an already-running meet sits at epoch 0 with no
coordinator id, `Fence.Accept` fails its first two clauses, **every** topology push is
rejected, and it receives no media — indefinitely, while the dashboard renders it healthy and
heartbeating. Broadcast rather than unicast because it is idempotent and therefore
self-healing: peers already at that epoch ignore it at zero cost, and *any* peer that has
somehow lost its fence is repaired by the next membership change.

**6 — telemetry.** `metrics.Reporter` sends a `Report` every 3 s, immediate-then-ticker, and a
separate 1 Hz `Heartbeat` carrying **realized** state — the parent this peer actually has, the
children it actually serves, and the pion `PeerConnectionState` of each edge, plus its fence
`(Epoch, Rev)` and its cumulative count of refused pushes. The two are separate frames because
the cadences want to differ and, after Phase 6, they have **different destinations**: a
heartbeat is the arbiter's business (it runs elections on them), a report is the *coordinator
peer's* business.

**7 — recompute.** Inside the coordinator's single `Run` goroutine:

```
eligible?         len(members) >= 2  AND  (everyone reported OR settle expired)
root              PickRoot(nodes, published, streamKbps)     — sticky, never provisional
working           patch(published, ratifiedPromotions, departures)
next              BuildTree(nodes, working, cons)            — sticky attempt
   on failure     BuildTree(nodes, nil,     cons)            — one relaxed retry
gate              Validate(next, nodes, cons)                — independent oracle
gate              ValidateLocalRepair(published, next, …)    — transition oracle (tests)
publish           enqueue one push per member; published = next
```

A `Validate` failure in production means **keep the previous tree and log an error**. Never
publish a tree that fails its own oracle: *"a stale tree still carries media, and an invalid
one is the mysterious missing stream this project refuses to ship."*

**8 — the fence, then the diff.** `media.Router.applyTopology` runs `Fence.Accept(from, topo)`
**first, before anything is mutated**. This is the fence's only call site in the whole
codebase. If it passes, `diffTopology` computes a pure diff of *(self, wanted tree, current
reality)* — note the baseline is **reality**, not the previously pushed tree, so a partially
realized state converges rather than diverging. Its output has seven buckets, applied in a
fixed order:

```
fence → recreate → add → addLegs → removeLegs → remove → keyframe → reparent (async)
```

Leg **adds before removes** — "a child must never lose a source it is about to regain; the
overlap is harmless, a gap is not". Three things force a full session re-create rather than a
patch: the offerer role inverted (§5.12); *our* relay-ness changed (a peer promoted from leaf
to relay keeps a parent edge with no upstream and no forward loop, so its new child receives
nothing, forever); and the *peer's* relay-ness changed (otherwise the promoted peer tears down
alone and, if it is the answerer, waits forever for an offer the far end has no reason to
send). That third bucket is not redundant with the first: a promotion usually flips the role
too, **but only when the promoted peer sorts above its parent alphabetically** — so watching
only the role makes the core feature a coin flip on peer names.

**9 — negotiation.** One deterministic offerer per edge (`Topology.Offers`): the relay offers,
because only a fresh offer can add forwarded m-lines and an answer cannot. SDP and ICE travel
as opaque `json.RawMessage` through the hub, which never imports pion.

**10 — media.** A relay's `OnTrack` goroutine is the single reader of each `TrackRemote`; it
loops `ReadRTP` → fan out → per-leg `rtpRewriter.Rewrite` → `TrackLocalStaticRTP.WriteRTP`.
**No decode, no re-encode** — each hop is SRTP-decrypt → rewrite SSRC/payload type → SRTP-
encrypt. Sources are keyed by **origin**, never by the neighbour that handed them over.

One SFU footgun is handled explicitly. When a child asks for a keyframe, it sends a PLI naming
**the SSRC the relay assigned it**. Forwarding that verbatim upstream makes the original
source ignore it as an unknown SSRC — black video, no error. `requestUpstreamKeyframe` builds
a *fresh* PLI carrying the source's own upstream SSRC, throttled at 300 ms **per source, not
per child** ("a keyframe is not per-subscriber"), on the injected clock.

### 4.2 A relay fails

```
t=0      relay R's uplink dies
t≈0-3s   child C's pion PeerConnection to R reaches `failed`
         (or sits `disconnected` for ParentDisconnectGrace = 2s)
         ── FAST PATH, entirely local, nobody is asked ──
         C: clearLocalBackup()          ← so a second failure fails FAST
         C: B := topo.BackupOf(C)
         C: open a session to B as the ANSWERER, then    } make-before-break:
            send B a `backup-promote` frame                 R's session is left
            (the backup edge is not in the tree, so         intact and receiving
             Offers() is meaningless on it)
         B: accept that frame from a topology-unknown sender IFF
            topo.BackupOf(C) == B, against B's OWN current topology,
            and OFFER — only an offer can carry the forwarded m-lines
         C: rpOpening --Connected--> rpAwaitingMedia --track--> commit
                        (5s timeout)                    (3s timeout)
         C: rebindUpstreamSession; rtpRewriter.Switch() on every downstream leg;
            request an upstream keyframe; THEN close R's session
         C ──▶ TypeReparented{From:R, To:B, OK:true, Epoch, Rev}
t≈8s     ── SLOW PATH, the backstop ──
         coordinator: R silent for GoneAfter → HealthGone → threshold event
         coordinator: ratify C's choice into `working`, then rebuild
```

Six things in that trace are load-bearing.

**Nothing blocks.** The re-parent is an asynchronous state machine over an internal 32-deep
channel, because `applyTopology` and every pion callback run on goroutines that also *feed*
the negotiation they would be waiting for. Waiting for `Connected` on the `Run` goroutine
starves the offer/answer frames `deliver()` must route, so **the wait times out 100% of the
time**. Timers and pion callbacks *post* and return; every transition is handled on the `Run`
goroutine, which stays the only mutator of the peer map and the topology.

**Make-before-break, and the RTP continuity it forces.** The old parent's session stays open
and receiving until the new one carries media. That costs one transient extra
`PeerConnection` and one duplicated inbound stream for at most 8 s — and it creates a problem
a keyframe cannot fix. `TrackLocalStaticRTP.WriteRTP` rewrites SSRC and payload type per
binding but passes **sequence number and timestamp through untouched**. That is exactly right
while one source feeds one leg forever and exactly wrong the instant a second, unrelated
upstream feeds the same leg: the child sees a stream that jumps backwards and forwards in
sequence space, which pion's own NACK and jitter-buffer interceptors read as catastrophic
loss. *A keyframe fixes reference state; the damage here is to transport ordering, one layer
below.* So each downstream leg carries an `rtpRewriter` holding a `(sequence, timestamp)`
**offset**, recomputed only at a `Switch()`. Offsets rather than a counter, because gaps
*within* an upstream must survive to the child — a lost packet has to stay visible or loss
reporting silently stops working — while the discontinuity *between* upstreams must not. After
a `Switch()` the leg drops packets until a VP8 keyframe arrives (a continuous-but-undecodable
stream is worse than a gap: the decoder renders garbage rather than freezing), and the
keyframe is requested immediately, so the drop window is one PLI round trip.

**Downstream sessions are never touched.** Their forwarded tracks are the same objects
throughout; only the upstream feeding them changes. That is what keeps a re-parent local to
**one hop** instead of cascading down the subtree. During the switch two `TrackRemote`s
coexist and the non-active one **still reads** — an undrained `TrackRemote` stalls its
receiver and its interceptor chain — but its packets are discarded.

**The backup edge is authorized, not assumed.** A backup edge is not in the tree, so `Offers`
is meaningless on it and the far end has no topology reason to expect a connection. Two rules
close that: the promoter **asks** — one `backup-promote` frame, relayed peer-to-peer like an
offer — and the backup parent honours it **iff its own current topology names it as that
sender's backup**, in which case it takes the offerer role. The warrant is a fence-accepted
`Backups` assignment from the coordinator. It **fails closed**: a backup parent holding an
older `Rev` that lacks the assignment drops the frame, the promoter hits its 5 s timeout,
reports `OK: false`, and a stranded peer bypasses the recompute cooldown so the coordinator
repairs it promptly. Rejecting is correct — two ends disagreeing about the tree must not
silently form an edge outside it. This is a *consistency* check, not a security boundary;
nothing authenticates peers.

**And the promoter is the wrong end to offer**, which the first version of this got wrong and
§9.5 measured live. Forcing the promoter offerer is the obvious reading — it is the end that
knows the failure happened — but only an *offer* can add forwarded m-lines (§5.12), so a
backup parent that answers is structurally unable to publish the tracks it was promoted to
carry. `ReparentMediaTimeout` then expired on every backup edge whose peer expects media. The
roles are inverted: the child asks and **answers**, the authorized parent **offers**, and its
first offer carries the whole forwarded track set (`setupRelayEdge` already computes it with
`legsToward`, which takes any peer rather than only a tree neighbour, precisely for this
edge). The older path — accepting an unsolicited *offer* from an authorized backup child —
still exists and still fails closed on the same predicate; nothing the shipped promoter does
reaches it.

**And the exemption is bounded.** A late refinement (the most recent commit on the branch)
closes the gap between promotion and ratification: the diff would otherwise see that
accepted backup child as a stranger the tree does not name and close the edge — taking away
the parent that peer has only just failed over to. `promotionPending` exempts it, on two
clauses that are both exits: the coordinator must still name us as that child's backup (the
warrant), *and* the child's parent-of-record must be unchanged since we accepted it (which
distinguishes "the coordinator has not acted yet" from "the coordinator acted and placed this
child elsewhere"). The third exit needs no code: once the coordinator ratifies, the edge is a
real tree edge and the ordinary diff takes over. As the comment notes, this **cannot be
derived from the pushed topology alone** — "has the coordinator acted" is a question about
*change*, so it needs a before as well as an after.

### 4.3 The coordinator hands over

```
arbiter                              old coordinator A        new coordinator B      peers
   │ 1 A !Live (arbiter's OWN                                                          │
   │   liveness view) or Score(A)                                                      │
   │   below floor past dwell                                                          │
   │ 2 elect: candidates by                                                            │
   │   (score desc, name asc, id asc)                                                  │
   │ 3 ms.epoch++            ← the ONLY place an epoch is minted                        │
   │ 4 ──── TypeCoordinator{Epoch: E+1, Coordinator: B, Reason} broadcast ─────────────▶│
   │                                    │ Fence: was self, is not     │                 │
   │                                    │ → cancel control loop ctx   │ SetEpoch(room,  │
   │                                    │ → drop in-flight pushes     │   E+1) → Rev=0  │
   │                                                                  │ rebuild window  │
   │ 5 every peer: Fence.AdoptAnnouncement(E+1, B); Rev := 0; send one immediate        │
   │   out-of-cycle Heartbeat + Report (ticker RESET, not merely read)                  │
   │ 6 B accumulates for ≤ RebuildWindow (3s) or until every rostered member spoke      │
   │ 7 B reconstructs a stickiness baseline from REALIZED parent/children — over the    │
   │   REDUCED heard-from set — validates it, and uses it as `prev`                     │
   │ 8 B publishes Rev = 1 under Epoch E+1                                              │
```

**The ambiguity window, and precisely what makes it safe.** The window opens when the arbiter
announces `E+1` and closes when the last peer has processed it. During it, `A` (epoch `E`) may
still be pushing and `B` (epoch `E+1`) may have started. The safety argument is four steps:

1. Epochs are minted by a single writer that never repeats one, so **two coordinators can
   never hold the same epoch**.
2. A peer's epoch is monotonically non-decreasing and changes **only** on an arbiter
   announcement, so a peer's notion of authority cannot be moved by a coordinator.
3. A peer accepts a topology only when `topo.Epoch == curEpoch` **and** `From == coordID`.
   Therefore, from the instant a peer processes the `E+1` announcement, **every `E`-stamped
   push it receives is rejected** — regardless of arrival order, duplication, delay, or how
   confused `A` is about its own status.
4. The residual window is the set of peers that have not yet processed the announcement. Such
   a peer may still apply an `E`-stamped topology. **This is safe** rather than merely
   tolerable: an `E` topology is a valid tree over the membership `A` knew — `Validate` passed
   before it was published — so applying it cannot produce an illegal local state. It can only
   be **stale**, and staleness is self-correcting: the announcement is already queued on the
   *same FIFO socket*, so the peer processes it and then accepts `B`'s first `E+1` push.

> **A peer's realized topology may be STALE, but it is never INCONSISTENT — it is always some
> tree that some legitimate coordinator computed and validated.**

What would break it is two coordinators at the same epoch, and that is prevented
*structurally* by single-writer minting: not by timing, not by a lock ordering, not by a
heuristic.

**Three failure modes around the handover, and how each is closed.**

*A lost announcement.* If the broadcast is dropped — a full 32-deep send buffer is enough —
that peer stays fenced out, rejecting every push and receiving nothing, **until somebody
happens to join or leave. In a static meet that is indefinite.** Closed by a timer-free
repair: on every heartbeat whose epoch lags the meet's, past `RebuildWindow` since the term
started, re-send the current announcement **verbatim and unicast**. Verbatim is required, not
incidental — minting a fresh one with a different `Reason` would leave two peers disagreeing
about why the coordinator holds the role. Unicast because broadcasting would turn one wedged
peer into N frames per second. **No cap and no backoff**, because the condition is indefinite
so the repair must be: it costs ~150 bytes per lagging peer per second, which is nothing
beside a 2 Mbit/s video stream, whereas giving up leaves a participant permanently dark. The
dashboard sees it on **transition only** (enter/leave lagging), as a distinct `announce_repair`
kind — deliberately not folded into the election log, whose value is that it lists real
leadership changes, and deliberately pairable with the peer-side symptom `stale_rejected`, so
an operator can correlate cause and effect.

*A demoted coordinator that missed its announcement.* Already safe — peers fence it out — but
permanently noisy. No new mechanism: it is a peer, it heartbeats, its epoch lags, so the same
repair re-announces to it, and the adoption rule cancels its control loop. The partitioned
variant (no heartbeat reaches the arbiter, so no repair reaches it) is covered separately: on
`GoneAfter` of silence *from the arbiter*, a coordinator peer stops its own loop rather than
shouting into the void.

*No eligible successor.* The arbiter **announces the vacancy** — empty coordinator,
`ReasonVacated`, at a **bumped** epoch — and retains it. Bumping when no term begins looks
odd until you notice the epoch is a *fencing token*, not a term counter, and the thing being
fenced is the old coordinator. It buys three things dropping the announcement does not: it is
the only way to tell a live-but-demoted coordinator to stop (silence cannot); a joiner is
fenced to the *true* state rather than to nothing (`AdoptAnnouncement` succeeds, `coordID` is
`""`, and every push is correctly rejected because no sender can match `""`); and any
straggler push from the old term is fenced by the bump. The meet keeps running on the tree it
already realized — **the data plane does not need a coordinator.**

An **empty** meet is the opposite case and gets the opposite treatment: the retained
announcement is dropped, because there is nobody to announce a vacancy to and keeping one that
names a departed coordinator would fence the next joiner to a corpse.

**And one hard rule.** On a failed announcement broadcast the epoch is **not** rolled back.
"Reissuing a consumed epoch is the one thing that could put two coordinators in the same term,
which is the only state the fence cannot survive."

---

## 5. The design trade-offs

This is the heart of the document. Each entry states what was chosen, what was rejected, and
why. They are ordered roughly from the graph layer up to the media layer.

### 5.1 Greedy vs optimal tree building

**Chosen: a greedy heuristic with a frozen lexicographic preference ordering.**

The target — a degree-constrained, depth-limited, minimum-latency spanning tree — is
**NP-hard**; it generalises degree-bounded minimum spanning tree. An exact solver is not
merely slower, it is the wrong instrument: the inputs are noisy declared bandwidth and
sampled RTT, so a provably optimal tree over bad numbers is false precision.

`BuildTree` places nodes one at a time in a frozen processing order, choosing each node's
parent by a five-rank lexicographic comparator:

```
rank 0   hard filters — attached, able to parent, within depth, spare capacity
rank 0b  impairment filter (SOFT — see §5.2)
rank 1   INCUMBENCY — u's parent in prev wins unless a candidate beats it
         by more than c.StickinessMs of RTT
rank 2   minimum RTT (unknown scores worst)
rank 3   fewest children (load balance)
rank 4   name ascending (determinism; no ties survive)
```

Capacity is integer children: `floor(effectiveUpload / StreamKbps)`, where
`effectiveUpload = UploadKbps × (1 − min(LossPct, 50)/100)`. A TURN-relayed node has capacity
0 by definition and is therefore a forced leaf.

**Rejected: a scored objective function** — `cost = α·rtt + β·churn + γ·load`, take the
argmin. It is more expressive and one knob would trade all three off smoothly. It lost on
**explainability**: a lexicographic ordering lets you answer "why is B parented to R?" by
walking four rules, while a weighted sum requires reconstructing three floats and their
weights. For a system whose whole point is that every decision is defensible, and whose
inputs are noisy enough that false precision is a real risk, lexicographic is the better
instrument.

The greedy choice is recorded in the code rather than hidden, and the failure message is
actionable: when no parent can take a node, `BuildTree` fails loud rather than dropping the
node, because "silently dropping the node would be the *mysterious missing stream* we refuse
to ship."

### 5.2 Stickiness, and its path-dependence

**Chosen: rank 1 sits above RTT and above load, and below the hard constraints.**

That placement *is* the design:

- **Above RTT** (with a margin): re-parenting costs a stream interruption. A 5 ms RTT win is
  not worth a visible freeze; a 200 ms win is. `DefaultStickinessMs = 25.0` is where the line
  is drawn — ~6% of the 150–400 ms usable range, and comfortably above the 5–15 ms
  sample-to-sample noise on a residential link.
- **Above load**: letting "P now has 3 children and Q has 1" re-parent a node would churn the
  tree on every join. That was exactly the Phase 4 defect.
- **Below the hard constraints**: an incumbent that is gone, TURN-bound, full, or too deep is
  simply not a candidate. Stickiness must never produce an invalid tree.

Impairment is expressed in **exactly one rank, 0b**, and it is a *soft* filter coupled to a
`healthyAvailable` flag: impaired candidates are excluded only if at least one healthy
candidate survives rank 0; otherwise they are re-admitted. **A degraded parent beats no
parent.** That one rank produces both impairment consequences: an impaired node takes no new
children, *and* it loses the children it has — because eliminating it from the candidate set
means its own incumbents cannot re-select it at rank 1. The second consequence is the entire
mechanism by which a fired degradation-dwell timer becomes an actual re-parent.

#### The consequence nobody wanted, and the right way to state it

An early version of the contract asserted a property that is **false**: that no permutation
of first-report arrival order may change the converged tree. A reviewer produced a
counterexample and it is correct. Fleet `C = 12000`, `A = B = D = E = 4000` kbit/s,
`StreamKbps = 2000`, `MaxDepth = 2`. `C` roots with capacity 6; the others have capacity 2
each. Depending on which of the four reports arrives first — and therefore which incumbency
`prev` records before the next rebuild — the fleet converges to trees with the same root and
different edges (`D` under `C` in one history, under `A` in another). Both are valid. Both
are minimal with respect to their own history. Neither is wrong.

Enumerating all **120** first-report permutations of that fleet yields **9 distinct valid
trees, and the converged root is identical across all 120.**

> **The root converges; the shape does not.**

This is not a bug to patch. It is what stickiness *is*:

> A stability-preserving builder is **path-dependent by construction.** Rank 1 says "keep the
> parent you already have", which is a statement *about history*. A builder whose output
> depends only on the current node set is exactly a memoryless builder — that is Phase 4, and
> its memorylessness is the defect stickiness exists to fix. **Path-independence and
> minimal-disruption rebuilds are mutually exclusive, and this project chose the second. You
> cannot assert the first without unchoosing the second.**

The false equality property was replaced with three properties that are true and worth the
same amount:

**(a) Every produced tree is legal and every transition is minimal.**
`overlay.Validate(next, nodes, cons)` and
`overlay.ValidateLocalRepair(published, next, nodes, cons, churn)` hold at every step of
every scenario — over *every* history, not a chosen one.

**(b) The edge-set delta between consecutive trees is bounded by the churn that caused the
rebuild.** This is the property a user actually feels, because each changed edge is one
stream interruption:

| Churn | Bound on nodes whose parent changed |
|---|---|
| one join, no re-root | **exactly 1** (the joiner) |
| one leaf departs | **0** |
| one relay `X` departs, no re-root | **≤ `len(childrenOf_prev(X))`** |
| one self-promotion | **exactly 1**, and strictly `next.ParentOf(u) == published.BackupOf(u)` |
| one node becomes `Impaired` | **≤ `len(childrenOf_prev(node))`** |
| an RTT improvement past `StickinessMs` | **≤ 1 per improving node** |
| the relaxed retry ran (§5.5) | **unbounded** — assert `Outcome == OutcomeRelaxed` was published |
| the root departs | **unbounded** — assert `Reroot == true` was published |

The last two rows are the honest ones. Re-root and relaxed-retry are the two cases where
"local repair" does not apply at all, and the test asserts the coordinator **said so** rather
than that it avoided them. *A design that cannot always be local must at minimum always be
legible about when it was not.*

**(c) Determinism given the same event SEQUENCE — not the same event SET.** Same seed and the
same *ordered* script ⇒ byte-identical trace, over ≥50 repeats. That says a *history*
determines a tree, which is exactly what stickiness promises.

The permutation test was not deleted — it was **re-pointed**. It still enumerates orderings
with `Scenario.ReportOrder`, but asserts (a) and (b) on each rather than equality across
them. That is strictly more valuable: the old test would have caught "arrival order leaked
into a decision" (which (b) also catches — a leaked dependency shows up as an *unjustified*
changed parent), while additionally catching real churn bugs the equality test was blind to.
The 9-trees/1-root measurement is pinned as a regression check on the trade itself.

### 5.3 The three-tree split: `published` / `working` / `next`

**Chosen: the coordinator holds three distinct topology references per meet, and conflating
any two is a silent correctness bug rather than a compile error.**

| Name | What it is | Who consumes it |
|---|---|---|
| **`published`** | The last tree actually sent to peers. Carries the `(Epoch, Rev)` peers are fencing against and — load-bearing — the `Backups` assignment they promoted under. | `ValidateLocalRepair`'s `prev`; the dashboard; `Fence` reasoning |
| **`working`** | `published`, patched with everything the coordinator already knows changed: every ratified promotion and the removal of every departed/gone node. Derived fresh each round; never sent anywhere. | `BuildTree`'s `prev` |
| **`next`** | This round's build output. On publish it *becomes* `published`. | the peers |

One recompute round is five lines:

```
working := patch(published, ratifiedPromotions, departures)
next    := BuildTree(nodes, working, cons)                 // sticky attempt (§5.5)
_       =  Validate(next, nodes, cons)                     // gate
_       =  ValidateLocalRepair(published, next, nodes, cons, churn)   // NOT working
publish(next); published = next; ratifiedPromotions = nil
```

The asymmetry is the whole point, and it fits in one sentence:

> **The builder should be told what the peers have already *done*, so stickiness protects it;
> the oracle should be told what the peers were last *instructed* to do, so it can check that
> what they did was allowed.**

**Rejected: feed the oracle the patched working copy** — which is what the contract said for
two revisions before an implementer refused to follow it. Two reasons, the second decisive:

1. An oracle fed the patched copy is asserting against the very belief it exists to check.
   The promotion is already baked in, so the promoted node shows **no parent change**, and
   the oracle can confirm only that the coordinator believes what it believes.
2. **The published tree is the only artifact that still carries the `Backups` assignment the
   promotion must be checked against.** Given the published `prev`, the oracle can assert
   `next.ParentOf(u) == prev.BackupOf(u)` — that a promoted node landed on *the backup it was
   actually assigned*, not on some arbitrary node it decided it liked. Against the patched
   copy that check is not merely weaker, it is **impossible**: the patch overwrote the
   evidence.

There is no double-counting with `Churn.Promoted`: `prev` supplies the BEFORE state and the
backup assignment; `Promoted` supplies the *justification class* for a change that `Gone` and
`Joined` cannot explain. Two roles, both required.

This mistake also produced a **numerically wrong** test bound that survived two revisions.
Against `working`, a promotion is invisible, so the contract's bound for a self-promotion was
"**0** nodes changed parent". Against `published` it is a real, checkable change, so the
bound is "**exactly 1**, and strictly the assigned backup". The wrong bound was in the
direction that would have made a *correct implementation look like a bug*.

`deriveWorking` carries one more deliberate omission with a reason attached: it does **not**
copy `Backups` forward, "because nothing reads them here, and copying them would invite
exactly the published/working conflation this split prevents."

### 5.4 `Epoch` vs `Rev` — two counters, two single writers

**Chosen: a two-field fencing token `(Epoch, Rev)`, compared lexicographically.**

- **`Epoch`** is the arbiter-minted coordinator *term*. It changes only on a coordinator
  change. **Only the arbiter may raise it.**
- **`Rev`** is the coordinator's revision within its term, starting at 1 and incrementing by
  exactly 1 per published tree, resetting to 0 when the epoch advances. **Only the sitting
  coordinator may raise it.**

**Rejected: one monotonic counter.** A single counter cannot both fence coordinator handovers
and order successive trees from one coordinator without one writer being able to forge the
other's authority — a coordinator that could mint the number would be able to fence the
arbiter's own announcement, which is the precise failure the epoch exists to prevent. This is
**Raft's `(term, index)` split, for the same reason**, and the code says so.

The rule is enforced structurally: `SetEpoch(roomID, epoch)` is the coordinator's *only* way
to learn an epoch, `arbiter.announce` is the only place `ms.epoch++` appears, and
`overlay.Fence.AdoptAnnouncement` is the only method that may raise a peer's epoch.

One signature detail is load-bearing. `SetEpoch` takes a `roomID`; an earlier draft made it
process-global. Epoch is minted **per meet**, so a global setter would stamp both meets of a
two-meet server with whichever epoch was set last, silently mis-fencing every peer in the
other one.

### 5.5 The two-attempt build, and what "unbuildable" is allowed to mean

**Stickiness can make a satisfiable fleet unbuildable.** A sticky rebuild pins incumbents
before newcomers, so it can fail where a from-scratch build would succeed: the incumbents'
claims on capacity are honoured first, and a newcomer that a fresh build would have placed
near the root finds nothing left. That is inherent to the stability preference, not a bug.

**Chosen: `recompute` performs a mandatory two-attempt build.** Sticky first; if it fails,
exactly one retry with `prev = nil`. A successful retry publishes with
`Outcome = OutcomeRelaxed`, carrying the *first* attempt's error text as the reason — because
that error is the only explanation of why every peer is about to be re-parented.

Four rules on the retry, each freezing a way it could be misused:

- **It does not bypass `RecomputeCooldown`.** A relaxed build re-parents nearly everyone; it
  is the single most expensive thing the control plane does. (The cooldown is checked once
  before the build; both attempts live inside one already-permitted build.)
- **It is a retry, not a mode.** One extra attempt, same tick, same inputs. There is no
  "relaxed mode" to get stuck in — the next recompute starts sticky again from the newly
  published tree.
- **It is loud.** Warn, with both error texts. A meet that goes relaxed repeatedly is telling
  you the fleet is chronically near its capacity bound.
- **`PickRoot == ""` short-circuits both attempts.** Dropping the stability preference does
  not create upload capacity, so an unrootable fleet is over-constrained immediately.

The payoff is a much stronger signal: `unbuildable` now means **genuinely over-constrained**,
never "stickiness painted us into a corner". It is operator-actionable — drop a peer, raise
`-max-depth`, lower `-stream-kbps` — and the dashboard renders it as a persistent critical
banner.

**Rejected: a boolean on `OutcomeBuilt`.** Four outcomes exist because there are four
audiences: `built` is normal (Info), `settling` is normal startup (Debug — *a warning that
fires on every healthy startup teaches operators to ignore warnings*), `relaxed` is a rare
expensive success the operator should see but not act on (Warn), `unbuildable` is a fault
requiring a human (Warn + banner). A boolean would have to be plumbed into the UI as a de
facto fourth state anyway, with none of the enum's exhaustiveness.

### 5.6 Rebuild-from-peers vs snapshot-and-ship

When the coordinator role moves, the new coordinator needs state. Two ways:

| | **Snapshot-and-ship** | **Rebuild-from-peers** |
|---|---|---|
| How | The outgoing coordinator serializes {topology, per-node reports, dwell state, backups} and ships it via the arbiter. | The new coordinator starts empty. Every peer, on adopting the new epoch, immediately sends an out-of-cycle heartbeat + report carrying its **realized** parent/children. |
| Warm-up | Instant | ≤ `RebuildWindow` (3 s) |
| Works on a crash | **No.** Needs the outgoing node alive and cooperative — the case that matters least. | **Yes.** Identical code path either way. |
| New wire surface | A whole versioned snapshot message kept in sync with coordinator internals | **None.** Reuses `Heartbeat` + `Report`, already exercised every second. |
| Fidelity | The old coordinator's *beliefs* — which may already disagree with what peers realized | Peers' *ground truth* |
| Failure mode | A path exercised only on graceful handover — i.e. almost never — silently rots | The only path, exercised on every handover including every test |

**Chosen: rebuild-from-peers, and only that.** Snapshot-and-ship is not implemented, not even
as a fast path.

The deciding argument is not the 3 seconds. It is that **a fallback path which only runs on
crashes is a path that is never tested and is therefore broken when you need it.** Building
both means the graceful path gets all the exercise and the crash path gets all the bugs — in
the phase whose entire purpose is surviving a crash.

The 3-second cost is also smaller than it looks, and this is the honest framing: **the data
plane does not need the coordinator.** During the rebuild window every relay keeps
forwarding, every peer keeps receiving, and every peer still holds its precomputed backup
parent from the last published tree — so even a *parent failure during the rebuild window* is
handled locally. What is suspended for ≤3 s is re-optimization, not the call.

One correction inside this design is worth recording because it changed the stated limitation
into the real one. The reconstruction is validated over the **reduced heard-from node set**,
not the full roster. Validating over the full roster meant a single member whose heartbeat
was lost or late made the reconstruction "disconnected", failed `Validate`, and dropped to
`prev = nil` — **a global re-parent caused by one dropped frame**, while the limitations
section claimed the trigger was "heavy churn". Reducing the node set makes an un-heard-from
member simply *absent from `prev`*, which the builder already handles perfectly: absent from
`prev` means "newcomer", and newcomers are attached after incumbents. One lost heartbeat now
moves exactly one node instead of all of them, and the residual fallback is now what it was
always claimed to be — a genuinely torn tree.

### 5.7 Arbiter-as-arbiter vs Raft among peers

**Rejected: consensus among the participants.**

- Fully decentralized, no central node to depend on or pay for. That is the whole case for
  it, and it is real.
- But home PCs are the **worst possible quorum members**: they churn constantly, sit behind
  NATs, have asymmetric links, and suspend without warning. Consensus assumes a stable-ish
  membership set; ours is *defined* by instability.
- And it solves a problem we do not have. Raft's entire purpose is agreeing on a leader
  *without a trusted node*. We already operate one reliable node — the bootstrap server every
  peer must contact anyway to join. Paying full consensus overhead to avoid using a node we
  already depend on is wasted complexity.

**Chosen: the central server is the arbiter and the single writer of the epoch.**

Mechanically: the epoch counter is a plain `uint64` field on the arbiter's per-meet state,
owned by the arbiter's single `Run` goroutine, with every input arriving as an event on one
channel. **There is no lock to get wrong and no distributed agreement to reach, because there
is exactly one writer.** The fencing token is free.

The cost is named honestly: the control plane is a SPOF. If the arbiter is down, nobody joins,
nothing re-optimizes, and no handover completes. It is a **coordination** SPOF, not a
**conversation** SPOF — an established relay tree keeps forwarding media with the server
unreachable, because media never touches it.

Two details of the arbiter's discipline follow from single-writer ownership:

- The arbiter's `Run` loop has **no timer at all** — two select cases, `ctx.Done()` and the
  event channel. The reasoning is stated: "every action it can take requires at least one
  live peer to promote, demote, or fail over to, and a live peer is by definition producing
  heartbeats. A meet in which nobody is speaking is a meet in which there is nothing to
  decide." Meet reaping is likewise **lazy**, performed on registry mutations and listings,
  never on a sweep.
- Reads go through `query(ctx, fn)` — a closure executed *on* the Run goroutine — "which is
  why no field in this package needs a mutex."

And one hard rule: on a failed announcement broadcast, the epoch is **not rolled back**.
"Reissuing a consumed epoch is the one thing that could put two coordinators in the same
term, which is the only state the fence cannot survive."

### 5.8 Single-goroutine control loop vs mutexes

**Chosen: all coordinator state lives in one goroutine; every input arrives as an event on
one buffered channel (256 deep); every public method just enqueues.**

`Run`'s select has exactly three cases: `ctx.Done()`, one multiplexed deadline timer, and the
event channel. Ten event kinds (`evJoin`, `evLeave`, `evReport`, `evBeat`, `evReparent`,
`evRoster`, `evEpoch`, `evYield`, `evSnapshot`, `evSync`) cover every input including the two
test/observability barriers.

**Rejected: a mutex-guarded struct.** For a Go beginner coming from Java the mutex is the
obvious shape, and it would work. It loses three things this design needs:

1. **A quiescence barrier becomes possible.** `Sync` enqueues a no-op event and waits for the
   loop to ack it. Because the channel is FIFO with exactly one consumer, the ack *proves*
   every previously enqueued event has been fully processed. Under a mutex there is no such
   proof — "the lock is free" says nothing about whether a reaction is still pending. This
   barrier is what makes the deterministic simulation harness possible at all (§6.2).
2. **Snapshots cannot tear.** `Snapshot` round-trips the same loop and deep-copies what it
   returns, so a dashboard read is a consistent point-in-time view by construction rather than
   by lock-ordering discipline.
3. **The lock-ordering question never arises**, in a component that touches per-meet state,
   per-node state, timers, and two outbound queues.

Two refinements make the single loop safe under real I/O:

**The outbound plane is a separate goroutine.** `SendTopology` is called only from a
dedicated sender goroutine reading a 1024-deep queue, and `enqueueSend` is **non-blocking
with a drop** on a full queue. The interface itself carries the requirement — *"`SendTopology`
MUST NOT BLOCK and MUST NOT perform network I/O on the calling goroutine"* — so a future
adapter (an HTTP push, a retrying sender) cannot quietly reintroduce a stall. Defence in
depth: even an adapter that violates the rule degrades one meet's push latency instead of
freezing every meet in the process. Dropping is the right failure: every push is a complete
state snapshot, so a missed tree self-heals on the next threshold event.

**`Sync` and `Snapshot` deliberately differ in what they ride.** `Sync` rides *two* queues in
order — the event channel, then the outbound queue — so an assertion about what was *pushed*
is safe. `Snapshot` rides only the first, so it still answers while the outbound plane is
wedged, which is exactly when an operator is looking at it. (This asymmetry turned out to be
what made a Phase 6 ambiguity-window test expressible at all: the test holds a push open, and
a `Sync` there would block on the very push under test, while a `Snapshot` would not.)

**One timer multiplexes every deadline in the process** — six classes (`gone`, `degraded`,
`dwell`, `rebuild`, `settle`, `cooldown`) ordered by `(time, class, room, peer)`, fired one
at a time. Two reasons, and the second is the interesting one:

> A `select` cannot watch a dynamic set of channels, so the alternative is a polling ticker
> (which quantises every threshold to the tick) or a goroutine per timer (which puts state
> mutation back on many goroutines). **And a single wake channel is what makes the `Sync`
> drain a COMPLETE barrier: "a deadline is due" and "the wake channel has a value" are the
> same statement, so draining one drains all.**

That drain is not an optimisation. A fired timer and a `Sync` arriving at one parked `select`
are resolved by Go's **uniform-random** choice among ready cases, so a loop that acked without
first draining the timer would report "quiescent" while the reaction the barrier exists to
wait for had not happened. That is a permanently-flaky-test generator, and it gets blamed on
the harness.

### 5.9 Fast peer-local failover vs slow central detection

**Chosen: both, at deliberately different speeds, with the central one as a backstop.**

| Path | Latency | Mechanism |
|---|---|---|
| **Fast, local** | seconds (pion's ICE consent-freshness) | A child sees its parent's `PeerConnection` reach `failed` — or sit in `disconnected` for `ParentDisconnectGrace = 2s` — and **promotes its precomputed backup immediately, without asking anyone.** |
| **Slow, central** | `GoneAfter` = 8 missed heartbeats | The coordinator's health FSM declares the node gone and recomputes. |

`GoneAfter` being slow is the point, and the comment says so: "8 s looks slow for a system
claiming keyframe-bounded failover, and it IS slow — on purpose. This timer is the BACKSTOP,
not the fast path... By the time this timer fires, the tree has usually already healed itself
and the coordinator is merely ratifying. Keeping the backstop conservative is therefore free,
and it buys immunity to the ugliest false positive there is: declaring a healthy peer dead
because the ARBITER's own network hiccuped."

Three supporting decisions:

**Three health states, not two.** `healthy → degraded → gone`. Collapsing `degraded` into
`gone` would make every GC pause a re-parenting event; collapsing it into `healthy` would
leave the operator blind to a node about to fail. Degraded is **visible but not actionable** —
it colours the dashboard and arms the degradation dwell, and never re-parents anything.

**Thresholds multiply the cadence the peer *declared*, not a server constant.**
`metrics.DegradedAfter(interval) = 3×interval`, `GoneAfter(interval) = 8×interval`, read from
`Heartbeat.IntervalMs`. Hardcoding `3 × HeartbeatInterval` is wrong the moment a peer runs
with `-heartbeat 2s`: a perfectly healthy slow-beating peer would be declared gone every
time.

**And that fix opened a hole that had to be closed with a floor.** Because `GoneAfter` is a
function of a cadence the *peer* declares, a peer running `-heartbeat 500ms` shrinks its own
threshold to 4 s — below the 7 s worst-case socket-death detection window
(`WSPingInterval + WSPingTimeout` = 5s + 2s). The peer, not the operator, breaks the
invariant, so no amount of flag validation can catch it. Every per-node threshold is
therefore floored:

```
goneThreshold(node) = max(metrics.GoneAfter(node.declaredInterval), cfg.SocketDetection)
```

The inequality must run in this direction: the health FSM must be the **slower** detector, so
that whenever the coordinator declares a peer gone, the Hub has either already reaped the
socket (a real death) or the socket is genuinely live (a transient — and then the peer is
re-admitted by its next beat). Reversing it produces a peer ejected from the tree while the
Hub still considers it present, with no event able to bring it back. That was a real bug: a
peer whose Wi-Fi roamed for 9 s was removed from the tree, healed, and then heartbeat forever
into a coordinator that had no record of it — permanently dark, while the dashboard cheerfully
showed the meet healthy.

The fix is the **resurrection rule**: `gone` never deletes a node's record. A heartbeat,
metrics, or reparented frame from an unknown-or-`gone` peer *resurrects* it — the frame
arriving **is itself proof the socket is live** — and it counts as a join for threshold
purposes. Only an actual socket close deletes. The Hub holds no health state at all, by
design, so it invokes the observer for every frame on a live socket unconditionally; that is
what makes "a heartbeat arrived" usable as proof of return.

**Ratification, not correction.** When a peer reports a successful self-promotion, the
coordinator patches its *working copy* so the peer's chosen backup becomes the incumbent
parent, and *then* rebuilds. If it rebuilt from the pre-failure tree, stickiness would see the
*dead* parent as the incumbent, find it ineligible, and re-choose freely — quite possibly
moving the peer a second time, to a parent no better than the one it already connected to.
Two interruptions where one was needed. The coordinator ratifies the peer's local decision
rather than fighting it. It only ratifies a promotion reported against the *currently
published* `(Epoch, Rev)`: "a success reported against a tree already replaced says nothing
about the current one, and patching it in would defend an edge from a dead topology."

**`OK: true` requires MEDIA, not just ICE.** The promoting peer reports success only when the
new parent's session is `Connected` **and** at least one remote track has arrived within
`ReparentMediaTimeout = 3s`. Reaching `Connected` to backup `B` proves the peer can reach
`B`; it proves nothing about whether `B` is still attached to the root. Ratifying on
`Connected` alone means a correlated failure (where `B` was orphaned by the same event)
produces a coordinator that ratifies and then *stickily defends* an edge carrying no media —
the worst available outcome, because stickiness exists to protect working edges and would now
be protecting a dead one.

### 5.10 Backup capacity is not reserved — but backup fan-in is capped

**Chosen: no reservation, plus a hard fan-in bound of `BackupOvershootAllowance = 1`.**

**Rejected: carve each relay's capacity into primary slots and reserved backup slots.** At the
scale this system actually runs — 4–8 peers on residential upload — capacity is already the
binding constraint, and halving usable fan-out to insure against a failure that triggers one
recompute is a bad trade.

But *unbounded* fan-in is a different and worse problem, and the first draft got it wrong. If
every child of relay `P` names the root as its backup, `P`'s death promotes all
`|Subtree(P)| − 1` of them onto one node simultaneously — the overshoot is the **size of the
failed subtree**, exactly when the surviving relays are least able to absorb it. Rank 0c
spends a budget across backup assignment: `children[B] + backupLoad[B] < capacityOf(B) + 1`,
where `backupLoad` counts backups already promised to `B` in this pass.

The result is a claim that is *literally true* rather than aspirational: **at the instant of a
failover a relay may serve at most one child more than its computed capacity**, until the
recompute lands. The cost is that in a tight fleet some nodes get no backup at all — which is
correct and visible: "a backup that would oversubscribe its target by four is not insurance,
it is a second outage with extra steps." That overshoot exists only in a peer's *realized*
state; it is never encoded in a `Topology`, so `Validate` still holds for everything the
package emits.

Two more backup rules earn their place:

**The invariant is `B ∉ Subtree(P)`, not `B ∉ Subtree(u)`.** The brief asked for the weaker
rule. The failure being insured against is "`P` is gone", and that single event orphans
*everything* in `Subtree(P)`. A backup that is a **descendant of `u`** makes a cycle on
failover. A backup that is a **sibling of `u`** passes the weaker rule but is orphaned by the
very same event, leaving two orphans hanging off each other. Since
`Subtree(P) ⊇ Subtree(u) ∪ siblings(u)`, the stated invariant implies the brief's and rules
out the sibling trap as well.

**The root's direct children have no backup, by construction.** If `P == Root` then
`Subtree(P)` is the whole tree and no valid `B` exists. This is correct rather than a gap:
losing the root is a whole-subnet event that re-roots, and losing the coordinator is an
election. Both are full rebuilds, not per-node warm standby. The dashboard renders "no backup
(root child)" rather than an alarming blank. For a shallow tree this is a large fraction of
the nodes, and §8 records it as a limitation.

**And the zero value is the safe case.** `RouterConfig` carries `DisableBackup bool`, not
`Backup bool`. "Default true" is unachievable for a plain Go bool — its zero value is
`false` — so a caller who forgets the field would silently get **failover disabled**, the
wrong direction to fail in for the entire point of the phase. Inverting is the fix, not a
`*bool` (which trades a wrong default for a nil-deref surface on a boolean) and not "the
caller must remember" (which makes correctness a matter of memory). The CLI keeps `-backup`
defaulting to `true`, because flags express non-zero defaults fine; `cmd/peer` is the single
place the polarity flips.

### 5.11 `Supersedes` (ordering) vs `Fence` (authorization)

This was the sharpest single finding in the adversarial review of the contract, and it was a
**privilege-escalation bug in the specification**.

The contract mandated `Topology.Supersedes` — lexicographic `(Epoch, Rev)` comparison — as
"the ONE place the fencing comparison is written", while a different section required
rejecting a *higher* epoch. As literally written, any peer that stamped `Epoch = MaxUint64−1`
on a self-computed tree would supersede everything and be **universally obeyed**.

**Chosen: split the two questions, and give each its own function.**

`Supersedes` answers *"which of these two trees is more recent"*. Its call sites are
enumerated in its doc comment and the enumeration is the safety property: the coordinator,
ordering its own successive trees; the dashboard, detecting a stale snapshot. **Never the
peer's apply path.**

`Fence.Accept(from, topo)` answers *"may I apply this"*, and accepts iff **all** of:

```
f.Epoch != 0             the peer has been told who is in charge
from == f.CoordinatorID  the sender is that node (From is server-stamped, unforgeable by a peer)
t.Epoch == f.Epoch       EXACT match — a HIGHER epoch is REJECTED
t.Rev > f.Rev            strictly newer within the term
```

**The third clause is equality, not `>=`, and that asymmetry is the design.** A peer may learn
about a new coordinator **only from the arbiter**, never from the node claiming the job. If a
topology could raise the epoch, any peer could promote itself by stamping a bigger number and
the fence would authenticate nothing. The cost is one extra round trip on handover — the
arbiter must announce before the new coordinator's first push is accepted — which is bounded
and well inside the rebuild window anyway.

Supporting design:

- The **zero `Fence` is the unauthorised state**. Epoch 0, no coordinator, Rev 0; `Accept`
  returns false for everything. A peer that has not been told who is in charge obeys nobody.
- `AdoptAnnouncement` is the **only** method that may raise `Epoch`. That exclusivity is the
  entire mechanism of the safety argument.
- `Applied` is separate from `Accept`, so a caller can accept, attempt the fallible media
  work, and advance `Rev` only on success — a failed apply is retried by the next push rather
  than silently skipped.
- `Reset()` is called on `TypeJoined` and only there, and it is what closes the
  arbiter-restart hole: a restarted arbiter's meets are gone, so every peer must rejoin, and
  rejoining drops the peer's authority back to zero.

**Rejected: delete `Supersedes` entirely** (a reviewer's suggestion). Not taken. Ordering is
genuinely needed in two places, and deleting it would push a lexicographic comparison into
those two call sites instead of one. The defect was never that the function existed; it was
mandating it as the *authorization* check.

A related rationale had to be corrected the same way. `StaticEpoch = MaxUint64` — the epoch
stamped on a hand-authored `-topology` file — was originally justified by "no coordinator push
can supersede it", which is the same conflation. What actually protects a static peer is
**structural**: a static peer is not managed, so `applyTopology` returns early on every pushed
topology, and it never adopts an announcement, so its fence stays at the zero value and
rejects everything. *The file cannot be overridden because the code path that would override
it does not run.* The value is kept at max-uint64 for three smaller, honest reasons: it
satisfies `Validate`'s `Epoch ≥ 1`, it is instantly recognisable in a log line as
operator-authored, and it sorts newest for display.

### 5.12 Single-offerer negotiation, because pion v4 cannot roll back

**Chosen: one deterministic offerer per edge, derived from the topology
(`Topology.Offers(a, b)`: the relay offers; a name tiebreak when both endpoints are relays),
plus a serializer for our *own* successive offers.**

The reason is a hard constraint of the library, settled in Phase 1: **pion v4 cannot roll back
a local offer.** There is no `SetLocal` + rollback transition out of `have-local-offer`. The
W3C "perfect negotiation" pattern resolves a glare — two peers offering simultaneously — by
having the polite peer roll back. With no rollback, glare is unrecoverable, so the design
makes glare **impossible** instead: exactly one side of every edge ever offers.

What that leaves is the *other* half of perfect negotiation — serializing our own successive
offers — because Phase 5 adds and removes tracks mid-call. A second `AddTrack` while the
`PeerConnection` is in `have-local-offer` makes both calls fail and the offer is dropped with
a log line: a permanently half-negotiated session.

So the design is **perfect negotiation's bookkeeping minus rollback**, and the bookkeeping is
deliberately a *guard*, not a *scheduler*:

> We suppress our own duplicate offers and let pion decide when another is needed.

Two guards cover each other's window: `negotiating` (our intent flag) and
`SignalingState() != stable` (pion's own truth). A reviewer called the second one near-dead
code; that reading was inverted and the guard stayed — see §7, where it turns out to be the
thing that makes the corrected clear-before-`SetRemoteDescription` ordering safe rather than
merely lucky.

**Explicitly not implemented:** polite/impolite roles, `ignoreOffer`, rollback. An inbound
offer arriving at the offerer is not glare here — it is a **role disagreement, i.e. a bug** —
and the code logs and drops it.

The single-offerer rule has one consequence that was missed and is worth stating, because it
is the sort of thing that only shows up when the tree becomes dynamic. `Offers(self, peer)` is
evaluated once, at session creation, and baked into the session for its life. A leaf `L`
attached to relay `R` is the answerer. A rebuild gives `L` children of its own — now both
endpoints are relays, the tiebreak may name `L`, and `L`'s session object is still baked as an
answerer whose negotiation handler returns early. So `L` can never add the forwarded m-lines
it now needs. The failure mode is the nasty one: **not "both offer" but "neither can"** — no
glare, no error, no log line, just a relay that never publishes its forwarded tracks. The
diff-and-apply path therefore carries a fourth bucket, `invert`, and **tears down and
re-creates the session** for that edge.

*Rejected: a mutable role plus a "please offer me" handshake.* It sounds cheaper — no
reconnect — but pion gives an answerer no way to add m-lines without an offer, so we would
have to invent a control frame asking the peer to offer. That is a new wire type, a new round
trip, and a brand-new glare surface (both peers can decide to ask) in a codebase whose entire
negotiation design exists because glare is unrecoverable. Re-creating the session reuses a
path already exercised on every join, and it only fires when the tree's shape at that edge
changed anyway — i.e. an interruption was already being paid.

**The one place that handshake was built anyway: the backup edge.** `TypeBackupPromote`
("please offer me", relayed peer-to-peer by the Hub, never touched by the Observer) is
exactly the frame rejected above — and the reason it is safe here is the reason it was
rejected there, read in the other direction.

The `invert` bucket's problem is *symmetry*. Both endpoints of a tree edge see the same
pushed tree, both compute the same inversion, and both can decide to ask; a handshake there
manufactures the glare the whole design exists to make impossible. A backup edge has no
symmetry to lose. Exactly one end can promote — the child whose parent died — so exactly one
end ever sends the frame. The other end does not get a vote: it either finds itself named as
that child's backup in its own current topology and offers, or it drops the frame. There is no
state in which both ends offer, and the child's session is created as the answerer *before*
the frame goes out, so there is no window in which it could.

That last clause used to carry a proviso — *provided neither end already holds a session to the
other*, which only a `Validate`-clean topology guarantees, because a backup is never a
neighbour (`B ∉ Subtree(ParentOf(node))`). And that guarantee is held **upstream**:
`Router.applyTopology` unmarshals and fences a pushed tree but never calls `overlay.Validate`
(§8.3), and a hand-authored `-topology` file is a supported mode, so an operator can put a tree
into a peer where the backup already *is* a neighbour. `startPeerOpt` is idempotent per peer and
returns *before* it reads the override, so on such a tree the promoter's answerer role is
dropped and it stays whatever the tree made it — possibly the offerer. Sending the frame anyway
asked the far end to offer as well, and a far end holding no session back would have done it:
both in `have-local-offer`, no rollback, no error, no timeout. Every other failure on this path
is a `ReparentConnectTimeout` that heals; that one did not.

**The frame is now gated on the role having actually been applied.** `startPeerOpt` reports
whether it created the session, and the promoter sends `TypeBackupPromote` only when it did —
which is exactly the condition under which the promoter is provably the answerer. The receiving
end is closed by the same guard from the other side: `onBackupPromote` mints an offerer session
only when it held no session to that peer at all, and returns (at WARN) when it did. So on
**any** tree, `Validate`-clean or not, *the promotion path cannot produce a second offerer* —
no proviso. Pinned by `TestPromoteFrameIsWithheldWhenTheAnswererRoleWasDiscarded`, which asserts
the frame's absence on the wire rather than a role in memory.

What that does **not** claim, and what still rests on the coordinator being correct:

* **Roles a session already holds still come from the tree.** Two ends evaluating *different*
  trees can still derive incompatible roles for an in-tree edge, `Offers` being symmetric only
  on identical inputs. This change removes the promotion path as a *producer* of glare; it does
  not make the peer self-sufficient about roles in general.
* **A promotion onto an existing session still cannot invert it, and now does not even try.**
  On a tree `Validate` would reject, the backup edge keeps the tree's role, no offer is
  exchanged, and the promotion fails on `ReparentConnectTimeout` — the same rung a refused or
  lost frame lands on. Both ends log the discard at WARN with the peer and the role they wanted,
  and the promoted parent logs its success line only when the offerer role actually took. The
  outcome is a failure the coordinator repairs, not a wedge nothing notices.

The mirror of that admission is a path that was **deleted**: `deliver` used to conjure a
session for an unsolicited *offer* from a peer whose topology named us its backup, which is how
this edge formed before the inversion. It has no producer any more. The promoter creates its
session as the answerer, and `Session.onNegotiationNeeded` returns immediately for a
non-offerer, so no peer on this build can offer on a backup edge; `TypeBackupPromote` — which
asks, and can be refused — is the only way onto an edge the tree does not contain. The dead
branch was carried by no test at all (deleting it left the suite green), which is precisely the
condition under which a comment claiming it "still fails closed" outlives the truth of it.

What forces it is `Offers` being **meaningless**, not merely inconvenient, on an edge the tree
does not contain: there is no role to derive, so a role has to be assigned, and the assignment
has to be the one that lets the parent publish. `Offers(a, b)` is untouched, and no in-tree
edge negotiates differently because of this.

The cost is honest: one more wire type, one more round trip on a failover path, and a frame
whose loss is indistinguishable from a backup parent that refuses. Both land in the same
place — `ReparentConnectTimeout`, `OK: false`, the coordinator repairs — which is why the
loss case needed no new machinery, only a test
(`TestBackupPromoteThatNeverLandsFailsClosed`).

### 5.13 Two smaller decisions worth naming

**No bespoke `overlay.Repair()`.** The obvious design for local repair is a function that
patches only the orphans' edges. It was rejected because it would be a *second*
implementation of the same constraint logic (capacity, depth, TURN, backups), living next to
`BuildTree`, free to drift from it and from `Validate`. Stickiness makes local repair fall
out of the general builder for free: every surviving non-orphan keeps its incumbent parent,
so only the orphans move. **One algorithm, one oracle, one place to be wrong.** The cost is
that the builder does slightly more work than strictly necessary, which is irrelevant when a
rebuild is microseconds.

**The join settle is a composite of three mechanisms, not one of them.** The brief offered
three options for "when is a meet ready to build" and asked for one; each leaves a hole the
others close. (a) *Wait for every known member to report* is unbounded — one wedged peer hangs
the meet forever. (b) *A bounded window* shrinks the race but does not remove it: if the
strong peer is still silent when the window closes, a weak reported peer is still the best
candidate and still gets rooted. (c) *Never root an unreported node* removes the dangerous
part but still publishes a throwaway tree over a provisional fleet, rebuilt seconds later.

The shipped rule is **(c) as a structural invariant + (b) as a re-arming gate + (a) as the
early exit + a minimum size**: `Node.Provisional` makes a provisional root *impossible* rather
than unlikely; `JoinSettle = 1500ms` re-arms on **every** join (so a burst coalesces into one
build) and supplies the bound; "everyone has reported" is the fast path; and
`MinBuildableMembers = 2` closes the degenerate case that broke the first version — without
it, (a) and (b) are both trivially satisfied by a meet of one, which then seeds the stickiness
baseline with a root chosen from a fleet of one and entrenches whoever dialled in first.
A join **bypasses `RecomputeCooldown`** but is subject to the settle, so a joiner waits at
most 1.5 s for media rather than up to 5 s of black screen.

---

## 6. What testing this actually required

Two thirds of the Go in this repository is test code: **22,751 test lines against 17,505
non-test lines**, 372 test functions and 297 `t.Run` subtests across 69 files. That ratio is
not diligence for its own sake. A system whose defining behaviours are *temporal* (hysteresis,
timeouts, dwells), *concurrent* (one control loop, many peer goroutines, pion's own dispatch
goroutines), and *distributed* (a fence, a handover, an ambiguity window) has almost no
behaviour that a straightforward unit test can reach.

### 6.1 Deterministic simulation, and the virtual clock

`internal/simnet` is a media-free harness that drives the **real** control plane — the real
`overlay.BuildTree`, the real `coordinator.Coordinator` — over a virtual clock, with no
sockets, no ICE, no codecs. It is the FoundationDB / TigerBeetle "deterministic simulation"
idea in miniature.

**`VirtualClock`** satisfies `clock.Clock`; time moves only when `Advance` is called. Three
rules make it a *replayable* clock rather than merely a fake one:

- **Total order `(deadline, seq)`.** A monotonic sequence number is stamped at `NewTimer` and
  re-stamped on every `Reset` and every ticker re-arm, so "same instant" ties are broken by
  most-recently-armed order and never by map iteration. There is deliberately no heap — the
  armed set is a map, and ranging it is legal *only* because the range selects a unique
  minimum under a total order rather than building an order out of the iteration.
- **One deadline per iteration**, then the armed set is re-examined — so a timer *re-armed by
  an earlier handler* (which is exactly what a dwell reset does) takes its position from
  virtual time rather than from goroutine scheduling.
- **Cap-1 channels with a non-blocking send.** A test that has not drained a fired timer
  cannot deadlock `Advance`; it simply misses the tick, exactly like a real `time.Ticker`.

Guard rails are chosen on one principle, stated in the code: *"a hanging test is strictly
worse than a failing one: it yields no stack, no seed, and no clue."* `maxFiresPerAdvance =
100_000` panics; `AdvanceTo` panics on a rewind; `NewTicker(<=0)` panics.

The clock seam itself is production-wide: `coordinator`, `arbiter`, `signaling`, `metrics`,
and `media` all take a `clock.Clock` on their config struct, defaulting to `clock.System()`
when nil. Four alternatives were rejected and are worth naming because they are the common
ones: a package-level `var now = time.Now` swapped in tests (a global — two parallel tests
swapping it race, and `-race` will not necessarily catch it); carrying the clock on
`context.Context` (`context.Value` for a *dependency* is a documented Go anti-pattern —
untyped, invisible to the compiler, and it makes every function that might need time take a
ctx it otherwise would not); passing the clock to every call site (churns dozens of signatures
for a value fixed for the object's lifetime); and per-consumer clock interfaces (which breaks
on `NewTimer`'s return type, §3.5).

### 6.2 `Settle` — the barrier that makes assertions safe

Firing a timer is not enough. The goroutine that receives it has not necessarily *finished
reacting* when `Advance` returns, so a naive assertion races. The fix exploits a property the
codebase already has: **the coordinator is a single-goroutine event loop.** Round-tripping a
no-op event through it proves every previously enqueued event has been fully processed, because
the channel is FIFO with exactly one consumer.

`VirtualClock.SetBarrier(fn)` is invoked *between* consecutive firings, wired by
`NewScenario` to `Scenario.Settle`, which calls every registered barrier under a 5 s context
deadline. That deadline is the **only** wall-clock reference in the whole harness — and it is
a `context.WithTimeout`, not a `time.After`, precisely so `make check-determinism` stays green.

The subtlety is the load-bearing part, and it is documented on `AddBarrier`:

> A barrier is only sound if the loop it round-trips through **DRAINS ITS FIRED TIMERS before
> acking**. A fire and a sync arriving at one parked `select` are resolved by Go's
> uniform-random choice, so a loop that acks without draining can report quiescent while the
> reaction the barrier exists to wait for has not happened.

This is not hypothetical. It is a permanently-flaky-test generator, and the flake gets blamed
on the harness rather than on the loop. It is also why the coordinator multiplexes every
deadline onto **one** timer (§5.8): with one wake channel, "a deadline is due" and "the wake
channel holds a value" are the same statement, so draining one drains all — and the barrier is
*provably* complete rather than complete-in-practice.

### 6.3 `Validate` as an independent oracle

The tests never re-derive the expected tree. That would just re-implement the heuristic and
prove nothing — a mirror, not an oracle. Instead:

> Construct with one function, verify with an independent one.

`overlay.Validate(t, nodes, c)` re-derives everything from the tree itself and checks ten
independent properties: `Epoch ≥ 1`, `Rev ≥ 1`, `Root` agrees with `Edges`, is-a-tree
(single parent, exactly one parentless node), connectivity by BFS with
`len(reached) == len(nodes)` (which rules out both cycles and shared children), depth ≤
`MaxDepth`, topological edge order, degree ≤ capacity-derived-from-upload, TURN-bound nodes
have no children, closed world, and the full backup legality set including
`B ∉ Subtree(ParentOf(node))` and the fan-in bound.

`ValidateLocalRepair` is a **second, separate oracle over transitions** — "was this change
minimal?" is a property of a pair of trees, and conflating it into `Validate` would force
`Validate` to take a `prev` it does not otherwise want.

Both are asserted from `overlay`'s own tests, from `coordinator`'s tests against the real
loop, and from `simnet`'s churn scenarios after **every** rebuild. `TestChurnBoundedEdgeDelta`
runs seeds `{1, 7, 42, 1337, 20260901}` × 200 steps with both oracles green at every step.

The oracle is also honest about what it does *not* claim, which is stated in its own doc
comment: it checks a **necessary** condition, not a sufficient one. It asserts every move was
*permitted* by the preference ordering; it does not assert the result was optimal, and it
deliberately does **not** assert the converse (that a node which could have improved did
move). Greedy makes no such promise — an eligible closer parent may have been filled by an
earlier node — so asserting it would fail on correct output.

### 6.4 Property and permutation tests

- **`TestStragglerPathIsPathDependent`** enumerates all 120 first-report permutations of the
  counterexample fleet, asserts each tree is `Validate`-clean and holds every node, asserts
  the root is identical across all 120, and asserts **at least two permutations differ**. It
  logs `converged to 9 distinct valid trees over the same fleet`. The deliberate
  differ-assertion is a regression check on the *trade*: "if the divergence ever disappears,
  the stability trade-off has changed and the contract must be revisited."
- **`TestReportOrderInvarianceSettledPath`** asserts the opposite for the *settled* path —
  byte-identical trees across all 120 permutations — and fails loudly if the permutation
  count is not 120, so the enumeration can never silently shrink.
- **`TestPickRootNeverRootsAGuess`** replays a real 3-peer incident: over every arrival
  permutation, whatever `PickRoot` returns must `BuildTree` cleanly.
- **Replay determinism**: `TestChurnReplayIsDeterministic` runs two seeds × 120 steps and
  compares marshalled traces byte-for-byte across replays.
- **Zero fuzz targets and zero benchmarks** in the repository. Recorded in §8.

`simnet` also keeps a `modelCoordinator` for fast property sweeps, and pins it to the shipped
loop with a **differential test**, `TestModelAgreesWithTheRealCoordinator`. Its own comment
records the payoff: *"That differential immediately earned itself: it caught that the harness
was conflating JOIN arrival with REPORT arrival."*

### 6.5 The mutation-testing discipline — the most valuable finding in the build

Adversarial reviewers were asked not "is this code correct" but "**which line can I delete and
keep the suite green?**" The answer, repeatedly, was: quite a few. The manager log records
**eight** green-tests-measuring-nothing found by the review fleet plus a ninth caught by its
own author, and `internal/coordinator/guards_test.go` — a file that exists solely because of
that audit — opens with:

> Guards a mutation audit found undefended: each of these deletes cleanly from the
> implementation without any other test noticing.

It holds eight such tests. Three concrete cases are worth walking through, because each fails
in a different way.

**Case A — a test that could never fire.** `TestStaleRejectedShimIsStillNeeded` is a
self-removing scaffold: it must fail the moment `metrics.Heartbeat` grows a `stale_rejected`
field, so the temporary shim reading the raw key can be deleted. It marshalled a
`metrics.Heartbeat{Name: "a", Seq: 1}` to JSON and checked for the key. From the fix commit:

> That could never fire: the shipped field is tagged `omitempty`, so a zero value omits the
> key and the test concluded the field did not exist — a test passing for the wrong reason,
> which is the exact class of bug the scaffold was built to prevent.

The fix asks the **type**, not an instance: reflect over the struct's JSON tags. This is the
purest example in the repo of a test that was green, looked like coverage, and measured
nothing at all.

**Case B — a discriminator that discriminated only one of two mutations.** The comment on
`barrier_internal_test.go` claimed a test was "the discriminator" for the Sync-drains-first
rule. The audit found it was true for only one mutation:

> **TWO MUTATIONS, TWO TESTS.** *Deleting* the drain is caught by
> `TestSyncDrainsFiredDeadlinesBeforeAcking`. **Reordering** it — acking before draining — is
> invisible to that test, because both statements complete before `handle()` returns.

The fix added `TestSyncDrainsBeforeItAcks`, which parks the publisher on its first event to
freeze the loop *inside* the reaction, then asserts the ack is **not yet closed** — "the
ordering question asked as a state question, which is the only way to ask it without a race."
The same file records that the naive end-to-end version does **not** discriminate:
`syncBarrierTrials = 200` exists precisely because "on a multi-core machine the `Run`
goroutine is almost always already awake, so a broken loop passes anyway, and the test reads
as coverage while proving nothing."

**Case C — a comment justifying code with a dependency that does not exist.** A sort in the
coordinator's node projection was justified with `// Never a map range: the projection order
feeds PickRoot's tie-break and BuildTree's…`. The mutation sweep proved that false:
`PickRoot` computes a maximum under a total order, so `BuildTree`'s output is invariant to
input slice order — *given unique names*. The comment was rewritten to the three real reasons,
and a new test, `TestDuplicateNameResolutionIsDeterministic`, runs the fleet **20 independent
times**, because "a projection that ranged the map would have to win a coin flip every time to
survive this." The finding is not that the code was wrong; it is that **the stated reason was
wrong, which is how the next person deletes it.**

A fourth is worth one line because it is the same discipline applied inside a media test.
`TestRemovalNudgeSurvivesTheAnswer` has to *wait for the spawned nudge to be spent* before
letting the answer through — otherwise "that nudge is spawned, so its scheduling is a lottery,
and on a fast machine it usually lands after the answer has already returned the pc to stable,
quietly covering for the answer path even when the answer path is wrong."

The habit shows up as inline vacuity guards, too:
`t.Fatal("no backups assigned at all; the assertion above is vacuous")`;
`"ValidateLocalRepair(nil prev) must fail loud rather than pass vacuously"`;
`// Note what does NOT discriminate: r.rp is non-nil either way…`.

**The generalisable lesson**, and the one worth taking into any codebase: *a passing test is
evidence only if you know which mutation it would have caught.* Write the mutation down next
to the assertion.

### 6.6 Library-behaviour pins

Several of this system's designs depend on facts about pion that are not in its documentation.
Those facts are pinned by tests that assert the **library's** behaviour, not ours — using raw
pion and deliberately bypassing our own wrappers — so a pion upgrade that changes them fails a
test instead of shipping a silent regression.
`TestPionRenegotiatesARemovalOnceConnected` is the sharpest, and it is also the one that had
to be **replaced by its own opposite**: an earlier pin asserted that pion never renegotiates a
`RemoveTrack`, and that turned out to be an artefact of how the test was invoked. §7.2 is the
whole story, and it is the most useful thing in this document.

The replacement carries the lesson in its shape: it waits for `connected` *before* measuring,
so it asserts the same thing under `go test` and `go test -race`. A pin whose verdict depends
on the invocation is not a pin.

### 6.7 The gate

`make check` = `fmt` + `vet` + `check-determinism` + `go test -race ./...`.

`check-determinism` greps the four control-plane packages — including their test files — for
every entry point into wall-clock time and exits 1 on any hit. It is currently **silent**: zero
matches. Repo-wide, the only non-test wall-clock calls outside `internal/clock` are three in
`internal/media`, which is deliberately outside the gate because it runs real pion.

`make check` **passes**: all 13 packages `ok`, no failures and no skips. The gate *specifies*
`-race` because the concurrency here is the design — a data race in this codebase is a design
violation, not merely a bug — but the suite does not *depend* on it: plain `go test ./...` is
13/13 green as well. That was not always true, and the one test that used to disagree with
itself across the two invocations is the subject of §7.2.

The wall-clock profile is itself the argument for the architecture:

```
              with -race        without -race
media           88.4s              13.9s      ← real pion, real ICE, real DTLS on loopback
cmd/peer         6.5s               0.53s
everything else  1–2s each          0.1–0.3s each
```

Every deterministic control-plane package runs in a fraction of a second because it runs on a
virtual clock; the one package that must touch a real network dominates the suite either way.
That gap is the entire reason the control plane was kept pion-free — and, as §7.2 shows, the
6× dilation `-race` imposes on the media package is not merely slow, it is a *different
experiment*.

---

## 7. Where the contract was wrong

`docs/PLAN.md` was frozen before implementation, reviewed adversarially by two independent
reviewers (12 critical + ~15 major findings), and amended in place with every ruling recorded
in §15. Even so, it was wrong in places, and the interesting wrongness is not typos — it is
**claims about a library.** Several were fixed by reading the vendored source instead of the
spec; one of those fixes then turned out to be wrong itself, because reading the source told
us what the code does without telling us **which state it does it in** (§7.2). That is the
sharpest thing in this section: verifying against the implementation is a large improvement
over trusting documentation, and it is still not the same as measuring the regime you actually
run in.

### 7.1 Five places the contract was wrong about real pion

Measured against **pion/webrtc v4.2.16**. These are corrections of *fact*, not of taste.

| # | The contract said | Real pion | Consequence if shipped |
|---|---|---|---|
| 1 | `RemoveTrack` fires `OnNegotiationNeeded`, so the serializer emits the renegotiating offer — **unconditionally**. | **Conditional.** On a *connected, stable* pc pion fires immediately and reliably. When the removal lands while the pc is **not stable** — an offer already in flight, which is the ordinary case during a topology apply — pion's `negotiationNeededOp` aborts on its signaling-state check and the notification is simply **lost**. Nothing re-raises it. *(This row was itself wrong for two revisions — see §7.2.)* | **SILENT, PERMANENT** in the non-stable case: the removal never renegotiates and the departed source's m-line lingers in the far end's SDP as a stale `sendrecv` forever. |
| 2 | Clear the `negotiating` flag **after** `SetRemoteDescription(answer)`. | `SetRemoteDescription` reaches `stable` *inside* the call and re-fires negotiation-needed from pion's ops goroutine. With our flag still set, our handler early-returns — and pion has now **consumed its own flag**, so it never fires again. | **SILENT, PERMANENT.** The renegotiation is lost forever: no error, no log, a far end whose SDP is permanently stale. |
| 3 | On a failure, the retry re-enters the full negotiation path. | pion forbids `SetLocal(offer)` from `have-local-offer` (`checkNextSignalingState`). | The retry is illegal from the state it most often runs in. |
| 4 | A backup promotion just opens a session to the backup. | The backup edge is not in the tree, so `Offers` is meaningless on it and the far end has no reason to expect a connection — it drops the offer as coming from an unknown peer. | **The promotion deadlocks as specified**, in the mechanism the whole phase exists for. |
| 5 | A role inversion is handled by re-creating the session. | The two ends re-create at *different moments*. For that window the far end still holds a session baked as offerer and correctly drops our offer as a role disagreement; we sit in `have-local-offer` forever, it waits for an offer already discarded. | A permanently deadlocked edge. Neither end is wrong and neither recovers. |

**Items 1 and 2 are the two that would have shipped as silent permanent failures**, and they
are opposites that look identical in the code. Item 2's fix — clear the flag *before*
`SetRemoteDescription` — is only safe **because of the two-guard design**: if
`SetRemoteDescription` fails, we have cleared `negotiating` while the pc is still in
`have-local-offer`, and `SignalingState() != stable` catches it. That is the guard a reviewer
called near-dead code and recommended deleting. The correction inverts the finding: once the
racing `renegotiate` flag was removed, `SignalingState()` became the **primary** guard —
pion's own truth about the connection — and `negotiating` became the secondary intent flag.
Removing it was the wrong reading.

Item 1's fix, `pendingLocalChange`, is visually identical to a flag that had just been
*removed* for causing double offers, and is its exact opposite. The code freezes the asymmetry
in a table so nobody "simplifies" it back:

| | removed `renegotiate` | added `pendingLocalChange` |
|---|---|---|
| Covers | `AddTrack` — a path where pion **does** re-fire | `RemoveTrack` from a **non-stable** pc — a path where pion's notification is dropped |
| Effect | Races pion's trigger ⇒ two offers for one change | Supplies the missing trigger ⇒ one offer for one change |
| Without it | Correct | Permanently stale far-end SDP |

The rule generalises, and it is the sentence to remember — with the qualifier that §7.2 had to
put back into it:

> Our bookkeeping supplements pion **exactly where its notification is known not to arrive,
> and nowhere else.** Where pion does fire, adding our own trigger is a race. Where it does
> not, omitting one is a permanent stall. The two look identical in the code and are opposites
> in effect — and *which regime you are in is a property of the connection's state, not of the
> API you called.*

Two further corrections were found during implementation, on top of the five:

- **Where the removal nudge runs.** Issued before `SetRemoteDescription`, it raced it — the
  handler read the signaling state while the pc could still be in `have-local-offer`,
  deferred, and left `pendingLocalChange` set, which nothing then picked up. It must run
  *after* the pc has returned to `stable`, and **inline**, not spawned: "a goroutine
  reintroduces the same race in miniature — its scheduling decides which state it observes."
- **"Giving up is self-healing" was false**, and an earlier comment claimed it. When the
  retry ladder is exhausted, the pc is left in `have-local-offer`, both guards then refuse
  every future offer, and a coordinator re-push changes nothing — the diff sees that neighbour
  as live, wanted and same-role, so the session survives untouched and every new forwarded
  track lands on a pc that will never offer again. Exhaustion therefore **reports itself**
  through `OnNegotiationFailed` and the owner re-creates the edge.

### 7.2 The finding that was right for the wrong reason

Item 1 above deserves its own section, because the *second* time it was measured it came out
differently, and the story is a better lesson than the finding was.

**What the contract recorded.** Verified against the vendored pion source rather than the
spec — already better practice than most — the contract stated flatly that `pc.RemoveTrack`
never causes pion to re-fire `OnNegotiationNeeded`, because `checkNegotiationNeeded` concludes
nothing changed. `pendingLocalChange` was built on that, a test
(`TestPionRemoveTrackDoesNotRenegotiate`) pinned it, and its comment said *"if it fails, the
fix is to delete `pendingLocalChange`, not to loosen it."*

**Then that test started failing — but only without `-race`.** Five failures out of five under
plain `go test`, green every time under `go test -race`. The tempting move at that point is
obvious and wrong: widen the quiescence window until the flake goes away, or declare the
package race-only. Instead it was **re-measured**, six runs varying nothing but the delay
before the removal:

| pc state at the moment of `RemoveTrack` | pion's `OnNegotiationNeeded` |
|---|---|
| `new` | fired 251 ms later — **at the instant the pc reached `connected`** |
| `connecting` | never fired (the pc never connected inside the window) |
| `connected` (×3) | fired **the same millisecond** |

**The determining variable is connectedness**, and nobody had been varying it. On a connected
PeerConnection pion v4.2.16 fires for a removal immediately and reliably. The original
measurement had been taken under `-race`, where DTLS on loopback takes *seconds* — so every
quiescence window expired while the pc was still `connecting`, and **`-race` had been acting
as an unrecognised proxy for "not yet connected."** That is the only regime the finding ever
sampled, and it is the opposite of production, where a relay removes a departed source's track
from sessions that have been carrying media for minutes.

The timing gap is visible in the suite itself: `internal/media` takes **13.9 s** without
`-race` and **88.4 s** with it. A 6× slowdown is not a nuisance in a test that is implicitly
timing a handshake — it is a different experiment.

**The machinery survived, for a reason nobody had identified.** The conditional claim is true
and load-bearing: when a removal lands while the pc is **not stable** — an offer already in
flight, which is exactly what happens during a topology apply — pion's `negotiationNeededOp`
aborts on its signaling-state check and the notification is dropped with nothing to re-raise
it. `TestRemovalNudgeSurvivesTheAnswer` still fails deterministically with the nudge removed.
So `pendingLocalChange` was necessary all along; the stated reason for it was false.

And the risk the corrected finding *introduces* was measured too, rather than argued: on a
connected session two independent sources now want an offer for one removal — pion's trigger
and ours. Double-offering would reintroduce the precise bug the serializer exists to prevent,
through its own fix. Measured at **12/12 runs with a delta of exactly one offer**, six with
`-race` and six without. Whichever trigger arrives first takes `negotiating`; the other returns
at the guard. The serializer does its job.

**What replaced the pin.** `TestPionRenegotiatesARemovalOnceConnected` now waits for
`connected` *first* — which is what makes it invocation-independent, passing identically with
and without `-race` — and then asserts what is actually true. A companion,
`TestSessionRemoveTrackOffersExactlyOnceWhenConnected`, pins the exactly-one-offer invariant on
a connected session, and `TestRemovalNudgeSurvivesTheAnswer` covers the unconnected regime.
One behaviour, two regimes, three tests, and none of them depend on how the suite was invoked.

**The lesson, which is the part worth carrying anywhere.**

> A dependency's behaviour was measured rather than assumed — good. It was recorded in the
> contract and machinery was built on it — good. But the measurement was taken under a single
> invocation mode that turned out to be a **proxy for a state variable nobody was tracking**,
> and that state never occurs in production. The claim was false; the code was right anyway.

Three things fall out of it:

1. **`-race` is not a neutral observer.** It is a 6× time dilation, and any test whose subject
   is a race between your goroutine and a library's dispatch goroutine is *measuring a
   different system* under it. A test that passes under one invocation and fails under the
   other is not flaky — it is reporting that the invocation is an input.
2. **When a test behaves oddly, the first question is "what is it actually measuring?", not
   "how do I make it green."** Widening the window here would have preserved a false belief
   *and* kept the correct code, which is the worst of both: nothing fails, and the next person
   deletes the machinery on the strength of a comment that was never true.
3. **Being right for the wrong reason is a real hazard, not a lucky escape.** The comment on
   `pendingLocalChange` was load-bearing documentation for a flag that looks identical to one
   the project had just deleted. A wrong justification on correct code is precisely the setup
   for a confident, well-reasoned regression.

### 7.3 Two CRITICALs an adversarial review found in the finished system

**A. DNS rebinding bypassed the WebSocket origin gate — on both `/ws` and the dashboard
socket.** `coder/websocket`'s own origin check returns *allow* as soon as the `Origin`
header's host equals the request's `Host` — **before** it consults `OriginPatterns`, and
ignoring the scheme. `Host` is a client-supplied header, so that rule lets a caller authorise
itself. An attacker rebinds `evil.example` to the server's address; the victim's browser sends
`Host: evil.example` with `Origin: http://evil.example`; the two agree; the attacker has a
socket into a process they could never dial directly. That reachability is the *entire* reason
"it only listens on localhost" is considered safe.

The fix: gate on our own `policy.Origins.AllowUpgrade` and return **403 before the upgrade**,
then set `InsecureSkipVerify: true` so the library's shortcut is off the path entirely. The
code carries the reversed-looking line with its explanation, because setting
`InsecureSkipVerify` reads backwards without it.

**The root cause of the vacuous test is the part worth remembering.** The existing origin test
used `websocket.Dial`, which **derives `Host` from the URL** — so it *cannot express the
attack*. The test was green and could never have been anything else. The replacement drives a
raw HTTP request with `Host` and `Origin` set independently, and the dashboard's version
asserts the equivalence directly: for every origin, the WS upgrade decision must equal the
CORS decision, checked "rather than by inspection."

**B. A leaf promoted to relay never forwarded its parent's media.** Two bugs stacked. The
upstream is bound at session-creation time, so a peer promoted from leaf to relay keeps a
parent edge with no upstream and no forward loop — its new child receives nothing, forever.
And the obvious detector, "the offerer role inverted", **only fires when the promoted peer
sorts above its parent alphabetically**: `Offers(L, P)` falls to a name tiebreak. So the core
feature of the phase was a **50/50 coin flip on peer names**.

The fix added two more re-create reasons to the diff — *our* relay-ness changed, and *the
peer's* relay-ness changed — with the reasoning recorded: "reason 2 is invisible half the time
without this bucket, because a promotion usually flips the role as well, but only when the
promoted peer sorts above its parent."

### 7.4 Where the code now contradicts the contract

Three places, found while writing this document. All three are the **contract** being stale,
not the code being wrong — but they are worth listing because "the contract said so" is not a
defence a reader can check.

1. **`Fence.Reset` and `Fence.AdoptAnnouncement` live in `media`, not `cmd/peer`.**
   `PLAN.md` §3.10's "where each predicate is used" table assigns both to `cmd/peer`. In the
   code, `overlay.Fence` is a field on `media.Router`; `Reset()` is called in exactly one
   place — the Router's `TypeJoined` handler — and `AdoptCoordinator` is the Router method
   that raises the epoch. `cmd/peer` decodes the announcement frame and calls that method. The
   shipped placement is better: it keeps the fence next to its only consumer,
   `applyTopology`, and it means the exclusivity argument ("only one method may raise the
   epoch") is checkable inside one package rather than across a binary boundary.
2. **The contract's recorded "KNOWN GAP" on the fence is closed.** §15.13 records that
   `overlay.Fence` shipped after the media work item branched, so `applyTopology` enforced
   only the *ordering* half of the fencing rule (`Supersedes`), leaving authorization
   unenforced. In the code, `applyTopology`'s first act is `Fence.Accept(from, topo)`, and it
   is the only call site of that predicate anywhere. The gap is closed; the note is stale.
3. **A code comment describes a superseded revision of the contract.** `session.go`'s
   clear-before-`SetRemoteDescription` block says *"the contract specifies the opposite
   order."* That was true of contract v2.4; v2.5 adopted the correction, so the code and the
   contract now agree and the parenthetical reads as a live disagreement when it is a
   historical one.

None of these change behaviour. They are recorded here because a design document that only
reports the contract's version of events is a document that cannot be checked.

---

## 8. Limitations

Stated plainly and specifically, because a design document that oversells is worse than one
that admits its edges. Everything here is checkable in the code.

### 8.1 The telemetry is measured now — and what that changed, including a bug

`metrics.Report` has eight fields. In a live run, `cmd/peer`'s `telemetry.sample`
populates all of them:

| Field | Live source |
|---|---|
| `Name`, `Coordinatable` | flags |
| `UploadKbps` | **`-upload-kbps` flag** (default 3000). No bandwidth probe exists. |
| `NAT` | **The nominated ICE candidate pair's LOCAL candidate type** (`media.relayedPath`), folded across every live edge by `media.natClass`, with a measured *direct* verdict remembered for `media.NATDirectMemory`. `-nat direct\|turn` still FORCES a value; the default is now `auto`. |
| `CPUPct` | **`/proc/stat` deltas** (`metrics.CPUSampler`). Host-wide busy %, Linux only; reports nothing elsewhere. |
| `RTTServerMs` | **WebSocket protocol ping** to the arbiter (`metrics.RTTProbe` → `signaling.Client.Ping`). |
| `LossPct` | **RTCP receiver reports** from downstream children, worst leg (`media.lossTracker`). |
| `PeerRTT` | **The nominated ICE candidate pair's round trip** (`media.rttStore`). |

Two of those sources are worth explaining, because the obvious choice does not work.

**pion collects no sender stats at all.** The textbook source for uplink loss and RTT
is `RemoteInboundRTPStreamStats` — the RTCP Receiver Report the far end sends back
about our outbound stream. pion v4.2.16 declares the type and never produces it:
`PeerConnection.GetStats` calls `collectStats` on the ICE gatherer, the ICE transport,
data channels, the SCTP transport, certificates, the media engine, and every
`RTPReceiver` — there is no `RTPSender` collector (`peerconnection.go:2732-2780`). So
loss is taken instead from the `rtcp.ReceiverReport`s the forwarder's drain loop
**already reads** for keyframe requests, which costs nothing extra, and RTT from the
ICE candidate pair, which is better for the purpose anyway: it needs no media flowing,
both ends measure the same path, and it exists as soon as the pair is nominated.
`TestPionPopulatesSelectedPairRTT` pins it.

**Pairwise RTT is remembered, not just measured.** `BuildTree`'s min-latency rank reads
`Node.RTT[candidate]`, and a peer only holds a PeerConnection to its *current* tree
neighbours — so a sensor that forgot a peer when its edge closed would give the
incumbent parent a real number and leave every challenger at `unknownRTT`. No
challenger could win and `-stickiness-ms` could never bind. `media.RTTMemory` keeps a
measurement for 2 minutes after the edge goes, which is what turns a former parent into
a comparable candidate.

Three consequences follow, and each is sharper than "the fields are populated now".

**(a) The degradation machinery can fire in production.** The dwell arms on
`LossPct ≥ 5`, `RTTServerMs ≥ 400`, or `CPUPct ≥ 90`, and all three are now real
numbers. A failed control-link probe reports `metrics.UnreachableRTTMs` (1000) rather
than its measured ~0 elapsed time — a closed socket returns instantly, and recording
that would score a peer whose arbiter connection just died *better* than a healthy peer
on a 20 ms link.

**(b) Voluntary demotion requires all three signals, and this is arithmetic, not
policy.** `Score` weights `0.30 cpu + 0.35 rtt + 0.20 loss + 0.15 uptime` and
`DemoteBelowScore` is `0.35` on a **strict** `<`. A settled peer that is maximally bad
on CPU *and* RTT but clean on loss scores exactly `0.20 + 0.15 = 0.35` and cannot be
demoted. CPU exhaustion alone removes 0.30 of a possible 1.0 and never comes close. So
live demotion needs a loss signal too, which in this system means the peer must also be
a relay with children actually losing packets. `TestScoreFloorOfAFullyDegradedPeer`
pins the numbers.

**(c) Measuring the inputs broke the election, and the fix is `ScoreQuantum`.** This is
the finding worth carrying forward, because nothing about it is visible in the code it
broke. While every eligible peer scored identically, `candidates()` fell through to its
name tiebreak and "the best challenger" was perfectly stable. With real sensors two
comparable peers differ by microseconds of RTT and trade places about once a second —
and the election dwell restarts whenever its target changes. A live three-peer meet sat
on the arbiter for five minutes announcing nothing but its bootstrap. The dwell's own
comment says it exists so "a flapping metric" cannot move the role; the defect was the
same mechanism failing the other way, with a flapping metric *pinning* it. Candidate
ranking now compares scores quantized to `ScoreQuantum` (0.01) — twenty times the
sensor noise, fifteen times smaller than the smallest meaningful term — so a difference
the sensors cannot support cannot reorder anybody. A residual remains and is measured
rather than assumed: a pair whose drifting scores straddle a bucket boundary still
trade places while crossing it, which *delays* a handover without preventing one.

**The NAT class is measured now too, and it is a BEHAVIOURAL classification.** It is
not NAT-type discovery: RFC 5780 needs a STUN server with two addresses, `pion/stun`
does not implement it, and it answers a taxonomic question conclave never asks.
conclave consumes exactly one bit — *may this node be a parent?* — and that bit is
directly observable from machinery already in the tree. `selectedPairRTTMs` walks the
stats report for the nominated, succeeded `ICECandidatePairStats`; that pair carries a
`LocalCandidateID`, and the report also holds the `ICECandidateStats` it names. A local
candidate of type **relay** means the packets leave through a TURN allocation. So the
claim the code makes, and the only one it makes, is *"this peer's media paths are going
through a relay"* — stated in exactly those words in `relayedPath`'s doc comment and in
`metrics.Report.NAT`'s.

Three things shape it, and each was a decision rather than a default.

**It follows the pair, not the report.** Scanning for any relay-typed candidate would
be simpler and would be wrong: every peer configured with `-turn` *gathers* a relay
candidate whether or not it uses one, so a scan classifies an entire TURN-configured
fleet as forced leaves. The remote candidate is ignored for the same reason in mirror
image — the far end being relay-bound says nothing about whether **this** node can
serve children.

**`NATRelayedThreshold` is 1.0, and that is the hysteresis.** A peer is classified
relayed only when EVERY nominated pair it holds is relay-typed. One relayed edge is not
evidence that a node cannot relay — it is demonstrably relaying to the neighbours it
reaches directly — and the bit is brutal enough (forced leaf, disqualified as
coordinator) that flipping on it would tear a working subtree apart on the weakest
available evidence. The cost is recorded rather than hidden: a peer with one stubborn
direct neighbour and four relayed ones stays eligible and may be given children it can
only reach over TURN. That is the conservative direction; the tree still works, it is
merely more expensive.

**A measured direct verdict is REMEMBERED for two minutes, because the verdict was
otherwise a one-way latch that closes on its own.** This is the correction an adversarial
review forced, and it is worth stating as a trajectory rather than as a rule, because the
arithmetic is correct at every single step. `BuildTree` and `Validate` deny a `NATRelayed`
node children. Take peer P, CGNAT-bound upstream but holding a child C on its own LAN:

| step | P's edges | `natClass` | consequence |
|---|---|---|---|
| 1 | `{parent A: relayed, child C: host}` | `natClass(2,1)` → `NATDirect` | eligible, correctly |
| 2 | a rebuild re-parents C away → `{A: relayed}` | `natClass(1,1)` → `NATRelayed` | forced leaf |
| 3 | a forced leaf is never given a child | only edge is the relayed uplink | `NATRelayed` **forever** |

P can never regain the edge that *proved* it was direct; the tree's reaction to the
verdict destroyed the only evidence that could clear it. Both of the `TestNATClass` rows
covering steps 1 and 2 were green, because they test the arithmetic and not the
trajectory. `media.RTTMemory`'s doc comment argues verbatim this structure for the RTT
sensor — "no challenger could ever win" — and the cure is the same: `NATDirectMemory`
(2 min, the same horizon and the same justification) keeps a measured direct verdict
alive after the edge that produced it closes.

The memory is **one-sided on purpose**, and that asymmetry is what makes it safe. A
relayed verdict is never remembered: latching toward `NATRelayed` is the harm (forced
leaf, disqualified as coordinator, no path back), while latching toward `NATDirect`
merely *delays* a demotion and its correction path is immediate — the first measurement
past the horizon reclassifies the peer with no further evidence needed. An unmeasured
tick does not refresh it either, because `natClass`'s permissive answer for a peer with
no edges is an absence of data, not a direct path; treating it as one would restore the
latch by another route. `TestNATVerdictSurvivesLosingItsOnlyDirectEdge` walks the three
steps above; `TestNATDirectMemoryExpires`, `TestNATRelayedIsNeverRemembered` and
`TestNATNoEdgesDoesNotRefreshTheMemory` pin the three ways the fix could over-reach.

What the memory does **not** fix, stated because it is structural: a peer that goes
genuinely TURN-bound stays eligible for up to two minutes and may be handed children it
can only reach over a relay during that window. That is the conservative direction — the
same one `NATRelayedThreshold` already chooses — but it is a real window, not zero. And
a peer that has never had a direct edge has nothing to remember, so first attachment is
unchanged.

**It fails safe on absence, and the chicken-and-egg is the same one `RTTMemory` already
records.** The class is only observable AFTER a PeerConnection exists, so a peer that
has just joined is unclassified — "first attachment is RTT-blind", now also
NAT-blind, and for the identical reason: WebRTC offers no way to characterise a path
without forming it. An unmeasured peer reports `NATDirect`, which is exactly what it
reported when the class was a flag. The opposite default would make every peer a forced
leaf for its first seconds, so a meet's first tree could not be built and the first
election would have no eligible candidate.

**A mutation run moved the seam.** With only the two pure readers pinned, a `Router`
that classified every edge correctly and then returned a **constant** `NATDirect` passed
the entire suite — measured and thrown away is indistinguishable from declared. The fold
now lives in `Router.linkStatsFrom`, taking one stats report per edge, so the only part
of the sensor that needs a live PeerConnection to exercise is the single
`session.stats()` delegation.

The infrastructure that makes the class reachable landed with it: **`cmd/turn`**, a TURN
relay in ~50 lines over `github.com/pion/turn/v5` — already in the module graph, because
pion/webrtc depends on it for the ICE *client* — and `-turn`/`-turn-user`/`-turn-pass`
on the peer. coturn remains the deployment answer and is in `deploy/docker-compose.yml`
behind an opt-in profile; nothing in the test path touches it. A relay nobody can start
is a relay nobody verifies.

`cmd/turn` **denies the private ranges by default** (`-denied-peers`, the same set
compose gives coturn plus multicast), because a relay that will forward into the LAN or
loopback it sits in is a pivot into that network and that is not a state anybody should
reach by omission — the argument `-users` already makes about anonymous allocations. The
tension is real and is resolved in the open rather than by weakening the default:
`docs/verify-turn.md` runs inside a netns on `10.99.0.0/24`, which the default denies, so
the recipe passes an explicit narrowed list and a test reads the document to prove the two
have not drifted apart (`TestVerifyTurnDeniedPeersAdmitsTheNamespace`). A refused
permission is logged at warn, because otherwise a safe default is indistinguishable from
a broken server: ICE simply never completes.

What is still **declared rather than measured**: `UploadKbps`, alone. Phase 7 closes
**one** of the two declared fields, not both. A real bandwidth probe means saturating
the uplink to measure it, in a system whose entire thesis is that the uplink is the
scarce resource; it was not attempted, and pretending otherwise would be the one
dishonest number in this document.

### 8.2 No authentication, anywhere

There is no token, password, credential, JWT, or TLS termination in any non-test file.

- Anyone who can reach `/ws` can join any meet under any unused name.
- Anyone who can reach `/api` can create meets and read every meet's telemetry.
- With `-demo` on (off by default, and logged loudly at startup), anyone who can reach `/api`
  can evict a peer and force an election.

What exists instead: server-stamped identity so a peer cannot impersonate another *on the
wire*; the epoch fence on the control plane; and the origin allow-list on the upgrade and CORS
surfaces. Mitigations in place of auth: unguessable meet ids from `crypto/rand` (40 bits of
Crockford base32 — "a meet id is the only thing standing between a stranger and a meeting"),
`MaxMeets = 100`, `MaxEndedMeets = 20`, `snapshotMinInterval`, `maxSubscribersPerMeet = 64`,
and `maxTrackedMeets` with LRU eviction.

Two consequences deserve their own line. **`Fence` protects the peer, not the arbiter**: it
stops a peer obeying the wrong coordinator; it does not stop a peer claiming someone else's
name at join time (the Hub's unique-name check is first-come, not authenticated). And **the
`-demo` off-by-default is expressed structurally**, not as a boolean a handler must remember to
check — the routes are simply not registered, so "an unregistered route cannot be reached by a
bug in a permission check."

### 8.3 No Byzantine tolerance

The epoch fence stops **stale** actors, not **dishonest** ones. A peer that lies about its
upload budget gets the tree position it lied for. A malicious coordinator can compute an
arbitrarily bad tree within its own epoch. The backup-edge admission check
(`topo.BackupOf(sender) == self`) is explicitly a *consistency* check, not a security
boundary — it stops a confused peer, not a lying one.

**And a peer does not check the tree it is given at all.** `Router.applyTopology` unmarshals a
pushed topology, runs `Fence.Accept` on it (authorization: right sender, right epoch, newer
revision) and adopts it. It never calls `overlay.Validate`, so every peer-side invariant that
reads as structural — depth, degree, a backup that is not already a neighbour — is really a
property of *the coordinator being correct*, enforced where the tree is built and nowhere else.
A hand-authored `-topology` file gets the same trust. Adding the call is a design change with
its own question attached (what should a peer *do* with a tree it has just rejected, when the
alternative is the older tree it already knows is wrong?), so it was not made here; what is
written down is that the guarantee lives upstream. §5.12 names the one place a peer's behaviour
visibly depends on it.

### 8.4 The control plane is a single point of failure, and its epoch is in memory

- The arbiter is one process with no replication and no persistence. If it is down, nobody
  joins, nothing re-optimizes, no handover completes. Media keeps flowing; nothing adapts.
- The epoch counter, the meet registry, and the 20-entry tombstone ring are **in memory**. An
  arbiter restart drops every meet and restarts epochs at 1. That hole is closed by *behaviour*
  rather than by persistence: a restarted arbiter has no meets, so every peer must rejoin, and
  `TypeJoined` resets the peer's fence to the unauthorised zero state. Persisting the counter
  would be the right fix if this were more than a learning system.
- **Meet creation is state-bounded, not rate-bounded.** `MaxMeets` + `MeetTTL` + lazy reaping
  bound how much memory an unauthenticated caller can hold; nothing bounds how fast they can
  churn create/reap. Per-IP rate limiting is out of scope for a system with no auth at all, and
  unlike the demo routes there is no off-by-default answer available, because meet creation
  *is* the product.

### 8.5 Structural limits of the tree design

- **The root's direct children have no backup**, by construction (§5.10). For a shallow tree —
  and `-max-depth` defaults to 2 — that is a large fraction of the nodes. Losing the root is a
  full rebuild, not a warm failover.
- **Backup capacity is not reserved**, so a failover can transiently oversubscribe a relay by
  one child until the next recompute. This claim is true only because rank 0c caps backup
  fan-in; without it the overshoot would be the size of the failed subtree.
- **Trees are path-dependent** (§5.2). Two identical fleets reached by different histories may
  hold different, equally valid trees, and neither is "the right one". Anyone comparing two
  runs and expecting identical topologies is applying the wrong invariant.
- **The root is a function of join order as much as of capacity.** `PickRoot` keeps the
  incumbent unless a challenger beats it by `RootChangeMarginKbps = 2000` (one stream's worth),
  so a peer joining later with 4500 kbit/s never displaces a 3000 kbit/s incumbent. That is
  intended — re-rooting re-parents the whole meet — but a meet whose strongest machine arrives
  second runs permanently on its second-best relay. A mitigation exists (allow re-rooting while
  the meet is small enough that the disruption is trivial) and was deliberately not taken,
  because it adds a size-dependent branch to the one decision that most needs to be boring.
- **A handover during genuinely torn state can still cause one global re-parent.** If the
  reconstruction over the *reduced* heard-from set still fails `Validate` — a tree captured
  mid-re-parent — the new coordinator builds from `prev = nil`.
- **`deriveWorking` has a recorded gap at depth ≥ 3.** An orphan has no edge in the working
  copy, so with `MaxDepth ≥ 3` a *grandchild* can be re-parented gratuitously. The code says
  so, and says it is not fixable without changing `overlay.processingOrder`. The default
  `-max-depth 2` keeps it unreachable in practice.

### 8.6 Concurrency seams that are argued rather than enforced

- **`Publisher.Publish` is called on the coordinator's `Run` goroutine.** Its doc comment says
  it MUST NOT block, and the dashboard's implementation enqueues and returns — but unlike
  `Sender`, there is no dedicated goroutine and no bounded-queue-with-drop in front of it. A
  blocking publisher would stall every meet in the process. This is the one remaining
  unguarded seam of that shape.
- **`pendingLocalChange` is cleared inside `negotiate`, after `SetLocalDescription` and before
  `sendDescription`.** That placement is sound — the committed offer is a full snapshot
  carrying the change, and the retry path re-sends *that exact description* — but **no test
  pins it**. Moving the clear earlier, to before `CreateOffer`, would lose a removal arriving
  in that window, and every existing test would still pass.
- **`pendingLocalChange` is *set* before `pc.RemoveTrack`, not after** — a second ordering in
  the same few lines, added for a real reason and **verified only by argument**. On a connected
  pc, `RemoveTrack` makes pion fire negotiation-needed on its own operations goroutine, which
  can create and send the entire offer before a set-after line would run; the flag would then
  be left set for a change already on the wire, and the next answer would spend it on a
  redundant second renegotiation. Setting it first means any offer the removal provokes clears
  it. The window is real but small, and **12 runs could not be made to discriminate the two
  orderings** — so this is reasoned-sound and unverified, and it is recorded here in the same
  terms as the entry above rather than presented as tested. (What *is* measured is the
  invariant it protects: exactly one offer per removal, 12/12 runs, with and without `-race`.)
- **`simnet` still keeps a `modelCoordinator`** for fast property sweeps. It is pinned to the
  real loop by a differential test, and a known asymmetry is documented (the model learns the
  roster at t0; the real loop learns it per `PeerJoined`), but a sweep that runs only against
  the model cannot discriminate a mutation in the shipped loop.

### 8.7 Verification limits

- **Every live run has been single-host.** All the demos ran multiple processes on one machine
  over loopback. There is no cross-machine result, no real NAT traversal, no real packet loss,
  no real congestion control. The `≈6 Mbit/s at 5 peers` figure is arithmetic on a measured
  per-stream bitrate, not an observed collapse (§1.1).
- **No benchmarks and no fuzz targets** exist in the repository. Performance is last in the
  stated priority order and has not been measured beyond the upload meter.
- **`internal/media` is outside the determinism gate** and holds three wall-clock calls. Its
  tests take 88 s under `-race` (14 s without) because they run real pion, real ICE, and real
  DTLS — and §7.2 records what that 6× dilation did to one measurement before anyone noticed
  it was an input.
- **PLI *response* is unproven.** File and synthetic sources have no live encoder, so keyframe
  request *plumbing* (including the upstream SSRC translation) is proven, but a source
  actually producing a keyframe on demand awaits a browser sender.
- **Phase 7 is partly built.** No simulcast and no SVC: a relay still sends one quality
  layer to every downstream. TURN infrastructure and the measured NAT class DO exist
  (`cmd/turn`, `media.relayedPath`), but **the TURN path has not been exercised live**.
  What has been run by hand is the relay in isolation — a `pion/turn` client allocated
  `127.0.0.1:49176`, inside the pinned `49160-49200`, and a wrong password was refused.
  What has NOT been run is the two together: a real ICE negotiation nominating a relay
  pair with the direct path blocked, and the classification flipping as a result. The
  command sequence believed to prove it is written down in `docs/verify-turn.md` and is
  marked as proposed, not as evidence. That recipe's own firewall rules were WRONG on
  first writing — they blocked host↔host only, leaving the `host↔relay` pair alive, and
  by RFC 8445 §6.1.2.3 that pair *outranks* `relay↔relay` (`2·MAX` is a host priority,
  ~2.1e9, against a relay's ~1.7e7). ICE would have nominated it, only one end would have
  held a relay-typed local candidate, and the run's stated expectation — `turn` for BOTH
  peers — was unreachable. The rules and the flow table are corrected; the point worth
  keeping is that an unexecuted recipe is not evidence about anything, including itself.

### 8.8 Naming and surface honesty

- **`internal/metrics` is a misnomer for two of the three payload types it owns.**
  `Heartbeat` and `Reparented` are liveness/control facts, not telemetry. The rename to
  `internal/control` was costed and deferred, with a stated trigger: split it when a *fourth*
  non-telemetry payload type appears.
- **The dashboard shows two different truths and must label them.** The list endpoint is
  REALIZED (reconstructed from heartbeats — what peers actually did); the detail endpoint and
  the WS snapshot are INTENDED (the coordinator's last published tree). They genuinely
  disagree during convergence, and the gap is diagnostic rather than erroneous. A UI that
  renders whichever it fetched last will be confidently wrong at exactly the moments that
  matter, so `converged`/`diverged` is computed and shown rather than averaged away. Relatedly,
  the per-member fitness value is named `fitness_lower_bound` on the wire, because uptime is
  unreachable from a `MemberSnapshot` — "a value quietly 0.15 too low is *invisibly* wrong."
- **`NAT` is a misnomer, kept for wire compatibility and flagged here rather than
  renamed.** The field, `overlay.NATType`, and the `-nat` flag all say "NAT" while what
  is measured is *whether this peer's media paths go through a relay* — a behavioural
  classification, not a NAT type. `NATRelayed`/`"turn"` is accurate about the
  consequence; `NATDirect`/`"direct"` quietly asserts more than the sensor knows, since
  it is also what an unmeasured peer reports. The honest names would be
  `RelayBound`/`Unclassified`, and the rename was **costed and declined**: the string
  values ride the wire in `metrics.Report`, appear in the dashboard body, and are typed
  by operators as `-nat turn` — a rename touches all three for a clarification that a
  doc comment delivers for free. The stated trigger for revisiting it is the same shape
  as `internal/metrics`': split it when a **third** class appears, because at that point
  the two-valued spelling stops working anyway.
- **`-nat` changed meaning without changing its spelling.** It was the SOURCE of the
  class; it is now an OVERRIDE, defaulting to `auto`. Every existing invocation of
  `-nat turn` still means exactly what it meant, which is why the flag was kept rather
  than deleted — but a reader of an old command line cannot tell from the text whether
  the value was measured or forced. That is what `natSource` in the startup log line is
  for (`nat=measured` vs `nat=forced:turn`), and it is the only place the difference is
  visible: every later telemetry frame carries a bare class either way.
- **The dashboard is read-mostly and eventually consistent.** A `seq` gap triggers a resync;
  that is the only consistency mechanism. It is an operator's lens, not a control surface.
- **The frontend is dark-theme only.** The design system scoped a light variant out
  explicitly; the tokens are structured so a light palette is a re-derivation away, but no
  naive inversion was shipped.

---

## 9. Numbers

### 9.1 Size

| | Lines |
|---|---|
| Go, non-test | **17,505** |
| Go, test | **22,751** |
| Go, total | **40,256** |
| Frontend (`web/`: JS + CSS + HTML) | 2,526 |
| Architecture contract (`docs/PLAN.md`) | 4,999 |

543 non-test functions, 372 test functions, 297 `t.Run` subtests, 69 test files, 13 Go
packages, 5 direct third-party dependencies (`pion/webrtc/v4` v4.2.16, `pion/rtp`,
`pion/rtcp`, `pion/interceptor`, `coder/websocket`), and 145 commits since the Phase 4
baseline (159 in the repository).

### 9.2 Per package

| Package | Src LOC | Test LOC | Non-test funcs | Test funcs | `t.Run` |
|---|---:|---:|---:|---:|---:|
| `internal/media` | 3,973 | 3,514 | 130 | 38 | 24 |
| `internal/coordinator` | 2,544 | 3,714 | 65 | 87 | 7 |
| `internal/dashboard` | 2,319 | 2,889 | 64 | 52 | 28 |
| `internal/arbiter` | 1,690 | 2,421 | 48 | 31 | 82 |
| `internal/overlay` | 1,678 | 2,892 | 35 | 41 | 21 |
| `cmd/server` | 1,252 | 1,139 | 46 | 23 | 25 |
| `internal/simnet` | 1,240 | 2,700 | 75 | 41 | 26 |
| `internal/signaling` | 1,113 | 1,208 | 27 | 18 | 29 |
| `cmd/peer` | 870 | 1,157 | 24 | 17 | 17 |
| `internal/metrics` | 380 | 606 | 9 | 12 | 21 |
| `internal/policy` | 267 | 346 | 8 | 9 | 10 |
| `internal/clock` | 106 | 117 | 10 | 2 | 6 |
| `internal/logging` | 73 | 48 | 2 | 1 | 1 |

Note the ratios rather than the totals. `overlay` — the package holding the algorithm and its
two oracles — carries **1.7× more test than source**. `simnet` carries **2.2×**. `media`, the
only package that cannot be tested deterministically, is the one place the ratio drops below
1:1.

### 9.3 The gate

`make check` (fmt + vet + check-determinism + `go test -race ./...`) **passes**: 13 packages
`ok`, zero failures, zero skips. Plain `go test ./...` — no `-race` — is also 13/13 green; the
gate *specifies* `-race` for the race detector's sake, not because the suite needs it.
`check-determinism` reports **zero** wall-clock references across `overlay`, `simnet`,
`coordinator`, and `arbiter`, test files included.

```
-race:     media 88.405s  cmd/peer 6.530s  everything else 1–2s each
no -race:  media 13.926s  cmd/peer 0.526s  everything else 0.1–0.3s each
```

### 9.4 Measured, historically, per phase

| Phase | What was measured | Result |
|---|---|---|
| 1 | A 2-peer VP8 call over SRTP/UDP | The received file is decodable VP8 per `ffprobe` |
| 2 | Mesh upload cost, via the lock-free meter | 720p VP8 ≈1.5 Mbit/s per stream ⇒ ≈4.6 Mbit/s at 4 peers, ≈6 Mbit/s at 5 (arithmetic on the measured rate; single host, §8.7) |
| 3 | Relay forwarding, topologically proven | A leaf recorded **124 decodable frames** it could only have received *through* the relay — it had no session to the source |
| 4 | The tree computed from telemetry | A leaf recorded **33 decodable VP8 frames** forwarded through a relay the coordinator elected from live reports |
| 5–6 | Path-dependence of the sticky builder | 120 first-report permutations ⇒ **9 distinct valid trees, 1 identical root** |
| 5–6 | Churn under both oracles | 5 seeds × 200 steps, `Validate` + `ValidateLocalRepair` green at every step |
| baseline | The pre-Phase-5 end-to-end run at `c2d25f0` | **322 VP8 frames** relayed through a computed tree, clean shutdown — *recorded in the build log, not in the repository; treat it as a run note rather than a committed artifact* |

### 9.5 Live end-to-end verification

Multi-process runs on one host. Everything below is from a process's own log or the
arbiter's HTTP surface, not from a test harness.

**Phases 5–6, earlier runs.** 323 decodable VP8 frames through a computed relay (no
regression against the pre-Phase-5 baseline of 322); a relay killed with 3 children put
its orphans back on media in 0.76 s with nobody else moved; a killed coordinator had a
successor announced 0.4 ms later at a bumped epoch; the dashboard held live data with
zero console errors and never rolled an epoch backwards.

**The telemetry sensors.** A three-peer meet, one peer's control link behind a
125 ms-each-way TCP proxy:

```
peer      rtt_server_ms   cpu_pct   fitness
alpha             0.320      3.60     0.839
bravo             0.260      3.60     0.839
charlie         121.459      3.60     0.698     <- behind the proxy
```

Loading the host to saturation moved `cpu_pct` 3.6 → 99.30 and fitness 0.839 → 0.551
on the same peers. Before the sensors every one of these was structurally 0 and every
peer scored 0.85–1.0.

**The election, before and after `ScoreQuantum`.** The same three-peer shape run with
`-coordinate -elect`: **before**, the arbiter held the meet for five minutes and logged
one announcement (its own bootstrap), because the two comparable peers traded places on
raw score about once a second and restarted the dwell. **After**, `epoch 2,
coordinator alpha, reason promotion`, 95.2 s after bootstrap — and instrumentation
counted 8 challenger flips over 370 evaluations where before it flipped on nearly every
one.

**Live demotion.** A four-process meet inside an unprivileged user+network namespace,
with the sitting coordinator impaired on all three axes at once (nftables dropping 40%
of its media UDP, a 125 ms-each-way control-link proxy, and a namespaced `/proc/stat`
standing in for a saturated machine): `epoch 2, reason demotion`,
reproduced across two runs. The victim held the role until t+60 s, lost it in the
following interval, and the successor then held it for four more minutes without
flapping.

Note *which* peer replaced it, both times: the one advertising **500 kbit/s**, over a
candidate advertising 9000. That is not a defect — `UploadKbps` does not appear in
`arbiter.Fitness` at all, so the two score alike on the control-plane axes and the name
tiebreak decides. §2.1's control/data-plane split, visible in a live log line rather
than only in the reflection test that enforces it.

**The backup-parent path, and the A/B that finally exercised it.** Cutting a relay's
media with `nft` while leaving its WebSocket alive is what the backup path was designed
for, and it is a failure `SIGKILL` cannot produce:

| Fault | Orphan detection | `via_backup` | Relay health at the coordinator |
|---|---|---|---|
| `SIGKILL` the relay | **9 ms** | `false` | gone — reaped, tree repaired centrally |
| Drop the relay's media UDP only | **8.2 s** | **`true`** | **healthy** — still heartbeating |

Under the UDP cut both children reached `disconnected` at 6.2 s (ICE consent
freshness), waited out `ParentDisconnectGrace`, promoted their precomputed backup at
8.2 s, and the new edge connected in **5 ms**. The coordinator never acted: it still
saw the relay as healthy. Under `SIGKILL` the socket closes instantly and the
coordinator's push arrives in 9 ms, so the peer-local path never runs — which is
exactly why every previous attempt logged `via_backup=false`.

**One thing that run also found**, and it was a real defect rather than a harness
artefact: the promotion reported `ok: false, reason: "new parent connected but carried
no media"`. The promoter forced itself offerer (because `Offers()` is meaningless on an
edge outside the tree), but only an *offer* can add forwarded m-lines (§5.12) — so the
new parent answered, could not publish the forwarded tracks in that exchange, and
`ReparentMediaTimeout` (3 s) expired before any media arrived. The edge was still
committed and the meet converged once the coordinator ratified and the parent
renegotiated as offerer, but the *fast* path's media-evidence clause was unsatisfiable
on a backup edge for any peer expecting forwarded media.

**That is fixed, and the diagnosis above is now history rather than a limitation.** The
roles on a backup edge are inverted: the child asks with a `backup-promote` frame and
**answers**, the authorized parent **offers**, so the first exchange on the promoted edge
carries the forwarded track set (§4.2, §5.12). Three automated tests pin it — the two ends'
roles independently, the *count* of forwarded sources arriving over the promoted edge, and
the failure ladder a promote frame that never lands falls into. The count is the load-bearing
one: the pre-existing `TestRouterPromotesBackupParent` had a single source behind the backup
parent, and a single m-line is exactly what an answering parent can still reuse — so it
passed throughout, on the shape that hides the bug.

The live claim this replaces is narrower than the fix, and worth stating as such: **the fix is
verified by test, not yet by a repeat of the `nft` media-cut A/B.** Until that run is redone,
"failover restores the tree peer-locally in 8.2 s" is measured and "and restores media in the
same exchange" is not.

**A neighbouring defect the fix's fixture exposed, and did NOT fix.** The same m-line
arithmetic bites an *in-tree* edge between two relays. `Offers` breaks a relay-vs-relay tie on
`self > peer`, so in a chain `a → b → c` the middle relay `b` wins the tie and the root `a`
answers — and with two publishers behind the root, `a` owes `b` two forwarded tracks and can
add only the one m-line `b`'s offer already carries. Measured on a five-peer fixture: `b`
receives **1** of the 2 sources from `a`, indefinitely; renaming the root so it wins the
tiebreak makes both arrive. It is out of scope here (the fix is forbidden from touching the
in-tree offerer rule) and it needs its own decision — the `invert` bucket's teardown applied
to a *track-set* change, or a real answerer-side renegotiation trigger.

---

*This is a living document. It describes the system at commit `6b959d1`; when the system
changes, this changes with it.*
