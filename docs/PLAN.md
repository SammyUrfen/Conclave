# PLAN.md — the Phase 5–6 architecture contract

> **Status:** FROZEN as of 2026-09-01, against Phase 4 complete (gofmt/vet clean,
> `go test -race ./...` green, 28 test funcs). This document is the contract every
> Phase 5 and Phase 6 implementation agent builds against.
>
> **Companions:** [`ROADMAP.md`](./ROADMAP.md) is still the source of truth for *what*
> each phase proves and *why the architecture is shaped this way*. `ARCHITECTURE.md` is
> the narrative. **This file supersedes both on names, shapes, and wire formats**, and
> it makes exactly two design calls that ARCHITECTURE.md left open (state handover in
> §6.6, and the additive-apply limitation in §7.4). Those two sections of
> ARCHITECTURE.md must be edited when this lands — see §11 WI-10.

---

## 0. THE CONTRACT CLAUSE — read this before you write a line

Everything below that is written as a Go declaration, a JSON key, a CLI flag name, a
constant name, or a table entry is **FROZEN**.

**Frozen means:**

- **Package names** (`internal/clock`, `internal/arbiter`, `internal/dashboard`, …)
- **Exported type names, field names, and method names** on every declaration in this doc
- **JSON wire keys** on every struct tag and every HTTP body
- **Interface method sets** — exact names, exact parameter and return types, exact order
- **CLI flag names and their defaults**
- **Named constants** and their values
- **The vocabulary in §1**, used identically in Go identifiers, in `docs/`, and in the UI

**An implementer who believes a frozen name is wrong MUST NOT rename it.** Implement it as
specified, and say so in your final report: *"§2.3 froze `Topology.Rev`; I think it should
be `Version` because X."* The owner arbitrates. A silent rename breaks the parallel agents
that compiled against this document, and it breaks the doc, which is worse — the doc is the
only thing keeping five agents from writing five different systems.

**What is NOT frozen:** unexported identifiers, internal algorithm structure, helper
function decomposition, log message text, comment wording, and test names. Use your
judgement freely there.

**When this doc is wrong** — and it will be somewhere — the escalation is: implement what
is written, note the defect in your report, and let the owner amend this file. Do not fork
the contract in a code comment.

---

## 1. Vocabulary (frozen)

| Term | Means | Identifier in Go | Identifier on the wire / UI |
|---|---|---|---|
| **meet** | A room. The unit a user creates and joins. | `roomID string` | `meet_id`, `?room=` |
| **subnet** | The relay tree inside one meet. | `*overlay.Topology` | `subnet` (UI), `topology` (frame) |
| **arbiter** | The central server (`cmd/server`). Sole source of truth for "who is coordinator". **Never a media relay.** | `*arbiter.Arbiter` | "arbiter" (UI) |
| **coordinator** | Control-plane role: ingests telemetry, computes the subnet, pushes it. | `*coordinator.Coordinator` | `role: "coordinator"` |
| **relay** | Data-plane role: a peer with ≥1 child; forwards RTP. | `Topology.IsRelay(name)` | `role: "relay"` |
| **leaf** | A peer with no children. | — | `role: "leaf"` |
| **epoch** | Arbiter-minted monotonic coordinator term. Fences stale actors. | `Epoch uint64` | `epoch` |
| **rev** | Coordinator-minted revision within one epoch. | `Rev uint64` | `rev` |

### 1.1 The meet↔room mapping (this is the rename you must NOT do)

The existing Go code and the existing signaling wire call a meet a **room**: `roomID`,
`?room=`, `room_id`, `signaling.Hub.rooms`. **Those names stay.** `meet` is the
user-facing vocabulary used in the HTTP API, the dashboard UI, and the docs.

> **Frozen identity:** `meet.id == roomID`, character for character. There is no
> translation table, no prefix, no encoding.

Rejected alternative: renaming `roomID` → `meetID` across `signaling`, `coordinator`,
`media`, and both binaries. It lost because (a) the brief freezes the existing frames' JSON
keys, so `?room=` and `room_id` would survive the rename anyway and the codebase would then
carry *both* words for one concept — strictly worse than carrying one word per layer with a
stated mapping; and (b) it is a ~200-line mechanical diff across every package, i.e. a
guaranteed merge conflict between every parallel agent, bought for zero behaviour.

### 1.2 Roles never merge

`coordinator`, `relay`, and `leaf` are **three independent facts** about a peer, and the
ROADMAP is emphatic that control and data roles must not collapse into one "supernode"
score. Concretely, this contract enforces it in three places:

1. `arbiter.Score` (§6.1) consumes **only** control-plane inputs (`CPUFreePct`,
   `RTTServerMs`, `LossPct`, `UptimeSec`, `NAT`). It is a **compile-time and review
   error** for `UploadKbps` to appear in the fitness formula.
2. `overlay.BuildTree` consumes **only** data-plane inputs (`UploadKbps`, `RTT`, `NAT`).
   It does not know who the coordinator is and must never take it as a parameter.
3. A peer's UI role string is a **set**, not a scalar: a node may render as
   `coordinator + relay`. The dashboard renders two independent badges (§9.5).

---

## 2. Package map and the dependency DAG

### 2.1 Existing packages — continuing responsibility

| Package | Continuing responsibility | Change in Phases 5–6 |
|---|---|---|
| `internal/logging` | slog construction, level/format parsing. Leaf. | none |
| `internal/signaling` | WS transport, meets (rooms), frame relay, server-stamped identity, `Observer` seam, `SendTo`. SDP/ICE/Payload stay opaque `json.RawMessage`. | +3 frame types, +`SendRoom`, +2 `Observer` methods, +`ServerID`, +origin policy, +WS keepalive ping (§4) |
| `internal/overlay` | The subnet model + pure `BuildTree` + independent `Validate` oracle. **PURE: no sockets, no clock, no pion, no `internal` imports.** | +epoch/rev/root/backups, +stability-preserving build, +`ValidateLocalRepair` (§3) |
| `internal/media` | Data plane: pion sessions, mesh + tree relay, RTP forwarding, upstream PLI, upload meter. | +negotiation serializer, +`RemoveTrack`, +diff-and-apply `applyTopology`, +backup promotion (§7) |
| `internal/metrics` | The **peer → control-plane payload types** and the peer-side `Reporter`. | +`Heartbeat`, +`Reparented`, +`HeartbeatSender` (§4, §8) |
| `internal/coordinator` | Control-plane brain: fan-in → `BuildTree` → push. Single-goroutine owned state. **Imports NEITHER `signaling` NOR `media`.** | +health FSM, +dwell, +local repair, +backup ratification, +`Publisher`, +`Sync`, +epoch adoption (§5, §6) |
| `internal/simnet` | Deterministic, media-free harness driving the *real* control plane. Test-only. | +virtual clock, +event queue, +failure injection (§3 of the simnet contract, §4) |
| `cmd/server` | Arbiter binary: rendezvous, signaling, coordinator host, dashboard host. Wires every seam. | +dashboard mount, +arbiter, +new flags (§10) |
| `cmd/peer` | Participant binary. | +heartbeat, +backup promotion, +epoch handling, +new flags (§10) |

> **`metrics` naming smell, acknowledged.** `metrics.Heartbeat` and `metrics.Reparented`
> are liveness/control facts, not telemetry. The alternative was a new
> `internal/control` package holding all three peer→control-plane payload types.
> **Rejected for now:** it buys a better name at the cost of a fourth tiny leaf package,
> a new import edge in `coordinator`, `metrics`, and both binaries, and a rename of
> `metrics.Report` (a frozen wire type). **Trigger to revisit:** the moment a *fourth*
> non-telemetry payload type appears, split `internal/control` out then. Amend the
> `metrics` package doc to say it owns "the peer → control-plane wire payloads".

### 2.2 New packages

| Package | Responsibility | May import (internal) | Imported by |
|---|---|---|---|
| `internal/clock` | The injectable time seam: `Clock`, `Timer`, `Ticker` interfaces + the real implementation. **Leaf. No internal imports, ever.** | — | `signaling`, `metrics`, `media`, `coordinator`, `arbiter`, `simnet`, `cmd/*` |
| `internal/arbiter` | Meet registry; epoch minting; coordinator-fitness scoring; election policy; the announcement. **No sockets** — defines its own `Announcer` and `Publisher` seams. | `overlay`, `clock` | `cmd/server`, `dashboard` (via consumer interfaces), tests |
| `internal/dashboard` | The browser-facing HTTP + WS surface: REST commands, the live event stream, the event envelope, CORS/origin policy, the demo control surface. | `overlay`, `coordinator`, `clock` | `cmd/server` |
| `web/` | The zero-build static frontend (vanilla ES modules, no bundler, no framework). **Not a Go package.** | — | — (served by GitHub Pages) |
| `deploy/` | `Dockerfile` + deploy docs for a hosted `wss://` arbiter. **Not a Go package.** | — | — |

`internal/dashboard` imports `coordinator` for `coordinator.Event` / `coordinator.RoomSnapshot`
and `arbiter` for nothing — it declares its own `MeetSource` and `DemoControl` consumer
interfaces (§9.7) that `cmd/server` satisfies with `*arbiter.Arbiter`. That keeps the
dashboard agent and the arbiter agent from blocking each other.

### 2.3 The full allowed dependency DAG

```
                       ┌──────────────┐   ┌──────────────┐
   binaries (top)      │  cmd/server  │   │   cmd/peer   │      may import anything
                       └──────┬───────┘   └──────┬───────┘
              ┌───────────────┼──────────┐       │
              ▼               ▼          ▼       ▼
        ┌───────────┐   ┌──────────┐  ┌──────────────┐
        │ dashboard │   │ arbiter  │  │    media     │
        └─────┬─────┘   └────┬─────┘  └──────┬───────┘
              │              │               │
              ▼              │        ┌──────┴──────┐
       ┌─────────────┐       │        ▼             ▼
       │ coordinator │◄──────┘  ┌───────────┐  ┌─────────┐
       └──────┬──────┘          │ signaling │  │ overlay │
              │                 └─────┬─────┘  └────▲────┘
              ▼                       │             │
        ┌───────────┐                 │             │
        │  metrics  │─────────────────┼─────────────┘
        └─────┬─────┘                 │
              │                       │
              ▼                       ▼
        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │ overlay │  │  clock  │  │ logging │      leaves: no internal imports
        └─────────┘  └─────────┘  └─────────┘

   simnet (test-only, top-of-graph for tests) → overlay, clock, coordinator, metrics
```

Written as an edge list, which is what you check against:

| Package | May import (internal), exhaustively |
|---|---|
| `logging` | — |
| `clock` | — |
| `overlay` | — |
| `metrics` | `overlay`, `clock`, `logging` |
| `signaling` | `logging`, `clock` |
| `coordinator` | `overlay`, `metrics`, `clock`, `logging` |
| `arbiter` | `overlay`, `clock`, `logging` |
| `media` | `overlay`, `signaling`, `clock`, `logging` |
| `dashboard` | `overlay`, `coordinator`, `clock`, `logging` |
| `simnet` | `overlay`, `coordinator`, `metrics`, `clock` |
| `cmd/*` | anything under `internal/` |

**Hard rules, restated because they are the ones that get violated:**

- **`overlay` stays pure.** No `clock`, no `signaling`, no `media`, no `time.Now()`.
  Everything temporal is passed in as a value. This is what makes `BuildTree` replayable
  and what keeps `simnet` pion-free.
- **`simnet` imports nothing from `media` or `signaling`,** and **production never
  imports `simnet`**. If a control-plane behaviour cannot be tested from `simnet`, the
  fix is to move that behaviour behind an interface, not to import pion into the harness.
- **`coordinator` imports neither `signaling` nor `media`.** Both seams stay
  consumer-defined and are adapted in `cmd/server/main.go`: inbound it *structurally
  satisfies* `signaling.Observer` (no import needed — Go's implicit interfaces), outbound
  it *declares* `Sender` and `Publisher` and takes values of them.
- **A cycle is a contract violation, not a refactor.** If you find yourself needing a
  back-edge, the answer is always the same idiom: declare the interface at the point of
  use, and let `cmd/*` wire the concrete ends. Go forbids cycles at compile time, so you
  will find out immediately — but "I made it compile by moving a type" is exactly the move
  that erodes the DAG. Report it instead.

> **Go idiom, briefly (load-bearing here).** Go interfaces are *structural* and
> *consumer-defined*: the implementer never names the interface, so the dependency arrow
> points from the consumer to the data, not from the producer to a shared "interfaces"
> package. That is the entire mechanism keeping this DAG acyclic. Coming from Java, the
> instinct is to put `Publisher` next to its implementation in `dashboard` and have
> `coordinator` import it — that creates the back-edge. Put it where it is *used*.

### 2.4 The one place structural typing is not enough

`clock.Clock` **must** be a shared package, not a per-consumer interface, and the reason is
worth understanding because it is the exception that proves the rule above.

A consumer-defined `interface{ Now() time.Time }` works fine — any clock satisfies it
structurally. But the moment the interface has `NewTimer(d) Timer`, the *return type*
becomes part of the method signature. A method returning `coordinator.Timer` does **not**
satisfy an interface requiring `simnet.Timer`, even if the two interfaces are identical:
Go compares the named types, not their shapes, in a return position. So every consumer
duplicating the interface would need a distinct fake per consumer.

Hence one leaf package, `internal/clock`, owning `Clock`, `Timer`, `Ticker`. It sits
alongside `logging` as an *ambient capability* (like `*slog.Logger`), not a service seam.

---

## 3. The `overlay` contract

### 3.1 `Topology` — the new shape

```go
package overlay

// Topology is a relay tree (a "subnet") over peers named by stable label, stamped with
// the control-plane term that authorised it. It stays pion-free and clock-free.
//
// (Epoch, Rev) is the fencing token, compared LEXICOGRAPHICALLY. Epoch is the arbiter's
// coordinator term and changes only on a coordinator handover; Rev is the coordinator's
// own revision counter within its term. Two fields rather than one because they have two
// different single writers (§6.4): only the arbiter may raise Epoch, only the sitting
// coordinator may raise Rev. Collapsing them into one counter would mean a coordinator
// could mint a number that fences the arbiter's own announcement — the precise failure
// the epoch exists to prevent. This is Raft's (term, index) split, for the same reason.
type Topology struct {
	// Epoch is the arbiter-minted coordinator term this tree was computed under.
	// Always ≥ 1 in a computed tree; 0 only in a not-yet-stamped zero value.
	Epoch uint64 `json:"epoch"`
	// Rev is the coordinator's revision within Epoch, starting at 1 and incrementing
	// on every published tree. Resets to 1 when Epoch advances.
	Rev uint64 `json:"rev"`
	// Root is the tree's source node — the unique node with no parent. Stored
	// explicitly (rather than derived by scanning Edges) so a peer, the media Router,
	// and the dashboard can each answer "am I / who is the root" in O(1) without
	// re-deriving it three different ways. Validate enforces that it agrees with Edges.
	Root string `json:"root"`
	// Edges are directed parent→child links, IN TOPOLOGICAL (ATTACH) ORDER: for every
	// i, Edges[i].Parent is either Root or appears as some Edges[j].Child with j < i.
	// This ordering is an INVARIANT, not an accident — the stability-preserving rebuild
	// in §3.3 replays it directly, and Validate checks it.
	Edges []Edge `json:"edges"`
	// Backups are the precomputed warm secondary parents, at most one per node. A node
	// absent from this slice has no backup and must wait for a coordinator push on
	// parent failure (see §3.5 for when that legitimately happens).
	Backups []Backup `json:"backups,omitempty"`
}

// Edge is one parent→child link, named by peer label. Unchanged from Phase 3.
type Edge struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
}

// Backup assigns Node a warm secondary parent to fail over to when its primary parent
// is lost, without waiting for a coordinator recompute.
type Backup struct {
	Node   string `json:"node"`
	Parent string `json:"parent"`
}
```

**Why `Backups []Backup` and not `map[string]string`.** `encoding/json` sorts map keys, so
determinism is *not* the discriminator — both marshal reproducibly. The slice wins on two
smaller points: it preserves the builder's assignment order, which makes a diff between two
published trees readable in the dashboard event stream; and it lets `Validate` report a
failing index (`backup 3: …`) the way it already reports `edge 3: …` for `Edges`, keeping
one error idiom in the package. Accessors hide the difference at every call site anyway.

**Why `Root` is stored despite being derivable.** It is redundant, and redundancy is a
chance to disagree. The mitigation is that `Validate` treats disagreement as a hard error,
so the redundancy can never survive into a published tree. What it buys: `media.Router`
needs "am I root" on every apply, `coordinator` needs it to detect a re-root event (§5.4),
and the dashboard needs it to lay the graph out — three scans replaced by one field.

### 3.2 New and changed accessors

```go
// BackupOf returns the precomputed secondary parent for name, or "" if none.
func (t *Topology) BackupOf(name string) string

// Depth returns the hop distance from Root to name, or -1 if name is not in the tree.
func (t *Topology) Depth(name string) int

// Subtree returns name plus every node reachable downward from it, in BFS order.
// Used by the backup-parent invariant (§3.5) and by local repair (§5.4).
func (t *Topology) Subtree(name string) []string

// Supersedes reports whether t is strictly newer than prev under the lexicographic
// (Epoch, Rev) order. A nil prev is superseded by anything. This is the ONE place the
// fencing comparison is written; every peer, the Router, and the coordinator call it
// rather than hand-rolling the comparison.
func (t *Topology) Supersedes(prev *Topology) bool
```

Unchanged and still frozen: `ParentOf`, `ChildrenOf`, `NeighborsOf`, `IsRelay`, `Nodes`,
`Offers`.

> **`Offers` needs no change.** Its rule (the relay offers on every edge; a name tie-break
> when both endpoints are relays) is still glare-free under re-parenting, because it is a
> pure function of the *new* topology and both endpoints apply the same new topology. A
> transient disagreement during a push is handled by the negotiation serializer (§7.1),
> not by changing this rule.

### 3.3 `BuildTree` — stability-preserving

```go
// Constraints parameterise a build.
type Constraints struct {
	Root       string // node at depth 0; required
	MaxDepth   int    // max hops root→leaf; ≥ 1
	StreamKbps int    // per-child upload cost; > 0
	// Epoch is stamped onto the result. The coordinator supplies the term it is
	// serving under; BuildTree never invents one. Must be ≥ 1.
	Epoch uint64
	// Rev is stamped onto the result. Must be ≥ 1 and, when prev is non-nil with the
	// same Epoch, must be > prev.Rev — BuildTree rejects a non-advancing revision
	// rather than silently emitting a tree no peer will accept.
	Rev uint64
	// StickinessMs is the RTT margin (ms) by which a challenger must beat a node's
	// INCUMBENT parent before the node is re-parented. It is the anti-thrash knob at
	// the graph layer: 0 makes BuildTree behave like Phase 4 (memoryless, purely
	// greedy); a large value pins the tree in place. See DefaultStickinessMs.
	StickinessMs float64
}

// DefaultStickinessMs is the re-parent margin used unless a caller overrides it.
//
// 25 ms is chosen against the ARCHITECTURE's own latency budget: ~150 ms one-way is
// "good" and ~400 ms is the ceiling, so a 25 ms improvement is ~6% of the usable range —
// small enough that trading a stream interruption for it is a bad deal, large enough that
// a genuinely closer relay (a LAN peer at 5 ms vs a WAN peer at 60 ms) still wins
// immediately. It is also comfortably above the sampling noise on a residential link,
// where consecutive RTT samples routinely differ by 5–15 ms.
const DefaultStickinessMs = 25.0

// BuildTree computes a degree-bounded, depth-limited, min-latency relay tree by a greedy
// heuristic that PREFERS THE PREVIOUS TREE. It remains pure: no sockets, no clock, no
// randomness — same (nodes, prev, c) always yields the same tree.
//
// prev may be nil (first build for a meet, or after a coordinator handover that has not
// yet rebuilt state). A non-nil prev makes the build stability-preserving, which is what
// Phase 5's "local repair — re-parent only the subtree" requires: with prev supplied, a
// node's parent changes only when it HAS to (§3.4), so a rebuild after one departure moves
// exactly the orphans and nobody else. That property is asserted by ValidateLocalRepair.
func BuildTree(nodes []Node, prev *Topology, c Constraints) (*Topology, error)
```

> **Why `prev` is a parameter and not a field on a stateful builder.** A stateful
> `Builder` holding the last tree would put mutable state in the one package this project
> keeps pure, and would make the function's output depend on call history — the exact
> thing that makes a control algorithm untestable. Passing the previous tree in keeps
> `BuildTree` a pure function of its arguments, so a `simnet` scenario can replay any
> sequence of trees by replaying the argument list. The coordinator owns the "what was
> last published" state, because the coordinator already owns all state (§5.1).

### 3.4 The EXACT preference ordering (frozen)

`BuildTree` runs in two nested orderings. Both are frozen.

**(A) Node processing order** — this is the fix for the memoryless-rebuild defect. Phase 4
sorted every node by upload-desc, so a strong joiner claimed capacity ahead of incumbents
and could re-parent the world. The new order is:

1. **Incumbents first**, in `prev.Edges` order (which §3.1 guarantees is topological, so
   parents are placed before their children — this is why that invariant is load-bearing
   and not decoration). Skip any incumbent not present in `nodes`.
2. **Then newcomers** — nodes in `nodes` with no entry in `prev` — sorted upload
   descending, then name ascending.
3. With `prev == nil`, step 1 is empty and this degenerates exactly to Phase 4's order.

Consequence: a strong newcomer can no longer displace an incumbent's claim on a parent's
capacity, because the capacity was already spent when the incumbent was placed. It can
still *become* a relay for later newcomers — which is the behaviour we want.

**(B) Parent selection for node `u`** — `bestParent`. Applied strictly in this order:

| Rank | Rule | Kind |
|---:|---|---|
| 0 | **Hard filters.** Candidate `p` must be: already attached in this build; `capacityOf(p, c) > 0` (not TURN-bound, enough upload); `depth[p] < c.MaxDepth`; `children[p] < capacityOf(p, c)`; `p != u`. | eliminate |
| 1 | **Incumbency.** If `prev.ParentOf(u) == P` and `P` survived rank 0, then **`P` wins** — unless some surviving candidate `q` satisfies `rtt(u,q) + c.StickinessMs < rtt(u,P)`. Only a *materially* closer parent breaks incumbency. A merely-less-loaded or alphabetically-earlier parent never does. | prefer |
| 2 | **Minimum RTT.** Among the remaining candidates (all of them, when rank 1 did not decide), lowest `rtt(u, p)`. Unknown RTT scores `unknownRTT` (worst), unchanged. | prefer |
| 3 | **Fewest children.** Load balance. This is what makes attachment sane on a LAN where no RTT has been measured. | prefer |
| 4 | **Name ascending.** Determinism. Total order, no ties survive. | prefer |

Read rank 1 carefully: **"keep the existing parent" sits ABOVE RTT and ABOVE load, but
BELOW the hard constraints.** That placement is the whole design.

- *Above RTT* (with a margin): re-parenting costs a stream interruption. A 5 ms RTT win is
  not worth a visible freeze; a 200 ms win is. `StickinessMs` is where that line is drawn,
  and it is a flag (§10) precisely because the ROADMAP demands the anti-thrash constants be
  tunable and tested at both extremes.
- *Above load*: load-balancing is a nice-to-have; stability is a correctness-adjacent
  property here. Letting "P now has 3 children and Q has 1" re-parent a node would make the
  tree churn on every join, which is exactly the Phase 4 defect.
- *Below the hard constraints*: an incumbent parent that is gone, TURN-bound, at capacity,
  or too deep is simply not a candidate. Stickiness must never produce an invalid tree —
  `Validate` is an independent oracle and would catch it, loudly.

> **Rejected alternative: a scored objective function** (`cost = α·rtt + β·churn + γ·load`,
> pick the argmin). It is more expressive and would let one knob trade all three off
> smoothly. It lost on *explainability*: a lexicographic ordering lets you answer "why is
> B parented to R?" by walking four rules, while a weighted sum requires reconstructing
> three floats and their weights. For a learning project whose whole point is that every
> decision is explainable, and where the inputs are noisy enough that false precision is a
> real risk (ARCHITECTURE §6), the lexicographic rule is the better instrument.

### 3.5 Backup parents

Computed **after** the primary tree is complete, as a second pass over `Edges` in order.

**The invariant (frozen):**

> For a node `u` with primary parent `P`, its backup `B` must satisfy
> **`B ∉ Subtree(P)`** — the backup must lie entirely outside the subtree rooted at `u`'s
> own parent.

**Why this, and why it is stronger than "not inside its own subtree".** The failure the
backup insures against is *"`P` is gone"*. That single event orphans **everything in
`Subtree(P)`**, not just `u`. So:

- If `B` were a *descendant of `u`*, then on failover `u` attaches to `B` while `B`'s path
  to the root runs through `u` — a cycle, and both are severed from the root. This is the
  rule the brief named, and it is necessary.
- If `B` were a *sibling of `u`* (another child of `P`), it is outside `Subtree(u)` and so
  passes the weaker rule — but `B` is orphaned by the very same event. Failing over to it
  produces two orphans hanging off each other, still disconnected from the root.

`Subtree(P) ⊇ Subtree(u) ∪ siblings(u)`, so the stated invariant implies the brief's rule
and additionally rules out the sibling trap. **This is a place where I deliberately
strengthened the brief; it is flagged in §12.**

**Selection among valid candidates**, in this exact order:

| Rank | Rule | Why |
|---:|---|---|
| 0 | Hard: `B != u`, `B != P`, `B ∉ Subtree(P)`, `capacityOf(B, c) > 0`, `Depth(B) < c.MaxDepth`. | must be a legal parent that survives P's loss |
| 1 | `children[B] < capacityOf(B, c)` — **has spare capacity** — preferred over one that does not. | a failover into spare capacity is non-disruptive |
| 2 | `Depth(B) ≤ Depth(P)` — **no deeper than the parent it replaces** — preferred. | keeps `u`'s subtree from sinking past `MaxDepth` on failover |
| 3 | Minimum `rtt(u, B)`. | the failover target should also be a good parent |
| 4 | Fewest children, then name ascending. | balance, then determinism |

**Capacity is NOT reserved for backups.** The alternative — carve each relay's child
capacity into "primary slots" and "reserved backup slots" — was rejected because at the
scale this system actually runs (4–8 peers on residential upload), capacity is already the
binding constraint; halving usable fan-out to insure against a failure that triggers one
recompute is a bad trade. The consequence is honest and bounded: **at the instant of a
failover a relay may transiently serve one child more than its computed capacity**, until
the coordinator's recompute lands (bounded by `RecomputeCooldown`, §5.2). Note that this
overshoot exists only in a peer's *realized* state — it is never encoded in a `Topology`,
so `Validate` still holds for everything this package produces.

**Nodes with no backup.** If `P == Root`, then `Subtree(P)` is the entire tree and no valid
`B` exists. So **the root's direct children have no backup**, by construction. This is
correct, not a gap: losing the root is a whole-subnet event that re-roots the tree (§5.4),
and losing the *coordinator* — which is often but not always the root — is a Phase 6
election (§6). Both are handled by a full rebuild, not by a per-node warm standby. Say so
in the UI: the dashboard renders "no backup (root child)" rather than an alarming blank.

### 3.6 What `Validate` must additionally check

`Validate(t *Topology, nodes []Node, c Constraints) error` keeps every Phase 4 property
(single root, is-a-tree, connected, depth ≤ MaxDepth, capacity, TURN-leaf, closed world)
and gains:

1. **`t.Epoch ≥ 1` and `t.Rev ≥ 1`.** A zero-stamped tree is unpublishable; reject it here
   rather than letting every peer silently ignore it.
2. **`t.Root` agrees with `t.Edges`**: `t.Root` is exactly the unique node with no parent,
   and `t.Root == c.Root`.
3. **Topological edge order**: for every `i`, `Edges[i].Parent == t.Root` or
   `Edges[i].Parent` appears as `Edges[j].Child` for some `j < i`.
4. **Backup well-formedness**: at most one `Backup` per `Node`; every `Backup.Node` is a
   known non-root node; every `Backup.Parent` is a known node; `Parent != Node`;
   `Parent != ParentOf(Node)`.
5. **The backup invariant**: `Backup.Parent ∉ Subtree(ParentOf(Backup.Node))`.
6. **Backup legality**: `capacityOf(Backup.Parent, c) > 0` and
   `Depth(Backup.Parent) < c.MaxDepth`.

Plus a **second, separate oracle** for the churn property — the one that makes "local
repair" testable rather than merely asserted:

```go
// ValidateLocalRepair asserts that next differs from prev only where it had to.
//
// gone is the set of node names that departed (or were declared gone) between the two
// builds. joined is the set that arrived. The property: every node present in BOTH prev
// and next whose parent CHANGED must be justified — it must be in gone's prev-subtree
// (orphaned), or its prev parent must be absent from next's node set, or its prev parent
// must have become ineligible (over capacity or past depth) in next.
//
// This is deliberately a separate function from Validate: Validate answers "is this tree
// legal?", which is a property of one tree; this answers "was this change minimal?",
// which is a property of a TRANSITION. Conflating them would make Validate need a prev
// it does not otherwise want, and would make the churn assertion silently skippable.
func ValidateLocalRepair(prev, next *Topology, gone, joined []string) error
```

### 3.7 `Node.Provisional` — telling a default apart from a measurement

```go
// Node gains ONE field. Everything else is unchanged.
type Node struct {
	Name       string
	UploadKbps int
	RTT        map[string]float64
	NAT        NATType
	// Provisional marks a node whose telemetry is an ASSUMED DEFAULT, not a measurement
	// — a peer that has joined but whose first metrics report has not arrived. It is set
	// by the coordinator's projection (coordinator.Config.DefaultUploadKbps supplies the
	// number; this flag records that the number is a guess).
	//
	// Why overlay needs to know: a provisional node may be ATTACHED (as a leaf, which is
	// what a 0-upload default already makes it) but may NEVER be chosen as ROOT. Rooting
	// on a guess is how the tree ends up rerooting a second later when the real number
	// arrives — the most expensive reconfiguration there is, triggered by nothing but
	// WebSocket arrival order. Expressing it as a plain value on Node keeps overlay pure:
	// the package still has no idea what a "report" is, only that this datum is soft.
	Provisional bool
}
```

### 3.8 `PickRoot` — sticky, and never provisional

Root churn is the single most expensive thing this control plane can do: with a
stability-preserving builder (§3.4), a *changed root* invalidates every parent choice in the
tree at once — the exact opposite of the "move one subtree, not the world" property Phase 5
exists to provide. So root selection gets its own stability rule, stronger than the
per-node one.

```go
// PickRoot chooses the tree's root. Three rules, in order:
//
//  1. NEVER a provisional node, and never a node that cannot parent (TURN-bound, or too
//     little upload). Rooting on an assumed default is how the root flaps on telemetry
//     ARRIVAL ORDER rather than on telemetry CONTENT — nondeterminism the deterministic
//     simulation contract exists to eliminate.
//  2. KEEP THE INCUMBENT. If prev is non-nil and prev.Root is present, non-provisional,
//     and still able to parent, it is retained unless some eligible challenger advertises
//     at least RootChangeMarginKbps MORE upload than it.
//  3. Otherwise: highest-upload eligible node, ties broken by name.
//
// Returns "" when no eligible node exists. A lone node is returned as-is (a one-node tree
// is trivially valid) EVEN IF provisional — there is nothing to be wrong about.
func PickRoot(nodes []Node, prev *Topology) string

// RootChangeMarginKbps is how much more upload a challenger must advertise before it is
// worth re-rooting the entire subnet.
//
// 2000 kbit/s is exactly one stream at the default -stream-kbps, i.e. the challenger must
// be able to serve at least one MORE child than the incumbent before the swap is even
// considered. That is the smallest difference that buys anything structural; anything
// less re-parents every peer in the meet to gain nothing. Deliberately an ABSOLUTE margin
// rather than a ratio: capacity here is integer children (floor(upload / streamKbps)), so
// a percentage margin would mean different things at different fleet sizes, while "one
// more child's worth" means the same thing everywhere.
const RootChangeMarginKbps = 2000
```

Call sites to update: `coordinator.recompute` and `simnet.Network.Build`. Both pass their
current topology; both pass `nil` on the first build.

> **This closes an observed live defect.** In a 3-peer managed run, `PickRoot(nodes)` was
> called on every threshold event over a projection in which unreported peers sit at
> `DefaultUploadKbps = 0`. In the window where a weak leaf had reported and the strong
> relay had not, `PickRoot` legitimately returned the weak node, `BuildTree` correctly
> rejected it (`root "leaf-b" cannot serve any children`), and the whole recompute was
> discarded with a WARN — three times, before self-correcting. Benign in Phase 4 (the apply
> was additive and the next report fixed it); **not** benign once the builder is
> stability-preserving, because a root that flaps on frame arrival order defeats the entire
> stability design, and because in Phase 6 the same partially-populated telemetry view also
> feeds fitness ranking. Rules 1 and 2 above fix the root half; §5.9 fixes the "should we
> have built at all" half.

### 3.9 `LoadTopology` must keep working

The Phase 3 static-file path is still supported (`peer -topology tree.json`), and existing
files have no `epoch`, `rev`, `root`, or `backups`.

**Frozen compatibility rule:** after unmarshalling and the existing structural validation,
`LoadTopology` fills in defaults:

- `Epoch == 0` → set to `StaticEpoch`
- `Rev == 0` → set to `1`
- `Root == ""` → derive it as the unique parentless node; if there is not exactly one,
  fail loud with the same fail-loud discipline the loader already has.
- `Backups` stays nil. A static tree has no failover, which is correct — Phase 3's whole
  premise is a hand-authored, unchanging tree.

```go
// StaticEpoch is the epoch stamped on a hand-authored topology file. It is deliberately
// the MAXIMUM uint64 rather than 1: a static-tree peer is not participating in the
// election plane at all, and stamping it max means no coordinator push can ever supersede
// the operator's explicit file (Supersedes returns false for every real epoch). A peer
// run with -topology is pinned, by definition. Using 1 instead would let a coordinator on
// the same server silently overwrite the operator's tree, which is a surprising and
// hard-to-debug interaction between two modes that are supposed to be independent.
const StaticEpoch uint64 = ^uint64(0)
```

Note the existing guard already stops the two modes combining: `cmd/peer` rejects
`-topology` together with `-managed`. `StaticEpoch` is defence in depth for the case where
a static peer shares a meet with managed ones.

---

## 4. The `simnet` contract and the clock seam

### 4.1 `internal/clock` — the injectable time seam

```go
// Package clock is the injectable time seam. Production wires clock.System(); tests and
// simnet wire a virtual clock, so no control-plane test ever touches the wall clock.
//
// Timer and Ticker expose their channel through a C() METHOD rather than the C FIELD that
// time.Timer/time.Ticker use. That is forced: a Go interface can declare methods but not
// fields, so a *time.Timer cannot satisfy an interface that mentions C. The real
// implementation is therefore a one-line wrapper. This is the standard shape for clock
// injection in Go and the reason a "just use time.Timer" shortcut does not exist.
package clock

import "time"

// Clock is the subset of package time this project depends on.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
	After(d time.Duration) <-chan time.Time
}

// Timer mirrors *time.Timer.
type Timer interface {
	C() <-chan time.Time
	// Reset restarts the timer for d. Like time.Timer.Reset, the return value reports
	// whether the timer was active; like time.Timer.Reset, callers must not assume the
	// channel is drained. The dwell timers in §5.2 only ever Reset a timer they own on
	// a single goroutine, which is the one usage pattern where Reset is unambiguous.
	Reset(d time.Duration) bool
	Stop() bool
}

// Ticker mirrors *time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// System returns the real, wall-clock implementation backed by package time.
func System() Clock
```

### 4.2 How production obtains a clock (frozen)

**By construction-time injection, stored on the struct, defaulting to `clock.System()` when
the field is nil.** Every constructor that needs time gains exactly one config field:

```go
coordinator.Config{ …, Clock clock.Clock }   // nil ⇒ clock.System()
arbiter.Config{    …, Clock clock.Clock }    // nil ⇒ clock.System()
signaling.HubConfig{ …, Clock clock.Clock }  // nil ⇒ clock.System()
metrics.NewReporter(log, interval, clk clock.Clock, sample, send)
media.RouterConfig{ …, Clock clock.Clock }   // nil ⇒ clock.System()
media.SessionConfig{ …, Clock clock.Clock }  // nil ⇒ clock.System()
```

> `signaling.NewHub(log)` gains a sibling `signaling.NewHubWithConfig(HubConfig)`;
> `NewHub` stays as-is and delegates with defaults, so no existing call site changes.
> Same pattern for `metrics.NewReporter` — add the parameter, and update the three
> existing call sites in one commit (WI-0).

Rejected alternatives, all of which are common and all of which lose:

| Alternative | Why it lost |
|---|---|
| Package-level `var now = time.Now`, swapped in tests | A global. Two parallel tests swapping it race; `go test -race` will not necessarily catch it, and it makes "which clock is this code on" invisible at the call site. |
| Carry the clock on `context.Context` | `context.Value` for a *dependency* is a documented Go anti-pattern: untyped, invisible to the compiler, and it makes every function that might need time take a ctx it otherwise wouldn't. Context is for cancellation and deadlines, not DI. |
| Pass `clock.Clock` as a parameter to every call site | Churns dozens of signatures for a value that is fixed for the lifetime of the object. The struct field is the same injection with none of the noise. |
| Define the clock interface per-consumer | Breaks on the `NewTimer` return type — see §2.4. |

### 4.3 `simnet` — virtual clock

```go
package simnet

// VirtualClock is a deterministic clock.Clock: time advances only when Advance is
// called, never on its own. It is the mechanism that makes hysteresis and heartbeat
// timeouts testable — both are inherently temporal, and neither can be asserted on
// reliably against a wall clock without sleeps that are either flaky or slow.
type VirtualClock struct{ /* unexported */ }

// NewVirtualClock returns a clock stopped at start.
func NewVirtualClock(start time.Time) *VirtualClock

// Now, NewTimer, NewTicker, After satisfy clock.Clock.

// Advance moves virtual time forward by d, firing every timer and ticker that becomes
// due, in (deadline, creation-sequence) order.
//
// DETERMINISM RULE: timers due at the same virtual instant fire in the order they were
// created (or last Reset). A monotonic sequence number is assigned at NewTimer/Reset time
// and is the tiebreak — never map iteration order, which Go randomises.
//
// DELIVERY RULE: every timer channel is buffered with capacity 1 and Advance performs a
// non-blocking send. A test that has not drained a fired timer therefore cannot deadlock
// Advance; it simply misses the tick, exactly like a real time.Ticker. The barrier that
// makes an assertion safe is Settle (§4.5), not Advance.
func (c *VirtualClock) Advance(d time.Duration)

// AdvanceTo moves virtual time to t. Panics if t is before Now (time never runs backward;
// a test that wants that has a bug, and a silent no-op would hide it).
func (c *VirtualClock) AdvanceTo(t time.Time)

// Pending reports how many timers/tickers are armed. A scenario that ends with unexpected
// pending timers usually means a control loop leaked one.
func (c *VirtualClock) Pending() int
```

### 4.4 `simnet` — the event queue

```go
// Scenario is a deterministic, replayable script over a Network and a VirtualClock. The
// same seed and the same script always produce the same trace — which is the entire point
// of the harness and the only sane way to develop Phases 5 and 6.
type Scenario struct{ /* unexported */ }

// ScenarioConfig parameterises a run.
type ScenarioConfig struct {
	Seed        int64
	Start       time.Time     // virtual t0; defaults to a fixed epoch, NOT time.Now()
	Constraints overlay.Constraints
}

// NewScenario builds a Scenario over a fresh Network and VirtualClock.
func NewScenario(cfg ScenarioConfig) *Scenario

// Net returns the fleet under test.
func (s *Scenario) Net() *Network

// Clock returns the virtual clock, for wiring into the coordinator under test.
func (s *Scenario) Clock() *VirtualClock

// At schedules f to run at virtual time t0+d. Steps at the same instant run in
// registration order. f runs on the Scenario's own goroutine, so it may touch the Network
// and drive the coordinator without a lock.
func (s *Scenario) At(d time.Duration, f func()) *Scenario

// Observe registers a callback invoked after every step and every Settle, for recording
// a trace to assert on afterwards.
func (s *Scenario) Observe(f func(at time.Duration)) *Scenario

// ReportOrder fixes the order in which the fleet's telemetry reports are delivered to the
// coordinator for the next round. Names not listed report after the listed ones, in
// insertion order.
//
// It exists for one specific, non-negotiable property test (§12.2): NO PERMUTATION OF
// FIRST-REPORT ARRIVAL ORDER MAY CHANGE THE CONVERGED TREE. That property is the direct
// encoding of the live root-flap defect in §3.8 — the discarded recompute there was a pure
// function of which peer's WebSocket frame arrived first — and it is cheap to assert over
// every permutation of a small fleet.
func (s *Scenario) ReportOrder(names ...string) *Scenario

// Run executes every scheduled step in virtual-time order, advancing the clock between
// them and calling Settle after each. It returns the first error a step reported, or the
// error from a Settle that timed out.
//
// Note simnet does NOT import "testing": the harness stays a plain library so it can also
// back a CLI replay tool, and so a scenario is not accidentally coupled to a *testing.T's
// lifetime. Tests call Run and assert on the returned error and the recorded trace.
func (s *Scenario) Run() error
```

### 4.5 The `Settle` barrier — the hard part, solved

Firing a timer is not enough: the goroutine that receives it has not necessarily *finished
reacting* when `Advance` returns, so a naive assertion races. The fix uses a property the
codebase already has — **the coordinator is a single-goroutine event loop** (Phase 4's
channel fan-in). Round-tripping a no-op event through that loop proves every previously
enqueued event has been fully processed.

```go
// coordinator (§5.6 declares it formally)
func (c *Coordinator) Sync(ctx context.Context) error
```

`Sync` enqueues an internal `evSync{ack chan struct{}}` and blocks until the Run goroutine
closes `ack` or `ctx` is done. Because the events channel is FIFO and there is exactly one
consumer, the ack proves the loop drained everything ahead of it.

```go
// Settle blocks until every barrier registered with AddBarrier reports quiescent, or
// until SettleTimeout of REAL time elapses (a deadlock guard, and the only wall-clock
// reference anywhere in the harness — it never affects the trace, only whether the test
// hangs). Scenario.Run calls it after each step.
func (s *Scenario) Settle() error

// AddBarrier registers a quiescence barrier — in practice coordinator.Sync, bound to a
// ctx. A scenario driving more than one control loop registers one per loop.
func (s *Scenario) AddBarrier(name string, fn func(context.Context) error) *Scenario

// SettleTimeout bounds Settle in REAL time. 5s is far beyond any legitimate in-memory
// control-loop round trip (they complete in microseconds), so exceeding it means a
// deadlock, not slowness — and a test that hangs forever is worse than one that fails.
const SettleTimeout = 5 * time.Second
```

### 4.6 Failure injection API

```go
// Kill removes name WITHOUT any graceful notice — the machine dropped off the network.
// No leave event is generated; the node simply stops heartbeating, so only the liveness
// timers (§5.3) will notice. This is the failure mode Phase 5 exists for, and it is
// strictly different from Leave.
func (n *Network) Kill(name string)

// Leave removes name gracefully (the peer closed its connection). Equivalent to the
// existing Remove, kept under a role-explicit name so a scenario reads as a story.
func (n *Network) Leave(name string)

// Partition makes the control link between a and b lossy in BOTH directions: frames
// between them are dropped until Heal. Symmetric because an asymmetric partition is a
// much rarer real failure and doubles the state space for no extra insight at this stage.
func (n *Network) Partition(a, b string)

// Heal removes a partition between a and b.
func (n *Network) Heal(a, b string)

// Degrade sets the injected link quality between a and b: round-trip ms and loss percent.
// These feed the synthetic metrics the coordinator ingests, so a scenario can drive the
// degradation dwell timer (§5.2) without any real network.
func (n *Network) Degrade(a, b string, rttMs, lossPct float64)

// Restore clears an injected degradation between a and b.
func (n *Network) Restore(a, b string)

// Isolate partitions name from EVERY other node — the "my Wi-Fi died but my process is
// alive" case, which is distinct from Kill because the node keeps its own state and may
// come back with stale beliefs. This is the scenario that exercises epoch fencing (§6.5).
func (n *Network) Isolate(name string)

// Rejoin clears every partition involving name.
func (n *Network) Rejoin(name string)
```

Existing `Add`, `Remove`, `SetRTT`, `RemoveRandom`, `Len`, `OverlayNodes`, `Build`, and
`Random` keep their signatures except `Build`, which now takes the previous topology:

```go
func (n *Network) Build(prev *overlay.Topology, cons overlay.Constraints) (*overlay.Topology, overlay.Constraints, error)
```

### 4.7 The two non-negotiable harness rules

1. **A scenario replays identically from a seed.** Same `ScenarioConfig.Seed`, same script
   → byte-identical trace. Enforced by: `VirtualClock`'s (deadline, seq) ordering; the
   `Network`'s insertion-order iteration (never map order); `BuildTree`'s purity; and a
   single `*rand.Rand` seeded from `cfg.Seed` as the only randomness source.
2. **No test in `overlay`, `simnet`, `coordinator`, or `arbiter` may reference wall-clock
   time.** No `time.Now()`, no `time.Sleep`, no `time.After`, no `time.Tick`. The only
   permitted real-time reference in the whole control-plane test surface is
   `simnet.SettleTimeout`, which is a deadlock guard and never affects a trace.

Enforced mechanically, not by discipline — add to the `Makefile`:

```make
check-determinism:   ## fail if a control-plane package reaches for the wall clock
	@! grep -rnE 'time\.(Now|Sleep|After|Tick)\(' \
	    internal/overlay internal/simnet internal/coordinator internal/arbiter \
	  || (echo "control plane must use the injected clock.Clock (see docs/PLAN.md §4)"; exit 1)
```

and make `check` depend on it. This is a structural guard: it makes the bad outcome
impossible to merge rather than merely discouraged in review.

---

## 5. Liveness, and the Phase 5 coordinator

### 5.1 Two levels of liveness, deliberately

There are two independent ways a peer can stop being useful, and one detector cannot see
both:

| Level | Detects | Mechanism | Owner |
|---|---|---|---|
| **Control link** | The peer's process/machine is gone, or its path to the arbiter is broken. | WebSocket protocol ping from the arbiter (`conn.Ping(ctx)`). | `signaling.Hub` |
| **Data link** | The peer is fine, but its media edge to its parent (or a child) is broken. | Application `heartbeat` frame carrying the peer's *realized* parent/children and their PeerConnection states. | `coordinator` |

A TCP connection with no FIN — the sleeping laptop, the yanked Ethernet cable — is exactly
the case the current code cannot see, because a `wsjson.Read` on a dead-but-unclosed socket
blocks forever. `conn.Ping(ctx)` fixes it: `coder/websocket` sends a protocol ping and waits
for the matching pong, and a timeout is a definitive "this socket is dead", which closes the
connection and runs the existing `unregister` path. **No new signaling frame is needed for
this level** — the WS protocol already has one.

> **Why not merge the two into one frame?** A merged design would carry realized-edge state
> on the metrics tick (3 s) and drop the WS ping. It loses on two counts: the cadences want
> to differ (liveness wants ~1 s, telemetry ~3 s, and paying 3× the telemetry cost to get
> 1 s liveness is wasteful); and after Phase 6 they have **different destinations** —
> heartbeats are the arbiter's business (it runs elections on them) while metrics are the
> *coordinator peer's* business. Splitting them now avoids splitting them later, mid-phase.

### 5.2 The wire frames

```go
package metrics

// Heartbeat is the peer's 1-second liveness beat and its report of REALIZED topology
// state — what it has actually connected, as opposed to what the coordinator believes it
// told it to connect. That distinction is what makes rebuild-from-peers (§6.6) possible:
// the new coordinator reconstructs ground truth, not the old coordinator's beliefs.
type Heartbeat struct {
	// Name is the peer's stable label. (Its id is server-stamped on the frame; a
	// self-reported id would let a peer file liveness as someone else.)
	Name string `json:"name"`
	// Seq is a per-peer monotonic counter starting at 1, reset on rejoin. It lets the
	// coordinator detect gaps (missed beats) without depending on its own clock, and
	// lets a late duplicate be dropped idempotently.
	Seq uint64 `json:"seq"`
	// Epoch is the coordinator term this peer believes is current, and Rev the topology
	// revision it has realized. The coordinator uses these to spot a peer that is
	// running behind and re-push to it specifically.
	Epoch uint64 `json:"epoch"`
	Rev   uint64 `json:"rev"`
	// Parent is the peer's realized upstream neighbour name, "" if it is the root or
	// currently parentless. ParentState is the pion PeerConnectionState string of that
	// edge ("new"/"connecting"/"connected"/"disconnected"/"failed"/"closed").
	Parent      string `json:"parent,omitempty"`
	ParentState string `json:"parent_state,omitempty"`
	// Children maps each realized downstream neighbour name to its edge state. Absent
	// on a leaf.
	Children map[string]string `json:"children,omitempty"`
}

// Reparented tells the control plane that a peer changed its own parent WITHOUT being
// told to — either it promoted its precomputed backup after its primary failed, or it
// tried and could not. It is the peer→coordinator half of the fast-failover path (§5.5).
type Reparented struct {
	Name string `json:"name"`
	// From is the parent that was lost; To is the parent now in use ("" when OK is
	// false and the peer is parentless).
	From string `json:"from"`
	To   string `json:"to,omitempty"`
	// OK is false when the peer could not attach to any parent and is stranded. The
	// coordinator must treat that as an urgent threshold event.
	OK bool `json:"ok"`
	// Epoch/Rev of the topology the peer was acting under, so the coordinator can tell
	// a failover under the current tree from one under a stale tree it already replaced.
	Epoch  uint64 `json:"epoch"`
	Rev    uint64 `json:"rev"`
	Reason string `json:"reason,omitempty"` // free text for the dashboard; never parsed
}
```

```go
// HeartbeatInterval is how often a peer beats.
//
// 1 s is the floor that is still robust: a heartbeat is ~120 bytes, so 1 Hz costs ~1 kbit/s
// per peer — nothing next to a 2 Mbit/s video stream. Going faster (250 ms) would start
// producing false misses from ordinary scheduler jitter and GC pauses on a loaded home PC,
// which is exactly the "too sensitive" failure mode the ROADMAP warns about; going slower
// (5 s) would make the arbiter's backstop detection (§5.3) unusably sluggish.
const HeartbeatInterval = 1 * time.Second
```

### 5.3 The health state machine

Owned by `coordinator`, one instance per (meet, node), advanced only inside the Run
goroutine, driven only by the injected clock.

```
                    heartbeat (any)                heartbeat (any)
        ┌───────────────────────────────┐  ┌────────────────────────────┐
        │                               │  │                            │
        ▼                               │  ▼                            │
   ┌─────────┐  no beat for      ┌──────────┐  no beat for       ┌──────────┐
   │ healthy │ ───────────────►  │ degraded │ ─────────────────► │   gone   │
   └─────────┘   DegradedAfter   └──────────┘    GoneAfter       └──────────┘
        ▲                                                              │
        │                     rejoin = a NEW join event (§5.7)         │
        └──────────────────────────────────────────────────────────────┘

   healthy  → in the tree, in the dashboard as green. No action.
   degraded → still in the tree, rendered amber, still receives pushes.
              Advisory ONLY: degraded NEVER re-parents anything.
   gone     → removed from the node set; a THRESHOLD EVENT; triggers local repair.
```

```go
package coordinator

// Health is a node's liveness classification. It is deliberately three states, not two:
// collapsing "degraded" into "gone" would make every GC pause a re-parenting event, and
// collapsing it into "healthy" would leave the operator blind to a node that is about to
// fail. Degraded is the state that is VISIBLE but not ACTIONABLE.
type Health string

const (
	HealthHealthy  Health = "healthy"
	HealthDegraded Health = "degraded"
	HealthGone     Health = "gone"
)

// DegradedAfter is the silence that demotes a node from healthy to degraded: 3 missed
// beats at HeartbeatInterval. Three rather than one because a single missed beat is a GC
// pause, a Wi-Fi retransmit, or a scheduler hiccup — not a failure. Three consecutive
// misses is a signal. Nothing acts on this transition; it only colours the dashboard and
// arms the degradation dwell (§5.4).
const DegradedAfter = 3 * metrics.HeartbeatInterval // 3s

// GoneAfter is the silence that declares a node gone and triggers repair: 8 missed beats.
//
// 8s looks slow for a system claiming keyframe-bounded failover, and it IS slow — on
// purpose. This timer is the BACKSTOP, not the fast path. The fast path is entirely local:
// a child sees its parent's PeerConnection go `failed` (pion's own ICE consent-freshness
// check fires in a few seconds) and promotes its precomputed backup IMMEDIATELY, without
// asking anyone (§5.5). By the time this 8s timer fires, the tree has usually already
// healed itself and the coordinator is merely ratifying. Keeping the backstop conservative
// is therefore free, and it buys immunity to the ugliest false positive there is:
// declaring a healthy peer dead because the ARBITER's own network hiccuped.
const GoneAfter = 8 * metrics.HeartbeatInterval // 8s

// WSPingInterval / WSPingTimeout bound the signaling.Hub's control-link liveness check.
// 15s between pings is cheap and well inside any NAT/proxy idle timeout (typically 60s+),
// so it doubles as keepalive. 5s for the pong is generous for a frame that needs no
// application processing — a socket that cannot turn one around in 5s is dead, not slow.
const WSPingInterval = 15 * time.Second
const WSPingTimeout  = 5 * time.Second
```

> `WSPingInterval`/`WSPingTimeout` live in `signaling` (it owns the socket);
> `DegradedAfter`/`GoneAfter` live in `coordinator` (it owns the health FSM);
> `HeartbeatInterval` lives in `metrics` (it owns the wire cadence). Each constant sits in
> the package that would have to change if it changed.

### 5.4 Hysteresis: the dwell timer and the exhaustive threshold-event list

```go
// DegradationDwell is how long a node's metrics must stay bad before "sustained
// degradation" counts as a threshold event. The timer is created when a node's metrics
// first cross a threshold and RESET on every good sample — so only an unbroken run of bad
// samples ever fires it. A single bad RTT resets nothing.
//
// 10s ≈ 3 metrics ticks at metrics.DefaultInterval. The ROADMAP is explicit that there is
// no correct constant here (too short → thrash; too long → sluggish), so this is a
// starting point that is deliberately CONFIGURABLE (-dwell, §10) and tested at both
// extremes in simnet. It is not presented as tuned.
const DegradationDwell = 10 * time.Second

// RecomputeCooldown is the minimum interval between two published trees for ONE meet.
// It is the last line of anti-thrash defence: even if threshold events arrive in a burst
// (a 4-peer group all leaving at once), the coordinator coalesces them into at most one
// rebuild per window. A recompute that is suppressed by the cooldown sets a dirty flag and
// runs when the window closes — it is never dropped.
//
// 5s is chosen so that a burst of churn produces one interruption, not five, while a
// single isolated event still lands well inside a human's tolerance for "the app noticed".
const RecomputeCooldown = 5 * time.Second

// The metric thresholds that ARM the dwell timer. Crossing one starts (or leaves running)
// the dwell; falling back under all of them resets it.
//
// DegradedLossPct 5%: VP8 with NACK tolerates ~1-2% loss invisibly; past ~5% the decoder
// starts showing artefacts that a keyframe cannot hide.
// DegradedRTTMs 400: the ARCHITECTURE's stated tolerable ceiling for interactive
// conversation, applied to the round trip.
// DegradedCPUPct 90: above this a peer cannot be relied on to keep its forwarding loops
// scheduled, whatever its bandwidth says.
const DegradedLossPct = 5.0
const DegradedRTTMs   = 400.0
const DegradedCPUPct  = 90.0
```

**The threshold-event list is exhaustive and frozen.** A recompute happens if and only if
one of these occurs:

| # | Event | Source |
|---:|---|---|
| 1 | A named member **joins** the meet | `signaling.Observer.PeerJoined` |
| 2 | A member **leaves** gracefully | `signaling.Observer.PeerLeft` |
| 3 | A member is declared **gone** by the health FSM | `GoneAfter` timer |
| 4 | A member's **first** metrics report arrives | `Observer.Metrics`, `!reported` |
| 5 | A member's **degradation dwell** fires | `DegradationDwell` timer |
| 6 | A member reports a **self-promotion** (`Reparented`) | `Observer.Reparented` |
| 7 | This node **becomes** the coordinator for a new epoch (Phase 6) | `arbiter` announcement |
| 8 | A meet's **first-build settle timer** fires (§5.9) | `FirstBuildSettle` timer |

Everything else — a routine metrics tick, a `healthy→degraded` transition, a heartbeat, a
`degraded→healthy` recovery — updates stored state and publishes a dashboard event, and
**does not rebuild the tree.** That rule is enforced structurally: `recompute` stays the
only caller of `BuildTree`, and only the eight handlers above call `recompute`.

### 5.5 Local subtree repair — emergent, not bespoke

When node `X` is declared gone (or leaves):

1. Compute `orphans = prev.Subtree(X) \ {X}` — for the dashboard event and for
   `ValidateLocalRepair`'s `gone` argument.
2. If `X == prev.Root`, this is a **re-root**: `PickRoot` will choose a new root and the
   whole tree is rebuilt. Publish a `failover` event flagged `reroot: true` so the operator
   sees why everything moved. This is the one legitimately global repair.
3. Otherwise build `prevPatched` = `prev` with `X` and every edge touching `X` removed.
   `Root`, `Epoch`, and the surviving `Edges`' order are carried over.
4. Call `BuildTree(nodesWithoutX, prevPatched, cons)` with `Rev = prev.Rev + 1`.
5. Assert `overlay.Validate` and `overlay.ValidateLocalRepair(prev, next, []string{X}, nil)`
   in tests; in production, `Validate` failure means **keep the previous tree and log an
   error** — never publish a tree that fails its own oracle.

> **Why there is no `overlay.Repair()` function.** The obvious design is a bespoke repair
> that patches only the orphans' edges. It was rejected because it would be a *second*
> implementation of the same constraint logic (capacity, depth, TURN, backups), living next
> to `BuildTree`, free to drift from it and from `Validate`. Stickiness (§3.4 rank 1) makes
> local repair fall out of the general builder for free: every surviving non-orphan keeps
> its incumbent parent, so only the orphans move. One algorithm, one oracle, one place to be
> wrong. The cost is that the builder does slightly more work than strictly necessary —
> irrelevant at this scale, where a rebuild is microseconds.

### 5.6 Backup-parent promotion, and the ratification rule

The fast path, end to end:

1. Peer `u`'s parent edge reaches `PeerConnectionStateFailed` (or `Disconnected` held for
   `ParentDisconnectGrace`, §7.3).
2. `u` reads `topo.BackupOf(u)`. If non-empty, it opens a session to that backup as its new
   parent **immediately, without asking the coordinator** (§7.3 gives the exact ordering).
3. `u` sends `Reparented{From: oldParent, To: backup, OK: true, Epoch, Rev}`.
4. If there is no backup, or the backup also fails, `u` sends
   `Reparented{From: oldParent, OK: false}` and waits for a push.

The coordinator's response is the rule that keeps this from thrashing:

> **RATIFICATION RULE (frozen).** On a successful `Reparented`, the coordinator patches
> `prev` so that `u`'s parent is the backup `u` actually chose, **and then** rebuilds from
> that patched `prev`. It does not rebuild from the pre-failure tree.

Why this matters: if the coordinator rebuilt from the old tree, stickiness would see `u`'s
incumbent parent as the *dead* one, find it ineligible, and re-choose freely — quite
possibly moving `u` a second time, to a parent that is no better than the backup it already
connected to. That is two interruptions where one was needed. By patching first, `u`'s new
parent *becomes* the incumbent, and stickiness protects it. The coordinator ratifies the
peer's local decision rather than fighting it.

The unsuccessful case (`OK: false`) is a threshold event handled as an urgent recompute
that bypasses `RecomputeCooldown` — a stranded peer is receiving nothing, so the anti-thrash
cooldown is the wrong trade there. This is the **only** cooldown bypass, and it is frozen.

### 5.7 Epoch and rev stamping (frozen rules)

1. **Only the arbiter mints `Epoch`.** It is a per-meet `uint64` starting at 1, incremented
   under the arbiter's single-goroutine ownership, and it advances **only** on a coordinator
   change. Nothing else may write it.
2. **Only the sitting coordinator mints `Rev`.** It starts at 1 for each new epoch and
   increments by exactly 1 per published tree.
3. **A coordinator never raises its own `Epoch`.** If it wants a new term, it asks the
   arbiter. (In Phase 5 the coordinator runs inside the arbiter process and reads the
   current epoch directly; the *rule* is identical, which is what makes the Phase 6 move a
   wiring change rather than a redesign.)
4. **A peer accepts a topology iff** `msg.From` is the announced coordinator for its current
   epoch **and** `topo.Epoch == peer.curEpoch` **and** `topo.Rev > peer.curRev`. Any other
   combination is dropped and counted (§6.5).
5. **A peer's `curEpoch` changes only on an arbiter announcement** (`TypeCoordinator`), never
   on a topology. See §6.5 for why that asymmetry is the load-bearing part of the fence.
6. **`curEpoch` resets to 0 on `TypeJoined`.** A rejoining peer adopts whatever the arbiter
   announces next. This is what closes the arbiter-restart hole (§6.7).

### 5.8 The `coordinator` surface (frozen)

```go
package coordinator

// Config parameterises the coordinator. Every duration is honoured through Clock, so a
// simnet scenario can drive all of them in virtual time.
type Config struct {
	MaxDepth          int
	StreamKbps        int
	DefaultUploadKbps int
	StickinessMs      float64       // 0 ⇒ overlay.DefaultStickinessMs
	Dwell             time.Duration // 0 ⇒ DegradationDwell
	RecomputeCooldown time.Duration // 0 ⇒ RecomputeCooldown
	DegradedAfter     time.Duration // 0 ⇒ DegradedAfter
	GoneAfter         time.Duration // 0 ⇒ GoneAfter
	FirstBuildSettle  time.Duration // 0 ⇒ FirstBuildSettle (§5.9)
	Clock             clock.Clock   // nil ⇒ clock.System()
	// SelfName is the coordinator's own peer name. Empty means the coordinator is
	// running inside the arbiter process (Phase 5) rather than on an elected peer.
	SelfName string
}

// Sender is unchanged from Phase 4 — consumer-defined here so this package never imports
// signaling. cmd/server adapts it to hub.SendTo.
type Sender interface {
	SendTopology(roomID, peerID string, topo *overlay.Topology) error
}

// Publisher is the SEAM through which the coordinator emits observable events for the
// dashboard. It lives HERE, in coordinator, and its method set mentions only stdlib types
// and overlay types — so declaring it adds no import to this package and creates no cycle
// with internal/dashboard, which implements it.
//
// One method taking a struct, rather than one method per event kind. The struct wins for
// the same reason signaling.Message is one frame type with omitempty fields rather than a
// union of nine: adding a new event kind later is a non-breaking change for every
// implementer, whereas adding a method breaks all of them. What it gives up is
// compile-time exhaustiveness on the consumer side — named, and accepted, because the
// dashboard fans every kind into one JSON envelope anyway (§9.4).
//
// Publish is called from the coordinator's single Run goroutine and MUST NOT BLOCK: an
// implementation that needs to do work should enqueue and return. A nil Publisher is
// legal and means "publish nothing".
type Publisher interface {
	Publish(Event)
}

// EventKind enumerates what the coordinator observes. Frozen.
type EventKind string

const (
	EventTopology EventKind = "topology"       // a new tree was computed and pushed
	EventMember   EventKind = "member"         // a member joined (Present) or left (!Present)
	EventHealth   EventKind = "health"         // a node's Health changed
	EventReparent EventKind = "reparent"       // a peer self-promoted to its backup
	EventFailover EventKind = "failover"       // a node was declared gone; repair ran
	EventStale    EventKind = "stale_rejected" // a stale-epoch instruction was refused

	// §5.9 build outcomes. Settling is normal startup; Unbuildable is operator-actionable.
	EventSettling    EventKind = "settling"
	EventUnbuildable EventKind = "unbuildable"
)

// Event is one observable control-plane fact. Fields not relevant to a Kind are zero.
type Event struct {
	Kind       EventKind
	RoomID     string
	At         time.Time // from the injected clock, never time.Now()
	Epoch      uint64
	Rev        uint64
	Node       string   // the node this event is about
	Parent     string   // new parent (reparent/failover)
	PrevParent string   // parent that was lost (reparent/failover)
	Health     Health   // EventHealth
	Present    bool     // EventMember
	Orphans    []string // EventFailover: the subtree that had to move
	Reroot     bool     // EventFailover: the root itself was lost
	Waiting    []string // EventSettling: members not yet heard from
	Outcome    BuildOutcome // EventTopology/EventSettling/EventUnbuildable
	Reason     string   // human-readable; never parsed
	Topo       *overlay.Topology // EventTopology: the tree just published (do not mutate)
}

// RoomSnapshot is a consistent, deep-copied view of one meet's control state, for the
// dashboard's initial frame and for a client recovering from a sequence gap.
type RoomSnapshot struct {
	RoomID  string
	Epoch   uint64
	Rev     uint64
	Topo    *overlay.Topology
	Members []MemberSnapshot
	At      time.Time
}

// MemberSnapshot is one member's control state.
type MemberSnapshot struct {
	ID          string
	Name        string
	Health      Health
	Report      metrics.Report
	Reported    bool
	Parent      string   // realized, from the last heartbeat
	Children    []string // realized, from the last heartbeat
	Backup      string   // assigned by the current tree
	LastBeatSeq uint64
	LastBeatAt  time.Time
}

// Snapshot returns a consistent view of roomID. It round-trips through the Run goroutine
// (like Sync) so the snapshot can never tear against a concurrent recompute, and it deep
// copies everything it returns so the caller cannot mutate coordinator state. Returns a
// zero RoomSnapshot and no error for an unknown meet.
func (c *Coordinator) Snapshot(ctx context.Context, roomID string) (RoomSnapshot, error)

// Sync blocks until every event enqueued before the call has been processed. It is the
// deterministic-testing barrier (§4.5) and the quiescence primitive the dashboard uses
// before serving a snapshot. Returns ctx.Err() if the context ends first.
func (c *Coordinator) Sync(ctx context.Context) error

// Observer surface — Phase 4's three methods plus two. All are called from Hub goroutines,
// must be safe concurrently, and must be cheap (each only enqueues an event).
func (c *Coordinator) PeerJoined(roomID, peerID, name string)
func (c *Coordinator) PeerLeft(roomID, peerID string)
func (c *Coordinator) Metrics(roomID, peerID string, payload []byte)
func (c *Coordinator) Heartbeat(roomID, peerID string, payload []byte)   // NEW
func (c *Coordinator) Reparented(roomID, peerID string, payload []byte)  // NEW

// SetEpoch tells the coordinator which term it is serving. Phase 5: called once at
// startup with 1. Phase 6: called on every arbiter announcement naming this node; a call
// with a HIGHER epoch resets Rev to 0 and puts the coordinator in the rebuild window
// (§6.6); a call naming a DIFFERENT coordinator cancels the control loop (SetEpoch is not
// how that happens — see Coordinator.Yield).
func (c *Coordinator) SetEpoch(epoch uint64)

// Yield stops the coordinator from publishing anything further for roomID: it is no
// longer the coordinator there. Idempotent. Phase 6 (§6.6).
func (c *Coordinator) Yield(roomID string, newEpoch uint64)
```

`New` gains the publisher:

```go
func New(log *slog.Logger, cfg Config, send Sender, pub Publisher) *Coordinator
```

### 5.9 The first-build settle rule, and the three build outcomes

The defect in §3.8 has a second half. Even with a root that can never be provisional, a meet
whose members have *all* joined but none of whom have reported yet is not a fleet you can
compute a sensible tree over — every node looks like a 0-upload leaf, so either the build
fails or it produces a tree that will be thrown away one report later. Building immediately
buys nothing and costs a reconfiguration.

**The rule (frozen).**

> A meet becomes **eligible for its FIRST tree** when either (a) every named member the
> coordinator knows about has delivered at least one metrics report, **or** (b)
> `FirstBuildSettle` has elapsed since the meet's first named join — whichever happens
> first. Until then, threshold events update state and publish `settling`, and **no tree is
> built**.
>
> After the first tree exists, **the settle never applies again.** Every subsequent
> threshold event recomputes immediately.

```go
// FirstBuildSettle bounds how long a brand-new meet waits for its members' first telemetry
// before it builds anyway.
//
// 1500ms. metrics.Reporter emits its first report IMMEDIATELY on Run (that is the
// deliberate "immediate-then-ticker" shape from Phase 4), so a healthy peer's first report
// lands within roughly one round trip of its join. 1.5s is ~10x that even on a poor WAN
// path, and it is imperceptible at startup — nobody notices a meeting taking 1.5s to draw
// its first tree. A silent peer past the window is simply attached as a provisional leaf
// and refined by whatever it eventually says; it can never stall the meet, which is the
// bound that rules (a)-alone lacks.
const FirstBuildSettle = 1500 * time.Millisecond
```

**Why this combination, and what was rejected:**

| Option | Verdict |
|---|---|
| **(a) Wait until every known member has reported** | **Rejected alone.** Unbounded: one peer that joins and never reports (a wedged process, a peer that joined without `-managed`, a lost frame) hangs the meet forever with no tree and no media. A liveness rule must never depend on an unreliable party doing something. |
| **(b) Bounded settle window after the first join** | **Rejected alone.** It bounds the wait, but it does not prevent the bad outcome *inside* the window: if the strong peer is still silent when the window closes, a weak-but-reported peer is still the best-looking candidate and still gets rooted. The window shrinks the race; it does not remove it. |
| **(c) Build immediately, never promote an unreported node to root** | **Rejected alone.** It removes the root flap (which is the dangerous part) but still publishes a tree computed over a mostly-provisional fleet, which is then rebuilt seconds later as the reports land — two visible reconfigurations where one would do, at exactly the moment the user is watching the meeting start. |
| **(c) as an invariant + (b) as the gate, with (a) as the early exit** | **CHOSEN.** (c) makes a wrong root *impossible* rather than merely unlikely — it is a structural guard, not a timing one. (b) supplies the bound (a) lacks. (a) supplies the fast path so the common case (everyone reports promptly) costs nothing. Each mechanism covers exactly the hole the others leave. |

**The three build outcomes — "not ready yet" is NOT a warning.**

Today both "the fleet is genuinely over-constrained" and "we haven't heard from anyone yet"
surface as the same discarded recompute plus the same WARN line. They are different
conditions with different audiences, and the dashboard renders them very differently.

```go
package coordinator

// BuildOutcome classifies why a recompute did or did not publish a tree. It exists because
// the three cases have three different audiences: one is normal, one is a startup detail,
// and one is an operator-actionable fault.
type BuildOutcome string

const (
	// OutcomeBuilt: a tree was computed, validated, and pushed. Log: Info.
	OutcomeBuilt BuildOutcome = "built"
	// OutcomeSettling: the meet has not met the §5.9 eligibility rule yet. Entirely
	// normal, happens once per meet, lasts at most FirstBuildSettle. Log: DEBUG, never
	// Warn — a warning that fires on every healthy startup teaches operators to ignore
	// warnings.
	OutcomeSettling BuildOutcome = "settling"
	// OutcomeUnbuildable: the fleet is over-constrained — BuildTree rejected it, or
	// PickRoot found no eligible root, over a fleet that HAS settled. This is a real,
	// operator-visible condition (not enough aggregate upload for this many peers, or a
	// depth bound too shallow) and the previous tree is retained. Log: Warn.
	OutcomeUnbuildable BuildOutcome = "unbuildable"
)
```

Two new `EventKind` values carry these through the `Publisher` seam:

```go
const (
	EventSettling    EventKind = "settling"    // Event.Reason names what is being waited on
	EventUnbuildable EventKind = "unbuildable" // Event.Reason carries the BuildTree error text
)
```

- `EventSettling` is emitted **once** per meet, when the meet first becomes ineligible (not
  on every suppressed event — that would be a per-frame event storm at exactly the moment
  the dashboard is connecting). `Event.Reason` names the members not yet heard from.
- `EventUnbuildable` is emitted on every failed post-settle build, with `Event.Reason` set
  to the `BuildTree`/`PickRoot` error text. The dashboard renders it as a persistent banner
  in `--crit`, because it means some participant is receiving nothing and a human has to
  change something (drop a peer, raise `-max-depth`, lower `-stream-kbps`).

The settle timer firing is a threshold event in its own right — it is row 8 in §5.4's
exhaustive list, added there.

---

## 6. The Phase 6 election and migration contract

### 6.1 Fitness — control plane only

```go
package arbiter

// Fitness is the CONTROL-PLANE evidence about one peer's suitability to coordinate.
// It deliberately does NOT contain UploadKbps. Upload is the relay (data-plane) resource;
// mixing it in here is the "supernode score" the ROADMAP forbids, and in practice it would
// keep electing the fat-pipe desktop that is pegged at 100% CPU.
type Fitness struct {
	CPUFreePct  float64          // 100 - metrics.Report.CPUPct
	RTTServerMs float64          // metrics.Report.RTTServerMs
	LossPct     float64          // metrics.Report.LossPct
	UptimeSec   float64          // seconds since this peer joined this meet
	NAT         overlay.NATType  // NATRelayed is a hard disqualifier
	Health      string           // must equal string(coordinator.HealthHealthy)
}

// Score maps a Fitness to [0, 1]. Higher is fitter. Returns exactly 0 for a peer that is
// disqualified (TURN-bound, or not healthy) — a hard zero rather than a penalty, because
// these are not "worse", they are "ineligible", and a large enough bonus elsewhere must
// never be able to buy an ineligible peer the job.
func Score(f Fitness) float64
```

Frozen formula and weights:

```
score = 0.30 * clamp(CPUFreePct/100, 0, 1)
      + 0.35 * (1 - clamp(RTTServerMs / MaxUsefulRTTMs, 0, 1))
      + 0.20 * (1 - clamp(LossPct / MaxUsefulLossPct, 0, 1))
      + 0.15 * clamp(UptimeSec / StableUptimeSec, 0, 1)
```

```go
// MaxUsefulRTTMs: past 300ms round-trip to the arbiter, a peer's control loop is reacting
// to a picture of the meet that is already a third of a second old. Everything worse is
// equally unusable, so the term saturates rather than continuing to discriminate.
const MaxUsefulRTTMs = 300.0

// MaxUsefulLossPct: 10% loss on the CONTROL link means topology pushes need retries; past
// that the distinction between bad and terrible does not matter.
const MaxUsefulLossPct = 10.0

// StableUptimeSec: a peer that has been in the meet 2 minutes has demonstrated it is not
// a drive-by joiner. Beyond that, more uptime is not more evidence, so the term saturates.
const StableUptimeSec = 120.0
```

Weight rationale, since these are the numbers a reviewer will ask about: **network
reliability (0.35) outweighs CPU (0.30)** because the coordinator's work is tiny (a
millisecond of `BuildTree` per event) while its *reach* is everything — a fast machine that
cannot deliver a push is useless. **Loss (0.20)** is separate from RTT because a link can be
fast and lossy. **Uptime (0.15)** is the smallest term and exists purely as an anti-flap
prior: all else equal, prefer the peer that has already proven it stays.

**Honest limitation:** `CPUPct`, `LossPct`, and `RTTServerMs` are declared in
`metrics.Report` but not yet measured (Phase 4 left them as forward-compatible fields).
Until real sensors land, `Score` is driven by `UptimeSec` and the NAT disqualifier in
production, and by injected values in `simnet`. **The election logic is fully testable and
the formula is real; the inputs are not yet.** Do not claim otherwise in a demo.

### 6.2 Promotion, demotion, and failure triggers

| Trigger | Condition | Latency |
|---|---|---|
| **Bootstrap** | A meet has members and no coordinator. | immediate |
| **Failure** | The sitting coordinator's `Health` reaches `gone`, or its WS closes. | immediate — no dwell |
| **Demotion** | `Score(incumbent) < DemoteBelowScore`, sustained past `ElectionDwell`. | ≥ 20 s |
| **Promotion** | `Score(challenger) - Score(incumbent) > PromoteMarginScore`, sustained past `ElectionDwell`, and `MinTermDuration` has elapsed since the last change. | ≥ 20 s |
| **Manual** | `POST /api/demo/meets/{id}/elect` (demo surface only, §9.6). | immediate |

```go
// ElectionDwell is the sustained-condition window for a VOLUNTARY handover. It is twice
// the tree's DegradationDwell on purpose: re-parenting one subtree costs one stream a
// keyframe, but migrating the coordinator stalls the entire control loop for the rebuild
// window (§6.6). The more expensive the action, the more evidence it should require.
const ElectionDwell = 20 * time.Second

// PromoteMarginScore is how much fitter a challenger must be before it is worth the
// handover. 0.20 on a [0,1] scale is a large, visible gap — roughly "the incumbent has
// lost most of its CPU headroom AND its link got worse". A marginal improvement is not
// worth a control-plane stall; this is the election-layer twin of overlay stickiness.
const PromoteMarginScore = 0.20

// DemoteBelowScore is the absolute floor below which the incumbent is replaced by the
// best available candidate even without a large margin. 0.35 is chosen to sit below what
// a healthy peer on a mediocre home link scores (~0.55-0.7) and above what a saturated or
// half-broken one scores.
const DemoteBelowScore = 0.35

// MinTermDuration is the hard floor on how often the role may move voluntarily. It does
// NOT apply to the failure path — a dead coordinator is replaced instantly regardless.
// One minute makes a flapping-election bug obvious (it becomes a once-a-minute event in
// the dashboard) rather than invisible (a hundred handovers a second).
const MinTermDuration = 60 * time.Second

// RebuildWindow bounds how long a freshly promoted coordinator waits for peers to report
// in before it publishes its first tree (§6.6). 3s ≈ 3 heartbeats + 1 metrics tick, which
// is enough for every present peer to have spoken at least once.
const RebuildWindow = 3 * time.Second
```

### 6.3 The arbiter's announcement frame

```go
package arbiter

// Reason explains a coordinator change, for the dashboard and the logs. Frozen enum.
type Reason string

const (
	ReasonBootstrap Reason = "bootstrap" // first coordinator for this meet
	ReasonFailover  Reason = "failover"  // the incumbent was lost
	ReasonPromotion Reason = "promotion" // a materially fitter peer took over
	ReasonDemotion  Reason = "demotion"  // the incumbent fell below the floor
	ReasonManual    Reason = "manual"    // forced through the demo surface
)

// Announcement is the arbiter's authoritative statement of who coordinates a meet, under
// which epoch. It is the ONLY thing in the system that may advance a peer's epoch, and
// the arbiter is its only author.
type Announcement struct {
	RoomID         string `json:"room_id"`
	Epoch          uint64 `json:"epoch"`
	Coordinator    string `json:"coordinator"`      // peer NAME; "" ⇒ the arbiter itself
	CoordinatorID  string `json:"coordinator_id"`   // server-assigned peer id, or signaling.ServerID
	Prev           string `json:"prev,omitempty"`   // the outgoing coordinator's name
	Reason         Reason `json:"reason"`
	IssuedAtUnixMs int64  `json:"issued_at_unix_ms"`
}

// Announcer ships an Announcement to every member of a meet. Consumer-defined here so
// arbiter never imports signaling; cmd/server adapts it to hub.SendRoom.
type Announcer interface {
	AnnounceCoordinator(roomID string, a Announcement) error
}

// Publisher is the arbiter's dashboard seam, separate from coordinator.Publisher because
// each interface is owned by its emitter — that is what keeps both packages import-free of
// the dashboard. A nil Publisher means "publish nothing".
type Publisher interface {
	PublishElection(Announcement)
}
```

On the wire this is `signaling.TypeCoordinator` with the `Announcement` marshalled into
`Message.Payload`, sent from the server to every member of the meet.

### 6.4 The single-writer argument (why there is no consensus here)

The whole fence rests on one property:

> **Only the arbiter mints epochs, it is a single writer, and it never issues the same
> epoch twice for one meet.**

Mechanically: the epoch counter is a plain `uint64` field on the arbiter's per-meet state,
owned by the arbiter's single Run goroutine (same pattern as the coordinator's — all state
in one goroutine, every input arriving as an event on a channel). There is no lock to get
wrong and no distributed agreement to reach, because there is exactly one writer. That is
ARCHITECTURE §4 cashed out: we delegated consensus to the one reliable node instead of
running Raft among home PCs, and the payoff is that the fencing token is *free*.

### 6.5 Epoch fencing — exactly what a peer does

State a peer keeps: `curEpoch uint64`, `curRev uint64`, `coordID string`,
`staleRejected uint64`.

| Incoming | Condition | Action |
|---|---|---|
| `TypeJoined` | always | `curEpoch = 0; curRev = 0; coordID = ""` — a rejoin adopts fresh authority. |
| `TypeCoordinator` | `a.Epoch > curEpoch` | Adopt: `curEpoch = a.Epoch; curRev = 0; coordID = a.CoordinatorID`. If `a.CoordinatorID == selfID`, start the coordinator control loop for this meet. If it *was* self and is no longer, **cancel the control loop's context** and drop every in-flight push. |
| `TypeCoordinator` | `a.Epoch <= curEpoch` | **Ignore.** Log at debug, `staleRejected++`. |
| `TypeTopology` | `msg.From != coordID` | **Ignore**, `staleRejected++`. The server stamps `From`, so this is a trustworthy check. |
| `TypeTopology` | `topo.Epoch != curEpoch` | **Ignore**, `staleRejected++`. Note this rejects a HIGHER epoch too — see below. |
| `TypeTopology` | `topo.Rev <= curRev` | **Ignore**, `staleRejected++` (a duplicate or reordered push). |
| `TypeTopology` | all three pass | Apply (§7.4), then `curRev = topo.Rev`. |

**The asymmetry is the design.** A topology carrying a *higher* epoch is rejected, not
adopted. A peer may learn about a new coordinator **only from the arbiter**, never from the
would-be coordinator's own message. If a topology could raise the epoch, any peer could
promote itself by simply stamping a bigger number — the fence would authenticate nothing.
Routing every authority change through the single writer is what makes the token mean
something. The cost is one extra round trip on handover (the arbiter must announce before
the new coordinator's first push is accepted), which is bounded by the arbiter's own
fan-out and is well inside `RebuildWindow` anyway.

`staleRejected` is reported to the dashboard (`stale_rejected` event, §9.4). **A nonzero
count is the visible proof that the fence works, and it is the thing the Phase 6 demo
points at.** It is not an error condition.

### 6.6 State handover: snapshot-and-ship vs rebuild-from-peers

| | **Snapshot-and-ship** | **Rebuild-from-peers** |
|---|---|---|
| How | The outgoing coordinator serializes {topology, per-node last report, dwell timer state, backup assignments} and ships it to the incoming one via the arbiter. | The new coordinator starts empty. Every peer, on adopting a new epoch, immediately sends an out-of-cycle `Heartbeat` + `Report` carrying its **realized** parent/children. The coordinator reconstructs from those. |
| Warm-up | Instant. | ≤ `RebuildWindow` (3 s). |
| Works on a crash | **No.** Requires the outgoing node alive and cooperative — the case that matters least, since a graceful handover was never urgent. | **Yes.** Identical code path either way. |
| New wire surface | A whole snapshot message type, versioned, kept in sync with coordinator internals as they change. | None. Reuses `Heartbeat` + `Report`, which already exist and are already exercised every second. |
| Fidelity | Ships the old coordinator's *beliefs*, which may already disagree with what peers actually realized (a push it sent that never landed). | Ships peers' *ground truth*. |
| Failure mode | A path exercised only on graceful handover — i.e. almost never — is a path that silently rots. | The only path, exercised on every handover including every test. |

**DECISION: rebuild-from-peers, and only that.** Snapshot-and-ship is not implemented, not
as a fast path, not as an optimization.

The deciding argument is not the 3 seconds — it is that **a fallback path which only runs
on crashes is a path that is never tested and is therefore broken when you need it.**
Building both means the graceful path gets all the exercise and the crash path gets all the
bugs, in the phase whose entire purpose is surviving a crash. One path, always taken, always
tested.

The 3-second cost is also smaller than it looks, and this is the honest framing: **the data
plane does not need the coordinator.** During the rebuild window every existing relay keeps
forwarding, every peer keeps receiving, and every peer still has its precomputed backup
parent from the last published tree — so even a *parent failure during the rebuild window*
is handled locally. What is suspended for ≤ 3 s is re-optimization, not the call.

> **This supersedes ARCHITECTURE.md §8**, which lists both approaches and suggests "the
> pragmatic answer is likely both". It was written before Phase 5 existed; now that
> heartbeats carry realized state, rebuild-from-peers is strictly better and the second path
> earns nothing. WI-10 must update that section.

Mechanically, on adopting a new epoch:

1. Peer sends one immediate `Heartbeat` and one immediate `Report`, out of cycle (its
   ticker is *reset*, not merely read, so it does not double-beat).
2. New coordinator enters the rebuild window: it accumulates reports and **publishes
   nothing**.
3. It exits the window when it has heard from every member the arbiter says is present, or
   after `RebuildWindow` — whichever comes first.
4. It reconstructs `prev` from the realized parent/children in the heartbeats (not from any
   received topology), validates it with `overlay.Validate`, and — **if and only if it
   validates** — uses it as the stickiness baseline. If reconstruction fails to validate
   (a partially-realized tree, entirely possible mid-churn), it falls back to `prev = nil`
   and accepts that the first tree of the new epoch re-parents freely. That is a real cost,
   stated: **a handover during heavy churn can produce one global re-parent.**
5. It publishes `Rev = 1` under the new epoch.

### 6.7 The ambiguity window, and precisely what makes it safe

The window opens when the arbiter announces epoch `E+1` and closes when the last peer has
processed that announcement. During it, the old coordinator (epoch `E`) may still be
pushing, and the new one (epoch `E+1`) may have started.

**The safety argument, precisely:**

1. Epochs are minted by a single writer that never repeats one (§6.4). Therefore **two
   coordinators can never hold the same epoch.**
2. A peer's `curEpoch` is monotonically non-decreasing and changes **only** on an arbiter
   announcement (§6.5). Therefore a peer's notion of authority cannot be moved by a
   coordinator.
3. A peer accepts a topology only when `topo.Epoch == curEpoch` **and**
   `msg.From == coordID`. Therefore, **from the instant a peer processes the `E+1`
   announcement, every `E`-stamped push it receives is rejected** — regardless of arrival
   order, duplication, delay, or how confused the old coordinator is about its own status.
4. The residual window is the set of peers that have **not yet** processed the announcement.
   Such a peer may still apply an `E`-stamped topology. **This is safe**, and here is why
   it is safe rather than merely tolerable: an `E` topology is a *valid tree over the
   membership the old coordinator knew* — `Validate` passed before it was published. Applying
   it therefore cannot produce an illegal or contradictory local state. It can only be
   **stale**. And staleness is self-correcting: the peer will process the announcement
   (same FIFO socket, so it is already queued), then accept the new coordinator's first
   `E+1` push, which supersedes it.

The invariant that falls out, and the one to remember:

> **A peer's realized topology may be STALE, but it is never INCONSISTENT — it is always
> some tree that some legitimate coordinator computed and validated.**

What would break it: two coordinators at the same epoch. Prevented structurally by §6.4.
Not by timing, not by a lock ordering, not by a heuristic.

**Honest limitations of the fence:**

- **The arbiter's epoch counter is in-memory.** If the arbiter process restarts, its meets
  are gone and its counters restart at 1. A peer still holding `curEpoch = 9` would reject
  the fresh arbiter's epoch 1. This is closed by rule §5.7.6: a restarted arbiter has no
  meets, so every peer must rejoin, and `TypeJoined` resets `curEpoch = 0`. **Verify this
  in a simnet scenario**; it is the kind of hole that is obvious in a doc and forgotten in
  code. Persisting the counter (a file, a monotonic boot id) is out of scope and would be
  the right fix if this were ever more than a portfolio system.
- **The fence stops stale actors, not dishonest ones.** A peer that lies about its name, its
  metrics, or its realized parent is not defended against, and a malicious *coordinator*
  within its own epoch can compute an arbitrarily bad tree. There is no Byzantine tolerance
  here and this contract does not add any (ARCHITECTURE §10 already says so).
- **A partitioned old coordinator that cannot see the arbiter keeps believing it is in
  charge** for as long as the partition lasts. It is harmless because its pushes are fenced,
  but it will keep computing and keep logging, and its `slog` output will look alarming.
  Mitigation: on `GoneAfter` of silence *from the arbiter*, a coordinator peer stops its
  own control loop (`Yield`) rather than shouting into the void. Frozen as behaviour.

---

## 7. The media layer contract

### 7.1 The negotiation serializer

The defect: `Session.onNegotiationNeeded` calls `CreateOffer`/`SetLocalDescription`
unconditionally. Today every track is added *before* `Start()`, so it has never fired twice
concurrently. Phase 5 adds and removes tracks mid-call, so a second `AddTrack` while the
`pc` is in `have-local-offer` makes both calls fail and the offer is dropped with a log line
— a permanently half-negotiated session.

The fix is **perfect negotiation's bookkeeping minus rollback**. Pion v4 cannot roll back a
local offer (no `SetLocal`+rollback transition from `have-local-offer`), which is exactly why
Phase 1 chose the single-offerer scheme. That constraint is settled and **must not be
relitigated**: single-offerer means there is never a *colliding* offer to reconcile, so all
that is missing is serialization of *our own* successive offers.

```go
// Session gains, guarded by the existing s.mu:
//
//   negotiating bool  // an offer is outstanding: we have called SetLocalDescription and
//                     // have not yet applied the answer.
//   renegotiate bool  // a negotiation was requested while negotiating; run one more as
//                     // soon as the answer lands. A single bool, not a counter: N
//                     // requests during one in-flight offer collapse into exactly one
//                     // follow-up, because SDP is a full state snapshot — one offer
//                     // carries every accumulated change. Coalescing is correct here,
//                     // not lossy.
```

Exact behaviour:

**`onNegotiationNeeded()`**
1. If `!s.offerer` → return (unchanged; the answerer never initiates).
2. Lock. If `s.negotiating` → `s.renegotiate = true`, unlock, return.
3. Still locked: if `s.pc.SignalingState() != webrtc.SignalingStateStable` →
   `s.renegotiate = true`, unlock, return. **Both checks are required**: `negotiating` is our
   *intent* and covers the window between deciding to offer and pion actually leaving
   `stable`; `SignalingState()` is pion's *truth* and covers a state change we did not
   initiate.
4. `s.negotiating = true`, unlock.
5. `CreateOffer` → `SetLocalDescription` → `sendDescription`. On **any** error: lock,
   `s.negotiating = false`, `s.renegotiate = true`, unlock, and schedule a retry (below).

**On a successfully applied answer** (in the `SDPTypeAnswer` branch, after
`SetRemoteDescription` succeeds and `flushPending()` runs):
1. Lock. `s.negotiating = false`. `again := s.renegotiate`. `s.renegotiate = false`. Unlock.
2. If `again` → run `onNegotiationNeeded` **on a new goroutine**, not inline. Inline would
   recurse on the `consume` goroutine and could block it behind an SDP round trip, stalling
   every inbound frame for that peer.

**Retry:**
```go
// NegotiationRetryDelay is how long a Session waits before re-attempting a negotiation
// that failed. 250ms is long enough for a transient pion state to settle and short enough
// that a human does not perceive the extra wait on top of the ICE/DTLS time already being
// spent.
const NegotiationRetryDelay = 250 * time.Millisecond

// NegotiationRetries bounds the attempts before the Session gives up and logs at Error.
// 5 attempts ≈ 1.25s. Giving up is safe rather than fatal: the coordinator re-pushes the
// topology on the next threshold event, and applyTopology is idempotent, so a permanently
// wedged session self-heals on the next tree. Retrying forever would hide a real bug.
const NegotiationRetries = 5
```
The retry timer comes from the injected `clock.Clock` on `SessionConfig`, so a media test
can drive it deterministically.

**Explicitly NOT implemented:** the polite/impolite peer roles, `ignoreOffer`, and rollback.
An inbound offer arriving at the offerer is not glare here — it is a role disagreement, i.e.
a bug — and the existing code already logs and drops it. Keep that.

### 7.2 `Session.RemoveTrack`

```go
// RemoveTrack removes a previously added outbound track from this session. It fires
// OnNegotiationNeeded, so the serializer (§7.1) emits the renegotiating offer; the caller
// does not offer by hand.
//
// A closed PeerConnection returns webrtc.ErrConnectionClosed, which RemoveTrack maps to
// nil: a closed session has already removed every track, so "remove from a closed session"
// is a satisfied postcondition, not a failure. Making the caller special-case it at every
// site would be noise, and would tempt callers to ignore ALL errors from this method.
func (s *Session) RemoveTrack(sender *webrtc.RTPSender) error
```

The `forwarder` gains the mutation surface the Router needs, mirroring the existing
`addOut`/`removeChild` pair:

```go
// removeOut drops the single (src → child) leg and returns its sender so the Router can
// RemoveTrack it. Returns nil if no such leg exists (idempotent).
func (f *forwarder) removeOut(src, child string) *webrtc.RTPSender

// addOutLive is addOut for a leg created AFTER the session has started — the mid-call
// case. Identical bookkeeping; it exists as a separate name purely so the "added before
// Start" precondition on addOut stays a true, checkable statement.
func (f *forwarder) addOutLive(src, child string, track *webrtc.TrackLocalStaticRTP, sender *webrtc.RTPSender)

// rebindUpstream points src's forwardSource at a new upstream session after a re-parent
// and resets its ssrc to 0, so the next TrackRemote re-learns it. Without the reset, an
// upstream PLI would carry the OLD source SSRC and the new upstream would ignore it —
// the exact silent-black-video failure requestUpstreamKeyframe already documents.
func (f *forwarder) rebindUpstream(src string, upstream rtcpWriter)
```

### 7.3 Re-parent: the EXACT order of operations

Goal: the media gap stays inside one keyframe interval. That means **make-before-break**.

When `applyTopology` finds that *our own* parent changed `P_old → P_new`:

| Step | Action | Why this position |
|---:|---|---|
| 1 | **Open** a session to `P_new` (`startPeer`), leaving `P_old`'s session **fully intact and still receiving**. | Break-before-make would guarantee a gap of ICE + DTLS + first keyframe ≈ 1–3 s. Make-before-break costs one transient extra peer connection and one duplicated inbound stream for ≤ 5 s. |
| 2 | **Wait** for `P_new`'s `PeerConnectionStateConnected`, bounded by `ReparentConnectTimeout`. | Committing before the new leg is up is how you end up with no parent at all. |
| 3 | On connected, **request a keyframe upstream** on `P_new` for every source we expect from it. | pion's `OnTrack` will fire and the decoder will PLI eventually; asking proactively is what turns "black until the next natural I-frame" into "black for one keyframe". Same trick `keyframeForChild` already plays for children. |
| 4 | **Rebind the forwarder** (`rebindUpstream`) for each source that arrived via `P_old`, so a relay's downstream legs keep their tracks and simply start carrying packets from the new upstream. | Downstream sessions are never touched, so **our children see no renegotiation at all** — their tracks are the same objects. This is what keeps a re-parent local to one hop instead of cascading down the subtree. |
| 5 | **Then** tear down `P_old`: `link.cancel()`, `session.Close()`, `forwarder.removeChild`/`removeSource` as today. | Last, so nothing is lost if steps 1–3 fail. |
| 6 | **On timeout at step 2**: try `topo.BackupOf(self)`. If that also fails, **keep `P_old`** and send `Reparented{OK: false}`. | The one outcome that is never acceptable is ending up parentless because we obeyed an instruction we could not carry out. |

When *our children set* changes in the same apply, order is **adds before removes**:

| Step | Action | Why |
|---:|---|---|
| a | `AddForwardTrack` + `addOutLive` for every new `(source, child)` leg. | A child must never lose a source it is about to regain; adding first makes the overlap harmless. |
| b | `RemoveTrack` + `removeOut` for every dropped leg. | |
| c | Let the serializer (§7.1) coalesce (a) and (b) into **one** renegotiation per affected session. | Two offers per apply is two interruptions; SDP is a full snapshot, so one offer carries both. This is precisely why the `renegotiate` flag is a bool and not a queue. |
| d | `keyframeForChild` on every child whose source set changed. | New legs start mid-GOP. |

```go
// ReparentConnectTimeout bounds step 2. 5s covers ICE gathering + connectivity checks +
// DTLS on a normal path with a STUN round trip; a path that has not connected in 5s is
// usually not going to (a blocked candidate, a dead peer), and holding the old parent open
// longer than that means paying double upload for a leg that will not arrive.
const ReparentConnectTimeout = 5 * time.Second

// ParentDisconnectGrace is how long a parent edge may sit in PeerConnectionStateDisconnected
// before we treat it as failed and promote the backup. pion reaches `disconnected` on a
// short consent-freshness lapse and frequently recovers on its own; `failed` is the
// definitive state and we act on it immediately. This grace only covers the case where the
// edge sits in `disconnected` indefinitely without ever declaring `failed`.
const ParentDisconnectGrace = 2 * time.Second
```

### 7.4 `Router.applyTopology` becomes diff-and-apply

```go
// applyTopology realises a coordinator-pushed topology. It DIFFS the incoming tree against
// the one currently in force and applies the difference: open sessions to new neighbours,
// close sessions to dropped ones, re-parent (§7.3), add and remove forwarding legs, and
// request keyframes where a source changed.
//
// Fencing (§6.5) happens FIRST, before anything is mutated: a frame from a non-coordinator,
// a wrong epoch, or a non-advancing rev is counted and dropped.
func (r *Router) applyTopology(ctx context.Context, from string, payload []byte)
```

Signature gains `from string` (the server-stamped sender id) so the Router can enforce
`from == coordID`.

**The comment that must be deleted when this lands** (name it in your commit message):

> `internal/media/router.go`, the doc comment on `applyTopology`, the paragraph beginning
> **"Phase 4 is deliberately ADDITIVE: it connects new neighbours but does not tear down a
> session to a peer the new tree drops."** — delete the whole paragraph. It documents a
> limitation that no longer exists, and leaving it is worse than never having written it.

**The second comment that must be deleted:**

> `internal/media/relay.go`, the doc comment on `removeSource`, the paragraph beginning
> **"Honest limit (Phase 5): the source's downstream legs live on OTHER children's
> still-open sessions…"** — delete it, and remove the lingering-leg behaviour it describes
> by having `removeSource` return the affected `*webrtc.RTPSender`s so the Router can
> `RemoveTrack` them:
> ```go
> // removeSource drops the forwardSource for a departed SOURCE peer and returns every
> // downstream sender that was carrying its media, so the Router can RemoveTrack each one
> // and let the serializer fold the removals into one renegotiation per child.
> func (f *forwarder) removeSource(src string) []*webrtc.RTPSender
> ```

### 7.5 Backup promotion on the peer side

`Router` gains the local failover path:

```go
// RouterConfig gains:
//   Backup bool          // honour precomputed backup parents on primary failure (default true)
//   Clock  clock.Clock   // nil ⇒ clock.System()
//   OnReparented func(metrics.Reparented)  // called after a self-promotion so cmd/peer can
//                                          // ship the frame; nil ⇒ no report. Declared as a
//                                          // callback so media does not need to know the
//                                          // wire — same seam discipline as everywhere else.
```

Triggered from the parent session's `OnState` handler: on `Failed`, or on `Disconnected`
held for `ParentDisconnectGrace` (timed off the injected clock), run §7.3 steps 1–6 against
`topo.BackupOf(selfName)` and then invoke `OnReparented`.

---

## 8. The signaling wire protocol — the complete final frame table

Every existing frame keeps its exact `Type` constant name and its exact JSON key set. Three
new types, one new reserved id, one new Hub method, two new `Observer` methods.

| `signaling.Type` | Value | Direction | Payload / fields | Consumed by | Status |
|---|---|---|---|---|---|
| `TypeOffer` | `"offer"` | peer → server → peer | `SDP` (opaque) | `media.Session` | existing |
| `TypeAnswer` | `"answer"` | peer → server → peer | `SDP` (opaque) | `media.Session` | existing |
| `TypeCandidate` | `"candidate"` | peer → server → peer | `Candidate` (opaque) | `media.Session` | existing |
| `TypeJoined` | `"joined"` | server → peer | `To` = your new id, `Peers` = roster | `media.Router` | existing |
| `TypePeerJoined` | `"peer-joined"` | server → peer | `From`, `Name` | `media.Router` | existing |
| `TypePeerLeft` | `"peer-left"` | server → peer | `From`, `Name` | `media.Router` | existing |
| `TypeError` | `"error"` | server → peer | `Error` | `media.Router` | existing |
| `TypeMetrics` | `"metrics"` | peer → server (→ coordinator peer) | `Payload` = `metrics.Report` | `coordinator` | existing |
| `TypeTopology` | `"topology"` | coordinator → server → peer | `Payload` = `overlay.Topology`, `From` = coordinator id | `media.Router` | existing |
| **`TypeHeartbeat`** | **`"heartbeat"`** | peer → server (→ coordinator peer) | `Payload` = `metrics.Heartbeat` | `coordinator` | **new** |
| **`TypeReparented`** | **`"reparented"`** | peer → server (→ coordinator peer) | `Payload` = `metrics.Reparented` | `coordinator` | **new** |
| **`TypeCoordinator`** | **`"coordinator"`** | server → **every** peer in the meet | `Payload` = `arbiter.Announcement` | `media.Router`, `cmd/peer` | **new** |

```go
// ServerID is the reserved From value the Hub stamps on frames the SERVER originates
// (rather than relays). It lets a peer apply one uniform fencing rule (§6.5) in both
// phases: in Phase 5 the coordinator IS the server, so a topology arrives with
// From == ServerID; in Phase 6 it arrives with From == the elected peer's id. Without the
// reserved value the peer would need two different checks for the two phases.
//
// It is prefixed with '_' because peer ids are "p1", "p2", … — a collision is impossible
// by construction, and the prefix makes that obvious at a glance in a log line.
const ServerID = "_server"
```

**Routing rules the Hub must enforce (frozen):**

1. `TypeOffer` / `TypeAnswer` / `TypeCandidate` — relay to `To`, stamping `From`. Unchanged.
2. `TypeMetrics` / `TypeHeartbeat` / `TypeReparented` — **always terminate at the server.**
   The Hub hands the raw `Payload` plus the stamped sender id to the `Observer` and stops.
   When the coordinator is an elected peer, it is the *server's Observer implementation*
   that forwards the frame onward (as the same type, `From` = the original peer's id, `To` =
   the coordinator's id). One WS per peer; the peer never opens a second control link.
3. `TypeTopology` — has **two** legitimate origins. From a peer, relay it to `To` with
   `From` stamped (the elected coordinator pushing). From the server (`hub.SendTo`), stamp
   `From = ServerID`. A `TypeTopology` received from a peer that the arbiter has not named
   coordinator is relayed anyway — **the Hub does not police the control plane**; the
   receiving peer's fence (§6.5) rejects it. Keeping the Hub dumb is deliberate: it stays a
   transport and does not grow a second, divergent copy of the epoch rules.
4. `TypeCoordinator` — server-originated only. A peer that sends one gets the existing
   "ignoring unexpected frame type from peer" warning.

```go
// Hub gains:

// SendRoom delivers a server-originated frame to EVERY member of roomID and returns how
// many it reached. Like SendTo, the roster snapshot is taken under the lock and the sends
// happen after it is released. Used for the coordinator announcement, which is the first
// frame in this system that is genuinely a broadcast rather than a fan-out of per-peer
// content.
func (h *Hub) SendRoom(roomID string, msg Message) int

// Observer gains two methods. Both, like the existing three, take strings and raw bytes so
// signaling stays free of control-plane types.
type Observer interface {
	PeerJoined(roomID, peerID, name string)
	PeerLeft(roomID, peerID string)
	Metrics(roomID, peerID string, payload []byte)
	Heartbeat(roomID, peerID string, payload []byte)   // NEW
	Reparented(roomID, peerID string, payload []byte)  // NEW
}
```

> Adding methods to `Observer` is a breaking change for any implementer. There is exactly
> one (`*coordinator.Coordinator`), and WI-0 and WI-3 land together for that reason (§11).

**Origin policy.** `ServeWS` must pass `OriginPatterns` on `websocket.AcceptOptions`, drawn
from the same allow-list as the dashboard's CORS (§9.8) — one source of truth, injected via
`HubConfig.AllowedOrigins []string`. This is the note the existing comment in `hub.go`
already anticipates; delete that comment when you implement it.

---

## 9. The dashboard / arbiter HTTP + WS API

### 9.1 What the frontend is, and what it is not

A **zero-build static site** on GitHub Pages: vanilla ES modules, no bundler, no framework,
hand-rolled theming via CSS custom properties per `docs/design-system.md` Part 2.

> **THE BROWSER NEVER CARRIES MEDIA.** Not a `getUserMedia`, not an `RTCPeerConnection`, not
> a `<video>` element fed by the meet. The page is control-plane UI only: create and list
> meets, hand a participant the command to join, render the live subnet, show coordinator
> identity + epoch, show per-node telemetry, and stream the event log. If an implementer
> finds themselves reaching for WebRTC in `web/`, they have misread the contract — report
> it, do not build it.

The server it talks to is the **arbiter** — the top-level node that keeps the mapping of
which meets are running, redirects participants into a meet's subnetwork, elects and
delegates coordinators, and exposes the real-time view of the subnet's structure. It streams
no audio and no video.

### 9.2 Transport split, and why

**REST for commands and point-in-time reads. One WebSocket per open meet view for the live
stream.**

| | **WebSocket (chosen)** | **SSE (rejected)** |
|---|---|---|
| Reuses | The existing `coder/websocket` dependency, the existing accept path, the existing read limit, and — critically — **one** origin-check implementation shared with `/ws`. | A second streaming implementation and a second place to get origin/CORS right. |
| Client→server | Available. Used for per-meet `subscribe`/`unsubscribe` on one socket, and for demo controls without a REST round trip per click. | Not available; needs a REST call for every client→server action. |
| Browser connection cap | Not subject to the per-origin HTTP/1.1 limit. | Capped at ~6 per origin on HTTP/1.1 — reachable with a few tabs open. |
| Reconnect | Manual (exponential backoff in `web/api.js`). | Automatic, with `Last-Event-ID` replay. **This is SSE's real win, and it is given up knowingly** — the `seq` gap-detection in §9.4 recovers the same thing in ~15 lines. |
| Proxies | Needs `Upgrade` to survive intermediaries. | Plain HTTP, survives more proxies. |

The deciding factor is **one origin policy, one accept path, one dependency**. A security
control that exists in two places is a security control that will diverge.

### 9.3 The REST surface

All paths are under `/api/`. All bodies are `application/json`. All responses carry
`Content-Type: application/json` and the CORS headers from §9.8.

#### `GET /api/meets` — list meets

```json
{
  "meets": [
    {
      "id": "standup",
      "created_at_unix_ms": 1756684800000,
      "members": 4,
      "epoch": 3,
      "rev": 11,
      "coordinator": "alice",
      "relays": ["alice", "dave"],
      "depth": 2
    }
  ]
}
```

#### `POST /api/meets` — create a meet

Request:
```json
{ "id": "standup" }
```
`id` is optional; when absent or empty the arbiter generates one. When present it must match
`^[a-z0-9][a-z0-9_-]{0,63}$` — a meet id is used verbatim as a `?room=` query value and as a
URL path segment, so it is constrained at the boundary rather than escaped at every use.

Response `201`:
```json
{
  "id": "standup",
  "created_at_unix_ms": 1756684800000,
  "join": {
    "ws_url": "ws://localhost:9000/ws?room=standup",
    "peer_command": "peer -call -managed -server http://localhost:9000 -room standup -name YOUR_NAME"
  }
}
```

The `join` block is the **rendezvous** the brief describes: the browser hands a participant
the command (or link) to join the meet's subnetwork. The server builds it from its own
`-addr` and `-public-url`, so it is correct for a hosted deployment without the frontend
guessing.

Errors: `409` if the id already exists, `400` on a malformed id.

#### `GET /api/meets/{id}` — one meet, full snapshot

Response `200` — the same object the WS stream sends as its first `snapshot` frame:

```json
{
  "id": "standup",
  "epoch": 3,
  "rev": 11,
  "coordinator": "alice",
  "coordinator_id": "p2",
  "arbiter_is_coordinator": false,
  "root": "alice",
  "at_unix_ms": 1756684812345,
  "nodes": [
    {
      "id": "p2", "name": "alice",
      "roles": ["coordinator", "relay"],
      "health": "healthy",
      "parent": "", "backup": "",
      "children": ["bob", "carol"],
      "depth": 0,
      "upload_kbps": 8000, "nat": "direct",
      "rtt_server_ms": 12.5, "loss_pct": 0.2, "cpu_pct": 31.0,
      "fitness": 0.71,
      "last_beat_seq": 412, "last_beat_unix_ms": 1756684812100
    },
    {
      "id": "p3", "name": "bob",
      "roles": ["leaf"],
      "health": "degraded",
      "parent": "alice", "backup": "",
      "children": [], "depth": 1,
      "upload_kbps": 1000, "nat": "direct",
      "rtt_server_ms": 240.0, "loss_pct": 6.1, "cpu_pct": 55.0,
      "fitness": 0.29,
      "last_beat_seq": 407, "last_beat_unix_ms": 1756684808900
    }
  ],
  "edges": [ { "parent": "alice", "child": "bob" } ],
  "backups": [ { "node": "carol", "parent": "dave" } ],
  "stale_rejected": 2
}
```

`roles` is an **array** — §1.2. `backup` is `""` for a root child, and the UI renders that
as "no backup (root child)", not as a blank or a warning.

Errors: `404` for an unknown meet.

#### `GET /api/meets/{id}/events` — the live stream (WebSocket)

Upgrade required. Origin-checked against the same allow-list. The first frame is always a
`snapshot`; every subsequent frame is a delta.

Client → server frames on the same socket (the only two):
```json
{ "op": "ping" }
{ "op": "resync" }
```
`resync` makes the server send a fresh `snapshot` — the recovery path for a `seq` gap.

### 9.4 The event envelope (frozen)

Every frame on `/api/meets/{id}/events` has exactly this shape:

```json
{
  "seq": 148,
  "at_unix_ms": 1756684812345,
  "meet_id": "standup",
  "epoch": 3,
  "rev": 11,
  "kind": "reparent",
  "data": { }
}
```

| Field | Type | Meaning |
|---|---|---|
| `seq` | `uint64` | Monotonic **per meet**, starting at 1 for each subscriber's connection. A client that sees `seq` skip **must** send `{"op":"resync"}` — this is the `Last-Event-ID` replacement (§9.2). |
| `at_unix_ms` | `int64` | Server time. |
| `meet_id` | string | Redundant with the path; present so a client can multiplex several meets over one log view. |
| `epoch`, `rev` | `uint64` | The control-plane version this event was observed under. |
| `kind` | string | Frozen enum below. |
| `data` | object | Kind-specific. |

**Frozen `kind` enum** and the `data` shape for each:

| `kind` | `data` | Emitted by |
|---|---|---|
| `snapshot` | The full `GET /api/meets/{id}` body. | connect, and `resync` |
| `member_joined` | `{"id","name"}` | `coordinator.EventMember` (Present) |
| `member_left` | `{"id","name"}` | `coordinator.EventMember` (!Present) |
| `health_changed` | `{"name","health","prev_health"}` | `coordinator.EventHealth` |
| `topology` | `{"root","edges":[…],"backups":[…],"depth","relays":[…]}` | `coordinator.EventTopology` |
| `reparent` | `{"name","from","to","self_promoted":true,"reason"}` | `coordinator.EventReparent` |
| `failover` | `{"name","orphans":[…],"reroot":false,"reason"}` | `coordinator.EventFailover` |
| `election` | `{"epoch","coordinator","prev","reason"}` | `arbiter.Publisher` |
| `stale_rejected` | `{"name","epoch","rev","reason"}` | `coordinator.EventStale` |
| `settling` | `{"waiting":[…],"reason"}` | `coordinator.EventSettling` |
| `unbuildable` | `{"reason"}` | `coordinator.EventUnbuildable` |
| `demo` | `{"action","target","by"}` | the demo surface (§9.6) |

### 9.5 What the UI renders (contract for `web/`)

| Panel | Content |
|---|---|
| **Server bar** | The configurable server URL (default `localhost:9000`), a connect/disconnect control, and a live/stale indicator. |
| **Meets** | List + create. Each row shows members, epoch, coordinator. Creating one reveals the `join.peer_command` with a copy button — this is the rendezvous. |
| **Subnet** | The live tree. Coordinator badge = `--accent-primary` (#bd93f9), relay badge = `--accent-secondary` (#ff79c6), leaf = `--surface`. Edges `--edge` (#8be9fd); a degraded node's edge `--warn`; a failover flash `--crit`. **Two independent badges per node**, never one merged role. Backup parents drawn as a dashed edge. |
| **Node detail** | Per-node telemetry with `font-variant-numeric: tabular-nums` so live numbers do not jitter the layout. |
| **Event log** | The envelope stream, newest first, colour-coded by `kind`. |
| **Build state** | `settling` renders as a transient, non-alarming "waiting for telemetry from X, Y" line in `--fg-muted` — it is normal startup and must never look like a fault. `unbuildable` renders as a **persistent banner** in `--crit` with the reason text, because it means someone is receiving nothing and a human has to change a flag or drop a peer. Getting these two visually confused is the specific failure this distinction exists to prevent. |
| **Epoch** | Current epoch + rev, prominently. `stale_rejected` count next to it — this is the visible proof of the fence. |

Theming follows `docs/design-system.md` §2.1–2.4: Dracula tokens as CSS custom properties on
`:root`, monospace throughout, **dark is canonical**. Since design-system.md declares a light
variant out of scope, `web/` ships dark only and states it in a comment rather than shipping
a half-derived light theme.

### 9.6 The demo control surface — off by default

```
POST /api/demo/meets/{id}/evict   { "name": "bob" }
POST /api/demo/meets/{id}/elect   { "name": "carol" }   // "name" optional ⇒ best candidate
```

Both return `202` with `{"ok":true,"action":"evict","target":"bob"}` and emit a `demo`
event. `evict` closes that peer's WebSocket, producing exactly the failure the liveness path
is meant to handle. `elect` forces an `arbiter.ReasonManual` announcement, bypassing
`ElectionDwell` and `MinTermDuration`.

**Gated behind `-demo` (default `false`). When off, the routes are NOT REGISTERED AT ALL** —
`404`, not `403`.

Justification, plainly: these two endpoints let an **unauthenticated** caller terminate a
participant's connection and force a control-plane transition. That is a denial-of-service
primitive. The dashboard has no authentication by design (it is a portfolio demo, not a
product), so there is no auth layer to put in front of them. The only safe posture is that
the destructive surface **does not exist** unless the operator explicitly turns it on for a
demo session. Not-registered beats registered-and-403 because an unregistered route cannot
be reached by a bug in a permission check — the guard is structural, not conditional. This is
the same reasoning as `-coordinate` being off by default, one notch stricter because the
consequences are destructive rather than merely surprising.

### 9.7 The dashboard's consumer interfaces

Declared in `internal/dashboard`, satisfied by `*arbiter.Arbiter` and
`*coordinator.Coordinator`, wired in `cmd/server/main.go`:

```go
package dashboard

// MeetSource is what the dashboard needs from the arbiter. Consumer-defined here so
// dashboard does not import arbiter and the two can be built in parallel.
type MeetSource interface {
	ListMeets(ctx context.Context) ([]Meet, error)
	CreateMeet(ctx context.Context, id string) (Meet, error)
	GetMeet(ctx context.Context, id string) (Meet, error)
}

// SubnetSource is what the dashboard needs from the coordinator.
type SubnetSource interface {
	Snapshot(ctx context.Context, roomID string) (coordinator.RoomSnapshot, error)
}

// DemoControl is the gated destructive surface. cmd/server passes nil when -demo is off,
// and the dashboard does not register the routes when it is nil — so "off" is expressed as
// an absent dependency rather than a boolean the handler has to remember to check.
type DemoControl interface {
	Evict(ctx context.Context, roomID, name string) error
	ForceElection(ctx context.Context, roomID, name string) error
}

// Server implements coordinator.Publisher and arbiter.Publisher (both single-method), so
// cmd/server hands the same value to both.
func (s *Server) Publish(coordinator.Event)
func (s *Server) PublishElection(arbiter.Announcement)

// Handler returns the http.Handler to mount under /api/.
func (s *Server) Handler() http.Handler
```

### 9.8 CORS and origin policy

The Pages origin (`https://sammyurfen.github.io`) differs from the server origin
(`http://localhost:9000` or a hosted `wss://` host), so both the REST surface and the WS
upgrade need an explicit policy. **One allow-list, injected in both places.**

```go
// -allowed-origins, comma-separated. Default:
//   "http://localhost:*,http://127.0.0.1:*,https://sammyurfen.github.io"
// A single trailing '*' is permitted as a PORT wildcard only (host must match exactly).
```

REST response headers on every `/api/` response whose `Origin` matches:

```
Access-Control-Allow-Origin: <the matching origin, echoed>
Vary: Origin
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
Access-Control-Max-Age: 600
```

**Echo the matching origin; never `*`.** `*` is technically sufficient today (no cookies, no
`Authorization`), but it is a footgun that becomes a real vulnerability the moment anyone
adds credentials, and it costs one line to do correctly now. Preflight `OPTIONS` is answered
`204` with the same headers and no body.

**WebSocket upgrades are not subject to CORS** — the browser sends `Origin` and it is the
server's job to check it. Pass the *same* list as `websocket.AcceptOptions.OriginPatterns`
on both `/ws` and `/api/meets/{id}/events`. A non-matching origin gets the upgrade refused,
not a 200 with an empty stream.

### 9.9 Mixed content: an `https://` Pages site opening `ws://localhost` — confirmed

**Yes, this works, and it is the intended default.** W3C *Secure Contexts* classifies
`localhost`, `127.0.0.1`, and `[::1]` as **potentially trustworthy origins**, and the
mixed-content specification exempts potentially-trustworthy URLs from blocking. Chrome, Edge,
Firefox, and Safari all implement this. So a page served from `https://sammyurfen.github.io`
may open `ws://localhost:9000/ws` without a mixed-content block, and the default
`-server localhost:9000` in the UI is correct.

Three caveats, stated honestly because each one produces a confusing failure:

1. **The exemption is by hostname, not by network.** `ws://192.168.1.5:9000` from an
   `https://` page **is blocked**, even on your own LAN. Watching a second machine's arbiter
   from the hosted page therefore requires `wss://`.
2. **It depends on `localhost` resolving to loopback.** Chrome follows the spec and treats
   the literal name as trustworthy, but an environment whose DNS maps `localhost` elsewhere
   can break the assumption. Offering `127.0.0.1` as an alternative in the UI's server field
   is a cheap hedge.
3. **Any remote arbiter must be `wss://` with a real certificate.** That is what `deploy/`
   is for: a `Dockerfile` for the server plus a deploy doc covering a TLS terminator (Caddy
   is the obvious fit given the owner's existing setup) and the `-allowed-origins` value the
   Pages origin needs.

`deploy/` must contain, at minimum: a multi-stage `Dockerfile` (build with the Go toolchain,
ship a distroless/scratch binary), a `docker-compose.yml` for local `wss://` with Caddy, and
`docs/deploy.md` covering the hosted path and the exact `-allowed-origins` and `-public-url`
values.

---

## 10. CLI flags — the final set

### `cmd/server`

| Flag | Default | Status | Purpose |
|---|---|---|---|
| `-addr` | `:9000` | existing | Listen address |
| `-log-level` | `info` | existing | `debug`\|`info`\|`warn`\|`error` |
| `-log-format` | `text` | existing | `text`\|`json` |
| `-coordinate` | `false` | existing | Run the coordinator in-process (Phase 4/5 posture) |
| `-max-depth` | `2` | existing | Coordinator: max subnet depth, hops root→leaf |
| `-stream-kbps` | `2000` | existing | Coordinator: per-stream upload cost |
| `-default-upload-kbps` | `0` | existing | Coordinator: budget for an unreported peer |
| `-stickiness-ms` | `25` | **new** | Re-parent margin (`overlay.DefaultStickinessMs`); `0` ⇒ Phase-4 memoryless behaviour |
| `-root-change-margin-kbps` | `2000` | **new** | Extra upload a challenger needs before the subnet is re-rooted (`overlay.RootChangeMarginKbps`) |
| `-first-build-settle` | `1500ms` | **new** | How long a new meet waits for first telemetry before building anyway (§5.9) |
| `-dwell` | `10s` | **new** | Sustained-degradation dwell |
| `-recompute-cooldown` | `5s` | **new** | Minimum interval between published trees per meet |
| `-degraded-after` | `3s` | **new** | Silence before `healthy → degraded` |
| `-gone-after` | `8s` | **new** | Silence before `degraded → gone` (the repair backstop) |
| `-elect` | `false` | **new** | Phase 6: arbitrate coordinator election among peers |
| `-election-dwell` | `20s` | **new** | Sustained window for a voluntary handover |
| `-min-term` | `60s` | **new** | Floor between voluntary handovers |
| `-dashboard` | `true` | **new** | Mount the `/api/` surface |
| `-allowed-origins` | `http://localhost:*,http://127.0.0.1:*,https://sammyurfen.github.io` | **new** | CORS + WS origin allow-list (one list, both places) |
| `-public-url` | `""` | **new** | Externally reachable base URL used to build `join.ws_url` / `join.peer_command`; empty ⇒ derive from `-addr` |
| `-demo` | `false` | **new** | Register the destructive demo control routes (§9.6) |

**`-coordinate` vs `-elect` precedence (frozen).** `-coordinate` means "this process may host
the coordinator". `-elect` means "arbitrate the role among peers". With both set: the arbiter
hosts the coordinator until a peer scores above `DemoteBelowScore`, then hands over. With
`-elect` and not `-coordinate`: the arbiter never coordinates, and a meet with no fit peer has
no coordinator (and no tree) — logged at `warn` every `ElectionDwell`, not silently. With
neither: the plain Phase 1–3 signaling relay, unchanged.

Startup validation (extend `validateCoordinatorFlags`, same fail-loud discipline): every
duration flag `> 0`; `-degraded-after < -gone-after`; `-stickiness-ms >= 0`;
`-root-change-margin-kbps >= 0`; `-first-build-settle > 0`; `-allowed-origins` non-empty
when `-dashboard`; each origin parses.

### `cmd/peer`

| Flag | Default | Status | Purpose |
|---|---|---|---|
| `-server` | `http://localhost:9000` | existing | Arbiter base URL |
| `-log-level` | `info` | existing | |
| `-log-format` | `text` | existing | |
| `-timeout` | `5s` | existing | Probe-mode request timeout |
| `-call` | `false` | existing | Join a meet and run WebRTC |
| `-room` | `default` | existing | Meet id (the wire name stays `room`, §1.1) |
| `-send` | `false` | existing | Add an outbound video track |
| `-media` | `""` | existing | VP8 IVF file to send |
| `-record` | `""` | existing | Write the first received track here |
| `-stun` | `""` | existing | STUN URL |
| `-name` | `""` | existing | Stable subnet label |
| `-topology` | `""` | existing | Static tree file (Phase 3) |
| `-managed` | `false` | existing | Coordinator-managed mode |
| `-upload-kbps` | `3000` | existing | Advertised upload budget |
| `-nat` | `direct` | existing | Declared NAT class |
| `-heartbeat` | `1s` | **new** | Liveness beat interval (`metrics.HeartbeatInterval`) |
| `-backup` | `true` | **new** | Promote the precomputed backup parent on primary failure |
| `-coordinatable` | `true` | **new** | May this peer be elected coordinator (a laptop on battery says `false`) |

---

## 11. Build order and ownership

Each work item owns its files exclusively. **An agent that needs a change in a file it does
not own must request it in its report, not edit it.** That rule is what makes parallel work
safe.

| WI | Title | Owns (exclusive) | Depends on | Parallel with |
|---:|---|---|---|---|
| **WI-0** | Wire + clock foundation | `internal/clock/**`, `internal/signaling/message.go`, `internal/signaling/hub.go`, `internal/signaling/client.go`, `internal/metrics/report.go`, `Makefile` | — | **nothing — lands first** |
| **WI-1** | `overlay` epoch/backups/stability | `internal/overlay/**` | — | WI-0 |
| **WI-2** | `simnet` clock + scenarios + injection | `internal/simnet/**` | WI-0, WI-1 | WI-4 |
| **WI-3** | `coordinator` Phase 5 | `internal/coordinator/**` | WI-0, WI-1, WI-2 | WI-4, WI-5 |
| **WI-4** | `media` Phase 5 | `internal/media/**` | WI-0, WI-1 | WI-2, WI-3, WI-5 |
| **WI-5** | `arbiter` Phase 6 | `internal/arbiter/**` | WI-0, WI-1 | WI-3, WI-4 |
| **WI-6** | `coordinator` Phase 6 | `internal/coordinator/**` | WI-3, WI-5 | WI-7 |
| **WI-7** | `dashboard` + frontend | `internal/dashboard/**`, `web/**` | WI-0, WI-1, WI-3 | WI-6 |
| **WI-8** | `cmd/peer` | `cmd/peer/**` | WI-0, WI-1, WI-4 | WI-9 |
| **WI-9** | `cmd/server` wiring + deploy | `cmd/server/**`, `deploy/**` | WI-3, WI-5, WI-7 | WI-8 |
| **WI-10** | Living-docs sync | `docs/**`, `CLAUDE.md`, `README.md` | all | — (last) |

### Files more than one work item must touch — ownership prescribed

| File | Owner | Everyone else |
|---|---|---|
| `internal/signaling/message.go` | **WI-0 only.** All three new `Type` constants and `ServerID` land in one commit. | Consume the constants. Request additions in your report. |
| `internal/signaling/hub.go` | **WI-0 only.** `SendRoom`, the two `Observer` methods, `HubConfig`, WS ping, `OriginPatterns`. | Consume. |
| `internal/metrics/report.go` | **WI-0 only.** `Heartbeat`, `Reparented`, `HeartbeatInterval`, the `Reporter` clock parameter. | Consume. |
| `internal/coordinator/**` | **WI-3, then WI-6, sequentially — same agent preferred.** They edit the same files; running them in parallel guarantees a conflict. | — |
| `cmd/server/main.go` | **WI-9 only.** Every seam in this system is wired here; five agents editing it is five conflicts. | Export what `main` needs; WI-9 wires it. |
| `cmd/peer/main.go` | **WI-8 only.** | |
| `docs/**` | **WI-10 only.** Each earlier WI reports *what changed* and WI-10 writes it. | Do not edit docs inline. |
| `Makefile` | **WI-0 only** (adds `check-determinism`). | |

**WI-10's mandatory edits** (the living docs are only worth keeping if they stay true):

- `CLAUDE.md` phase table → Phases 5 and 6 ✅; add `clock`, `arbiter`, `dashboard` to the
  package-by-feature list.
- `docs/ROADMAP.md` → mark Phases 5 and 6 done; note that Phase 7's `/debug` dashboard was
  brought forward and reshaped into the arbiter's `/api` + Pages frontend.
- `docs/ARCHITECTURE.md` **§8 state handover** → replace the "likely both" table with the
  rebuild-from-peers decision and its reasoning (§6.6 here).
- `docs/ARCHITECTURE.md` **§6 third bullet** → the "Phase 4's apply is additive" caveat is
  obsolete; delete it.
- `docs/structure.md` → three new packages, `web/`, `deploy/`, and the updated dependency
  table from §2.3.
- `docs/design-system.md` → Part 2 moves from "reserved plan (Phase 7)" to **live**; add the
  new log fields (`epoch` → note the reserved name was `coord_epoch`; reconcile), `rev`,
  `health`, `backup`, `fitness`; update the CLI flag table from §10.
- `docs/testing.md`, `docs/usage.md`, `docs/troubleshooting.md`, `docs/setup.md` → new flags,
  new failure symptoms, the `make check-determinism` gate.

---

## 12. Test contract

### 12.1 The rule

> **The pure control plane — `overlay`, `simnet`, `coordinator`, `arbiter` — is tested
> deterministically: no sockets, no pion, no media, no wall clock.** Every temporal
> behaviour is driven through `clock.Clock` and asserted in virtual time. Enforced by
> `make check-determinism` (§4.7), not by review discipline.

### 12.2 What is tested where

| Layer | Package | Style | Must cover |
|---|---|---|---|
| **Pure graph** | `overlay` | Table-driven + the independent `Validate` oracle. Never re-derive the expected tree. | Epoch/rev stamping; stickiness at rank 1 (an incumbent survives a lower-load and a marginally-closer challenger, loses to one beating `StickinessMs`); processing order (a strong joiner does not re-parent incumbents); backup invariant `B ∉ Subtree(P)`; root children have no backup; `ValidateLocalRepair` catches a gratuitous re-parent; `Validate` catches each new violation class; `LoadTopology` still accepts a Phase-3 file and stamps `StaticEpoch`; determinism over ≥50 repeats. **`PickRoot` never returns a provisional node; an incumbent root survives a challenger inside `RootChangeMarginKbps` and loses to one beyond it.** |
| **Deterministic sim** | `simnet` | Seeded scenarios + property tests over many seeds. | Replay identity (same seed ⇒ identical trace); virtual-clock ordering (same-instant timers fire in creation order); each injection primitive; ≥200-step churn keeping `Validate` green; the dwell suppressing a noisy-but-stable metric stream; the dwell firing on a sustained one. **REPORT-ORDER INVARIANCE (below).** |
| **Control loop** | `coordinator` | `-race`, fake `Sender`, fake `Publisher`, `VirtualClock`, `Sync` barrier. | The eight threshold events and **only** those recompute; `RecomputeCooldown` coalescing a burst; the `OK:false` cooldown bypass; health FSM transitions in virtual time; local repair moves only orphans (asserted via `ValidateLocalRepair`); the ratification rule (a self-promoted parent survives the next rebuild); re-root on root loss; `Snapshot` never tears. **The settle rule: no tree before eligibility; the early exit fires when all report; the bound fires at `FirstBuildSettle`; the settle applies once and never again; `settling` is emitted exactly once per meet and `unbuildable` only post-settle.** |
| **Election** | `arbiter` | Table-driven `Score`; scenario-driven election. | `Score` returns 0 for TURN-bound and non-healthy; upload is absent from the formula (assert a large `UploadKbps` changes nothing); every trigger in §6.2; `MinTermDuration` blocks a voluntary handover and does **not** block a failure one; epochs strictly increase and never repeat. |
| **Fencing** | `simnet` + `coordinator` | Scenario. | Two coordinators mid-handover: the stale one's push is rejected and counted; a topology carrying a higher epoch is rejected; a rejoining peer resets `curEpoch`; the arbiter-restart hole (§6.7) is closed. |
| **Media, deterministic** | `media` | Unit, no network. | The negotiation serializer: a second `AddTrack` while `negotiating` sets `renegotiate` and produces **exactly one** follow-up offer; the retry path bounded by `NegotiationRetries`; `RemoveTrack` on a closed pc returns nil; the diff computation in `applyTopology` (pure function over two topologies — extract it so it is testable without pion). |
| **Media, integration** | `media` | `-race`, real loopback pion, as today. | Mid-call re-parent: a leaf moved from R1 to R2 keeps receiving, with the old session closed *after* the new one connects; a dropped source's downstream legs are actually removed (the §7.4 fix); backup promotion on a killed parent. |
| **Wire** | `signaling` | `-race`, two real WS clients, as today. | The three new frame types round-trip; `SendRoom` reaches every member; a dead socket is reaped by the WS ping within `WSPingInterval + WSPingTimeout`; origin rejection. |
| **HTTP API** | `dashboard` | `httptest`, fake `MeetSource`/`SubnetSource`. | Every path and status in §9.3; the envelope schema; `seq` monotonicity and the `resync` path; CORS headers present and echoing, never `*`; **demo routes return 404 when `DemoControl` is nil**. |
| **Frontend** | `web/` | Manual, plus a Playwright smoke test if cheap. | Loads with no console errors; renders against a live server; handles server-unreachable without a blank page; dark theme only. |

### 12.3 Report-order invariance — a required property test

> **PROPERTY (mandatory, `simnet`).** For a fixed fleet, **no permutation of the order in
> which members' first telemetry reports are delivered may change the converged tree.**

Concretely: take a fleet of 4–5 nodes with distinct upload budgets (at least one strong,
at least two weak), drive it through `Scenario.ReportOrder` over **every permutation** of
first-report arrival (≤120 permutations — cheap), advance past `FirstBuildSettle`, `Settle`,
and assert that the final `Topology.Edges` is byte-identical across all of them.

This is the direct encoding of the live defect in §3.8: the tree that got discarded there was
a pure function of which WebSocket frame arrived first. If this property ever fails, some
control-plane decision has leaked a dependency on arrival order, which is exactly what the
deterministic-simulation contract exists to forbid. Assert on the *converged* tree, not on
the intermediate ones — transient trees legitimately differ, and pinning them would make the
test brittle without testing anything real.

A second, weaker companion worth having: assert that across all permutations the number of
**published** trees is the same. Equal end-states reached via different numbers of
reconfigurations would still be a churn bug, just a quieter one.

### 12.4 The gate

`make check` = `fmt` + `vet` + `check-determinism` + `test -race`. Unchanged in shape; one
new step. **Never say done without running it and showing the output.**

---

## 13. Limitations — what this design does NOT solve

Stated plainly, because a contract that oversells is worse than one that admits its edges.

1. **Fitness inputs are not measured.** `CPUPct`, `LossPct`, and `RTTServerMs` are declared
   in `metrics.Report` and consumed by `arbiter.Score`, but nothing populates them yet. In
   production the election is driven by `UptimeSec` and the NAT disqualifier; in `simnet`
   the values are injected. **The election logic is real and tested; the sensors are not
   built.** Do not present a demo as "elects the fittest machine" — it elects on the inputs
   it has.
2. **Pairwise RTT is still unmeasured live.** `BuildTree`'s min-latency rank has no real
   data outside `simnet`, so live attachment falls through to load-balancing. The
   stickiness margin is therefore inert in production too — an incumbent always wins,
   because no challenger can beat an unknown RTT. This is not a bug; it means the anti-thrash
   property is *stronger* live than in simulation, and the min-latency property is *only*
   exercised in simulation. Say exactly that.
3. **Backup capacity is not reserved (§3.5).** A failover can transiently oversubscribe a
   relay by one child until the next recompute. Bounded and self-correcting, but real.
4. **Root children have no backup (§3.5).** Losing the root is a full rebuild, not a warm
   failover. For a shallow tree that is a large fraction of the nodes.
5. **A handover during heavy churn can cause one global re-parent (§6.6 step 4).** If the
   reconstructed tree fails `Validate`, the new coordinator builds from `prev = nil`.
6. **The arbiter's epoch counter is in-memory (§6.7).** Closed by the rejoin rule, not by
   persistence. An arbiter restart drops every meet.
7. **The control plane remains a SPOF.** Unchanged from ARCHITECTURE §4 and deliberate:
   media keeps flowing when the arbiter is down; nothing adapts.
8. **No authentication anywhere.** Anyone who can reach `/ws` can join any meet; anyone who
   can reach `/api` can create meets and read every meet's telemetry; with `-demo` on, evict
   peers. Acceptable for a local/portfolio system, unacceptable for anything else, and the
   `-demo` default is the only place this contract compensates.
9. **No Byzantine tolerance.** The epoch fence stops *stale* actors, not *dishonest* ones. A
   peer that lies about its metrics gets the tree position it lied for.
10. **The dashboard is read-mostly and eventually consistent.** It renders what the
    coordinator observed, with a `seq`-gap resync as the only consistency mechanism. It is
    an operator's lens, not a control surface (except the gated demo routes).
11. **`metrics` is a misnomer for two of the three payload types it now owns (§2.1).**
    Named, with a trigger to split.
12. **Root stability is heuristic, not proven.** `RootChangeMarginKbps` and
    `FirstBuildSettle` (§3.8, §5.9) make a root flap *very unlikely* and make a
    provisional root *impossible*, but a genuinely oscillating upload estimate that
    repeatedly crosses the 2000 kbit/s margin would still re-root repeatedly. There is no
    dwell on the root decision itself — the margin is the only damping. If the
    report-order-invariance property (§12.3) ever fails in a way the margin cannot fix, a
    root dwell is the next thing to add, and it should be added rather than the margin
    widened.
13. **Phase 7 is untouched.** No simulcast, no SVC, no TURN infrastructure. `overlay.NATRelayed`
    stays a *modelled* constraint declared by a flag; nothing detects a real symmetric NAT.
    A relay still sends one quality layer to every downstream.

---

## 14. Deviations from the brief — flagged for the owner

Per §0, these are the places where I implemented something other than, or more than, what
the brief specified. Each is a judgement call the owner should confirm or overrule.

1. **The backup invariant is stronger than requested.** The brief said "a node's backup must
   not be inside its own subtree". That is necessary but insufficient: it permits a *sibling*
   as backup, and a sibling is orphaned by the very failure the backup insures against. I
   froze `B ∉ Subtree(P)` instead (§3.5), which implies the brief's rule.
2. **`Epoch` alone was not enough; I added `Rev`.** A single monotonic epoch cannot both
   fence coordinator handovers (single writer: the arbiter) and order successive trees from
   one coordinator (single writer: the coordinator) without one writer being able to forge
   the other's authority. §3.1 and §5.7 split them, Raft-style.
3. **`StaticEpoch = MaxUint64`, not 1** (§3.9), so a coordinator push can never silently
   override an operator's hand-authored `-topology` file.
4. **Two liveness levels, not one** (§5.1): the WS ping *and* an application heartbeat. The
   brief named `conn.Ping`; a WS ping alone cannot see a broken *media* edge on a peer whose
   control link is fine.
5. **`GoneAfter` is deliberately slow (8 s)** (§5.3). The brief implies fast detection; I made
   the arbiter's timer a conservative backstop and put the speed in the peer's local backup
   promotion instead. If the owner wants a fast central detector too, that is a different
   design and should be said.
6. **ARCHITECTURE.md §8 is superseded, not extended** (§6.6). It proposes "likely both"
   handover strategies; this contract implements only rebuild-from-peers.
7. **`internal/clock` is a shared leaf package**, breaking the otherwise-universal
   consumer-defined-interface rule. §2.4 explains why Go's typing forces it.
8. **`internal/arbiter` was not in the brief.** Phase 6 needs epoch minting, fitness, and
   election policy to be unit-testable without a socket, which means a package, not
   `cmd/server`. If the owner would rather that logic live in `cmd/server`, it becomes
   untestable and I would push back.
9. **The `metrics` package now owns two non-telemetry payload types** (§2.1), with the
   `internal/control` split named and deferred rather than taken.
10. **The first-build settle is a composite, not one of the three options offered.** The
    brief listed (a) wait-for-all, (b) bounded window, (c) never-root-an-unreported-node and
    asked for one. I took **(c) as a structural invariant + (b) as the bound + (a) as the
    early exit** (§5.9), because each of the three leaves a hole the others close: (a) hangs
    on a silent peer, (b) only shrinks the race window instead of removing it, and (c) alone
    still publishes a throwaway tree built over a provisional fleet. It also required adding
    `overlay.Node.Provisional` (§3.7) so `overlay` can express "this number is a guess"
    without learning what a report is.
11. **`web/` is dark-theme only**, following `design-system.md` §2.4's explicit
    out-of-scope call for a light variant — despite the brief's "hand-rolled light/dark
    theming". The tokens are structured so a light palette is a re-derivation away, but I did
    not ship a naive inverted one. **This is the deviation most likely to be wrong; confirm.**

---

*This document is FROZEN. Amendments go through the owner, in this file, with the change
noted. Implementers: build what is written, and report what is wrong.*
