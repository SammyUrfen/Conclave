# PLAN.md — the Phase 5–6 architecture contract

> **Status: v2.2, FROZEN**, 2026-09-01. v1 was frozen the same day, reviewed adversarially by
> two independent reviewers (12 critical + ~15 major findings), and amended in place. **§15 is
> the amendment log — read it first if you built against v1**, because several frozen names
> changed. v2 also reconciles the document with what WI-0 actually shipped; where the shipped
> design differs and is better, the shipped design wins and is marked SHIPPED.
>
> This document is the contract every Phase 5 and Phase 6 implementation agent builds against.
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
| `internal/policy` | **Boundary policy shared by every externally-facing surface**: which origins may connect, and what a meet id may look like. Pure string predicates. **Leaf. No internal imports, ever.** | — | `signaling`, `dashboard`, `arbiter`, `cmd/*` |
| `internal/arbiter` | Meet registry; **its own liveness view**; epoch minting; coordinator-fitness scoring; election policy; the announcement. **No sockets** — defines its own `Announcer` and `Publisher` seams. | `overlay`, `metrics`, `clock`, `policy` | `cmd/server`, `dashboard`, tests |
| `internal/dashboard` | The browser-facing HTTP + WS surface: REST commands, the live event stream, the event envelope, CORS/origin policy, the demo control surface. | `overlay`, `coordinator`, `arbiter`, `clock`, `policy` | `cmd/server` |
| `web/` | The zero-build static frontend (vanilla ES modules, no bundler, no framework). **Not a Go package.** | — | — (served by GitHub Pages) |
| `deploy/` | `Dockerfile` + deploy docs for a hosted `wss://` arbiter. **Not a Go package.** | — | — |

**`dashboard` imports BOTH `coordinator` and `arbiter` (CORRECTED, see §15/C9).** v1 claimed
`dashboard` imported "arbiter for nothing", declaring consumer interfaces instead. That was a
misapplication of the very rule §2.5 states correctly for `clock.Timer`:

> **A consumer-defined interface avoids the import only when its signatures name types the
> IMPLEMENTER owns, or stdlib types.**

v1 broke it in both directions at once: `MeetSource` returned a `dashboard.Meet`, which would
have forced `arbiter` (the implementer) to import `dashboard`; and
`PublishElection(arbiter.Announcement)` names an arbiter type, so `dashboard` imported
`arbiter` regardless of the claim. The fix is to stop pretending: `dashboard -> arbiter` is a
legal downward edge, `arbiter.Meet` is arbiter-owned (which also supplies the type v1 never
defined), and `arbiter` imports nothing new. The §9.7 interfaces survive as *narrowing* seams
— they still let the dashboard be tested with fakes — but they now name only arbiter-owned
and coordinator-owned types.

**`arbiter` imports `metrics` (new edge).** It needs `metrics.Report` for fitness inputs and
`metrics.Heartbeat` for its own liveness view (M4). `metrics` imports only `overlay`,
`clock`, and `logging`, so the edge adds no cycle. It is also what makes `-coordinatable`
implementable at all (M12).

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
| `policy` | — |
| `overlay` | — |
| `metrics` | `overlay`, `clock`, `logging` |
| `signaling` | `logging`, `clock`, `policy` |
| `coordinator` | `overlay`, `metrics`, `clock`, `logging` |
| `arbiter` | `overlay`, `metrics`, `clock`, `policy`, `logging` |
| `media` | `overlay`, `signaling`, `clock`, `logging` |
| `dashboard` | `overlay`, `coordinator`, `arbiter`, `clock`, `policy`, `logging` |
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

### 2.4 RULING (requested by WI-0): `internal/policy`, a second shared leaf

**The problem.** §9.8 requires ONE origin allow-list shared between the WebSocket upgrade
check (in `signaling`, via `websocket.AcceptOptions.OriginPatterns`) and the REST CORS
surface (in `dashboard`). The DAG forbids `dashboard → signaling`, so the matcher — and
`ValidRoomID`, which `arbiter` also needs for meet creation — would have to be reimplemented
per surface. WI-0 is right that this is a correctness hazard and not mere duplication: **two
copies of a security-relevant matcher will drift, and the drift's failure mode is a silently
permissive CORS policy that no test notices**, because each surface's tests pass against its
own copy.

**RULING: extract a leaf package, `internal/policy`.**

```go
// Package policy holds the rules this process applies to UNTRUSTED INPUT AT ITS BOUNDARY:
// which browser origins may open a connection, and what a meet id may look like. Every
// externally-facing surface — the peer WebSocket, the dashboard REST API, the dashboard
// event stream, meet creation — must apply the SAME rules, and the failure mode of them
// diverging is silent and security-relevant. So there is one implementation.
//
// Leaf: imports nothing internal, ever. Same standing as clock and logging.
package policy

// MeetIDPattern constrains a meet id at the boundary rather than escaping it at every use.
// A meet id is interpolated into a `?room=` query value AND into a URL path segment, so it
// is restricted to characters that are unambiguous in both.
//
// The {0,63} bound (64 chars total) is not arbitrary: it is the longest single DNS label,
// which keeps a meet id usable verbatim as a subdomain if this ever grows per-meet hosts,
// and it is far above any human-typed room name while being far below anything that could
// bloat a log line or a URL. A bound was needed; this is the one with a reason attached.
const MeetIDPattern = `^[a-z0-9][a-z0-9_-]{0,63}$`

// ValidMeetID reports whether id is acceptable.
func ValidMeetID(id string) bool

// Origins is a parsed allow-list. A single trailing '*' is permitted as a PORT wildcard
// only; the host must match exactly.
type Origins []string

// ParseOrigins parses the comma-separated -allowed-origins value, failing loud on an entry
// that is not a valid origin or misuses the wildcard.
func ParseOrigins(csv string) (Origins, error)

// Patterns returns the list in the form coder/websocket's AcceptOptions.OriginPatterns
// expects.
func (o Origins) Patterns() []string

// Match reports whether origin is allowed and returns the value to echo in
// Access-Control-Allow-Origin. The echoed value is the REQUEST's origin, never "*"
// (§9.8).
//
// Match and Patterns MUST accept exactly the same set. That is the entire reason this
// package exists, and §12.2 requires a test that asserts it against the real
// websocket.Accept behaviour rather than against a second copy of the logic.
func (o Origins) Match(origin string) (string, bool)
```

`signaling` keeps the names WI-0 shipped, as one-line delegations, so no shipped call site
changes:

```go
const RoomIDPattern = policy.MeetIDPattern
func ValidRoomID(id string) bool { return policy.ValidMeetID(id) }
```

**Rejected: amend the DAG to permit `dashboard → signaling`.** It is the cheapest fix and it
creates no cycle. It loses because of what it enables *next*: it hands the unauthenticated,
browser-facing surface a compile-time handle on the peer-facing Hub, and the very next
convenience ("dashboard already imports signaling — just call `hub.SendRoom` from the demo
endpoint") puts control-plane authority directly behind an HTTP handler with no auth in front
of it. The forbidden edge is the thing preventing that, and it is worth more than one small
package.

**Rejected: accept the duplication with a cross-checking test.** Better than nothing, and it
was the honest third option. It loses arithmetically: a test asserting two implementations
agree is *more* code than one implementation, and it only catches drift on the cases the test
enumerates — which is exactly the wrong shape for a matcher whose dangerous inputs are the
ones nobody thought of.

**Precedent.** This is the second exception to "consumer-defined interfaces, no shared
utility packages", after `clock` (§2.5). Both are the same kind of exception and it is worth
naming the kind: a **shared vocabulary that multiple packages must agree on exactly**, where
the agreement is the point and divergence is silent. That is different from a service seam,
where the consumer's needs differ per consumer and an interface is right. If a third
candidate appears, apply that test to it rather than treating two exceptions as permission.

### 2.5 The one place structural typing is not enough

`clock.Clock` **must** be a shared package, not a per-consumer interface, and the reason is
worth understanding because it is the exception that proves the rule above.

A consumer-defined `interface{ Now() time.Time }` works fine — any clock satisfies it
structurally. But the moment the interface has `NewTimer(d) Timer`, the *return type*
becomes part of the method signature. A method returning `coordinator.Timer` does **not**
satisfy an interface requiring `simnet.Timer`, even if the two interfaces are identical:
Go compares the named types, not their shapes, in a return position. So every consumer
duplicating the interface would need a distinct fake per consumer.

Hence one leaf package, `internal/clock`, owning `Clock`, `Timer`, `Ticker`. It sits
alongside `logging` and `policy` as an *ambient capability* (like `*slog.Logger`), not a
service seam.

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

// Height returns the number of hops from name down to its deepest descendant (0 for a
// leaf), or -1 if name is not in the tree. Needed by the backup-legality rule (§3.5),
// which must bound the depth of the WHOLE subtree that moves on a promotion, not just the
// promoted node.
func (t *Topology) Height(name string) int

// Supersedes reports whether t is strictly newer than prev under the lexicographic
// (Epoch, Rev) order. A nil prev is superseded by anything.
//
// ***Supersedes is an ORDERING predicate, NOT an AUTHORIZATION predicate.*** It answers
// "which of these two trees is more recent". It does NOT answer "may I apply this one".
// Those are different questions with different answers, and conflating them is a
// privilege-escalation bug: a peer that stamped Epoch = MaxUint64-1 on a self-computed
// tree would supersede everything, and if Supersedes gated application it would be
// universally obeyed. Authorization is §3.10's Fence.Accept, and ONLY that.
//
// Call sites, exhaustively: the coordinator, to order its own successive trees; the
// dashboard, to detect that a snapshot it holds is stale. NOT the peer's apply path.
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

0. **The FORMER ROOT first, if it is being demoted** (SHIPPED, WI-1). A node that is
   `prev.Root` appears in `prev.Edges` only as a *Parent*, never as a Child — so a plain
   replay of `prev.Edges` never visits it. If the new root differs and the old root is still
   present, it needs a parent like anyone else, and placing it with the newcomers is
   actively harmful: its former children ARE in `prev.Edges` and get processed while their
   incumbent parent is not yet attached, so rank 1 finds `P` unattached, incumbency fails,
   and **the whole former-root subtree scatters** — the opposite of local repair, triggered
   by the single most disruptive event there is. Process it first, at the position
   `prev.Edges` implicitly gives it. (If the old root is also the new root it is seeded at
   depth 0 and this step is a no-op.)
1. **Then incumbents**, in `prev.Edges` order (which §3.1 guarantees is topological, so
   parents are placed before their children — this is why that invariant is load-bearing
   and not decoration). Skip any incumbent not present in `nodes`.
2. **Then newcomers** — nodes in `nodes` with no entry in `prev` — sorted upload
   descending, then name ascending.
3. With `prev == nil`, steps 0 and 1 are empty and this degenerates exactly to Phase 4's
   order.

Consequence: a strong newcomer can no longer displace an incumbent's claim on a parent's
capacity, because the capacity was already spent when the incumbent was placed. It can
still *become* a relay for later newcomers — which is the behaviour we want.

**(B) Parent selection for node `u`** — `bestParent`. Applied strictly in this order:

| Rank | Rule | Kind |
|---:|---|---|
| 0 | **Hard filters.** Candidate `p` must be: already attached in this build; `capacityOf(p, c) > 0` (not TURN-bound, enough derated upload); `depth[p] < c.MaxDepth`; `children[p] < capacityOf(p, c)`; `p != u`. | eliminate |
| 0b | **Impairment filter (SOFT).** Let **`healthyAvailable`** = "at least one candidate surviving rank 0 has `!Impaired`". If `healthyAvailable`, every `p.Impaired` candidate is excluded. If not, impaired candidates are **re-admitted** — degradation is a preference, disconnection is not, and a degraded parent beats no parent. Implement as two passes over the same candidate set, not as a score. | eliminate (soft) |
| 1 | **Incumbency.** If `prev.ParentOf(u) == P` and `P` survived rank 0, then **`P` wins** — unless some surviving candidate `q` satisfies `rtt(u,q) + c.StickinessMs < rtt(u,P)`. Only a *materially* closer parent breaks incumbency; a merely-less-loaded or alphabetically-earlier parent never does. **An impaired incumbent gets no protection — but ONLY when `healthyAvailable`.** See the coupling rule immediately below; this is the single rank that turns a fired dwell timer into an actual re-parent, and it is the reason the dwell is not dead machinery. | prefer |

> **COUPLING RULE (frozen).** Rank 0b's impairment *filter* and rank 1's impairment *void*
> are the same decision and **must read the same `healthyAvailable` flag**. One variable,
> computed once per node placement, consumed by both ranks.
>
> **Why (ruling, §15.9).** WI-1 observed that the rank 1 `!Impaired` clause is unobservable
> in normal fleets — 0b has already removed impaired candidates — and becomes observable in
> exactly one situation: when *every* candidate is impaired and 0b re-admits them all. v2.1
> as written then moved a child from one impaired parent to another marginally-closer
> impaired parent. **That is not intended.**
>
> The void exists for exactly one purpose, stated in its own justification below: *let the
> dwell move children off an impaired relay onto a healthy one.* When no healthy relay
> exists, the clause's entire premise is absent and all that remains is churn — a stream
> interruption bought for an RTT delta, in the one situation where the fleet is already
> degraded and least able to absorb interruptions. Worse, impairment is by definition a
> **sustained** condition, so an all-impaired fleet can stay that way for a long time and
> every recompute would reshuffle children between impaired parents. That is precisely the
> thrash Phase 5 exists to prevent, arriving through the mechanism meant to prevent it.
>
> So: **when there is nowhere healthy to go, stability is the only value left, and
> incumbency is preserved.** WI-1's instinct was right; it built the contract as written and
> flagged it rather than silently "fixing" it, which is exactly the behaviour §0 asks for.
>
> The clause remains observable and testable in its intended case — impaired incumbent,
> healthy candidate with spare capacity, children move — so C8's requirement that the dwell
> not be inert machinery is untouched.
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
- *Voided by impairment*: an incumbent that the control plane has classified impaired after
  a full `DegradationDwell` has, by construction, been bad for ten seconds straight. That is
  the evidence threshold at which stability stops being the right default — so rank 1 steps
  aside and rank 2 moves the children to a healthy relay if one has room.
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
| 0 | Hard: `B != u`, `B != P`, `B ∉ Subtree(P)`, `!B.Impaired`, `capacityOf(B, c) > 0`. | must be a legal, healthy parent that survives P's loss |
| 0b | **Hard depth bound on the WHOLE moving subtree:** `Depth(B) + 1 + Height(u) ≤ c.MaxDepth`. | promoting `u` moves `u` *and everything under it*. `Depth(B) < MaxDepth` only bounds `u`; a relay with children promoted one level deeper pushes its own children past the bound. This was a preference (old rank 2) and had to become a hard filter. |
| 0c | **Hard fan-in bound:** `children[B] + backupLoad[B] < capacityOf(B, c) + BackupOvershootAllowance`, where `backupLoad[B]` counts backups already assigned to `B` in this pass. | see below |
| 1 | `children[B] < capacityOf(B, c)` — **has spare capacity** — preferred over one that does not. | a failover into spare capacity is non-disruptive |
| 2 | `Depth(B) ≤ Depth(P)` — **no deeper than the parent it replaces** — preferred. | quality: keeps the tree shallow even when 0b would permit deeper |
| 3 | Minimum `rtt(u, B)`. | the failover target should also be a good parent |
| 4 | Fewest children, then fewest assigned backups, then name ascending. | balance, then determinism |

**Capacity is NOT reserved for backups, but backup FAN-IN is capped.** The alternative —
carve each relay's child capacity into "primary slots" and "reserved backup slots" — was
rejected because at the scale this system actually runs (4–8 peers on residential upload),
capacity is already the binding constraint; halving usable fan-out to insure against a
failure that triggers one recompute is a bad trade.

But *unbounded* fan-in is a different and worse problem, and the original draft got this
wrong. If every child of relay `P` names the root as its backup, then `P`'s death promotes
**all `|Subtree(P)| − 1` of them simultaneously** onto one node. The overshoot is not "one
child" — it is the size of the failed subtree, exactly when the surviving relays are least
able to absorb it. Rank 0c fixes it by spending a budget across backup assignment:

```go
// BackupOvershootAllowance is how many backup assignments a node may hold BEYOND its computed child
// capacity. 1, so a correlated failure can push any single relay at most one child past its
// budget — which is what makes the "transient one-child overshoot" claim in §13 TRUE rather
// than aspirational.
//
// This is NOT capacity reservation: a relay's primary children are unaffected and it may
// still fill every computed slot. It bounds only the INSURANCE written against that relay.
// The cost is that in a tight fleet some nodes get no backup at all, which is correct and
// visible — a backup that would oversubscribe its target by four is not insurance, it is a
// second outage with extra steps.
const BackupOvershootAllowance = 1
```

The consequence is now honest and genuinely bounded: **at the instant of a failover a relay
may serve at most `BackupOvershootAllowance` children more than its computed capacity**, until the
coordinator's recompute lands. That overshoot exists only in a peer's *realized* state — it
is never encoded in a `Topology`, so `Validate` still holds for everything this package
produces. Nodes for which rank 0c admits no candidate get **no backup**, and the dashboard
says so.

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
6. **Backup legality**: `capacityOf(Backup.Parent, c) > 0`, `!Backup.Parent.Impaired`, and
   the hard depth bound `Depth(Backup.Parent) + 1 + Height(Backup.Node) <= c.MaxDepth`.
7. **Backup fan-in**: for every node `B`, `children[B] + backupLoad[B] <= capacityOf(B, c) + BackupOvershootAllowance`.
8. **Epoch/Rev advance**: when `Validate` is given a `prev` (via `ValidateLocalRepair`, which
   calls into the same helper), `next` must not go backwards in `(Epoch, Rev)`.

Plus a **second, separate oracle** for the churn property — the one that makes "local
repair" testable rather than merely asserted:

```go
// ValidateLocalRepair asserts that next differs from prev only where it had to.
//
// Churn describes what happened between prev and next. It is a struct rather than four
// []string parameters because C8 already added a fourth justification class (impairment)
// and a fifth is plausible; widening a struct is a non-event, widening a signature churns
// every call site.
type Churn struct {
	// Gone departed or were declared gone.
	Gone []string
	// Joined arrived.
	Joined []string
	// Promoted RE-PARENTED THEMSELVES to their backup (§5.6). Without this the oracle
	// contradicts the ratification rule: a peer whose parent was healthy but whose EDGE
	// to it failed shows a changed parent that neither Gone nor Joined can justify, and a
	// correct repair is flagged as a defect.
	Promoted []string
	// Impaired had their dwell timer fire this round (§5.4). Their children lose
	// incumbency protection by design (§3.4 rank 1), so those moves are justified.
	Impaired []string
}

// ValidateLocalRepair asserts that next differs from prev only where it had to.
//
// ***prev MUST BE THE LAST PUBLISHED TREE*** — the tree peers were actually running — NOT
// the coordinator's patched working copy. v2 said the opposite; WI-1 shipped this and is
// right. See §5.6a for the three distinct trees this contract now names, and §15.7 for the
// ruling.
//
// Two reasons, the second decisive:
//
//  1. An oracle fed the patched copy is asserting against the very belief it exists to
//     check. The promotion has already been baked in, so the promoted node shows NO parent
//     change, and the oracle can confirm only that the coordinator believes what it
//     believes.
//  2. THE PUBLISHED TREE IS THE ONLY ARTIFACT THAT STILL CARRIES THE BACKUP ASSIGNMENT the
//     promotion must be checked against. Given the published prev, the oracle can assert
//     that a promoted node moved to *the backup it was actually assigned* —
//     next.ParentOf(u) == prev.BackupOf(u) — which is a real check on the promotion's
//     legitimacy. The patched copy has already overwritten that evidence, so this check is
//     not merely weaker there, it is IMPOSSIBLE.
//
// There is no double-counting with Churn.Promoted: prev supplies the BEFORE state and the
// backup assignment; Promoted supplies the JUSTIFICATION CLASS for a change that gone and
// joined cannot explain. Different roles, both needed.
//
// It takes nodes and Constraints (RULING D) for the same reason Validate does. Minimality is
// defined relative to §3.4's PREFERENCE ORDERING, and rank 2 of that ordering is RTT-aware.
// An oracle that cannot see RTT cannot evaluate rank 2, so it cannot answer its own
// question — it flags a legitimate move onto a materially closer parent as gratuitous. That
// is not a weaker oracle; it is an oracle answering a narrower question than the one it
// claims to. Both oracles now take the same inputs and differ only in whether they see prev.
//
// A node u present in BOTH prev and next whose parent changed P -> Q is JUSTIFIED iff any of:
//
//	1. P is in Churn.Gone, or P is absent from next's node set;
//	2. u is in Churn.Promoted. And when the assigned backup B = prev.BackupOf(u) is still
//	   present and eligible in next, the oracle asserts the STRICT form: Q must EQUAL B —
//	   a promoted node must land on the backup it was given, not on an arbitrary node.
//	   When B is absent or ineligible in next (a correlated failure took the backup too),
//	   the strict form cannot hold and u falls through to the general rules below, so the
//	   oracle stays sound rather than firing falsely on a correct repair;
//	3. P became ineligible in next: over capacity, past MaxDepth, TURN-bound, or Impaired;
//	4. the move is what rank 1 permits: rtt(u,Q) + c.StickinessMs < rtt(u,P). This is the
//	   clause the narrow signature could not express.
//
// LIMIT, stated so nobody over-trusts it: this checks a NECESSARY condition, not a
// sufficient one. It asserts every move was PERMITTED by the ordering; it does not assert
// the result was optimal, and it deliberately does NOT assert the converse (that a node
// which could have improved did move). Greedy makes no such promise — an eligible closer
// parent may have been filled by an earlier node — so asserting it would fail on correct
// output.
//
// This is deliberately a separate function from Validate: Validate answers "is this tree
// legal?", a property of one tree; this answers "was this change minimal?", a property of a
// TRANSITION. Conflating them would make Validate need a prev it does not otherwise want.
func ValidateLocalRepair(prev, next *Topology, nodes []Node, c Constraints, ch Churn) error
```

> **Consequence for the churn suite.** WI-1's interim workaround — splitting the suite so
> RTT-free fleets assert both oracles while RTT-rich fleets assert only `Validate` plus replay
> determinism — was the right call against the narrow signature, but it left the *strongest*
> fleets checked by the *weaker* oracle, which is exactly backwards: the RTT-rich fleets are
> where rank 2 actually fires and where a churn bug would hide. **Collapse the split once the
> signature widens**: every fleet asserts both oracles.

### 3.7 `Node.Provisional` — telling a default apart from a measurement

```go
// Node gains ONE field. Everything else is unchanged.
type Node struct {
	Name       string
	UploadKbps int
	RTT        map[string]float64
	NAT        NATType
	// Impaired marks a node the control plane has classified as SUSTAINED-DEGRADED — its
	// degradation dwell timer fired (§5.4). It is NOT the same as coordinator.HealthDegraded,
	// which is a missed-heartbeat classification and is advisory only; the two were
	// deliberately given different names because they have different consequences.
	//
	// An Impaired node KEEPS its existing children (evicting them is the interruption we
	// are trying to avoid) but is disqualified from taking NEW ones, disqualified as root,
	// and — critically — LOSES INCUMBENCY PROTECTION over the children it has (§3.4 rank 1).
	// That last clause is the whole point: it is what makes a sustained-degradation event
	// produce a materially different tree instead of a byte-identical one.
	Impaired bool
	// LossPct is the node's measured uplink packet loss, 0-100. It DERATES the node's
	// usable upload, because retransmits and repair traffic genuinely consume the budget a
	// relay would otherwise spend on children. See effectiveUploadKbps.
	LossPct float64
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

// effectiveUploadKbps is the upload budget a node can actually spend on children:
// its advertised budget derated by measured loss. Capacity is floor(effective/StreamKbps).
//
//	effective = UploadKbps * (1 - min(LossPct, MaxLossDeratePct)/100)
//
// The derate is capped rather than unbounded: past MaxLossDeratePct the link is broken,
// not merely lossy, and the Impaired flag (a discrete decision made by the control plane
// after a DWELL) is the right instrument for that — not a continuous term that would let a
// single bad sample halve a relay's capacity and re-parent its children instantly. Named
// so a maintainer does not "fix" the cap.
func effectiveUploadKbps(n Node) int

// MaxLossDeratePct caps how much measured loss may shrink a node's advertised upload.
// 50%: a link losing half its packets cannot usefully relay anything, so there is nothing
// below this worth discriminating; the Impaired flag handles the rest.
const MaxLossDeratePct = 50.0
```

**How C8 is closed.** Before this amendment, `overlay.Node` carried no health input at all
and none of §3.4's ranks read one, so a sustained-degradation threshold event recomputed to a
byte-identical tree: `DegradationDwell`, the three threshold constants, `-dwell`, and the
"test both extremes" obligation were all dead machinery. The two fields above are the
minimum that makes the feature real, and each has a *stated effect on a specific rank*:
`Impaired` gates rank 0 and voids rank 1; `LossPct` moves `capacityOf`. If a future
amendment removes both, it must also remove the dwell — **inert control machinery is worse
than no control machinery, because it reads as a working safety property.**

### 3.8 `PickRoot` — sticky, and never provisional

Root churn is the single most expensive thing this control plane can do: with a
stability-preserving builder (§3.4), a *changed root* invalidates every parent choice in the
tree at once — the exact opposite of the "move one subtree, not the world" property Phase 5
exists to provide. So root selection gets its own stability rule, stronger than the
per-node one.

```go
// PickRoot chooses the tree's root.
//
// ELIGIBILITY (RULING E — streamKbps is why this parameter exists). A node is eligible iff
// ALL of: not Provisional, not Impaired, not NATRelayed, and it can serve AT LEAST ONE
// child — effectiveUploadKbps(n) / streamKbps >= 1.
//
// v2's first draft approximated "can parent" as UploadKbps > 0, which closes only HALF the
// live root-flap defect. Provisional closes the "rooted a guess" half in every arrival
// order — that part works. But a node reporting a REAL 1200 kbit/s against a 2000 kbit/s
// stream cost genuinely cannot parent anyone, and without streamKbps PickRoot cannot see
// that: it returns the unfit node and BuildTree then fails with "root cannot serve any
// children". That surfaces as an honest `unbuildable` over real telemetry rather than an
// arrival-order artifact, and §5.9's settle suppresses it in practice — but suppressed is
// not closed, and a defect that only shows up under load is the kind this contract exists
// to prevent. One scalar closes it.
//
// Three rules, in order:
//
//  1. Only ELIGIBLE nodes are considered, per above.
//  2. KEEP THE INCUMBENT. If prev is non-nil and prev.Root is present AND STILL ELIGIBLE
//     (the same test — an incumbent that has become unfit is not defended), it is retained
//     unless some eligible challenger advertises at least RootChangeMarginKbps MORE upload.
//  3. Otherwise: highest-upload eligible node, ties broken by name.
//
// Returns "" when no eligible node exists — a genuinely unrootable fleet, which the
// coordinator reports as `unbuildable` (§5.9) and which the prev=nil retry (§5.9a) cannot
// rescue, because dropping the stability preference does not create upload capacity.
//
// MaxDepth is deliberately NOT a parameter: the root sits at depth 0 and the depth bound
// constrains its descendants, not its own eligibility. Passing the whole Constraints was
// rejected for a sharper reason — Constraints.Root is the very thing PickRoot computes, so
// a Constraints parameter is self-referential and invites a caller to pass a stale root and
// quietly get it back. A scalar cannot be misused that way.
//
// A lone node is returned as-is (a one-node tree is trivially valid) even if ineligible;
// MinBuildableMembers (§5.9) means the coordinator never reaches that path, and it exists
// for simnet and unit tests.
func PickRoot(nodes []Node, prev *Topology, streamKbps int) string

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
current topology and their `StreamKbps`; both pass `nil` for `prev` on the first build.

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
// StaticEpoch is the epoch stamped on a hand-authored topology file: a SENTINEL marking
// "this tree came from an operator, not from a coordinator".
//
// CORRECTED RATIONALE (v2). The original draft justified the max-uint64 value by claiming
// no coordinator push could ever "supersede" it. That reasoning was wrong, because it read
// Supersedes as an authorization predicate — the same conflation C1 fixes in §3.2. What
// actually protects a static peer is STRUCTURAL and has nothing to do with this constant:
// a static peer is not managed, so Router.applyTopology returns early on every pushed
// topology, and it never adopts an arbiter announcement, so its Fence stays at the zero
// value and Fence.Accept rejects everything. The file cannot be overridden because the
// code path that would override it does not run.
//
// The value is kept at max-uint64 for three smaller, honest reasons: it satisfies
// Validate's Epoch >= 1 check; it is instantly recognisable in a log line or a dashboard
// cell as "operator-authored" rather than looking like a plausible term number; and if a
// static tree is ever ORDERED against a computed one (Supersedes, legitimately, for
// display), the operator's file sorts newest, which is the right display default.
const StaticEpoch uint64 = ^uint64(0)
```

Note the existing guard already stops the two modes combining: `cmd/peer` rejects
`-topology` together with `-managed`.

### 3.10 `Fence` — the AUTHORIZATION predicate (C1)

`Supersedes` orders trees. `Fence` decides whether one may be applied. They are different
questions and the original draft answered both with the first, which is a
privilege-escalation bug: §3.2 mandated `Supersedes` as "the ONE place the fencing
comparison is written" while §6.5 required rejecting a *higher* epoch, so as literally
written any peer that stamped `Epoch = MaxUint64-1` on a self-computed tree would have been
universally obeyed.

`Fence` lives in `overlay` because it is pure logic over strings and integers — no sockets,
no pion — so the fencing rules are table-testable, and because both `media` (the peer side)
and `coordinator` (which must reason about what peers will accept) need it.

```go
// Fence is a peer's view of control-plane AUTHORITY: which epoch the arbiter last told it
// is current, who the arbiter named coordinator for that epoch, and the highest revision it
// has applied. A plain value with pure methods, so every fencing rule in §6.5 is a
// table-driven test with no sockets and no media.
//
// The zero Fence is the UNAUTHORISED state: Epoch 0, no coordinator, Rev 0. Accept returns
// false for everything until an announcement is adopted. That is deliberate — a peer that
// has not been told who is in charge obeys nobody.
type Fence struct {
	Epoch         uint64
	CoordinatorID string
	Rev           uint64
}

// AdoptAnnouncement applies an arbiter announcement. It is the ONLY method that may raise
// Epoch, which is the entire mechanism of §6.7's safety argument. Returns true if adopted
// (a.Epoch > f.Epoch), in which case Rev resets to 0 and CoordinatorID is replaced; false
// if the announcement is stale or a duplicate, in which case f is untouched.
func (f *Fence) AdoptAnnouncement(epoch uint64, coordinatorID string) bool

// Reset returns f to the unauthorised zero state. Called on TypeJoined and ONLY there
// (§5.7 rule 6) — it is what closes the arbiter-restart hole in §6.7.
func (f *Fence) Reset()

// Accept decides whether a pushed topology may be applied, and if not, why. reason is a
// short stable string for the stale_rejected dashboard event and the debug log; it is
// never parsed.
//
// Accepts iff ALL of:
//   - f.Epoch != 0            (this peer has been told who is in charge)
//   - from == f.CoordinatorID (the sender is that node; From is server-stamped)
//   - t.Epoch == f.Epoch      (EXACT match — a HIGHER epoch is REJECTED, see §6.5)
//   - t.Rev > f.Rev           (strictly newer within the term)
//
// Note the third clause is equality, not >=. Rejecting a higher epoch is the asymmetry
// that makes the fence mean anything: a peer may learn about a new coordinator only from
// the arbiter, never from the node claiming the job.
func (f *Fence) Accept(from string, t *Topology) (ok bool, reason string)

// Applied records a successful application, advancing Rev. Separate from Accept so the
// caller can Accept, attempt the (fallible) media work, and advance only on success —
// which means a failed apply is retried by the next push rather than silently skipped.
func (f *Fence) Applied(t *Topology)
```

**Where each predicate is used, exhaustively:**

| Predicate | Used by | For |
|---|---|---|
| `Topology.Supersedes` | `coordinator` (ordering its own trees), `dashboard` (is my snapshot stale?) | **ordering only** |
| `Fence.Accept` | `media.Router.applyTopology`, and nowhere else | **authorization** |
| `Fence.AdoptAnnouncement` | `cmd/peer` on `TypeCoordinator` | raising authority |
| `Fence.Reset` | `cmd/peer` on `TypeJoined` | dropping authority |

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

**RULING (requested by WI-0): `NewHubWithConfig` returns an error.** SIGNATURE CHANGE from
what WI-0 shipped:

```go
func NewHub(log *slog.Logger) *Hub                        // unchanged, cannot fail
func NewHubWithConfig(cfg HubConfig) (*Hub, error)        // CHANGED: was (*Hub)
```

WI-0 correctly flagged that the shipped signature leaves `panic` as the only way to report a
self-contradictory config (`PingTimeout >= PingInterval`, an unparseable origin pattern). A
panic would be defensible for a pure programmer error — but **these values are operator
input.** They arrive from `-allowed-origins`, and the ping relationship is constrained against
`-gone-after` by `ValidateLivenessBudget`, which is itself flag-driven. The project's rule is
errors-as-values with fail-loud-at-startup translated by `run() error` into
`server: <message>` on stderr; a panic there produces a stack trace where the user needs one
sentence naming the flag they got wrong.

`NewHub(log)` keeps its non-erroring signature because its defaults are compile-time
constants that are known-good and are covered by a test asserting
`ValidateLivenessBudget(HeartbeatInterval, WSLivenessBudget) == nil`. A constructor that
cannot fail should not pretend it can.

Rejected alternatives, all of which are common and all of which lose:

| Alternative | Why it lost |
|---|---|
| Package-level `var now = time.Now`, swapped in tests | A global. Two parallel tests swapping it race; `go test -race` will not necessarily catch it, and it makes "which clock is this code on" invisible at the call site. |
| Carry the clock on `context.Context` | `context.Value` for a *dependency* is a documented Go anti-pattern: untyped, invisible to the compiler, and it makes every function that might need time take a ctx it otherwise wouldn't. Context is for cancellation and deadlines, not DI. |
| Pass `clock.Clock` as a parameter to every call site | Churns dozens of signatures for a value that is fixed for the lifetime of the object. The struct field is the same injection with none of the noise. |
| Define the clock interface per-consumer | Breaks on the `NewTimer` return type — see §2.5. |

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

// Advance moves virtual time forward by d, firing due timers and tickers ONE AT A TIME in
// (deadline, creation-sequence) order, invoking the barrier installed by SetBarrier
// BETWEEN consecutive firings.
//
// The one-at-a-time + barrier rule is load-bearing and was missing from v1. An Advance
// spanning several deadlines that fired them all before any handler ran would let a timer
// RE-ARMED by an earlier handler (which is exactly what a dwell reset does) take its
// position in the queue from real goroutine scheduling rather than from virtual time —
// reintroducing the nondeterminism the whole harness exists to remove, in the one mechanism
// the harness exists to test.
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

// SetBarrier installs the between-firings quiescence barrier. Scenario wires it to Settle.
// A nil barrier means "fire without settling", which is correct only for a clock driving no
// concurrent control loop (a pure unit test) and is NEVER correct in a Scenario.
func (c *VirtualClock) SetBarrier(fn func())

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
// It exists to DRIVE different event sequences deterministically, so §12.3 can assert what
// is actually true of each of them.
//
// It does NOT exist to assert that all orders converge to the same tree. THEY DO NOT, and
// v1 was wrong to claim they must — see §12.3. A stability-preserving builder is
// path-dependent by construction; that path-dependence is the thing we BOUGHT minimal
// disruption with. What ReportOrder buys is the ability to enumerate histories and assert
// the properties that survive: validity, bounded delta, and determinism per sequence.
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

// Report gains ONE field (M12). Every existing Report field and JSON key is unchanged:
//
//	// Coordinatable declares whether this peer is WILLING to be elected coordinator (the
//	// -coordinatable flag; a laptop on battery says false). A hard disqualifier in
//	// arbiter.Score, and the field that makes that flag implementable at all — v1 froze
//	// -coordinatable with no field to carry it and no import path to read one.
//	//
//	// Note the JSON zero value: absent ⇒ false. cmd/peer therefore ALWAYS emits it
//	// explicitly even though the flag defaults to true, so a peer running an older build
//	// is conservatively treated as unwilling rather than silently elected.
//	Coordinatable bool `json:"coordinatable"`

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
	// Children lists each realized downstream neighbour and its edge state, ASCENDING BY
	// NAME. A slice rather than v1's map[string]string because §6.6 reconstructs a
	// stickiness baseline from these frames, and a baseline rebuilt from an unordered
	// collection is not reproducible — the same defect class as Topology.Edges' ordering
	// invariant. Absent on a leaf. SHIPPED as []ChildLink (WI-0); the element type name is
	// ChildLink, not this document's first draft ChildState.
	Children []ChildLink `json:"children,omitempty"`
	// IntervalMs is the cadence THIS peer beats at (SHIPPED, WI-0). Read it through
	// Heartbeat.Interval(), never directly — see the cadence rule in §5.3.
	IntervalMs uint64 `json:"interval_ms,omitempty"`
}

// ChildLink is one realized downstream edge: the neighbour's name and the pion
// PeerConnectionState string of the edge to it. (SHIPPED, WI-0.)
type ChildLink struct {
	Name  string `json:"name"`
	State string `json:"state"`
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
   gone     → EXCLUDED from the tree projection; a THRESHOLD EVENT; triggers local repair.
              The record is NOT deleted (see resurrection, below).
```

**Resurrection — `gone` is not ejection (CORRECTED, see §15/C4).** v1 deleted a `gone`
node's record and relied on `TypeJoined` to re-admit it. `TypeJoined` only fires on a fresh
socket, so a peer whose Wi-Fi roamed for 9 s was removed from the tree, healed, and then
heartbeat forever into a coordinator that had no record of it — permanently dark, while the
dashboard cheerfully showed the meet healthy. The frame arriving *is itself* proof the
socket is live, so:

> **RESURRECTION RULE (frozen).** A `heartbeat`, `metrics`, or `reparented` frame from a
> peer id that is unknown-or-`gone` **resurrects** it: the record is created if absent
> (name taken from the frame body, id from the server stamp), `Health` returns to
> `healthy`, and it counts as a **join** for threshold purposes (§5.4 row 1) — including
> arming the join settle, since a returning peer is exactly a membership-growth episode.
>
> Consequently `gone` **never deletes** a node's record. Only `PeerLeft` (the socket
> actually closed) deletes. `gone` sets `Health = HealthGone` and excludes the node from
> `overlayNodes`; everything else about the record survives so the return is cheap.

**This is a two-party contract and WI-0 shipped only the Hub's half.** The Hub holds no
health state at all — by design — so it invokes `Observer.Heartbeat` for **every frame on a
live socket, unconditionally**, regardless of what the control plane currently believes about
that peer. That is what makes "a heartbeat arrived" usable as proof of return.

**The coordinator MUST implement the other half, and it is the half that does the work:**

| Observed | Coordinator's obligation |
|---|---|
| `Heartbeat`/`Metrics`/`Reparented` for a peer whose record is `HealthGone` | Set `Health = HealthHealthy`, re-include in `overlayNodes`, treat as a **join** threshold event (arms the join settle, §5.9), publish `EventMember{Present:true}` and `EventHealth`. |
| …for a peer with **no record at all** (deleted by `PeerLeft`, or this coordinator was just elected) | **Create the record** — name from the frame body, id from the server stamp — then exactly as above. Do NOT drop it as "unknown peer", which is what Phase 4's `evReport` handler does today and which is precisely the C4 bug. |
| …for a peer whose record is `healthy` | Refresh `lastBeat`/`Seq`. Not a threshold event. |

The Phase 4 code this replaces is the `if ns == nil { return }` early-out in the `evReport`
branch. That line is correct for Phase 4 and is a permanent blackhole from Phase 5 on; deleting
it is part of WI-3.

**Reconciling the two detectors.** The two liveness levels must not be able to disagree by
much, or the system holds contradictory beliefs for the difference. v1 had `GoneAfter` = 8 s
against a worst-case socket detection of `WSPingInterval + WSPingTimeout` = 20 s, i.e. a
12-second window in which a peer was simultaneously "gone" to the coordinator and "present"
to the Hub. The constants are re-tuned so the two agree within one ping interval:

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

// SHIPPED (WI-0) — and in a better shape than this document first specified. These are
// FUNCTIONS of the peer's DECLARED cadence, in package metrics, not constants in
// coordinator:
//
//	func metrics.DegradedAfter(interval time.Duration) time.Duration  // 3 missed beats
//	func metrics.GoneAfter(interval time.Duration) time.Duration      // 8 missed beats
//
// v1 hardcoded `3 * metrics.HeartbeatInterval`, which is wrong the moment a peer runs with
// `-heartbeat 2s`: its thresholds would be computed against a cadence it is not using, so a
// perfectly healthy slow-beating peer is declared gone every time. The shipped design reads
// the cadence the peer DECLARED on the wire (Heartbeat.IntervalMs, via Heartbeat.Interval())
// and multiplies that. The coordinator therefore stores a per-node cadence and evaluates
// per-node thresholds. The multipliers (3 and 8) and their justifications below are
// unchanged; only what they multiply is.
//
// DegradedAfter = 3 missed beats. Three rather than one because a single missed beat is a GC
// pause, a Wi-Fi retransmit, or a scheduler hiccup — not a failure. Three consecutive misses
// is a signal. Nothing acts on this transition; it only colours the dashboard and arms the
// degradation dwell (§5.4).

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
//
// GoneAfter = 8 missed beats, via metrics.GoneAfter(interval).

// WSPingInterval / WSPingTimeout bound the signaling.Hub's control-link liveness check.
// Worst-case socket-death detection is their sum, 8s — deliberately EQUAL to GoneAfter, so
// the health FSM and the Hub cannot disagree about a peer by more than one ping interval.
//
// v1 used 15s/5s on the reasoning that the ping doubles as a NAT keepalive. That reasoning
// was wrong here: application heartbeats already flow at 1 Hz, so the socket is never idle
// and needs no keepalive. The ping's ONLY job is to reap a socket whose peer has vanished
// without a FIN, and it should do that on the same timescale the health FSM uses.
//
// SHIPPED (WI-0) at 5s/2s, not the 5s/3s this document first wrote — 2s keeps a strict
// margin below the interval so consecutive pings can never overlap.
const WSPingInterval = 5 * time.Second
const WSPingTimeout  = 2 * time.Second

// WSLivenessBudget is worst-case socket-death detection: a peer can die immediately after a
// successful pong, so the Hub notices one full interval plus one timeout later.
const WSLivenessBudget = WSPingInterval + WSPingTimeout // 7s
```

**The liveness-budget invariant (REQUIRED, enforced at startup).** v1 reconciled the two
detectors by picking numbers that happened to agree. That is not a guarantee — any later edit
to any one of the three constants silently reopens the C4 ejection window, and nothing would
fail. WI-0 shipped the enforcement, and it is hereby frozen as an invariant of this contract:

> **`WSPingInterval + WSPingTimeout` ≤ `GoneAfter(interval)`**, for every cadence a peer may
> declare. Checked by `metrics.ValidateLivenessBudget(interval, socketDetection) error` and
> called from `cmd/server`'s startup validation, so a violating configuration fails loud with
> a message instead of producing a peer that is "gone" to the coordinator and "present" to
> the Hub for the difference.

```go
// (for reference; SHIPPED in metrics)
func ValidateLivenessBudget(interval, socketDetection time.Duration) error
```

**Startup validation is necessary but NOT sufficient, and v2 missed why.** The server can
only check the invariant against the *default* cadence, because `GoneAfter` is a function of
a cadence each **peer** declares at run time (`Heartbeat.IntervalMs`). A peer declaring a
FASTER cadence shrinks its own `GoneAfter` — at `-heartbeat 500ms` it is 4 s, well below the
7 s `WSLivenessBudget` — and reopens the exact C4 window this invariant exists to close.
**The peer, not the operator, breaks it**, so no amount of flag validation can catch it.

> **FROZEN: the coordinator floors every per-node gone threshold at the socket-detection
> budget.**
>
> ```
> goneThreshold(node) = max(metrics.GoneAfter(node.declaredInterval), cfg.SocketDetection)
> ```
>
> `coordinator.Config.SocketDetection` is a new field, wired in `cmd/server/main.go` from
> `hub.LivenessBudget()`. The max makes the invariant hold **unconditionally and per node**,
> with no rejection of the peer, no clamping of what it may declare, and no new API: a
> fast-beating peer simply gets the same floor everyone else effectively has. Zero (the
> field's zero value) disables the floor, which is what `simnet` wants — it drives virtual
> time and models no socket layer at all.

Why the inequality is in this direction: the health FSM must be the *slower* of the two, so
that whenever the coordinator declares a peer gone, the Hub has either already reaped the
socket (a real death — the peer is not coming back without a rejoin) or the socket is
genuinely live (a transient — and then §5.3's resurrection rule applies and the peer is
re-admitted by its next beat). Reversing it produces the v1 bug: a peer ejected from the tree
while the Hub still considers it present, with no event able to bring it back.

```go
// (remaining §5 constants)
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
// WHY 5s specifically (v1 justified HAVING a cooldown and never its value): it is bounded
// below by the cost of the thing it suppresses and above by the joiner's patience.
//
// Below: a rebuild that changes an edge costs that edge a re-negotiation plus a keyframe
// wait — call it 1-2s of degraded video. Two rebuilds inside that window would overlap,
// so the floor is ~2s or the cooldown does not actually prevent the harm.
// Above: the most common threshold event is a join, and 5s of black screen is well past
// what a user tolerates. That is why JOINS BYPASS the cooldown entirely (§5.9 rule 4) and
// are bounded by JoinSettle instead — which is what lets this constant be tuned for the
// churn case (leave / gone / dwell) without being hostage to the join case.
// Within 2-8s the choice is not sharp; 5s sits in the middle, is a round number in logs,
// and is a flag (-recompute-cooldown) precisely because it is not a derived value.
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
| 8 | A meet's **join settle timer** fires (§5.9) | `JoinSettle` timer |

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
3. Otherwise derive the **`working`** copy (§5.6a) — `published` with `X` and every edge
   touching `X` removed. `Root`, `Epoch`, and the surviving `Edges`' order are carried over.
4. Call `BuildTree(nodesWithoutX, working, cons)` with `Rev = published.Rev + 1`, subject to
   the two-attempt rule in §5.9a.
5. Assert `overlay.Validate(next, nodes, cons)` and
   `overlay.ValidateLocalRepair(published, next, nodes, cons, overlay.Churn{Gone: []string{X}})`
   in tests — **`published`, never `working`** (§5.6a). In production, a `Validate` failure
   means **keep the previous tree and log an error**; never publish a tree that fails its own
   oracle.

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
3. **`u` clears its own local backup immediately on promotion.** The pushed
   `Topology.Backups` still names the node `u` just moved *to* — it was computed before the
   failure — so a naive retry re-attempts the parent it already has. Clearing locally means a
   second failure reports `OK: false` at once instead of burning another
   `ReparentConnectTimeout` against its own current parent. `u` regains a backup on the
   coordinator's next push, which step 5's ratification triggers.
4. **`OK: true` requires MEDIA, not just ICE.** `u` reports success only when the new
   parent's session is `Connected` **and** at least one remote track has arrived from it
   within `ReparentMediaTimeout`. Otherwise it reports `OK: false`.

   Reaching `Connected` to backup `B` proves `u` can reach `B`. It proves nothing about
   whether `B` is still attached to the root. v1 ratified on `Connected` alone, so a
   correlated failure (where `B` was orphaned by the same event) produced a coordinator that
   ratified and then *stickily defended* an edge carrying no media — the worst outcome
   available, because stickiness exists to protect working edges and would now be protecting
   a dead one. An arrived track is cheap, immediate evidence that the path works end to end.
5. If there is no backup, or the backup fails either check, `u` sends
   `Reparented{From: oldParent, OK: false}` and waits for a push.

The coordinator ratifies **only `OK: true`** — which now means media actually flowed. An
`OK: false` is a stranded peer, handled by the urgent recompute below.

The coordinator's response is the rule that keeps this from thrashing:

> **RATIFICATION RULE (frozen).** On a successful `Reparented`, the coordinator patches its
> **working copy** so that `u`'s parent is the backup `u` actually chose, **and then** calls
> `BuildTree` with that patched working copy. It does not build from the pre-failure tree.
>
> This is about **`BuildTree`'s input only.** It is NOT about the oracle's input — see
> §5.6a, which exists because v2 conflated the two.

Why this matters: if the coordinator rebuilt from the old tree, stickiness would see `u`'s
incumbent parent as the *dead* one, find it ineligible, and re-choose freely — quite
possibly moving `u` a second time, to a parent that is no better than the backup it already
connected to. That is two interruptions where one was needed. By patching first, `u`'s new
parent *becomes* the incumbent, and stickiness protects it. The coordinator ratifies the
peer's local decision rather than fighting it.

#### 5.6a The three trees the coordinator holds — keep them distinct

v2 said "`prev` is the patched tree" in one place about `BuildTree` and in another about
`ValidateLocalRepair`, and they are **different trees**. WI-3 must keep three references per
meet, and confusing any two produces a silent correctness bug rather than a compile error:

| Name | What it is | Who consumes it |
|---|---|---|
| **`published`** | The last tree actually sent to peers. Carries the `Epoch`/`Rev` peers are fencing against and — load-bearing — the `Backups` assignment they promoted under. | `ValidateLocalRepair`'s `prev`; the dashboard snapshot; `Fence` reasoning |
| **`working`** | `published`, patched with everything the coordinator already knows changed: every ratified promotion (§5.6) **and** the removal of every departed/`gone` node and its edges (§5.5). Rebuilt from `published` each round; never sent anywhere. | `BuildTree`'s `prev` |
| **`next`** | `BuildTree`'s output for this round. | published on success, and then **becomes `published`** |

So one recompute round is:

```
working := patch(published, ratifiedPromotions, departures)
next    := BuildTree(nodes, working, cons)          // sticky attempt, §5.9a
_       =  Validate(next, nodes, cons)              // gate
_       =  ValidateLocalRepair(published, next, nodes, cons, churn)   // NOT working
publish(next); published = next; ratifiedPromotions = nil
```

The asymmetry is the point and is worth stating in one line: **the builder should be told
what the peers have already done, so stickiness protects it; the oracle should be told what
the peers were last *instructed* to do, so it can check that what they did was allowed.**

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
	JoinSettle        time.Duration // 0 ⇒ JoinSettle (§5.9)
	// SocketDetection is the Hub's worst-case socket-death detection window
	// (hub.LivenessBudget(); 7s by default). Every per-node gone threshold is FLOORED at
	// it, so a fast-beating peer cannot shrink its own threshold below the window in which
	// the Hub may still consider it present (§5.3). 0 disables the floor — correct only
	// for simnet, which models no socket layer.
	SocketDetection   time.Duration
	Clock             clock.Clock   // nil ⇒ clock.System()
	// SelfName is the coordinator's own peer name. Empty means the coordinator is
	// running inside the arbiter process (Phase 5) rather than on an elected peer.
	SelfName string
}

// Sender is consumer-defined here so this package never imports signaling. cmd/server
// adapts it to hub.SendTo.
//
// ***SendTopology MUST NOT BLOCK and MUST NOT perform network I/O on the calling
// goroutine.*** It is called from the coordinator's single Run goroutine, once per member,
// inside recompute. A blocking implementation would let ONE stalled peer stall every meet
// hosted by that process — and because Sync (the test barrier) rides the same loop, it
// would surface as an unreproducible test flake rather than as the availability bug it is.
//
// The existing hubSender adapter already satisfies this: hub.SendTo -> member.send writes
// to a 32-deep buffered channel and DROPS on full rather than blocking. This paragraph
// freezes that as a requirement on the interface so a future adapter (an HTTP push, a
// retrying sender) cannot quietly reintroduce the stall. An implementation that must do
// real I/O enqueues and returns.
//
// A returned error is transient and never fatal (the peer may have just left).
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

// SetEpoch tells the coordinator which term it is serving FOR ONE MEET. The roomID
// parameter is not optional and was missing in v1: Epoch is minted per meet (§5.7 rule 1),
// so a process-global setter would stamp both meets of a two-meet server with whichever
// epoch was set last — silently mis-fencing every peer in the other meet.
//
// Phase 5: called once per meet with 1. Phase 6: called on every arbiter announcement
// naming this node for that meet; a call with a HIGHER epoch resets that meet's Rev to 0
// and puts it in the rebuild window (§6.6). A call naming a DIFFERENT coordinator is not
// how demotion happens — see Yield.
func (c *Coordinator) SetEpoch(roomID string, epoch uint64)

// SetRoster reconciles this coordinator's membership for roomID against the arbiter's
// authoritative roster (§8, TypeMembership). Members in the roster but unknown here are
// added as provisional; members known here but absent from the roster are treated as left.
// It is the seam that makes §6.6 step 3 ("every member the arbiter says is present")
// reachable from an ELECTED PEER coordinator, which in v1 it was not.
//
// Member is coordinator-owned precisely so this method adds no import: cmd/peer translates
// []signaling.Peer into []Member at the edge.
func (c *Coordinator) SetRoster(roomID string, members []Member)

// Member is one meet participant as the coordinator's membership seam sees it.
type Member struct {
	ID   string
	Name string
}

// Yield stops the coordinator from publishing anything further for roomID: it is no
// longer the coordinator there. Idempotent. Phase 6 (§6.6).
func (c *Coordinator) Yield(roomID string, newEpoch uint64)
```

`New` gains the publisher:

```go
func New(log *slog.Logger, cfg Config, send Sender, pub Publisher) *Coordinator
```

### 5.9 The join settle rule, and the three build outcomes (CORRECTED — see §15/C2)

**v1's settle collapsed for the first joiner and was therefore dead for the whole meet.**
Eligibility rule (a) — "every member the coordinator knows about has reported" — is
trivially satisfied by the *first* member: it is the only one known, it reports immediately,
so a 1-node tree published in milliseconds. And because v1 said the settle "never applies
again" once a first tree exists, it was then permanently disarmed. The result was exactly
option (c)-alone, which §5.9 itself had rejected — plus sticky `PickRoot` entrenching
whoever happened to join first as root forever.

The fix is to arm the settle per **membership-growth episode**, not once per meet lifetime,
and to refuse to spend it on a meet too small to have a tree.

**The rule (frozen).**

> 1. **Every join arms the settle**: `settleUntil = now + JoinSettle`. A burst of joins
>    keeps pushing it out, so the burst coalesces into ONE build. Only joins arm it —
>    leave, gone, dwell, reparent, and the settle timer itself must act now.
> 2. **A meet with fewer than `MinBuildableMembers` named members builds nothing and does
>    not consume the settle.** A 1-node tree has no edges, carries no media, and teaches the
>    stickiness baseline a root it should never have been allowed to pick.
> 3. **A meet is eligible to build** when `len(members) >= MinBuildableMembers` AND either
>    (a) every known named member has delivered at least one metrics report, or (b)
>    `now >= settleUntil`.
> 4. **A join BYPASSES `RecomputeCooldown`** but is subject to the settle. So a joiner waits
>    at most `JoinSettle` for media, never up to `RecomputeCooldown`. (This also closes the
>    "no cooldown bypass for an unplaced joiner" finding — joining is the most common user
>    action and must not cost 5 s of black screen.)
> 5. While ineligible, threshold events update state and publish `settling`. No tree is
>    built and — importantly — **`prev` is not touched**, so an ineligible period cannot
>    entrench anything.

```go
// JoinSettle bounds how long the coordinator waits after a join for the fleet's telemetry
// before building. RENAMED from v1's FirstBuildSettle: the v1 name encoded the bug, because
// the settle is emphatically NOT once-per-meet.
//
// 1500ms. metrics.Reporter emits its first report IMMEDIATELY on Run (the deliberate
// "immediate-then-ticker" shape from Phase 4), so a healthy peer's first report lands within
// roughly one round trip of its join. 1.5s is ~10x that even on a poor WAN path and is
// imperceptible as a join latency. A silent peer past the window is attached as a
// provisional leaf and refined by whatever it eventually says; it can never stall the meet,
// which is the bound that rule (a)-alone lacks.
const JoinSettle = 1500 * time.Millisecond

// MinBuildableMembers is the smallest meet worth computing a tree for.
//
// 2, because a 1-member tree is not a tree: zero edges, zero media, and — the reason this
// is a correctness constant and not a micro-optimisation — publishing it would seed the
// stickiness baseline (prev) with a root chosen from a fleet of one. Sticky PickRoot would
// then defend that root against every later arrival inside RootChangeMarginKbps. A meet's
// root must never be decided by who dialled in first.
const MinBuildableMembers = 2
```

**Why this combination, and what was rejected:**

| Option | Verdict |
|---|---|
| **(a) Wait until every known member has reported** | **Rejected alone.** Unbounded: one peer that joins and never reports (a wedged process, a peer that joined without `-managed`, a lost frame) hangs the meet forever with no tree and no media. A liveness rule must never depend on an unreliable party doing something. |
| **(b) Bounded settle window after the first join** | **Rejected alone.** It bounds the wait, but it does not prevent the bad outcome *inside* the window: if the strong peer is still silent when the window closes, a weak-but-reported peer is still the best-looking candidate and still gets rooted. The window shrinks the race; it does not remove it. |
| **(c) Build immediately, never promote an unreported node to root** | **Rejected alone.** It removes the root flap (which is the dangerous part) but still publishes a tree computed over a mostly-provisional fleet, which is then rebuilt seconds later as the reports land — two visible reconfigurations where one would do, at exactly the moment the user is watching the meeting start. |
| **(c) as an invariant + (b) as a RE-ARMING gate + (a) as the early exit + a minimum size** | **CHOSEN.** (c) makes a provisional root *impossible* rather than merely unlikely — a structural guard, not a timing one. (b), **re-armed on every join**, supplies the bound (a) lacks *and* coalesces join bursts. (a) supplies the fast path so the common case costs nothing. `MinBuildableMembers` closes the degenerate case that broke v1: without it, rules (a)+(b) are both trivially satisfied by a meet of one. Each mechanism covers exactly the hole the others leave — and v1 shipped three of the four. |

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
	// OutcomeSettling: the meet has not met the §5.9 eligibility rule yet — too few
	// members, or a join settle still running. Entirely normal, lasts at most JoinSettle
	// per join episode (unbounded only while a meet has one member). Log: DEBUG, never
	// Warn — a warning that fires on every healthy startup teaches operators to ignore
	// warnings.
	OutcomeSettling BuildOutcome = "settling"
	// OutcomeRelaxed: the sticky build FAILED but a from-scratch build (prev = nil)
	// succeeded, so a tree WAS published — at the cost of dropping the stability
	// preference for this round. Expect a large re-parent. Log: Warn (it is rare,
	// expensive, and worth seeing), but it is a SUCCESS, not a fault. See §5.9a.
	OutcomeRelaxed BuildOutcome = "relaxed"
	// OutcomeUnbuildable: no tree exists for this fleet EVEN WITHOUT the stability
	// preference — PickRoot found no eligible root, or both build attempts failed. This
	// is a real, operator-actionable condition (not enough aggregate upload for this many
	// peers, or a depth bound too shallow) and the previous tree is retained. Log: Warn.
	//
	// Note how much stronger this signal is than v1's: because §5.9a requires the relaxed
	// retry first, `unbuildable` now means "genuinely over-constrained", never "stickiness
	// painted us into a corner".
	OutcomeUnbuildable BuildOutcome = "unbuildable"
)
```

### 5.9a The mandatory relaxed retry (RULING F)

**Stickiness can make a satisfiable fleet unbuildable.** A sticky rebuild pins incumbents
before newcomers (§3.4 order), so it can fail where a from-scratch build would succeed: the
incumbents' claims on capacity are honoured first, and a newcomer that a fresh build would
have placed near the root finds nothing left. This is **inherent to the stability preference,
not a bug** — it is the price of §3.4 rank 1, paid in a case the ordering cannot foresee.

But a meet must never be declared `unbuildable` while a perfectly good tree exists. So:

> **FROZEN: `recompute` performs a TWO-ATTEMPT build.**
>
> 1. `next, err := BuildTree(nodes, prev, cons)` — the sticky attempt.
> 2. If it succeeds → publish. `Outcome = OutcomeBuilt`. Done.
> 3. If it fails → **retry once with `prev = nil`**: `next, err2 := BuildTree(nodes, nil, cons)`.
>    - Succeeds → **publish it**. `Outcome = OutcomeRelaxed`. `Event.Reason` carries the
>      *first* attempt's error text, because that error is the only explanation of why every
>      peer is about to be re-parented and discarding it would make the event unreadable.
>      The relaxed tree becomes the new `prev`.
>    - Fails → `Outcome = OutcomeUnbuildable`. **Keep the previous tree**, publish
>      `EventUnbuildable` with the second attempt's error.
>
> `overlay.Validate` gates publication in both branches, unchanged: a tree that fails its own
> oracle is never published, whichever attempt produced it.

**Rules on the retry, each freezing a way it could be misused:**

- **It does NOT bypass `RecomputeCooldown`.** A relaxed build re-parents nearly everyone; it
  is the single most expensive thing the control plane does. The only cooldown bypasses
  remain the two in §5.6/§5.9: a stranded peer (`OK:false`) and an unplaced joiner.
- **It is a retry, not a fallback mode.** Exactly one extra attempt, same tick, same inputs.
  There is no "relaxed mode" the coordinator can get stuck in — the next recompute starts
  sticky again from the newly published `prev`.
- **It is loud.** Log at Warn with both error texts. A meet that goes relaxed repeatedly is
  telling you the fleet is chronically near its capacity bound, which is exactly the
  operational signal §13.1's honest-limits discipline wants surfaced rather than smoothed
  over.
- **`PickRoot` returning `""` short-circuits both attempts.** Dropping the stability
  preference does not create upload capacity, so an unrootable fleet is `unbuildable`
  immediately, with no pointless second call.

**Why a fourth outcome rather than a boolean on `OutcomeBuilt`.** The manager is right that a
stickiness-only failure is neither `settling` nor `unbuildable`, and the three audiences are
genuinely different: `settling` is normal startup (Debug, muted UI), `relaxed` is a rare
expensive success the operator should *see but not act on* (Warn, distinct UI treatment),
`unbuildable` is a fault requiring a human to change something (Warn, persistent `--crit`
banner). Three states, three renderings; a boolean would have to be plumbed into the UI as a
de facto fourth state anyway, with none of the enum's exhaustiveness.

`EventTopology` carries `Outcome` (the field already exists), so **no new `EventKind` is
needed** — a relaxed publish is still a topology event, because a tree really was published.
The dashboard branches on `data.outcome`.

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

The settle timer firing is a threshold event in its own right — row 8 in §5.4's exhaustive
list. `EventSettling` is emitted on each transition INTO ineligibility (not per suppressed
event, which would be a frame storm), so a meet that grows 2→3→4 members emits it up to
three times over its life — which is correct, because each is a real, separate wait.

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
	// Live is the ARBITER'S OWN liveness verdict, not the coordinator's.
	//
	// v1 typed this as a Health string sourced from coordinator.Health, which is circular in
	// the one case that matters: a frozen or partitioned coordinator's health FSM is the
	// thing that has stopped running, so it cannot be the detector that notices its own
	// death. The arbiter therefore keeps its OWN lastBeat map (methods below), fed by the
	// same frames the Hub already terminates, and declares a peer not-Live after GoneAfter
	// of silence. For every other peer this is a redundant second opinion; for the
	// coordinator it is the only opinion that can exist.
	Live bool
	// Coordinatable is the peer's own declaration that it is willing to be elected
	// (metrics.Report.Coordinatable, from -coordinatable). A hard disqualifier. v1 froze the
	// flag with no field to carry it and no import path to read one; this field plus the
	// arbiter->metrics edge are what make it implementable.
	Coordinatable bool
}

// The arbiter's own liveness view. cmd/server fans the Hub's Observer callbacks to BOTH the
// arbiter and the coordinator, so these mirror the coordinator's inputs without depending on
// the coordinator being alive to process them.
func (a *Arbiter) PeerJoined(roomID, peerID, name string)
func (a *Arbiter) PeerLeft(roomID, peerID string)
func (a *Arbiter) Heartbeat(roomID, peerID string, payload []byte)
func (a *Arbiter) Metrics(roomID, peerID string, payload []byte)

// Meet is the arbiter-owned description of one meet, and the payload of the dashboard's
// /api/meets endpoints (§9.3). v1 referenced a dashboard.Meet that was never defined and
// would have inverted the import; this is that type, owned by its producer.
//
// Relays is ascending. Depth is -1 when no tree exists.
type Meet struct {
	ID             string
	CreatedAt      time.Time
	Members        int
	Epoch          uint64
	Rev            uint64
	Coordinator    string // peer name; "" ⇒ none yet, or the arbiter itself
	CoordinatorID  string // signaling.ServerID when the arbiter coordinates
	Relays         []string
	Depth          int
	ArbiterIsCoord bool
}

// Score maps a Fitness to [0, 1]. Higher is fitter.
//
// Returns exactly 0 for a peer disqualified by ANY of: NAT == NATRelayed, !Live,
// !Coordinatable. A hard zero rather than a penalty, because these are not "worse", they are
// "ineligible", and no bonus elsewhere may buy an ineligible peer the job.
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
| **Re-announce** | ANY membership change in a meet that already has a coordinator. Epoch is **unchanged**; the announcement is re-broadcast so a joiner (or a peer that lost its fence) adopts it. Not an election — see §8 routing rule 5 / §15/C6. | immediate |
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
4. It reconstructs a stickiness baseline from the realized parent/children in the heartbeats
   (never from any received topology), **over the reduced node set it actually heard from** —
   not over the full roster.

   This is the M5 correction. v1 validated the reconstruction against the *full* node set, so
   a single member whose heartbeat was lost or late made the reconstruction "disconnected",
   failed `Validate`, and dropped to `prev = nil` — **a global re-parent caused by one
   dropped frame**, not by the "heavy churn" v1's limitations section claimed. Reducing the
   node set makes an un-heard-from member simply *absent from `prev`*, which the builder
   already handles perfectly: absent from `prev` means "newcomer", and §3.4's processing
   order attaches newcomers after incumbents. One lost heartbeat now moves exactly one node
   instead of all of them.

   Concretely: let `heard` be the members that reported in the window. Build `prevObserved`
   over `heard`, run `overlay.Validate(prevObserved, heard, cons)`, and use it as the
   baseline if it passes. Only if the *reduced* reconstruction still fails — a genuinely torn
   tree, e.g. a mid-flight re-parent captured half-applied — fall back to `prev = nil`. That
   residual case is real, but it is now what v1 claimed it was: heavy churn, not a packet.
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
//
// v1 also carried a `renegotiate bool` that re-ran negotiation ourselves once the answer
// landed. THAT IS REMOVED (M9). pion v4 implements the W3C "update the negotiation-needed
// flag" algorithm: a track added while the pc is not `stable` sets pion's own internal flag
// and pion re-fires OnNegotiationNeeded when the pc returns to `stable`. Our own re-run
// therefore RACED pion's, and the common outcome is two offers for one change — the exact
// bug the serializer exists to prevent, reintroduced by the fix.
//
// So the bookkeeping is a GUARD, not a SCHEDULER: we suppress our own duplicate offers and
// let pion decide when another is needed.
```

Exact behaviour:

**`onNegotiationNeeded()`**
1. If `!s.offerer` → return (unchanged; the answerer never initiates).
2. Lock. If `s.negotiating` → unlock, **return and do nothing else**. pion will call us again
   when the pc returns to `stable`.
3. Still locked: if `s.pc.SignalingState() != webrtc.SignalingStateStable` → unlock, return.
   This is now the *primary* guard, not the near-dead second opinion it was in v1: it covers
   any state change we did not initiate, and it is pion's own truth rather than our intent.
4. `s.negotiating = true`, unlock.
5. `CreateOffer` → `SetLocalDescription` → `sendDescription`. On **any** error: lock,
   `s.negotiating = false`, unlock, and schedule a retry (below) — pion will NOT re-fire for
   an error in our own call, which is why the retry path survives while the flag does not.

**On a successfully applied answer** (in the `SDPTypeAnswer` branch, after
`SetRemoteDescription` succeeds and `flushPending()` runs): lock, `s.negotiating = false`,
unlock. Nothing else. If more changes accumulated while the offer was in flight, pion's own
negotiation-needed flag is set and it fires `OnNegotiationNeeded` on its operations
goroutine — which is both correct and off our `consume` goroutine for free.

> **Implementer obligation.** This rests on a claim about pion v4's internals. §12.2 already
> requires a test that a second `AddTrack` during `have-local-offer` produces **exactly one**
> follow-up offer. **Run that test first.** If pion does *not* re-fire, restore the
> `renegotiate` flag exactly as v1 specified it (re-run on a new goroutine, never inline) and
> report it — do not leave a silently half-negotiated session either way.

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

### 7.3 Re-parent: an ASYNCHRONOUS state machine (CORRECTED — see §15/C10, C12)

Goal: the media gap stays inside one keyframe interval. That means **make-before-break** —
but v1 specified it as a *blocking sequence*, which cannot work, and glossed an RTP
continuity problem that a keyframe does not fix. Both are corrected here.

**C10 — nothing in this path may block.** v1 said step 2 "waits for `Connected`, bounded by
`ReparentConnectTimeout`". But `applyTopology` runs synchronously on `Router.Run`'s
goroutine — the *same* goroutine whose `deliver` feeds the new session's offer/answer/
candidate inbox. Waiting there starves the frames it is waiting for: the negotiation cannot
complete, and the wait times out **100% of the time**. §7.5 had the twin defect (blocking a
pion `OnState` callback for 5 s, which stalls pion's internal dispatch).

> **FROZEN RULE: no pion callback, and no step of `applyTopology`, may block on I/O or on a
> timer.** Every wait is expressed as a timer plus a state transition, resumed on the Run
> goroutine.

The mechanism is one new internal channel:

```go
// Router gains an internal event channel. Run's select gains a third case alongside
// ctx.Done() and the signaling stream:
//
//	case ev := <-r.internal:
//	    r.handleInternal(runCtx, ev)
//
// Pion callbacks and clock timers POST to it and return immediately; all handling happens
// on the Run goroutine, which is also the only goroutine that mutates r.topo and the peer
// map. That keeps the existing "one owner" discipline intact instead of adding locks.
//
// Buffered (32) and NON-BLOCKING on send: a pion callback that cannot enqueue drops the
// event and logs, because blocking a pion dispatch goroutine is precisely the failure
// being fixed.

// reparent is the per-Router state machine for a parent change in flight. At most one is
// live at a time; a second topology arriving mid-reparent supersedes it (the new target is
// adopted, the old pending session is closed).
type reparent struct {
	oldParent string
	newParent string
	deadline  clock.Timer // ReparentConnectTimeout
	connected bool        // new parent reached PeerConnectionStateConnected
	// mediaDeadline arms on connected; ReparentMediaTimeout (§5.6 step 4)
	mediaDeadline clock.Timer
	viaBackup bool
}
```

**States and transitions**, all handled on the Run goroutine:

| State | Entered by | On `connected` event | On `track arrived` event | On timer |
|---|---|---|---|---|
| `opening` | `applyTopology` diff, or a parent-failure event | → `awaiting-media`, arm `mediaDeadline`, request upstream keyframe | — | `deadline` → try backup (once), else `abandon` |
| `awaiting-media` | — | — | → `committing` | `mediaDeadline` → `abandon` |
| `committing` | — | tear down `P_old`, `rebindUpstream`, emit `Reparented{OK:true}` → `idle` | — | — |
| `abandon` | — | keep `P_old` if still alive, close the pending session, emit `Reparented{OK:false}` → `idle` | — | — |

When `applyTopology` finds that *our own* parent changed `P_old → P_new`, it enters
`opening` and **returns immediately.** The numbered steps below are the transitions, not a
blocking sequence:

| Step | Action | Why this position |
|---:|---|---|
| 1 | **Open** a session to `P_new` (`startPeer`), leaving `P_old`'s session **fully intact and still receiving**. Return. | Break-before-make would guarantee a gap of ICE + DTLS + first keyframe ≈ 1–3 s. Make-before-break costs one transient extra peer connection and one duplicated inbound stream for ≤ 5 s. |
| 2 | **Arm `ReparentConnectTimeout`.** `P_new`'s `OnState` posts to `r.internal` on `Connected`. Nothing blocks. | See C10 above: waiting here starves the very frames the wait depends on. |
| 3 | On connected, **request a keyframe upstream** on `P_new` for every source we expect from it. | pion's `OnTrack` will fire and the decoder will PLI eventually; asking proactively is what turns "black until the next natural I-frame" into "black for one keyframe". Same trick `keyframeForChild` already plays for children. |
| 4 | **Rebind the forwarder** (`rebindUpstream`) for each source that arrived via `P_old`, **and `Switch()` every affected downstream `rtpRewriter` (C12, below)**, so a relay's downstream legs keep their tracks and start carrying packets from the new upstream with a continuous RTP stream. | Downstream sessions are never touched, so **our children see no renegotiation at all** — their tracks are the same objects. This is what keeps a re-parent local to one hop instead of cascading down the subtree. |
| 5 | **Then** tear down `P_old`: `link.cancel()`, `session.Close()`, `forwarder.removeChild`/`removeSource` as today. | Last, so nothing is lost if steps 1–3 fail. |
| 6 | **On either timeout**: try `topo.BackupOf(self)` **once** (and only if it is not already the target — §5.6 step 3). If that also fails, **keep `P_old`** if it is still alive and send `Reparented{OK: false}`. | The one outcome that is never acceptable is ending up parentless because we obeyed an instruction we could not carry out. |

#### C12 — make-before-break corrupts the RTP stream unless the boundary rewrites it

`TrackLocalStaticRTP.WriteRTP` rewrites SSRC and PayloadType per binding and **passes
sequence number and timestamp through untouched**. The existing code comments that this is
"correct for one source → one track", and it is — *forever*, as long as the source behind a
leg never changes. Make-before-break changes it: for the overlap window there are two
`forward()` loops (old upstream and new) writing the same downstream tracks, with two
independent, unrelated sequence/timestamp series. The downstream decoder sees a stream that
jumps backwards and forwards in sequence space. **A keyframe does not repair that** — a
keyframe fixes *reference* state, not *transport* ordering, and pion's own NACK/jitter-buffer
interceptors will treat the discontinuity as massive loss and reorder or discard around it.

The fix is the standard SFU primitive, and building it is squarely in this project's spirit:

```go
// rtpRewriter makes ONE downstream leg's RTP stream continuous across an upstream switch.
// One per forwardOut, guarded by its own mutex (the forward loop is its only writer, but
// Switch is called from the Run goroutine).
type rtpRewriter struct { /* unexported */ }

// Rewrite adapts a packet copy for this leg: outgoing sequence continues monotonically from
// the last packet written, and the timestamp continues from the last written timestamp plus
// the observed inter-frame delta. Offsets are recomputed only at a Switch.
func (w *rtpRewriter) Rewrite(pkt *rtp.Packet) *rtp.Packet

// Switch declares that the next packet comes from a DIFFERENT upstream. It recomputes the
// sequence and timestamp offsets so the outgoing stream continues from where it left off,
// and it sets the drop-until-keyframe latch (below).
func (w *rtpRewriter) Switch()
```

Two rules that come with it, both non-optional:

1. **Drop until a keyframe.** After `Switch()`, the leg drops packets until a VP8 keyframe
   arrives from the new upstream, then resumes. A continuous-but-undecodable stream is not
   an improvement over a gap — the decoder would render garbage rather than freeze. Detect
   with `pion/rtp/codecs.VP8Packet`: the frame is a keyframe when it starts a partition
   (`S == 1`, `PID == 0`) and the VP8 payload header's first byte has bit 0 clear.
2. **Request the keyframe immediately** on `Switch()`, via the existing
   `requestUpstreamKeyframe` path. The drop window is then bounded by one PLI round trip,
   which is exactly the "gap inside one keyframe interval" the design promises.

```go
// TimestampGapTicks is the timestamp advance inserted at a Switch when the true inter-frame
// delta is unknown (the first packet after a switch). 3000 = one frame at 30fps on VP8's
// 90kHz clock. Being slightly wrong here costs a few ms of A/V drift, not a broken stream;
// being DISCONTINUOUS here costs the stream.
const TimestampGapTicks = 3000
```

> **Honest scoping.** This is the single genuinely SFU-hard piece of Phase 5 and it should be
> budgeted as such. **If it proves too costly, the documented fallback is to abandon
> make-before-break on the relay's upstream edge specifically** — break first, then open, and
> accept a 1–3 s gap for that one edge. That fallback must be taken *explicitly*, with a WHY
> comment naming this section, never by quietly shipping make-before-break without the
> rewriter. Shipping the overlap without continuity handling is strictly worse than the gap:
> it trades a visible freeze for intermittent corruption, which is much harder to diagnose.

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

// ReparentMediaTimeout is how long a peer waits, after its new parent's session reaches
// Connected, for actual media to arrive before declaring the promotion failed.
//
// 3s: one keyframe interval plus slack. The relay requests an upstream keyframe the moment a
// child connects (§7.3 step 3), so on a working path the first track arrives well inside
// this. Its whole job is to distinguish "I can reach B" from "B can reach the root", which is
// exactly the distinction that makes ratification safe.
const ReparentMediaTimeout = 3 * time.Second

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

#### C11 — role inversion on a SURVIVING edge silently kills media

`Topology.Offers(self, peer)` is evaluated once, in `NewSession`, and baked into
`Session.offerer` for the life of the session. Diff-and-apply *keeps* a surviving edge — that
is the point of a diff — but the tree around that edge can change such that the offerer role
**inverts**. Concretely: a leaf `L` attached to relay `R` is the answerer on that edge
(`Offers(R,L)` is true because only `R` is a relay). A rebuild gives `L` children of its own.
Now both endpoints are relays, so `Offers` falls to the name tie-break — and it can name `L`.
`L`'s session object is still baked as an answerer, and `onNegotiationNeeded` returns early
for a non-offerer. So `L` can never add the forwarded m-lines it now needs.

The failure mode is the nasty one: **not "both offer" but "neither can".** No glare, no
error, no log line — just a relay that never publishes its forwarded tracks. Silent.

**Detection.** The diff gains a fourth bucket:

```go
// invert lists SURVIVING neighbours where the topology's offerer role for the edge no
// longer matches the live session's baked-in role:
//     topo.Offers(selfName, peerName) != link.session.Offerer()
invert []string
```

**Handling: tear down and re-create the session for that edge**, as part of the same apply.
Adds-before-removes does not apply (it is the same peer), so the edge takes one session
establishment of downtime.

> **Rejected: a mutable role plus a "please offer me" handshake.** It sounds cheaper — no
> reconnect — but pion gives an answerer no way to add m-lines without an offer, so we would
> have to invent a control frame asking the peer to offer. That is a new wire type, a new
> round trip, and a brand-new glare surface (two peers can both decide to ask) in a codebase
> whose entire negotiation design is built on *never* having glare because pion cannot roll
> back. Re-creating the session reuses a path that is already exercised on every join and
> already correct. The cost is bounded and it only occurs when the tree's shape at that edge
> changed anyway — i.e. an interruption was already being paid.

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
| **`TypeMembership`** | **`"membership"`** | server → **every** peer in the meet | roster in **`Message.Peers`** (not `Payload`) | `cmd/peer` → `coordinator.SetRoster` | **new — SHIPPED (WI-0)** |

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

// SHIPPED (WI-0). The roster rides Message.Peers — the field that already exists for
// TypeJoined — rather than a new Payload type, and it is BROADCAST to every member rather
// than unicast to the coordinator.
//
// Both differences improve on this document's first draft and the shipped design wins:
// reusing Peers means no new type to version, and broadcasting means any peer can
// cross-check its own view (and a future dashboard-side peer view costs nothing). The Hub
// exposes the same data synchronously for the server's own use:
//
//	// Roster returns a snapshot of roomID's members, ordered by NAME then ID.
//	func (h *Hub) Roster(roomID string) []Peer
//
// Ordering is name-then-id, not id-only as this document first specified; §8.1 is corrected
// to match. Name-first is the better key because names are the tree's vocabulary.
```

**C5 — the Phase 6 membership seam.** v1 forwarded only metrics/heartbeat/reparented to an
elected coordinator peer. That left an elected coordinator unable to learn joins except by
inference and unable to learn *graceful leaves at all*, so §5.4 threshold events #1 and #2
were unreachable and §6.6 step 3 had no data source — and `arbiter` and `coordinator` may
not import each other, so there was no in-process path either. Two mechanisms close it, and
between them they cost exactly one new frame:

1. **Joins and leaves need no new frame.** The Hub *already* sends `TypePeerJoined` and
   `TypePeerLeft` to every member of a meet, and an elected coordinator is a member. So
   `cmd/peer`, when this process is the coordinator, feeds those frames straight into
   `coordinator.PeerJoined` / `coordinator.PeerLeft`. The seam is three lines in the peer's
   frame switch. This is why no `TypeMemberJoined` was invented: the frame exists.
2. **The authoritative roster needs one.** Inference from a stream of deltas is not the same
   as knowing the set — a coordinator promoted mid-call has missed every delta before it.
   `TypeMembership` gives it the set, broadcast on every membership change.

   **The delta frames still flow.** `TypePeerJoined`/`TypePeerLeft` remain the low-latency
   path; `TypeMembership` is the periodic snapshot that makes a *missed* delta
   self-correcting rather than permanent, and hands a newly elected coordinator ground truth
   with no handover protocol at all. That is the same
   deltas-plus-snapshot-for-convergence shape as §6.6's rebuild-from-peers, and it is why
   this seam needs no reconciliation logic beyond `SetRoster` being idempotent.

   `cmd/peer` translates `[]signaling.Peer` → `[]coordinator.Member` and calls `SetRoster`,
   so `coordinator` still imports nothing new.

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
4. `TypeCoordinator` and `TypeMembership` — server-originated only. A peer that sends either
   gets the existing "ignoring unexpected frame type from peer" warning.
5. **Announce-on-join (C6).** On EVERY membership change in a meet that has a coordinator,
   the arbiter re-broadcasts the current `Announcement` — same epoch, unchanged — to the
   whole meet via `SendRoom`.

   This closes a hole that silently blackholed the most ordinary case in the system. v1
   announced only on an *election*, so a peer joining an already-running meet had
   `curEpoch = 0` and `coordID = ""` forever: `Fence.Accept` fails its first two clauses, so
   every topology push was rejected, so it never received media — **indefinitely, while the
   dashboard rendered it healthy.** The same trap caught a peer resuming after a
   socket-closing partition.

   Broadcast rather than unicast-to-the-newcomer, because it is idempotent and therefore
   self-healing: `Fence.AdoptAnnouncement` returns false for every peer already at that
   epoch, so incumbents ignore it at zero cost, while *any* peer that has somehow lost its
   fence (a rejoin, a missed frame) is repaired by the next membership change. One
   mechanism covering the known case and the unknown ones beats a targeted fix for the known
   one.

   Ordering is safe for the same reason the Phase 4 topology push is: `TypeJoined` is queued
   on the newcomer's own outbound channel inside `register`, strictly before `obs.PeerJoined`
   fires, and the announcement is a consequence of `PeerJoined`. Same FIFO channel ⇒ the
   newcomer resets its fence and *then* adopts.

### 8.1 Ordering rules for every collection on the wire (M10)

`Topology.Edges` carries a topological-order invariant (§3.1) that a rebuild depends on. v1
then typed several other wire collections as Go maps, whose JSON key order is sorted but
whose *semantic* order is undefined — enough to make a reconstruction from the wire differ
run to run. Every collection now has a stated order:

| Field | Order |
|---|---|
| `Topology.Edges` | **topological (attach) order** — NOT sorted. The §3.1 invariant. |
| `Topology.Backups` | ascending by `Node` |
| `TypeMembership`'s `Message.Peers` / `Hub.Roster` | ascending by `Name`, then `ID` (SHIPPED) |
| `metrics.Heartbeat.Children` (`[]ChildLink`) | ascending by `Name` |
| `coordinator.MemberSnapshot.Children` | ascending |
| dashboard `nodes[]` | ascending by `name` |
| dashboard `edges[]` | topological, mirroring `Topology.Edges` |
| dashboard `meets[]` | `created_at_unix_ms` descending, then `id` ascending |

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
| `seq` | `uint64` | Monotonic **PER CONNECTION**, starting at 1 on the first frame this socket receives. v1 said "per meet, starting at 1 for each connection", which is self-contradictory; per-connection is the resolution, because the server keeps no event history and per-meet numbering would require one. A client that sees `seq` skip **must** send `{"op":"resync"}` — the `Last-Event-ID` replacement (§9.2). A gap means the server dropped frames for a slow consumer, which it does rather than buffer without bound. |
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
| `topology` | `{"root","edges":[…],"backups":[…],"depth","relays":[…],"outcome","reason"}` — `outcome` is `built` or `relaxed`; on `relaxed`, `reason` carries the sticky attempt's error (§5.9a) | `coordinator.EventTopology` |
| `reparent` | `{"name","from","to","self_promoted":true,"reason"}` | `coordinator.EventReparent` |
| `failover` | `{"name","orphans":[…],"reroot":false,"reason"}` | `coordinator.EventFailover` |
| `election` | `{"epoch","coordinator","prev","reason"}` | `arbiter.Publisher` |
| `stale_rejected` | `{"name","epoch","rev","reason"}` | `coordinator.EventStale` |
| `settling` | `{"waiting":[…],"reason"}` | `coordinator.EventSettling` |
| `unbuildable` | `{"reason"}` | `coordinator.EventUnbuildable` |
| `demo` | `{"action","target","by"}` | the demo surface (§9.6) |

### 9.4a Everything a frontend agent needs and v1 left undefined

A frontend agent codes against this section with no access to a running server. Each item
below was missing or ambiguous in v1.

**Envelope constant.** Every frame and every REST response body carries
`"api_version": 1` (integer, frozen at 1 for Phases 5–6). **Skew rule:** the client compares
it to its own compiled-in constant. Higher than expected → the client renders a
non-dismissable "this dashboard is older than the server; reload" banner and **stops applying
deltas** (it may still show the last snapshot). Lower than expected → the client proceeds and
logs to console. Never silently reinterpret an unknown version.

**Error body — the project's `{code, message, details}` envelope, everywhere.** Every non-2xx
REST response is exactly:

```json
{ "api_version": 1,
  "error": { "code": "meet_exists", "message": "meet \"standup\" already exists",
             "details": { "id": "standup" } } }
```

`code` is a stable snake_case token the UI may branch on; `message` is human-readable and may
change; `details` is an object and may be `{}`, never absent. Frozen `code` set:

| `code` | Status | Meaning |
|---|---|---|
| `bad_request` | 400 | Malformed JSON body |
| `invalid_meet_id` | 400 | Fails `policy.MeetIDPattern` |
| `meet_exists` | 409 | Create collided |
| `meet_not_found` | 404 | Unknown meet |
| `member_not_found` | 404 | Demo target not in the meet |
| `no_candidate` | 409 | Forced election found no eligible peer |
| `demo_disabled` | 404 | Never returned — the routes are unregistered (§9.6). Listed so nobody adds it. |
| `too_many_meets` | 429 | `MaxMeets` reached |
| `internal` | 500 | Anything else |

**Units and ranges, frozen.** `*_kbps` = kbit/s, integer. `*_ms` = milliseconds, float.
`loss_pct` and `cpu_pct` = percent on **0–100**, float, 1 decimal. `fitness` = **0–1**, float,
3 decimals. `*_unix_ms` = integer milliseconds since the Unix epoch, UTC. `depth` = integer
hops from root; **`-1` means "not in the tree"** and the UI must render it as "unattached",
never as a number. `epoch`/`rev`/`seq` = unsigned integers, and the client must treat them as
opaque and **compare only for equality/ordering** (they can exceed 2^53 in principle; parse as
`BigInt` or compare as strings — do not do arithmetic on them).

**Frozen enum value sets** (the UI must handle an unknown value by rendering it verbatim in a
neutral style, never by crashing):

| Field | Values |
|---|---|
| `health` | `healthy`, `degraded`, `gone` |
| `roles[]` | `coordinator`, `relay`, `leaf` |
| `nat` | `direct`, `turn` |
| `kind` | `snapshot`, `member_joined`, `member_left`, `health_changed`, `topology`, `reparent`, `failover`, `election`, `stale_rejected`, `settling`, `unbuildable`, `demo`, `pong` |
| `reason` (election) | `bootstrap`, `failover`, `promotion`, `demotion`, `manual` |
| `outcome` | `built`, `relaxed`, `settling`, `unbuildable` |

**WebSocket protocol, complete.** The client sends **exactly two** operations — `{"op":"ping"}`
and `{"op":"resync"}`. There is **no** `subscribe`/`unsubscribe`: the socket is
path-scoped to one meet, and a client watching two meets opens two sockets. (v1 mentioned
subscribe/unsubscribe in the transport rationale and then never specified them; they do not
exist.) The server answers a `ping` with a normal envelope of `kind: "pong"` and `data: {}`
— it is an envelope like everything else, so the client needs no special parse path.

Close codes the server may send:

| Code | Meaning | Client action |
|---|---|---|
| `1000` normal | Server shutting down | Reconnect with backoff |
| `1001` going away | Meet deleted | Stop; navigate to the meet list |
| `1008` policy violation | Origin rejected, or malformed `op` | **Stop. Do not reconnect** — retrying cannot help |
| `1011` internal error | Server fault | Reconnect with backoff |
| `4404` | Meet not found at upgrade time | Stop; navigate to the meet list |

**Reconnect backoff (client, frozen):** 500 ms, 1 s, 2 s, 4 s, 8 s, then every 15 s, each with
±20% jitter; reset to 500 ms after a connection survives 30 s. On every successful reconnect
the first frame is a fresh `snapshot`, so no client-side replay is needed. Never reconnect on
`1008` or `4404`.

**Meet ids.** Client-supplied ids must satisfy `policy.MeetIDPattern`. A server-generated id is
`"m-"` followed by 8 characters of lowercase Crockford base32 (`0-9a-hjkmnp-tv-z`), e.g.
`m-7fa3k2qd` — 40 bits, collision-safe at this scale, unambiguous when read aloud, and
matching the pattern by construction. `POST /api/meets` returns **`201`** with a
**`Location: /api/meets/{id}`** header alongside the body.

**Listing.** `GET /api/meets` returns `meets[]` ordered by `created_at_unix_ms` **descending**,
then `id` ascending. **No pagination** — the arbiter caps meets at `MaxMeets = 100` (in-memory,
single process, portfolio scale), and `MaxMeets` is exactly why pagination is unnecessary
rather than merely omitted. Exceeding it fails create with `too_many_meets`.

**`-public-url` derivation.** When `-public-url` is empty the server derives the join URLs from
`-addr`: if `-addr` has a host (`example.com:9000`), use it; if it is host-less (`:9000`, the
default), use **`localhost`** plus the port. Scheme is `http`/`ws` unless `-public-url` says
otherwise — the server does not know whether something terminates TLS in front of it, so a TLS
deployment **must** set `-public-url` and the deploy doc says so. `join.ws_url` and
`join.peer_command` are built from that one value, so they are correct or obviously wrong,
never subtly wrong.

**CORS details.** `Access-Control-Allow-Credentials` is **never** sent, and the client must
never set `credentials: "include"` — there is no auth, so credentials would only widen the
attack surface. A request with **no `Origin` header** (curl, a health checker) is served
normally **with no CORS headers at all**: CORS is a browser mechanism and there is nothing to
answer. A request whose `Origin` does not match is served the normal response body **without**
`Access-Control-Allow-Origin`, letting the browser block it — the server does not 403, because
returning a distinguishable error would leak the allow-list to a probing page.

```
Access-Control-Max-Age: 600
```
600 s (10 min) is the ceiling Chrome actually honours for preflight caching (Firefox caps at
24 h, Safari lower). Asking for more is silently clamped, so 600 is "the largest value that
means what it says" — it removes the preflight from the steady-state click path without
pretending to a cache lifetime no browser grants.

**`demo` events and the `by` field.** v1's `by` had no source, because there is no auth.
Renamed to **`by_remote_addr`** and documented as **advisory only**: it is the requester's
`RemoteAddr` as the server saw it, is trivially spoofable behind a proxy, and exists so a
demo operator can tell their own clicks from a colleague's on a shared screen. It is not an
identity and the UI must not present it as one.

### 9.5 What the UI renders (contract for `web/`)

| Panel | Content |
|---|---|
| **Server bar** | The configurable server URL (default `localhost:9000`), a connect/disconnect control, and a live/stale indicator. |
| **Meets** | List + create. Each row shows members, epoch, coordinator. Creating one reveals the `join.peer_command` with a copy button — this is the rendezvous. |
| **Subnet** | The live tree. Coordinator badge = `--accent-primary` (#bd93f9), relay badge = `--accent-secondary` (#ff79c6), leaf = `--surface`. Edges `--edge` (#8be9fd); a degraded node's edge `--warn`; a failover flash `--crit`. **Two independent badges per node**, never one merged role. Backup parents drawn as a dashed edge. |
| **Node detail** | Per-node telemetry with `font-variant-numeric: tabular-nums` so live numbers do not jitter the layout. |
| **Event log** | The envelope stream, newest first, colour-coded by `kind`. |
| **Build state** | Three distinct renderings, and confusing any two is the specific failure the enum exists to prevent. `settling` → transient "waiting for telemetry from X, Y" in `--fg-muted`; normal startup, must never look like a fault. `relaxed` → a one-shot `--warn` flash on the subnet panel reading "rebuilt from scratch — stability preference dropped", with the reason on hover; a rare, expensive *success* the operator should notice but need not act on. `unbuildable` → a **persistent `--crit` banner** with the reason; someone is receiving nothing and a human must change a flag or drop a peer. |
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

// MeetSource is what the dashboard needs from the arbiter.
//
// Every signature names ARBITER-OWNED types (arbiter.Meet), which is what lets
// *arbiter.Arbiter satisfy it without importing dashboard. v1 returned a dashboard.Meet and
// would have inverted the import — see §15/C9. dashboard imports arbiter; arbiter imports
// nothing new. The interface still earns its place: it is what lets the dashboard's handler
// tests run against a fake with no arbiter at all.
type MeetSource interface {
	ListMeets(ctx context.Context) ([]arbiter.Meet, error)
	CreateMeet(ctx context.Context, id string) (arbiter.Meet, error)
	GetMeet(ctx context.Context, id string) (arbiter.Meet, error)
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
| `-join-settle` | `1500ms` | **new** | How long the coordinator waits after a join for telemetry before building anyway (`coordinator.JoinSettle`, §5.9) |
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

**The WS ping family is COMPILE-TIME ONLY — deliberately not operator-tunable (frozen).**
`WSPingInterval` and `WSPingTimeout` get **no flags**. WI-0b was right to flag the gap rather
than invent one; the resolution is a decision, not an omission:

- They are **not independent knobs.** They are bound by
  `WSPingInterval + WSPingTimeout ≤ GoneAfter(interval)`, whose right-hand side depends on a
  cadence each *peer* declares. An operator turning these dials would have to reason about a
  relationship spanning two processes and a value they do not control — a footgun with no
  use case behind it.
- **What an operator actually wants to tune is already exposed.** "How fast is a dead peer
  noticed" is `-gone-after` / `-degraded-after` on the server and `-heartbeat` on the peer.
  The ping cadence is an implementation detail of the socket layer beneath that.
- `HubConfig.PingInterval` / `PingTimeout` remain **programmatic** overrides, which is
  exactly what the hub and simnet tests need. Configurable from Go, not from argv.

Consequently `NewHubWithConfig`'s error names **the `HubConfig` field**, not a flag — there
is no flag to name, and inventing one in an error message would send an operator hunting for
something that does not exist. WI-0b's instinct was correct. The flag-level failure is a
separate check that lives elsewhere: `cmd/server` calls `metrics.ValidateLivenessBudget` and
reports against **`-gone-after`**, which *is* a real flag an operator can act on.

**`-coordinate` vs `-elect` precedence (frozen).** `-coordinate` means "this process may host
the coordinator". `-elect` means "arbitrate the role among peers". With both set: the arbiter
hosts the coordinator until a peer scores above `DemoteBelowScore`, then hands over. With
`-elect` and not `-coordinate`: the arbiter never coordinates, and a meet with no fit peer has
no coordinator (and no tree) — logged at `warn` every `ElectionDwell`, not silently. With
neither: the plain Phase 1–3 signaling relay, unchanged.

Startup validation (extend `validateCoordinatorFlags`, same fail-loud discipline): every
duration flag `> 0`; `-degraded-after < -gone-after`; `-stickiness-ms >= 0`;
`-root-change-margin-kbps >= 0`; `-join-settle > 0`; `-allowed-origins` non-empty
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
| **WI-0** | Wire + clock + policy foundation | `internal/clock/**`, **`internal/policy/**`**, `internal/signaling/message.go`, `internal/signaling/hub.go`, `internal/signaling/client.go`, `internal/metrics/report.go`, `Makefile` | — | **nothing — lands first** |
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
| `internal/metrics/report.go` | **WI-0 only.** `Heartbeat`, `Reparented`, `HeartbeatInterval`, `DegradedAfter`/`GoneAfter`, `ValidateLivenessBudget`, `Report.Coordinatable`, the `Reporter` clock parameter. | Consume. |
| `internal/policy/**` | **WI-0 only.** The shared origin + meet-id matcher (§2.4). Both `dashboard` (WI-7) and `arbiter` (WI-5) consume it; neither reimplements it. | Consume. |
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
| **Pure graph** | `overlay` | Table-driven + the independent `Validate` oracle. Never re-derive the expected tree. | Epoch/rev stamping; stickiness at rank 1 (an incumbent survives a lower-load and a marginally-closer challenger, loses to one beating `StickinessMs`); processing order (a strong joiner does not re-parent incumbents); backup invariant `B ∉ Subtree(P)`; root children have no backup; `ValidateLocalRepair` catches a gratuitous re-parent; `Validate` catches each new violation class; `LoadTopology` still accepts a Phase-3 file and stamps `StaticEpoch`; determinism over ≥50 repeats. **`PickRoot` never returns a provisional, impaired, or under-capacity node (a real 1200 kbit/s report against a 2000 kbit/s stream cost is NOT rootable — ruling E); an incumbent root survives a challenger inside `RootChangeMarginKbps` and loses to one beyond it; a demoted former root is processed first and its former children stay put (§3.4 step 0); `ValidateLocalRepair` accepts an RTT-justified move past `StickinessMs` and still rejects an unjustified one (ruling D).** **The impairment coupling rule, BOTH branches: with a healthy candidate available, an impaired incumbent loses its children (the void fires); with EVERY candidate impaired, the same fleet keeps every incumbent edge unchanged (the void is suppressed) — a fleet that differs only in whether one candidate is impaired must produce those two different outcomes, or the coupling is not wired.** |
| **Deterministic sim** | `simnet` | Seeded scenarios + property tests over many seeds. | Replay identity (same seed ⇒ identical trace); virtual-clock ordering (same-instant timers fire in creation order); each injection primitive; ≥200-step churn keeping **both** oracles green (the interim RTT-free / RTT-rich split collapses once ruling D lands); the dwell suppressing a noisy-but-stable metric stream; the dwell firing on a sustained one. **The three §12.3 properties**, including the 120-permutation enumeration asserting validity + bounded delta on each — and pinning `9 distinct trees, 1 root` as a regression check on the trade itself. |
| **Control loop** | `coordinator` | `-race`, fake `Sender`, fake `Publisher`, `VirtualClock`, `Sync` barrier. | The eight threshold events and **only** those recompute; `RecomputeCooldown` coalescing a burst; the `OK:false` cooldown bypass; health FSM transitions in virtual time; local repair moves only orphans (asserted via `ValidateLocalRepair`); the ratification rule (a self-promoted parent survives the next rebuild); re-root on root loss; `Snapshot` never tears. **The settle rule: a 1-member meet never builds and never consumes the settle; every join RE-ARMS it; a join burst coalesces into one build; a join bypasses `RecomputeCooldown` but not the settle; `unbuildable` is emitted only post-settle.** **The §5.9a relaxed retry: a fleet that is sticky-unbuildable but fresh-buildable publishes with `OutcomeRelaxed` and NEVER reports `unbuildable`; a genuinely over-constrained fleet reports `unbuildable` and keeps its previous tree; the retry does not bypass the cooldown; `PickRoot == ""` short-circuits without a second attempt.** | |
| **Election** | `arbiter` | Table-driven `Score`; scenario-driven election. | `Score` returns 0 for TURN-bound and non-healthy; upload is absent from the formula (assert a large `UploadKbps` changes nothing); every trigger in §6.2; `MinTermDuration` blocks a voluntary handover and does **not** block a failure one; epochs strictly increase and never repeat. |
| **Fencing** | `simnet` + `coordinator` | Scenario. | Two coordinators mid-handover: the stale one's push is rejected and counted; a topology carrying a higher epoch is rejected; a rejoining peer resets `curEpoch`; the arbiter-restart hole (§6.7) is closed. |
| **Media, deterministic** | `media` | Unit, no network. | The negotiation serializer: a second `AddTrack` while `negotiating` sets `renegotiate` and produces **exactly one** follow-up offer; the retry path bounded by `NegotiationRetries`; `RemoveTrack` on a closed pc returns nil; the diff computation in `applyTopology` (pure function over two topologies — extract it so it is testable without pion). |
| **Media, integration** | `media` | `-race`, real loopback pion, as today. | Mid-call re-parent: a leaf moved from R1 to R2 keeps receiving, with the old session closed *after* the new one connects; a dropped source's downstream legs are actually removed (the §7.4 fix); backup promotion on a killed parent. |
| **Wire** | `signaling` | `-race`, two real WS clients, as today. | The three new frame types round-trip; `SendRoom` reaches every member; a dead socket is reaped by the WS ping within `WSPingInterval + WSPingTimeout`; origin rejection. |
| **Boundary policy** | `policy` | Table-driven, pure. | `ValidMeetID` accepts/rejects the frozen pattern's edges (leading `-`, 64 vs 65 chars, uppercase); `ParseOrigins` rejects a mid-string `*`; **`Origins.Match` accepts exactly the set `Origins.Patterns()` makes `websocket.Accept` accept** — asserted against the REAL library (this test lives in `signaling`, which may import both), never against a second copy of the matcher. That one assertion is the entire justification for the package existing (§2.4). |
| **HTTP API** | `dashboard` | `httptest`, fake `MeetSource`/`SubnetSource`. | Every path and status in §9.3; the envelope schema; `seq` monotonicity and the `resync` path; CORS headers present and echoing, never `*`; **demo routes return 404 when `DemoControl` is nil**. |
| **Frontend** | `web/` | Manual, plus a Playwright smoke test if cheap. | Loads with no console errors; renders against a live server; handles server-unreachable without a blank page; dark theme only. |

### 12.3 The three properties that are actually true (CORRECTED — see §15/C3)

**v1 asserted a property that is FALSE.** It required that no permutation of first-report
arrival order may change the converged tree. Reviewer A produced a counterexample and it is
correct: fleet `C=12000, A=B=D=E=4000`, `StreamKbps=2000`, `MaxDepth=2`. `C` roots with
capacity 6; `A`, `B`, `D`, `E` each have capacity 2. Depending on which of the four reports
first — and therefore which incumbency `prev` records before the next rebuild — the fleet
converges to trees with the same root and different `Edges` (e.g. `D` under `C` in one
history and under `A` in another). Both are valid. Both are minimal with respect to their own
history. Neither is wrong.

**MEASURED (WI-1, confirmed):** enumerating all **120** first-report permutations of that
fleet yields **9 distinct valid trees**, and **the converged root is identical across all
120.** That is the precise, honest statement of the trade, and it is a better result than
either extreme would have been:

> **The root converges; the shape does not.**

The root converging is `PickRoot`'s stickiness plus the `Provisional` rule doing exactly
their job — the one decision whose instability would re-parent the entire meet is
arrival-order independent. The shape not converging is rank 1 doing exactly *its* job. Nine
trees, all valid, all minimal with respect to their own history, none of them "the right
one".

**This is not a bug to patch. It is what stickiness IS.**

> A stability-preserving builder is **path-dependent by construction.** Rank 1 (§3.4) says
> "keep the parent you already have", which is a statement *about history*. A builder whose
> output depends only on the current node set is exactly a memoryless builder — that is
> Phase 4, and its memorylessness is the defect §3.4 exists to fix. **Path-independence and
> minimal-disruption rebuilds are mutually exclusive, and this project chose the second.**
> You cannot assert the first without unchoosing the second.

So the harness asserts the three properties that *are* true and that are worth the same
amount:

**(a) Every produced tree is legal, and every transition is minimal.**
For every step of every scenario: `overlay.Validate(next, nodes, cons)` passes, and
`overlay.ValidateLocalRepair(published, next, nodes, cons, churn)` passes — **`published`,
not `working`** (§5.6a, §15.7). This is the strongest correctness claim available and it
holds over *every* history, not a chosen one.

**(b) The edge-set delta between consecutive trees is BOUNDED by the churn that caused the
rebuild.** This is the real stability property — the one a user actually feels, because each
changed edge is one stream interruption. Assert, per rebuild, that

```
changedParents(published, next) ⊆ joined
                                ∪ childrenOf_published(gone)
                                ∪ promoted
                                ∪ impaired-children
                                ∪ { u : parentOf_published(u) became ineligible in next }
```

with the concrete numeric consequences pinned as separate table cases:

| Churn | Bound on nodes whose parent changed |
|---|---|
| one join, no re-root | **exactly 1** (the joiner) |
| one leaf departs | **0** |
| one relay `X` departs, no re-root | **≤ `len(childrenOf_prev(X))`** |
| one self-promotion (§5.6) | **exactly 1** (the promoted node), and the strict form must hold: `next.ParentOf(u) == published.BackupOf(u)`. Against `published` a promotion is a real, visible, checkable parent change — which is the whole point of §15.7. (This row carried a different bound before v2.1; see §15.9.) |
| one node becomes `Impaired` | **≤ `len(childrenOf_prev(node))`** (rank 1 is voided for exactly those children, §3.4) |
| an RTT improvement past `StickinessMs` | **≤ 1 per improving node**, and each must satisfy `ValidateLocalRepair`'s justification 4 — this row is only assertable because RULING D widened the oracle to see RTT |
| a sticky build failed and the relaxed retry ran (§5.9a) | **unbounded** — assert `Outcome == OutcomeRelaxed` was published, the same way the re-root row asserts `Reroot == true` |
| root departs | unbounded (re-root; assert `Reroot == true` was published) |

The last two rows are the honest ones: re-root and relaxed-retry are the two cases where
"local repair" does not apply at all, and the test asserts that the coordinator **said so** —
`Reroot == true`, `Outcome == OutcomeRelaxed` — rather than that it avoided them. A design
that cannot always be local must at minimum always be *legible* about when it was not.

**(c) Determinism given the same event SEQUENCE — not the same event SET.**
Same `ScenarioConfig.Seed` and the same *ordered* script ⇒ byte-identical trace, over ≥50
repeats. This is the replayability the whole harness rests on, and it is unaffected by (a
correct reading of) path-dependence: it says a *history* determines a tree, which is exactly
what stickiness promises.

**What replaces the deleted permutation test.** Still enumerate permutations with
`Scenario.ReportOrder` — but assert (a) and (b) on each, not equality across them. That is
strictly more valuable: the old test would have caught "arrival order leaked into a
decision", which (b) also catches (a leaked dependency shows up as an *unjustified* changed
parent), while additionally catching real churn bugs the equality test was blind to.

A worthwhile companion, kept from v1 but weakened to a **bound**, not an equality: across
permutations the number of published trees must be ≤ (number of threshold events), i.e. no
history may produce *more* reconfigurations than it had causes.

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
   relay by up to `BackupOvershootAllowance` (= 1) children until the next recompute. This claim is
   TRUE only because rank 0c bounds backup fan-in; without that cap the overshoot would be
   the size of the failed subtree, which is what v1 shipped and what §15/M1 corrects.
4. **Root children have no backup (§3.5).** Losing the root is a full rebuild, not a warm
   failover. For a shallow tree that is a large fraction of the nodes.
5. **A handover during genuinely torn state can cause one global re-parent (§6.6 step 4).**
   If the reconstruction *over the reduced heard-from node set* still fails `Validate` — a
   tree captured mid-re-parent — the new coordinator builds from `prev = nil`. v1 stated this
   as "heavy churn" while its own design triggered it on **one dropped heartbeat**; §15/M5
   makes the stated scope the real scope.
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
12. **The root is whoever joined earliest among the reported, and stays.** This replaces a
    v1 limitation that described an *impossible* failure (a root flapping on an oscillating
    upload estimate — `UploadKbps` comes from a static `-upload-kbps` flag and does not
    oscillate). The real limitation is the opposite one: `PickRoot` keeps the incumbent
    unless a challenger beats it by `RootChangeMarginKbps`, so a peer that joins later with
    4500 kbit/s **never** displaces a 3000 kbit/s incumbent. That is intended — re-rooting
    re-parents the whole meet — but it means **the root is a function of join order as much
    as of capacity**, and a meet whose strongest machine arrives second will run permanently
    on its second-best relay. Mitigation available if it ever matters: allow re-rooting when
    the meet is small enough that the disruption is trivial (say ≤ 3 members). Not taken now,
    because it adds a size-dependent branch to the one decision that most needs to be boring.
13. **Trees are path-dependent (§12.3).** Two identical fleets reached by different histories
    may hold different, equally valid trees, and neither is "the right one". This is not a
    defect to fix later: it is the direct consequence of stickiness, which is what buys
    minimal-disruption rebuilds. Anyone comparing two runs and expecting identical topologies
    is applying the wrong invariant — the right ones are validity, bounded delta, and
    determinism per event sequence.
14. **`Fence` protects the peer, not the arbiter.** Nothing authenticates a *peer* to the
    server. The fence stops a peer obeying the wrong coordinator; it does not stop a peer
    claiming someone else's name at join time (the Hub's unique-name check is first-come,
    not authenticated).
15. **The C12 rewriter is the riskiest piece of Phase 5.** RTP sequence/timestamp continuity
    plus drop-until-keyframe is real SFU work. If it is not built, make-before-break must be
    abandoned on the relay's upstream edge and the gap accepted — explicitly, per §7.3.
16. **Phase 7 is untouched.** No simulcast, no SVC, no TURN infrastructure. `overlay.NATRelayed`
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
   consumer-defined-interface rule. §2.5 explains why Go's typing forces it.
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

## 15. Amendment log (v1 → v2)

v1 was reviewed adversarially by two independent reviewers. Much of it survived — both
independently verified the `(Epoch, Rev)` split, the `B ∉ Subtree(P)` strengthening, the
"no bespoke `Repair()`" argument, and `coordinator`'s import isolation. What follows is
everything that changed, and why.

### 15.1 Frozen names that CHANGED — route these to running agents

| Old (v1) | New (v2) | Why |
|---|---|---|
| `coordinator.FirstBuildSettle` | **`coordinator.JoinSettle`** | C2 — the settle re-arms per join, not once per meet; the old name encoded the bug |
| `Config.FirstBuildSettle` | **`Config.JoinSettle`** | C2 |
| `-first-build-settle` | **`-join-settle`** | C2 |
| `coordinator.SetEpoch(epoch)` | **`SetEpoch(roomID, epoch)`** | C7 — epoch is per-meet |
| `overlay.ValidateLocalRepair(prev, next, gone, joined)` | **`(prev, next, nodes, c, ch Churn)`** — and `prev` is the **last published** tree | M2, then rulings D (§15.8) and §15.7. The intermediate `(prev,next,gone,joined,promoted)` form named in v2 was superseded before anyone built it; this row shows the final signature to stop a reader implementing the middle one. |
| `coordinator.DegradedAfter` / `GoneAfter` (constants) | **`metrics.DegradedAfter(interval)` / `metrics.GoneAfter(interval)`** (functions) | SHIPPED (WI-0) — thresholds must multiply the peer's *declared* cadence, else `-heartbeat 2s` ejects healthy peers |
| `signaling.WSPingTimeout = 3s` | **`= 2s`** | SHIPPED (WI-0) — strict margin below the interval |
| `signaling.WSPingInterval = 15s` | **`= 5s`** | C4 — reconcile with `GoneAfter` |
| `metrics.ChildState` | **`metrics.ChildLink`** | SHIPPED (WI-0) |
| `signaling.TypeRoster` / `signaling.Roster` | **`signaling.TypeMembership`** + `Hub.Roster()`; roster rides `Message.Peers` | SHIPPED (WI-0), broadcast not unicast |
| `signaling.NewHubWithConfig(cfg) *Hub` | **`(cfg) (*Hub, error)`** | Ruling B (§4.2) |
| `dashboard.Meet` (never defined) | **`arbiter.Meet`** | C9 — the type belongs to its producer |
| `dashboard.MeetSource` returning `dashboard.Meet` | returns **`arbiter.Meet`** | C9 |
| `arbiter.Fitness.Health string` | **`Fitness.Live bool`** + **`Fitness.Coordinatable bool`** | M4, M12 |
| dashboard event `demo.by` | **`by_remote_addr`** | no auth exists; the field is advisory |

**New frozen names:** `overlay.Fence` (+ `AdoptAnnouncement`, `Reset`, `Accept`, `Applied`),
`overlay.Height`, `overlay.Node.Impaired`, `overlay.Node.LossPct`, `overlay.MaxLossDeratePct`,
`overlay.BackupOvershootAllowance`, `coordinator.MinBuildableMembers`, `coordinator.Member`,
`coordinator.SetRoster`, `media.ReparentMediaTimeout`, `media.TimestampGapTicks`,
`metrics.Report.Coordinatable`, `internal/policy` (`MeetIDPattern`, `ValidMeetID`, `Origins`,
`ParseOrigins`, `Patterns`, `Match`), `simnet.VirtualClock.SetBarrier`, `arbiter.Meet`,
`arbiter.PeerJoined`/`PeerLeft`/`Heartbeat`/`Metrics`, `signaling.WSLivenessBudget`,
`metrics.ValidateLivenessBudget`, `metrics.Heartbeat.IntervalMs`/`Interval()`/`Normalize()`.

### 15.2 Critical findings

| # | Finding | Resolution | §|
|---|---|---|---|
| C1 | `Supersedes` mandated as the fencing check contradicted "reject a higher epoch" — a privilege escalation | Split ordering from authorization: `Supersedes` orders, new `Fence.Accept` authorizes. `StaticEpoch`'s rationale rewritten (its real protection is structural) | 3.2, 3.10, 3.9 |
| C2 | Settle collapsed for the first joiner and was then dead for the meet's life | Re-arm per membership-growth episode + `MinBuildableMembers = 2` + join bypasses the cooldown | 5.9 |
| C3 | The report-order-invariance property was **false**, not vacuous | Accepted path-dependence as architectural truth; replaced with validity + bounded-delta + per-sequence determinism | 12.3, 13.13 |
| C4 | A `gone` node with a live socket was permanently ejected | Resurrection rule (`gone` never deletes) + numeric liveness-budget invariant enforced at startup | 5.3 |
| C5 | Phase 6 had no membership seam; join/leave threshold events unreachable | Existing delta frames feed an elected coordinator; `TypeMembership` snapshot + `SetRoster` for ground truth | 8, 5.8 |
| C6 | A peer joining an existing meet was never announced → never got media, indefinitely | Re-broadcast the current announcement on every membership change | 8 rule 5, 6.2 |
| C7 | `SetEpoch` was process-global while epoch is per-meet | Signature takes `roomID` | 5.8 |
| C8 | The degradation dwell was inert — nothing read health | `Node.Impaired` voids incumbency (rank 1) and gates rank 0b; `Node.LossPct` derates capacity | 3.7, 3.4 |
| C9 | Import cycle in the dashboard seam, both directions | `dashboard → arbiter` is a legal edge; `arbiter.Meet` is producer-owned | 2.2, 2.3, 9.7 |
| C10 | `applyTopology` blocked on the goroutine feeding the frames it awaited — 100% timeout | Asynchronous state machine over an internal event channel; no callback or apply step may block | 7.3 |
| C11 | `Offers` baked at session creation; role inversion on a surviving edge silently killed media | Diff gains `invert`; the session is re-created | 7.4 |
| C12 | Make-before-break interleaved two RTP series; a keyframe does not fix seq/ts | `rtpRewriter` with continuity + drop-until-keyframe; documented fallback is to abandon MBB on that edge, explicitly | 7.3 |

### 15.3 Major findings

| Finding | Resolution | § |
|---|---|---|
| Backup fan-in unbounded; overshoot was `|Subtree(P)|−1`, not 1 | `BackupOvershootAllowance = 1` spent across assignment (rank 0c) | 3.5 |
| `ValidateLocalRepair` contradicted the ratification rule | `Churn.Promoted`; `prev` is the **last published** tree — **this row said "patched" in v2 and was WRONG, see §15.7** | 3.6, 5.6a |
| Promotion could violate `MaxDepth` for the promoted subtree | Hard rank 0b: `Depth(B) + 1 + Height(u) ≤ MaxDepth`, needing new `Topology.Height` | 3.5 |
| `Fitness.Health` had no source; coordinator-death detection was circular | The arbiter keeps its **own** liveness view; `Fitness.Live` | 6.1 |
| One lost heartbeat ⇒ `prev = nil` ⇒ global re-parent | Validate the reconstruction over the **reduced heard-from** set | 6.6 |
| No cooldown bypass for an unplaced joiner (5 s black screen) | Joins bypass `RecomputeCooldown`, bounded by `JoinSettle` instead | 5.9 |
| `BackupOf(u)` still named the just-promoted parent; `Connected ≠ root-connected` | Clear the local backup on promotion; `OK:true` requires **media**, not ICE | 5.6 |
| The single-goroutine `Run` loop did blocking network I/O | `Sender.SendTopology` **MUST NOT BLOCK** — frozen on the interface | 5.8 |
| pion re-fires negotiation-needed; the manual flag raced it | Removed `renegotiate`; the bookkeeping is a guard, not a scheduler, with a verification obligation | 7.1 |
| Map-typed wire fields broke the ordering invariant | Ordering rule for **every** wire collection | 8.1 |
| `Advance(d)` spanning deadlines fired without an intervening settle | Fire one at a time; `VirtualClock.SetBarrier` between firings | 4.3 |
| `-coordinatable` was unimplementable | `metrics.Report.Coordinatable` + the `arbiter → metrics` edge | 6.1, 8 |

### 15.4 Rulings requested by WI-0

- **A — the origin allow-list.** Extracted `internal/policy` as a second shared leaf
  (§2.4). Rejected amending the DAG to permit `dashboard → signaling` (it hands the
  unauthenticated browser surface a handle on the peer hub, and the next convenience is
  `hub.SendRoom` behind an HTTP handler); rejected duplication-plus-cross-check (more code
  than one implementation, and it only catches the drift a test enumerated).
- **B — `NewHubWithConfig` panic.** Widened to return an error (§4.2). The values are
  operator input from flags, not programmer error, so the project's fail-loud-with-a-message
  rule applies. `NewHub(log)` stays non-erroring because its defaults are compile-time
  constants covered by a test.

### 15.5 Undefined-in-v1 dashboard surface, now specified

`arbiter.Meet`; the `{code, message, details}` error envelope with a frozen `code` set;
`api_version` and its skew rule; WS close codes and client reconnect backoff; `seq` resolved
to **per-connection**; `ping`/`resync` as the only client ops with a `pong` envelope reply;
every enum's value set; units and ranges (including `depth = -1`); list ordering and the
`MaxMeets` no-pagination rationale; `Location` header and the generated-id format;
`-public-url` derivation from a host-less `-addr`; CORS credentials and the no-`Origin` case.
See §9.4a.

### 15.6 Magic numbers given rationale

`policy.MeetIDPattern`'s `{0,63}` (longest DNS label); `Access-Control-Max-Age: 600`
(Chrome's honoured ceiling); `RecomputeCooldown = 5s` (bounded below by the cost of the
interruption it suppresses, above by joiner patience — which joins escape entirely).

### 15.7 RULING: `ValidateLocalRepair`'s `prev` is the LAST PUBLISHED tree (v2 was wrong)

**WI-1 is right and has already shipped the correct behaviour.** v2's §3.6 and its §15.3 log
row both said `prev` is the coordinator's *patched* tree. That is wrong for the oracle, and
the correction is now in §3.6 with the mechanics in §5.6a.

WI-1's stated argument is correct: an oracle fed the patched copy asserts against the very
belief it exists to check — the promotion is already baked in, so the promoted node shows no
parent change and the oracle can only confirm that the coordinator believes what it believes.

**And there is a stronger reason it did not state.** The published tree is the only artifact
that still carries the **`Backups` assignment the promotion must be checked against.** Given
the published `prev`, the oracle can assert `next.ParentOf(u) == prev.BackupOf(u)` — that a
promoted node landed on *the backup it was actually assigned*, not on some arbitrary node
it decided it liked. That is a genuine check on the promotion's legitimacy, and against the
patched copy it is not merely weaker, it is **impossible**: the patch has overwritten the
evidence. So the shipped choice buys an assertion the rejected one cannot express, and §3.6's
justification 2 is strengthened to make it (with a guard for the correlated-failure case
where the assigned backup is itself gone, so the oracle stays sound instead of firing falsely
on a correct repair).

**No double-counting**, which was the manager's concern: `prev` (published) supplies the
BEFORE state and the backup assignment; `Churn.Promoted` supplies the JUSTIFICATION CLASS for
a change that `Gone`/`Joined` cannot explain. Two different roles; both required.

**The manager's hypothesis (c) is exactly how v2 went wrong.** §6.6 and §5.6 legitimately
patch a working copy — that is about **`BuildTree`'s input**, it is correct, and it is
unchanged. v2's amendment log conflated that with the **oracle's input**. Both statements were
about `prev`; they were about different functions and different trees. §5.6a now names all
three (`published` / `working` / `next`), gives the round in five lines, and states the
asymmetry in one:

> The builder should be told what the peers have already **done**, so stickiness protects it;
> the oracle should be told what the peers were last **instructed** to do, so it can check
> that what they did was allowed.

WI-3 keeps all three references per meet. Nothing needs reworking in WI-1.

### 15.8 Later rulings (D, E, F) and shipped-accuracy items

| Ruling | Resolution | § |
|---|---|---|
| **D** — the oracle could not see RTT and flagged a legitimate rank-2 move (seed 42, `n4` `n1`→`n3`, a 48 ms win) as gratuitous | **Widened.** `ValidateLocalRepair(prev, next, nodes []Node, c Constraints, ch Churn)`; the four `[]string` churn args collapse into `overlay.Churn{Gone, Joined, Promoted, Impaired}`. Justification 4 (an RTT win past `StickinessMs`) becomes expressible. WI-1's interim RTT-free/RTT-rich suite split **collapses** — it left the strongest fleets checked by the weaker oracle, which is backwards. | 3.6, 12.2 |
| **E** — `PickRoot` approximated "can parent" as `UploadKbps > 0`, so a real 1200 kbit/s report against a 2000 kbit/s stream cost still got rooted | **Widened.** `PickRoot(nodes, prev, streamKbps int)`; eligibility is `effectiveUploadKbps(n)/streamKbps >= 1`, plus not-Provisional, not-Impaired, not-TURN. Rejected passing the whole `Constraints`: `Constraints.Root` is the thing `PickRoot` computes, so it is a self-referential parameter that invites a caller to pass a stale root and get it back. `MaxDepth` is deliberately not needed — the root is at depth 0. | 3.8 |
| **F** — stickiness can make a satisfiable fleet unbuildable | **Mandatory two-attempt build (§5.9a):** sticky, then one retry with `prev = nil`. New fourth outcome `OutcomeRelaxed` (built, stability preference dropped, expect a large re-parent) carried on the existing `EventTopology.Outcome`, so no new `EventKind`. The retry does NOT bypass `RecomputeCooldown`, is one attempt not a mode, and is logged at Warn. `unbuildable` now means **genuinely over-constrained**, never "stickiness painted us into a corner" — a much stronger, actionable signal. | 5.9a |

**Shipped-accuracy items reflected:** a demoted former root is processed **first** (§3.4 step
0) — placing it with the newcomers scatters its former children, because their incumbent
parent is not yet attached when they are processed, turning the most disruptive event there
is into a whole-subtree scatter; and `BackupOvershoot` → **`BackupOvershootAllowance`**,
spent across backup assignment, which makes §13.3's one-child overshoot claim literally true.

**Measured and pinned (§12.3, §13.13):** 120 first-report permutations of the C3
counterexample fleet produce **9 distinct valid trees with an identical converged root** —
*the root converges, the shape does not*. That is the precise statement of the
stability/determinism trade and it is now a regression check on the trade itself.

### 15.9 v2.2 — stale-text sweep and three later rulings

**Stale text swept.** v2.1's three-tree ruling (§15.7) did not reach three live call sites,
which the WI-1 implementer correctly refused to follow. All corrected:

| Was | Now |
|---|---|
| §5.5 step 3–5: `prevPatched`, and `ValidateLocalRepair(prev, next, []string{X}, nil)` | `working` (§5.6a vocabulary), and `ValidateLocalRepair(published, next, nodes, cons, Churn{Gone: …})` |
| §12.3(a): `ValidateLocalRepair(prevPatched, next, gone, joined, promoted)` | `ValidateLocalRepair(published, next, nodes, cons, churn)` |
| §12.3(b) self-promotion row: "**0** — the ratified edge is already in `prevPatched`" | **exactly 1**, plus the strict form `next.ParentOf(u) == published.BackupOf(u)` |

That third one was not a wording slip: against `working` a promotion is invisible, against
`published` it is a real, checkable parent change — so the bound was *numerically wrong*, and
in the direction that would have made a correct implementation look like a bug. §5.6a's
`working` definition also now absorbs departures, so §5.5 and §5.6 share one vocabulary
instead of two.

The v2 name table in my report and the contract body disagreed on
`BackupOvershootAllowance`; **the contract body is correct** and has been consistent since
v2.1. WI-1 was right to refuse the rename.

**Ruling — the all-impaired case (§3.4).** WI-1 found the rank 1 `!Impaired` void is
unobservable in normal fleets and observable only when *every* candidate is impaired and
rank 0b re-admits them — where v2.1 then moved a child between two impaired parents for an
RTT delta. **Not intended.** The void's sole purpose is to let the dwell move children off an
impaired relay *onto a healthy one*; with no healthy relay its premise is absent and only
churn remains — in the one state where the fleet can least afford interruptions, and
repeatedly, since impairment is sustained by definition. Amended with a **coupling rule**:
0b's filter and 1's void read one `healthyAvailable` flag, so incumbency is preserved exactly
when there is nowhere healthy to go. The clause stays observable in its intended case, so
C8's "the dwell must not be inert" requirement is untouched; §12.2 now requires both branches
to be tested.

**Ruling — the WS ping family is compile-time only (§10).** No `-ws-ping-interval` /
`-ws-ping-timeout`. They are not independent knobs (bound to a cadence *peers* declare), and
the tunable an operator actually wants is already `-gone-after`/`-heartbeat`. `HubConfig`
keeps programmatic overrides for tests. So `NewHubWithConfig`'s error correctly names a
struct field — there is no flag to name, and WI-0b was right not to invent one.

**And a hole that gap exposed (§5.3).** Startup validation of the liveness budget is
necessary but insufficient: `GoneAfter` depends on a cadence each *peer* declares, so
`-heartbeat 500ms` yields a 4 s threshold under a 7 s socket-detection window and reopens C4
— broken by the **peer**, where no flag validation can reach. Closed by flooring every
per-node threshold: `max(metrics.GoneAfter(declared), Config.SocketDetection)`, with
`SocketDetection` wired from `hub.LivenessBudget()`. New `coordinator.Config.SocketDetection`.

### 15.10 Where I think a reviewer is wrong

- **"`Supersedes` should be deleted."** Not taken. It is genuinely needed for *ordering* —
  the coordinator sequencing its own trees, the dashboard detecting a stale snapshot. The
  defect was mandating it as the *authorization* check. Deleting it would push a
  lexicographic comparison into two call sites instead.
- **"The `SignalingState() != stable` guard is near-dead code."** Half right, and the
  correction inverts it: once the `renegotiate` flag is gone (M9), that guard is the
  **primary** one — pion's own truth about the connection — and `negotiating` becomes the
  secondary intent flag. Removing the guard is the wrong reading of the finding.

---

*This document is FROZEN at v2.2. Amendments go through the owner, in this file, recorded in
§15. Implementers: build what is written, and report what is wrong.*
