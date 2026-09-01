# conclave Design System

> **Living doc.** Last synced post–**Phase 6**. Update this file when the logging contract, CLI
> surface, or dashboard changes — it is meant to stay in step with the code.

conclave is a backend/systems project with **one rendered surface**: the arbiter dashboard in
`web/`. So "design system" here covers two parts, and **both are now live**:

| Part | Subject | Status | Where it lives |
|---|---|---|---|
| **1** | Observability & CLI conventions | **Live** — enforced in `cmd/*` and `internal/logging` | This doc + code |
| **2** | The arbiter dashboard | **LIVE** — built in Phases 5–6 | This doc + `web/css/tokens.css`, `web/css/{layout,components}.css` |

Part 2 was reserved for Phase 7 as a `/debug` page rendered by the server. It **shipped early
and in a different shape**: the observability work was pulled forward because a handover you
cannot watch is a handover you cannot demo, and it split into the arbiter's `/api` REST +
WebSocket surface (`internal/dashboard`) plus a **separate zero-build static frontend** (`web/`)
published to GitHub Pages. The palette and the semantic role vocabulary reserved here were
adopted verbatim — `web/css/tokens.css` cites this document's section numbers and says the token
names are frozen by it.

---

## Part 1 — Observability & CLI conventions (live)

The "design surface" of a headless distributed system **is its logs and its CLI**. Those are the
two things a human actually reads. Treat them with the same discipline you'd give a UI: a stable
vocabulary, consistent formatting, and predictable behavior across every node.

### 1.1 Structured logging contract

conclave logs through the stdlib `log/slog`. The rules below are already enforced in
`internal/logging` and both binaries.

- **One logger per binary, constructed once in `main`, injected downward. No globals, no
  `slog.Default()` writes.** The logger is a dependency like any other. Passing it explicitly is
  the same discipline as passing a DB handle instead of reaching for a package-level singleton — it
  keeps tests hermetic and makes "who logged this" a compile-time fact.
- **Every line is stamped with `service`** via `logger.With("service", …)` at construction:
  `conclave-server` or `conclave-peer`. When logs from many nodes are later merged, `service` is the
  first axis you filter on. *(As the system grows to N peers, `peer_id` becomes the second axis —
  see the reserved fields below.)*
- **Levels:** `debug` | `info` | `warn` | `error`. Parsed case-insensitively by
  `logging.ParseLevel`; `warning` is accepted as an alias for `warn`; an unknown level is an
  **error** that falls back to `LevelInfo` (fail-loud-but-keep-running, not silent-default).
  - `debug` — mechanism detail (SDP/ICE exchange, per-tick metrics). Off in normal runs.
  - `info` — lifecycle a human wants (server listening, peer joined, tree recomputed).
  - `warn` — recoverable degradation (a probe failed, retrying; a peer went silent).
  - `error` — an operation gave up (bind failed, handover aborted).
- **Two formats, selected by flag** (`-log-format text|json`, constructed by `logging.New`):

| Format | When | Why |
|---|---|---|
| `text` (default) | A human at a terminal during dev | Readable at a glance; the default because that's where the owner lives today |
| `json` | Aggregation / grep-across-nodes later | Machine-parseable; the format you switch to once logs from many peers land in one place. JSON is also the hard fallback if an unknown format string is passed |

  Trade-off named explicitly: text is for eyes, JSON is for tools. We don't pick one globally — the
  format is a per-invocation flag so the same binary serves both a live debugging session and a log
  pipeline without a rebuild.

### 1.2 Field-name vocabulary (standardize now, stay greppable later)

The single highest-leverage logging decision in a multi-node system is **agreeing on key names
before there are many nodes.** Once peers, relays, and a coordinator are all emitting logs,
`grep peer_id=` only works if *everyone* wrote `peer_id` and not `peerID`, `peer`, or `id`. This
table is that contract. Reserve the names now even though most aren't emitted yet.

**Conventions:** keys are `snake_case`; **units live in the name** (`rtt_ms`, `upload_bps`) so a
number is never ambiguous; a key means the same thing on every node; per-context values are bound
with `logger.With(...)` rather than repeated at each call site.

| Field | Type | Status | Meaning |
|---|---|---|---|
| `time` | timestamp | Live (slog built-in) | Event time |
| `level` | string | Live (slog built-in) | `debug`/`info`/`warn`/`error` |
| `msg` | string | Live (slog built-in) | Human-readable event |
| `service` | string | **Live** | `conclave-server` \| `conclave-peer` — the node kind |
| `addr` | string | **Live** | Listen address (server `-addr`, e.g. `:9000`) |
| `url` | string | **Live** | Target URL of an outbound request (peer health probe) |
| `error` | string | **Live** | Wrapped error text on a failed op (`%w` chains upstream) |
| `remote` | string | Standardized | Remote address of an inbound request/peer connection |
| `method` | string | Standardized | HTTP/WS method of a served request |
| `path` | string | Standardized | Request path (e.g. `/healthz`, later `/ws`) |
| `status_code` | int | Standardized | HTTP status returned |
| `peer_id` | string | **Live** | Stable per-peer identifier — the primary cross-node axis (the remote peer a line is about) |
| `self_id` | string | **Live** | This node's own server-assigned peer id (e.g. on `joined room`) |
| `room_id` | string | **Live** | Meeting/room the event belongs to |
| `room_size` | int | **Live** | Members in the room after a join/leave |
| `component` | string | **Live** | Sub-service within a binary: `signaling` \| `signaling-client` \| `media` \| `media-router` \| `relay` \| `upload-meter` \| `coordinator` \| `metrics-reporter` |
| `self_name` / `peer_name` | string | **Live** | Stable topology labels (tree mode): this peer's name, and the neighbour a line is about |
| `source` / `child` | string | **Live** | Relay forwarding: the media source being fanned out, and the downstream child a leg targets |
| `state` | string | **Live** | WebRTC state on a transition (`connecting`/`connected`/`failed`; ICE or PeerConnection) |
| `offerer` | bool | **Live** | Negotiation role — `true` if this peer initiates offers; the other only answers (glare-free single-offerer, since pion can't roll back a local offer) |
| `kbit_per_sec` | float64 | **Live** | Aggregate outbound send rate across all tracks, sampled each second (`upload` message) |
| `kbit_per_sec_per_peer` | float64 | **Live** | Per-peer outbound rate = aggregate ÷ live outbound peers (`upload` message) |
| `peers` | []string | **Live** | Roster of other peer ids (on `joined room`) |
| `codec` | string | **Live** | MIME type of a media track (e.g. `video/VP8`) |
| `ssrc` | uint32 | **Live** | RTP synchronization source of a remote track |
| `packets` | int | **Live** | RTP packet count recorded/received on a track |
| `from` / `type` / `sdp_type` | string | **Live** | Signaling-frame fields mirrored into logs (sender id, message type, SDP kind) |
| `root` | string | **Live (Phase 4)** | The relay-tree root the coordinator elected (`computed topology` message) |
| `nodes` / `edges` | int | **Live (Phase 4)** | Size of a computed tree (member count, edge count) on `computed topology` |
| `members` | int | **Live (Phase 4)** | Room members the coordinator tracks after a join/leave |
| `name` | string | **Live (Phase 4)** | A peer's stable label in a metrics/coordinator log line |
| `upload_kbps` | int | **Live (Phase 4)** | Advertised upload budget for forwarding, kbit/s (a peer's declared telemetry) |
| `nat` | string | **Live (Phase 4)** | Declared NAT class: `direct` \| `turn` (turn ⇒ forced leaf) |
| `relay` | bool | **Live (Phase 4)** | Whether this peer is a relay under the applied topology (`applied pushed topology`) |
| `neighbors` | []string | **Live (Phase 4)** | The topology neighbours this peer connects to after a push |
| `parent` / `old_parent` / `new_parent` | string | **Live** | Topology names in a re-parent line: the current parent, and the two ends of a promotion |
| `role` | — | **Dropped** | Never emitted. A peer's role is a *set*, not a scalar — a node can be coordinator and relay at once — so `relay` (bool) plus `coordinator_id` carry it instead |

**Phase 5–6 additions — the control-plane vocabulary.** These are the fields that make a
handover or a failover legible in a log stream, and they are the ones to grep for first.

| Field | Type | Status | Meaning |
|---|---|---|---|
| `epoch` | uint64 | **Live** | The arbiter-minted coordinator **term** — the fencing token. Only the arbiter raises it. Appears on announcements, on every pushed/rejected topology, and on adoption. *(This is the field reserved as `coord_epoch`; the shipped name is `epoch`.)* |
| `rev` | uint64 | **Live** | The sitting coordinator's revision **within** that term. Starts at 1, +1 per published tree, resets to 0 on an epoch change. Only the coordinator raises it. |
| `meet_epoch` / `peer_epoch` | uint64 | **Live** | The two sides of a fence comparison, on the arbiter's lagging-peer repair path. Seeing them differ is the *cause*; a peer's `stale_rejected` is the *effect*. |
| `coordinator` / `coordinator_id` | string | **Live** | Who holds the role. Empty on a **vacancy** announcement — which is a real state, not a missing value. |
| `reason` | string | **Live** | *Why* — and it is overloaded on purpose, because the question is the same at every layer: an `arbiter.Reason` on an announcement, a build reason on `settling`/`unbuildable`/`relaxed`, a refusal reason on a rejected topology, a failure reason on a re-parent. |
| `outcome` | string | **Live** | The `BuildOutcome`: `built` \| `settling` \| `relaxed` \| `unbuildable`. Four values because there are four audiences (§ below). |
| `health` | string | **Live** (dashboard wire) | `healthy` \| `degraded` \| `gone`. Paired with `prev_health`; **the two being equal is a valid shape**, not a bug — sustained degradation and recovery both ride it. |
| `backup` | bool / string | **Live** | On `cmd/peer` startup, whether autonomous backup promotion is enabled. On a topology, the assigned warm secondary parent. `via_backup` marks a re-parent that used one. |
| `fitness_lower_bound` | float64 | **Live** (dashboard wire) | A member's coordinator fitness, to three decimals. Named a *lower bound* rather than `fitness` because uptime is unreachable from a member snapshot — **a value quietly 0.15 too low is *invisibly* wrong**, so the name says so rather than a footnote. |
| `stale_rejected` | int | **Live** | A peer's cumulative count of topology pushes its fence refused. Non-zero during a handover is the fence **working**. |
| `waiting` | []string | **Live** | On `settling`: whose telemetry the coordinator is still waiting for. |
| `heard` / `reached` / `rounds` | int | **Live** | Rebuild-from-peers progress: how many members have reported, how many nodes the reconstruction reached. |
| `socket_detection` | duration | **Live** | The Hub's worst-case socket-death window, logged once at startup because it **floors** every per-node gone threshold. |
| `attempt` / `attempts` | int | **Live** | Position on a negotiation retry ladder; `attempts` on the line that gives up. |
| `add` / `remove` / `recreate` / `reparent` | []string | **Live** | The diff buckets applied on a pushed topology — the one line that says what actually changed. |
| `relay` / `relay_now` | bool | **Live** | Whether this peer is a relay under the applied topology, and whether that just changed (a relay-ness flip forces a session re-create). |
| `dropped_total` / `reparent_reports_dropped` | int | **Live** | Bounded-queue drops. Non-zero means a queue is saturating — a real signal, deliberately counted rather than logged per event. |
| `rtt_ms` | float64 | **Reserved** | Measured pairwise RTT. `BuildTree` consumes it; **nothing measures it**, so it is never emitted (see Limitations). |
| `nat_type` | string | **Reserved** | STUN-classified NAT, as distinct from the *declared* `nat`. Not built. |

> **Status legend.** *Live* = emitted today. *Standardized* = name is fixed; values flow wherever
> that surface exists. *Reserved* = name claimed so it can't drift; **nothing emits it, and in the
> two remaining cases nothing ever will until a sensor is built.** *Dropped* = the name was
> reserved and the design went another way; recorded so nobody re-adds it.

### Log level, by outcome — a contract worth stating

The four build outcomes map to four levels **because there are four audiences**, and this is the
clearest example in the codebase of level-as-design:

| Outcome | Level | Why |
|---|---|---|
| `built` | Info | Normal. |
| `settling` | **Debug** | Normal *startup*. *A warning that fires on every healthy startup teaches operators to ignore warnings.* |
| `relaxed` | Warn | A rare, expensive **success** the operator should see but not act on. |
| `unbuildable` | Warn (+ a critical banner) | A fault requiring a human. |

The same instinct applies elsewhere: an `announce_repair` is surfaced on **transition only**
(enter/leave lagging), never per repeat, because a per-repeat event would be a frame storm at
exactly the moment something is already wrong.

### 1.3 CLI conventions

Both binaries follow one shape. New commands must match it.

- **Thin `main()` → `run(args []string) error`.** `main` does nothing but call `run` and translate
  its error into an exit. All real logic (and all tests) target `run`, so `main` stays a
  three-liner with no branches to test.
- **Flags via a dedicated `flag.FlagSet` with `flag.ContinueOnError`** — *not* the global
  `flag.CommandLine`. This keeps flag parsing scoped and testable (you can parse a fake argv without
  touching process global state), and lets `run` return a parse error instead of the global set's
  default `os.Exit`.
- **Exit codes:** `0` on success, `1` on any error. No other codes are defined; if a richer taxonomy
  is ever needed it will be documented here first.
- **Error lines go to stderr, prefixed with the binary name**, e.g. `conclave-peer: <error>`. This
  is the standard Unix convention and makes the failing binary obvious when several run in one shell
  or under a supervisor.

**Current flag surface:** the full, current table lives in [`usage.md`](./usage.md) and is
generated from `-help`; duplicating it here guarantees one of the two copies is wrong. What
belongs *here* is the conventions the surface obeys:

- **Every flag names its mode.** `-help` text ends with `(call mode)`, `(tree mode)`, `(managed mode)`, `(probe mode)`, or `coordinator:` / `arbiter:` on the server. A reader should never have to guess whether a flag applies.
- **Units live in the name or the type.** Durations are Go durations (`1.5s`, `20s`, `1m0s`); bandwidth is `-*-kbps`; latency margins are `-*-ms`. No bare numbers with implied units.
- **A flag set in the wrong mode WARNS, it does not fail** — `WARN flag has no effect in this mode flag=upload-kbps` — and only for flags the operator *explicitly set*, so it never fires on a default. Failing would punish an over-specified script; silence would lose an afternoon.
- **A flag that cannot be honoured is refused, not ignored.** `-root-change-margin-kbps` is frozen as a compile-time constant in `overlay`, so any non-default value is a startup error naming the reason. Accepting and dropping it is the worse failure.
- **A destructive surface is absent, not guarded.** `-demo` does not flip a boolean a handler must remember to check; the routes are simply **not registered**, so the failure is `404` rather than a permission check that could have a bug in it.
- **Boolean polarity follows the safe zero value.** Internally the field is `DisableBackup`, not `Backup`, because a plain Go bool's zero is `false` and a forgotten field must not silently disable failover. The CLI keeps the readable positive `-backup` (flags express non-zero defaults fine), and `cmd/peer` is the single place the polarity flips.

## Part 2 — The arbiter dashboard (LIVE)

> **This section describes shipped code.** The frontend is `web/`: vanilla ES modules and
> hand-written CSS, **no build step, no bundler, no npm, no framework**. The directory that is
> committed is byte-for-byte the directory that is served, so what you debug against a local
> `file://` tree is what is live. `.github/workflows/pages.yml` publishes it to GitHub Pages on
> any push touching `web/`.

**What it is.** A read-only view of the live subnet — nodes (coordinator / relays / leaves),
edges (parent→child media links), the event log, and per-node telemetry — served from a
different origin than the arbiter it reads. It is **an operator's lens on the same data the
coordinator uses, not a control surface.** Read-mostly and eventually consistent: a `seq` gap
triggers a resync, and that is the only consistency mechanism.

**What shipped that the reservation did not anticipate:**

- **It is a separate site, not a `/debug` route.** The frontend is not a Go concern, and a page served by the arbiter would have to be rebuilt and redeployed with it.
- **It renders two truths and labels them.** The meets **list** is REALIZED (reconstructed from heartbeats — what peers actually did); the meet **detail** and the WS snapshot are INTENDED (the coordinator's last published tree). They genuinely disagree during convergence. A UI that rendered whichever it fetched last would be confidently wrong at exactly the moments that matter, so `converged` / `diverged` is **computed and shown** rather than averaged away.
- **Role is a badge set, not a colour.** A node can be coordinator *and* relay; the two are independent facts with independent promotion criteria, so they render as two independent badges rather than one blended fill.
- **A gated demo panel** appears only when `demo_enabled` is true on `GET /api/meets` — derived server-side from the same nil check that decides whether the routes exist, so the button and the route cannot disagree. An older server that omits the field is treated as demo-**off**, never probed for.

### 2.1 Palette & design tokens (shipped, frozen)

The theme is the **Dracula** family assigned to *roles*, not sprinkled by hex.
`web/css/tokens.css` defines them exactly once, on `:root`, and says the names are frozen by
this document.

| Role | Token | Hex | Usage |
|---|---|---|---|
| Canvas | `--bg` | `#282a36` | Page/graph background |
| Raised surface | `--surface` | `#44475a` | Panels, node fills, hovered rows |
| Text | `--fg` | `#f8f8f2` | Primary foreground / labels |
| Muted text | `--fg-muted` | `#6272a4` | Secondary labels, disabled, borders |
| Primary / coordinator | `--accent-primary` | `#bd93f9` | The elected coordinator; primary UI accent |
| Secondary / relay | `--accent-secondary` | `#ff79c6` | Relay nodes; attention |
| Link / edge | `--edge` | `#8be9fd` | Default overlay edges, hyperlinks |
| Healthy | `--ok` | `#50fa7b` | Healthy node/edge, passing metric |
| Degraded | `--warn` | `#ffb86c` | Degraded node, sustained-degradation warning |
| Failover / critical | `--crit` | `#ff5555` | Failover event, gone node, `unbuildable` |
| Caution | `--caution` | `#f1fa8c` | Near-threshold / dwell-pending |

Auxiliary tokens are named in the same file rather than left as bare literals, per this
project's own "named constants with a justifying comment" rule: `--font-mono`, `--radius`,
`--border`, and a four-step `--space-1..4` scale (4/8/16/24 px).

Rationale for the role split: **coordinator = purple** because it is the single special node and
purple is the brand primary; **relay = pink** to read as "structurally important, watch this";
leaves stay neutral so the eye lands on the load-bearing nodes; **edges = cyan** as a calm
default so that green/orange/red edges pop exactly when link health changes. Degraded and
failover get distinct warm hues because *"getting worse"* and *"already gone"* must never be
confused in an operator glance — which is precisely the distinction the three-state health FSM
exists to preserve, so the palette and the model agree.

### 2.2 Typography

- **Monospace throughout**, terminal-styled: `"Fira Code", "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace`, exposed as `--font-mono`.
- `font-variant-numeric: tabular-nums` for metric readouts, so columns of numbers do not jitter as they update live.
- One typeface; weight and size carry hierarchy. It should read as an instrument panel, not a marketing page.

### 2.3 Semantic roles in the rendered UI

| Element | Meaning | Token |
|---|---|---|
| Coordinator badge | Holds the control-plane role for this epoch | `--accent-primary` |
| Relay badge | Forwards others' media (a peer SFU) | `--accent-secondary` |
| Leaf node | Consumes only | `--surface` fill, `--fg` label |
| Edge (nominal) | Parent→child media link | `--edge` |
| Status dot `dot-ok` | Connected / healthy | `--ok` |
| Status dot `dot-warn` | Connecting / degraded | `--warn` |
| Status dot `dot-crit` | Failed / gone / rejected | `--crit` |
| Status dot `dot-muted` | Not connected / unknown | `--fg-muted` |
| `banner-settling` | Waiting for telemetry — informational | muted/`--caution` |
| `banner-relaxed` | A relaxed rebuild was published — notable, not actionable | `--warn` |
| `banner-unbuildable` | No legal tree exists — needs a human; carries `role="alert"` | `--crit` |
| Event kind `kind-muted` | `settling` and other routine events | `--fg-muted` |
| Event kind `kind-crit` | `unbuildable`, failover, demo actions | `--crit` |

**The banner severity ladder is the log-level table rendered.** `settling` is muted because it
is normal startup; `unbuildable` is the only one that gets `role="alert"`, because it is the
only one where a participant is receiving nothing and a human has to change something.

### 2.4 Light / dark

**Dark only, and that is the shipped decision — do not add a light variant.**

`tokens.css` defines every token exactly once on bare `:root`. There is deliberately **no**
`prefers-color-scheme` block and no `[data-theme]` block, and the file says so. The Dracula
palette *is* a dark palette; the whole thing is designed against `#282a36` and every role above
assumes that canvas. A light variant would have to be a real re-derivation (invert bg/fg, darken
the accents to hold WCAG contrast on a light ground), not a naive inversion — and it was scoped
out explicitly rather than half-done. The tokens are structured so that re-derivation is a
tractable future exercise; nothing about the current file pretends it has been done.

### 2.5 Rendering strategy (and the one exception to it)

The app **rebuilds each panel from scratch on every store change** — there is no virtual DOM and
no diffing, because at this size a full rebuild is simpler and provably correct.

The one deliberate exception is worth knowing before you "clean it up": the demo panel keeps a
small amount of module-level UI state (`pendingEvictName`, `pendingElectName`, the last status
line) across rebuilds. Without it, a live WebSocket frame arriving mid-interaction would
silently drop an in-progress dropdown pick. **Demo actions are rare, deliberate clicks, so
losing one mid-interaction is worse than the handful of variables it takes to preserve them.**

## Limitations & non-goals

- **The field vocabulary is a convention, not a validator.** Nothing rejects a mistyped key at compile time; discipline and review are the enforcement mechanism until a typed attribute helper exists.
- **Two reserved field names will never carry values as things stand.** `rtt_ms` and `nat_type` require sensors that are not built — and neither are `cpu_pct`, `loss_pct` or `rtt_server_ms`, which *are* consumed by real logic (the degradation dwell, `arbiter.Score`, `BuildTree`'s min-latency rank) and are structurally zero in production. The dashboard will faithfully render `0` for all of them. That is a limitation of the sensors, not of the UI, and `DESIGN.md` §8.1 states what it costs.
- **No accessibility audit has been done.** The palette's WCAG behaviour is a requirement for whoever revisits it, not a claim that it passes. `role="alert"` on the unbuildable banner is the only accessibility affordance currently asserted.
- **The dashboard is unauthenticated.** So is everything else — there is no auth anywhere in this system. Anyone who can reach `/api` can read every meet's telemetry, and with `-demo` on, evict a peer. Mitigations are unguessable meet ids, an origin allow-list, bounded state, and the demo routes being absent by default.
- **This document standardizes names and colours, not schemas or component APIs.** A reserved field name says nothing about the struct that will carry it, and a token says nothing about a component's markup.
- **`web/` has no automated test suite.** There is one hand-run harness, `web/tests/reconnect-backoff.test.html`, which drives the real `EventSocket` against a fake WebSocket and a fake clock and pins the backoff sequence exactly. Everything else about the frontend is verified by eye.
