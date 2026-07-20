# conclave Design System

> **Living doc.** Last synced: 2026-07-12 (post–Phase 0). Update this file when the logging
> contract, CLI surface, or dashboard plan changes — it is meant to stay in step with the code.

conclave is a backend/systems project. Right now it has **no UI** and no rendered surface of any
kind — two `main` binaries, structured logs, and an HTTP health check. So "design system" here is
scoped honestly to the two things that actually have design decisions today or soon:

| Part | Subject | Status | Where it lives |
|---|---|---|---|
| **1** | Observability & CLI conventions | **Live** — enforced now, in `cmd/*` and `internal/logging` | This doc + code |
| **2** | `/debug` dashboard theme | **Reserved plan (Phase 7)** — *not built* | This doc only |

If you came here looking for component libraries, spacing scales, or CSS: there is none yet, and
there won't be until Phase 7. Part 2 is a deliberate reservation so that when the dashboard is
built it already has a palette and a vocabulary, not a bikeshedding session.

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
| `component` | string | **Live** | Sub-service within a binary: `signaling` \| `signaling-client` \| `media` \| `media-router` \| `relay` \| `upload-meter` |
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
| `parent_id` | string | Reserved (Phase 4) | Coordinator-computed upstream node in the relay tree (Phase 3 uses `self_name`/`source`/`child`) |
| `role` | string | Reserved (Phase 4) | `coordinator` \| `relay` \| `leaf` |
| `rtt_ms` | int/float | Reserved (Phase 4) | Measured round-trip latency, milliseconds |
| `upload_bps` | int | Reserved (Phase 4) | Measured upload bandwidth, bits/sec (the scarce resource) |
| `coord_epoch` | uint64 | Reserved (Phase 6) | Election epoch/term — the fencing token against stale actors |
| `nat_type` | string | Reserved (Phase 7) | STUN classification (e.g. `full-cone`, `symmetric`); symmetric ⇒ forced leaf |

> **Status legend.** *Live* = emitted today. *Standardized* = name is fixed now; values start
> flowing the moment that surface exists (request logging in Phase 1). *Reserved (Phase N)* = name
> claimed so it can't drift; nothing emits it yet.

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

Current flag surface (Phase 1):

| Binary | Flag | Default | Purpose |
|---|---|---|---|
| `server` | `-addr` | `:9000` | Listen address |
| `server` | `-log-level` | `info` | `debug`/`info`/`warn`/`error` |
| `server` | `-log-format` | `text` | `text` \| `json` |
| `peer` | `-server` | `http://localhost:9000` | Central server base URL |
| `peer` | `-log-level` | `info` | as above |
| `peer` | `-log-format` | `text` | as above |
| `peer` | `-timeout` | `5s` | Per-request timeout (probe mode) |
| `peer` | `-call` | `false` | Call mode: join a room and establish a WebRTC call |
| `peer` | `-room` | `default` | Room to join (call mode) |
| `peer` | `-send` | `false` | Add an outbound video track (call mode) |
| `peer` | `-media` | `""` | VP8 IVF file to send; empty ⇒ synthetic (call mode) |
| `peer` | `-record` | `""` | Write received track to this IVF; empty ⇒ count (call mode) |
| `peer` | `-stun` | `""` | STUN server URL (call mode; empty on one host) |

---

## Part 2 — `/debug` dashboard theme (planned, Phase 7 — reserved, not built)

> **This section is a reservation, not an implementation.** No dashboard, HTML, CSS, or asset
> exists. The purpose here is to pin down a palette and a semantic color vocabulary now so the
> eventual live-graph observability view has a coherent identity on day one instead of being
> assembled ad hoc. Everything below is Phase 7 work.

**What it will be (planned):** a read-only `/debug` view served by the central server that renders
the live overlay — nodes (coordinator / relays / leaves), edges (parent→child media links), and
event flashes (join/leave/handover/failover) — plus current metrics per node. It is an operator's
lens on the same data the coordinator uses to compute the tree, not a control surface.

### 2.1 Palette & design tokens

The theme is the **Dracula** family — the owner's signature palette — but assigned to *roles*, not
sprinkled by hex. Reference for each role first; only reach for a raw hex when defining the token.

| Role | Token | Hex | Usage |
|---|---|---|---|
| Canvas | `--bg` | `#282a36` | Page/graph background (dark-first) |
| Raised surface | `--surface` | `#44475a` | Panels, node fills, hovered rows (Dracula "current line") |
| Text | `--fg` | `#f8f8f2` | Primary foreground / labels |
| Muted text | `--fg-muted` | `#6272a4` | Secondary labels, axis ticks, disabled (Dracula "comment") |
| Primary / coordinator | `--accent-primary` | `#bd93f9` | The elected coordinator; primary UI accent |
| Secondary / relay+alert | `--accent-secondary` | `#ff79c6` | Relay nodes; attention/alert accent |
| Link / edge | `--edge` | `#8be9fd` | Default overlay edges, hyperlinks |
| Healthy | `--ok` | `#50fa7b` | Healthy node/edge, passing metric |
| Degraded | `--warn` | `#ffb86c` | Degraded edge, sustained-degradation warning |
| Failover / critical | `--crit` | `#ff5555` | Failover event, dead node, hard error |
| Caution (optional) | `--caution` | `#f1fa8c` | Near-threshold / hysteresis-pending state |

Rationale for the role split: **coordinator = purple** because it's the single special node and
purple is the brand primary; **relay = pink** to read as "structurally important, watch this";
leaves stay neutral so the eye lands on the load-bearing nodes; **edges = cyan** as a calm default
so that **green/orange/red edges pop** exactly when link health changes. Degraded and failover get
distinct warm hues (orange vs red) because "getting worse" and "already gone" must never be
confused in an operator glance.

### 2.2 Typography

- **Monospace throughout**, terminal-styled to match the owner's aesthetic (his READMEs use a fake
  shell session and Fira Code). Stack:
  `"Fira Code", "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace`.
- Fira Code **ligatures on** for prose/labels; **`font-variant-numeric: tabular-nums`** for all
  metric readouts (`rtt_ms`, `upload_bps`) so columns of numbers align and don't jitter as they
  update live.
- One typeface, weight/size for hierarchy — no second display font. The whole thing should read as
  an instrument panel, not a marketing page.

### 2.3 Semantic color roles for graph elements

| Element | Meaning | Token | Hex |
|---|---|---|---|
| Coordinator node | The elected arbiter/orchestrator | `--accent-primary` | `#bd93f9` |
| Relay node | Forwards others' media (a peer SFU) | `--accent-secondary` | `#ff79c6` |
| Leaf node | Consumes only; 1 hop from a relay | `--surface` fill, `--fg` label | `#44475a` / `#f8f8f2` |
| Edge (nominal) | Parent→child media link, healthy default | `--edge` | `#8be9fd` |
| Healthy edge/metric | Within thresholds | `--ok` | `#50fa7b` |
| Degraded edge | Sustained degradation (re-opt candidate) | `--warn` | `#ffb86c` |
| Failover event | Handover / node death flash | `--crit` | `#ff5555` |

Motion (planned): failover flashes `--crit` then settles; a recompute animates edges rather than
snapping, so the operator can see *what changed* — consistent with the hysteresis model where
re-optimization is a discrete, rare event worth watching, not a constant churn.

### 2.4 Light / dark

**Dark-first, and dark is canonical.** The Dracula palette is a dark palette; the dashboard is
designed against `#282a36` and every role above assumes that canvas. A light variant is **out of
scope** for Phase 7. If one is ever added it must be a real re-derivation (invert bg/fg, darken the
accents to hold WCAG contrast on a light canvas) rather than a naive filter — but until then, the
only supported theme is dark, and tokens are defined once for it.

---

## Limitations & non-goals

- **Part 1 is the only enforced part.** Part 2 ships zero pixels today; do not cite it as an
  existing feature.
- This document standardizes **names and colors, not schemas or component APIs.** A reserved field
  name says nothing about the struct that will carry it.
- The field vocabulary is a **convention, not a validator.** Nothing yet rejects a mistyped key at
  compile time; discipline (and code review) is the enforcement mechanism until/unless a typed
  attribute helper is added.
- **No accessibility audit** has been done on the Phase 7 palette; the WCAG note in 2.4 is a
  requirement for whoever builds it, not a claim that it passes.
