# conclave — Usage

How to run and drive the two binaries — the central `server` (the **arbiter**) and the `peer` — plus the browser dashboard. Every flag below was captured from `-help` on the current build; every log line and HTTP response is real output, not hand-written. Anything not built is marked **Not built**.

> **Status — Phase 6.** The `server` answers `GET /healthz`, hosts the WebSocket signaling hub at `GET /ws?room=<id>&name=<label>`, mounts the browser-facing `/api/` surface, and — with `-coordinate` — runs the **coordinator** (ingest telemetry, compute a relay tree, push it). With `-elect` it also **arbitrates the coordinator role among peers**, minting the epoch that fences a handover. The `peer` has four flavours: a health probe, a plain mesh call, a **static tree** (`-topology`), and a **managed** call (`-managed`) where it reports telemetry, realises the pushed tree, and fails over to a precomputed backup parent on its own. Media flows peer-to-peer over WebRTC (SRTP/UDP); the server relays signaling frames and computes topology, and **never touches media**.

---

## Prerequisites

| Need | Version / note |
|---|---|
| Go | 1.26.4 (uses Go 1.22+ method-aware routing and `r.PathValue`) |
| C compiler | only for `-race`, which `make test`/`make check` run — links cgo |
| `make` | optional; every target is a thin wrapper over `go build`/`go run`/`go test` |

Optional tooling the Makefile *prefers but degrades without*: `golangci-lint` (falls back to `go vet`), `goimports`, `staticcheck`.

---

## Quick start

```console
$ make build          # -> ./bin/server and ./bin/peer
$ ./bin/server -coordinate &
$ ./bin/peer          # probe it once; exits 0 if healthy
```

Then open `web/index.html` in a browser and point it at `localhost:9000`.

---

## `server` — the arbiter

Always-up control node: meet rendezvous, the signaling relay, epoch minting, election arbitration, and the `/api` surface the dashboard reads. **It is never in the media path.**

### Core

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:9000` | TCP address to listen on, `host:port`. `:9000` binds all interfaces; `127.0.0.1:9000` binds loopback only. |
| `-log-level` | `info` | `debug` \| `info` \| `warn` \| `error`. Case-insensitive; `warning` is an accepted alias. Unknown value → **startup error**, exit 1. |
| `-log-format` | `text` | `text` (human `key=value`) or `json` (one object per line). Unknown value → **startup error** (the parse step exists precisely so `-log-format tex` cannot start a server that silently logs JSON). |

### Roles

| Flag | Default | Meaning |
|---|---|---|
| `-coordinate` | `false` | Run the coordinator **in this process**: compute and push relay trees from peer telemetry. Off ⇒ a plain signaling relay. |
| `-elect` | `false` | Arbitrate the coordinator role among peers: mint the epoch, elect, announce, and hand over. With `-coordinate` too, the arbiter holds the role until a peer earns it. |
| `-dashboard` | `true` | Mount the `/api/` dashboard surface. |
| `-demo` | `false` | Register the **DESTRUCTIVE** demo routes (evict a peer, force an election). See the warning below. |
| `-public-url` | `""` | Externally reachable base URL used to build join commands; empty ⇒ derive from `-addr`. **Mandatory behind a TLS terminator.** |
| `-allowed-origins` | `http://localhost:*,http://127.0.0.1:*,https://sammyurfen.github.io` | Comma-separated browser origin allow-list, applied to **both** CORS and the WS upgrade. A trailing `*` is a **port** wildcard only. |

### Coordinator tuning (only meaningful with `-coordinate`)

| Flag | Default | Meaning |
|---|---|---|
| `-max-depth` | `2` | Max relay-tree depth in hops root→leaf. Latency accumulates per hop, so keep it small. |
| `-stream-kbps` | `2000` | Assumed per-stream upload cost. A node's child capacity = its (loss-derated) upload budget ÷ this. |
| `-default-upload-kbps` | `0` | Budget assumed for a peer that has not reported yet. `0` ⇒ treat it as a leaf until its first report proves capacity. |
| `-stickiness-ms` | `25` | RTT margin (ms) a challenger must beat an incumbent parent by before the peer is re-parented. **`0` ⇒ memoryless rebuilds** — that is Phase 4's behaviour, and the defect stickiness exists to fix. |
| `-root-change-margin-kbps` | `2000` | Extra upload a challenger needs before the subnet is **re-rooted** (one stream's worth). Note: `overlay` froze this as a compile-time constant, so **only the default is achievable** — any other value is refused at startup rather than silently ignored. |
| `-join-settle` | `1.5s` | How long to wait after a join for telemetry before building anyway. Re-armed on **every** join, so a burst coalesces into one build. |
| `-recompute-cooldown` | `5s` | Minimum interval between two published trees for one meet. A join bypasses it; a relaxed build does **not**. |
| `-dwell` | `10s` | How long metrics must stay bad before sustained degradation counts. |
| `-degraded-after` | `3s` | Silence before a peer is marked degraded. Must be **strictly less** than `-gone-after`. |
| `-gone-after` | `8s` | Silence before a peer is declared gone — the repair **backstop**, deliberately slow. Must be **≥ the Hub's socket-detection window** (5s ping interval + 2s timeout = 7s) or the server refuses to start. |

### Election tuning (only meaningful with `-elect`)

| Flag | Default | Meaning |
|---|---|---|
| `-election-dwell` | `20s` | Sustained window before a **voluntary** coordinator handover. |
| `-min-term` | `1m0s` | Floor between two voluntary handovers. |

> **These two knobs are currently unreachable in production.** Voluntary promotion and demotion read `CPUPct`, `LossPct` and `RTTServerMs`, and **none of those is ever populated** — so every eligible peer scores 0.85–1.0 and neither `DemoteBelowScore` nor `PromoteMarginScore` can be crossed. Live, the election fires only on **bootstrap** (a meet with members and no coordinator) and **failover** (the incumbent is not live). The dwell/term flags still gate the code path; there is just nothing that trips it. See `DESIGN.md` §8.1.

> ### ⚠️ `-demo` registers a DoS primitive
> The two demo routes let an **unauthenticated** caller terminate a participant's connection and force a control-plane transition. There is no authentication anywhere on this surface, by design. The routes are therefore **not registered at all** unless the flag is set — you get `404`, not `403`, because an unregistered route cannot be reached by a bug in a permission check. Turn it on for a demo; turn it off after. It is logged loudly at startup.

---

## `peer` — a participant

Four modes, selected by flags.

| Flag | Default | Meaning |
|---|---|---|
| `-server` | `http://localhost:9000` | Base URL of the server. A bare `host:port` is accepted. A missing host is rejected with a hint (`-server :9000` mirrors the server's own `-addr :9000` and is easy to type by mistake). |
| `-log-level` / `-log-format` | `info` / `text` | As the server. |
| `-timeout` | `5s` | **Probe mode.** Overall deadline for the health probe. |
| `-call` | `false` | **Call mode.** Join a room and establish a WebRTC call instead of probing `/healthz`. |
| `-room` | `default` | Room (meet) to join. |
| `-send` | `false` | Add an outbound video track. Implied by `-media`. |
| `-media` | `""` | VP8 IVF file to send (looped); empty sends synthetic frames. |
| `-record` | `""` | Write the first received track to this IVF file; empty just counts packets. |
| `-stun` | `""` | STUN server URL. Empty is fine on one host. |
| `-turn` | `""` | TURN relay URL, e.g. `turn:10.0.0.5:3478`. |
| `-turn-user` / `-turn-pass` | `""` | TURN long-term credentials. **All three TURN flags or none** — a URL without credentials gathers no relay candidate and fails *silently*, so a partial set is rejected at startup. |
| `-name` | `""` | Stable topology name (`relay`, `leaf-b`, …). Required by `-topology` and `-managed`. Must match `^[a-z0-9][a-z0-9_-]{0,63}$`. |
| `-topology` | `""` | **Static tree.** Path to a JSON tree file (edges in names). Requires `-name`; fails loud on a malformed tree. Empty ⇒ full mesh. |
| `-managed` | `false` | **Managed tree.** Report telemetry and realise the coordinator's pushed tree. Requires `-name`; mutually exclusive with `-topology`. |
| `-upload-kbps` | `3000` | *(managed)* Advertised upload budget for forwarding others' media. **Declared, not measured.** |
| `-nat` | `auto` | *(managed)* NAT class. **`auto` MEASURES it** from this peer's own nominated ICE candidate pairs; `direct` \| `turn` **force** the value the sensor would otherwise report. `turn` ⇒ forced leaf, and disqualified as coordinator. See the note below. |
| `-coordinatable` | `true` | *(managed)* May this peer be elected coordinator? `false` declines — a laptop on battery. |
| `-heartbeat` | `1s` | *(managed)* Liveness beat interval. **Declared on the wire**, and what the coordinator computes its degraded/gone thresholds from. Anything under 1 ms is refused, because the wire carries whole milliseconds. |
| `-backup` | `true` | *(managed/tree)* On primary-parent failure, promote the coordinator's precomputed backup parent **without asking**. |

**A flag set in the wrong mode warns rather than failing:** `WARN flag has no effect in this mode flag=upload-kbps`. Only flags you explicitly set are named. The five managed-only flags are `-backup`, `-coordinatable`, `-heartbeat`, `-nat`, `-upload-kbps`.

> **What `-nat auto` measures, and what it does not.** It is **not** NAT-type detection.
> The peer reads the *local candidate type* of its nominated ICE candidate pairs and
> reports `turn` only when **every** path it holds is relay-typed — a behavioural
> "my media is going through a relay", nothing about mapping or filtering behaviour.
> Two consequences worth knowing before you read a report:
> **(a)** a peer that has not connected to anybody yet has measured nothing and reports
> `direct`, the permissive class (the same blind spot as first-attachment RTT); and
> **(b)** one direct path is enough to stay `direct`, deliberately — a peer that reaches
> *some* neighbours directly is demonstrably able to relay, and demoting it to a leaf
> would tear its subtree apart on the weakest evidence available. The startup line says
> which source is in play: `nat=measured` or `nat=forced:turn`.

> **Why `-backup` is a positive flag over a negative field.** `RouterConfig` carries `DisableBackup bool`, not `Backup bool`, because "default true" is unachievable for a plain Go bool — a caller who forgot the field would silently get **failover disabled**, the wrong direction to fail in for the entire point of the phase. The CLI keeps the positive polarity because flags express non-zero defaults fine; `cmd/peer` is the single place the polarity flips.

---

## `turn` — the TURN relay (Phase 7)

A media relay of last resort, for a peer that has no direct path to a neighbour. It is
~50 lines over `github.com/pion/turn/v5` — already in the module graph, because
pion/webrtc depends on it for the ICE *client* — so it needs no container and no system
package. **coturn** is the deployment answer and lives in `deploy/docker-compose.yml`
behind an opt-in `--profile turn`; nothing in the test path touches it.

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:3478` | UDP listen address. |
| `-public-ip` | `""` | **Required.** The address handed to clients as their relay address. Must be an IP literal — it goes into an ICE candidate, not a URL — and it cannot be inferred from a wildcard listener. |
| `-users` | `""` | **Required.** `user=password[,user=password…]`. There is no anonymous mode: an open TURN server is a bandwidth amplifier, and that must not be reachable by omission. |
| `-realm` | `conclave` | Long-term-credential realm. Part of the key derivation, so the peer must be issued credentials for the same realm or every correct password is refused with a bare 401. |
| `-relay-ports` | `""` (ephemeral) | Confine relay allocations to `lo-hi`. Same reasoning as the peer's `-media-ports`: a firewall rule can only name ports known in advance, and the TURN verification *is* such a rule. |
| `-log-level` / `-log-format` | `info` / `text` | As the server. |

```console
$ ./bin/turn -public-ip 10.99.0.1 -users conclave=hunter2 -relay-ports 49160-49200
level=INFO msg="turn relay listening" service=conclave-turn addr=:3478 \
  public_ip=10.99.0.1 realm=conclave users=1 relay_ports=49160-49200

$ ./bin/peer -call -managed -room $MEET -name alpha \
    -turn turn:10.99.0.1:3478 -turn-user conclave -turn-pass hunter2
```

The end-to-end recipe — blocking the direct path pair-specifically with `nft` inside an
unprivileged namespace so the TURN path is the only one left — is
[`verify-turn.md`](./verify-turn.md). **It has not been run yet**; the relay's own
allocation path has been checked by hand (`ALLOCATED relay address: 127.0.0.1:49176`,
inside the pinned range, with a wrong password refused).

---

## Make targets

Run `make` (or `make help`) for the live list. Pass binary flags through the `ARGS` variable.

| Target | Does |
|---|---|
| `make help` | List targets (default goal). |
| `make build` | Compile all three binaries into `./bin/server`, `./bin/peer`, `./bin/turn`. |
| `make run-server` | `go run ./cmd/server $(ARGS)`. |
| `make run-peer` | `go run ./cmd/peer $(ARGS)`. |
| `make run-turn` | `go run ./cmd/turn $(ARGS)` — the TURN relay. Needs `-public-ip` and `-users`. |
| `make test` | `go test -race ./...` — all packages, race detector on. |
| `make cover` | Run tests with a coverage profile and print a per-function report. |
| `make fmt` | `go fmt ./...`. |
| `make vet` | `go vet ./...`. |
| `make tidy` | `go mod tidy`. |
| `make lint` | `golangci-lint run` if installed, else prints an install hint and runs `go vet`. |
| `make check-determinism` | Fail if a control-plane package (`overlay`, `simnet`, `coordinator`, `arbiter`) reaches for the wall clock instead of the injected `clock.Clock`. Silent on success. |
| `make check` | `fmt` + `vet` + `check-determinism` + `test` — **the pre-commit gate**. |
| `make clean` | Remove `./bin`, `coverage.out`, `coverage.html`. |

**Passing flags via `ARGS`:**

```console
$ make run-server ARGS="-addr :9000 -log-format json"
$ make run-peer   ARGS="-server http://localhost:9000 -timeout 2s"
```

---

## Example session (a) — text logs, default port

Terminal 1 — start the server:

```console
$ make run-server
time=2026-07-12T11:31:07.688+05:30 level=INFO msg="http server listening" service=conclave-server addr=:9000
```

Terminal 2 — run the peer:

```console
$ make run-peer
time=2026-07-12T11:31:07.750+05:30 level=INFO msg="probing server health" service=conclave-peer url=http://localhost:9000/healthz
time=2026-07-12T11:31:07.753+05:30 level=INFO msg="server is healthy" service=conclave-peer status_code=200 server_status=ok server_service=conclave-server
$ echo $?
0
```

Meanwhile the server logs each request it served:

```
time=2026-07-12T11:31:07.752+05:30 level=INFO msg="served healthz" service=conclave-server remote=[::1]:58478 method=GET path=/healthz
```

Every record carries `service=conclave-server` or `service=conclave-peer` (stamped once via `logger.With(...)`), so a merged server+peer stream stays sortable by origin — the seed of cross-overlay correlation later.

---

## Example session (b) — JSON logs on a custom port

Terminal 1:

```console
$ ./bin/server -addr :9090 -log-format json
{"time":"2026-07-12T11:31:11.270316389+05:30","level":"INFO","msg":"http server listening","service":"conclave-server","addr":":9090"}
{"time":"2026-07-12T11:31:11.282545349+05:30","level":"INFO","msg":"served healthz","service":"conclave-server","remote":"[::1]:37786","method":"GET","path":"/healthz"}
```

Terminal 2 — point the peer at that port, JSON logs too:

```console
$ ./bin/peer -server http://localhost:9090 -log-format json
{"time":"2026-07-12T11:31:11.290280945+05:30","level":"INFO","msg":"probing server health","service":"conclave-peer","url":"http://localhost:9090/healthz"}
{"time":"2026-07-12T11:31:11.292346028+05:30","level":"INFO","msg":"server is healthy","service":"conclave-peer","status_code":200,"server_status":"ok","server_service":"conclave-server"}
```

The bare-host shorthand also works: `-server localhost:9090` is treated as `http://localhost:9090`.

---

## Example session (c) — what a healthy run looks like vs a failed one

**Healthy** — peer reaches a live server: two INFO lines ending in `server is healthy`, and **exit code 0** (shown in session (a)).

**Failed — server is down.** With nothing listening on the target port, the connect fails; the error is wrapped with context and printed to **stderr** prefixed by the binary name, and the process exits **non-zero**:

```console
$ ./bin/peer -server http://localhost:29999
time=2026-07-12T11:31:11.304+05:30 level=INFO msg="probing server health" service=conclave-peer url=http://localhost:29999/healthz
peer: GET http://localhost:29999/healthz: Get "http://localhost:29999/healthz": dial tcp [::1]:29999: connect: connection refused
$ echo $?
1
```

Read the error inside-out: the innermost `dial tcp ... connection refused` is the OS error, wrapped by Go's `http` client (`Get "…"`), then wrapped again by the peer (`GET http://… : %w`). Each layer added the context it had — this is `fmt.Errorf("…: %w", err)` chaining, the reason the message reads like a stack of causes. The exact tail (`dial tcp …`) varies by OS and network.

**Failed — bad input, caught before any network call:**

```console
$ ./bin/peer -server ftp://foo
peer: invalid -server URL "ftp://foo": scheme must be http or https      # exit 1

$ ./bin/peer -log-level bogus
peer: logging: unknown level "bogus" (want debug|info|warn|error)        # exit 1
```

A typo in a flag **fails loudly at startup** instead of silently defaulting and swallowing logs later.

---

## Example session (d) — a 2-peer WebRTC call (Phase 1)

Three terminals: the signaling server, a receiver, and a sender. First make a VP8
clip to send (any `.ivf` works):

```console
$ ffmpeg -f lavfi -i testsrc=duration=5:size=320x240:rate=30 -c:v libvpx -f ivf sample.ivf
```

Terminal 1 — the signaling hub:

```console
$ make run-server
… msg="http server listening" addr=:9000
… msg="peer joined" component=signaling peer_id=p1 room_id=demo room_size=1
… msg="peer joined" component=signaling peer_id=p2 room_id=demo room_size=2
```

Terminal 2 — the receiver joins room `demo` and records what it gets:

```console
$ go run ./cmd/peer -call -room demo -record received.ivf
… msg="joined room" self_id=p1 peers=[]
… msg="peer connection state" peer_id=p2 offerer=false state=connected
… msg="remote track arrived" peer_id=p2 codec=video/VP8 ssrc=…
… msg="recording finished" packets=258 path=received.ivf
```

Terminal 3 — the sender joins the same room and streams the file:

```console
$ go run ./cmd/peer -call -room demo -send -media sample.ivf
… msg="joined room" self_id=p2 peers=[p1]
… msg="peer connection state" peer_id=p1 offerer=true state=connected
```

Both peers' `peer connection state` climbs `connecting → connected`; `received.ivf`
is a real VP8 file (`ffprobe received.ivf` shows `codec_name=vp8`). `Ctrl-C` either
peer to end the call cleanly. Notes:

- **No `-media`?** The sender streams synthetic frames — the receiver still gets a
  track and counts packets, proving transport without an encoder or file.
- **On one host** ICE connects via host candidates; no STUN needed. Add
  `-stun stun:stun.l.google.com:19302` for two machines behind NAT.
- **Who offers whom:** one peer per pair is the *offerer*, chosen deterministically —
  the higher peer id (`offerer=…` in the logs) offers, the other only answers. This
  is glare-free by construction: pion **cannot roll back a local offer**, so we avoid
  simultaneous offers rather than reconcile them. Both directions still ride a single
  m-line — the answerer adds its track before answering, so pion folds it in.

### Full mesh + the upload ceiling (Phase 2)

Add more peers to the same room and each opens a `PeerConnection` to every other — a
full mesh. Each sender uploads an independent copy to every other peer, so upload
cost grows `O(N)`. The **upload meter** logs that once a second:

```console
$ for i in 1 2 3 4; do ./bin/peer -call -room mesh -send & done
… msg="upload" component=upload-meter peers=3 kbit_per_sec=184.3 kbit_per_sec_per_peer=61.4
```

`peers` is the number of live outbound streams (N−1); `kbit_per_sec` is the aggregate
you're pushing (synthetic frames here — 61.4 kbit/s/stream). With real 720p VP8
(≈1.5 Mbit/s/stream) that aggregate is **≈4.6 Mbit/s at 4 peers, ≈6 Mbit/s at 5** —
enough to saturate a typical home uplink. That linear climb is the ceiling the
elected relay (Phase 3+) exists to break. (Measured on one host over loopback, so
it's the honest per-stream bitrate × (N−1); the real cross-machine collapse point is
a projection from those numbers.)

### Tree relay — the peer-SFU (Phase 3)

Instead of everyone-to-everyone, elect one peer to *forward* for the others. The tree
is hardcoded in a shared JSON file (names, not runtime ids), and each peer is given
its name with `-name`:

```console
$ cat tree.json
{ "edges": [ {"parent":"relay","child":"leaf-b"}, {"parent":"relay","child":"leaf-d"} ] }
```

Four terminals — the server, the relay, a sender leaf, a recorder leaf:

```console
$ make run-server
$ ./bin/peer -call -room tree -name relay  -topology tree.json          # pure forwarder
$ ./bin/peer -call -room tree -name leaf-b -topology tree.json -media call720.ivf
$ ./bin/peer -call -room tree -name leaf-d -topology tree.json -record out.ivf
```

`leaf-d` connects **only** to the relay — never to `leaf-b` — yet `out.ivf` is
`leaf-b`'s video (`ffprobe out.ivf` → `codec_name=vp8`): the relay read `leaf-b`'s RTP
and fanned it out with no re-encode. Notes:

- **`-topology` requires `-name`** (a peer must know which node it is); a bad topology
  file fails loud at startup. Omit both ⇒ full-mesh mode.
- **The relay's upload is what scales.** Watch `msg="upload" component=upload-meter`
  on the relay climb with its child count while each leaf stays flat at one stream —
  the whole point of the elected SFU, and Phase 2's ceiling inverted.
- **Keyframes:** the relay forwards PLI (keyframe requests) upstream to the original
  sender with the SSRC translated (`-log-level debug` shows `forwarded keyframe
  request upstream`). A *file* source can't act on a PLI (no live encoder), so a late
  joiner decodes from the file's next natural keyframe; a real browser sender (Phase 7)
  would respond on demand.
- **Depth ≤ 2, single relay** for now — no election or migration yet (Phases 4–6).

### Managed room — the coordinator computes the tree (Phase 4)

Drop the JSON file: with `-coordinate` on the server and `-managed` on the peers, the
tree is *computed from telemetry*, not authored. Each peer advertises an upload budget
(`-upload-kbps`) and NAT class (`-nat`); the coordinator elects the highest-upload peer
as the root relay, runs the greedy `BuildTree`, and pushes the result down a `topology`
frame that each peer realises with the same relay machinery.

```console
$ make run-server ARGS="-coordinate -stream-kbps 2000 -max-depth 2"
… msg="coordinator enabled" component=… max_depth=2 stream_kbps=2000
… msg="computed topology" component=coordinator room_id=mgmt root=relay nodes=3 edges=2

$ ./bin/peer -call -managed -room mgmt -name relay  -upload-kbps 8000               # strong ⇒ elected relay
$ ./bin/peer -call -managed -room mgmt -name leaf-b -upload-kbps 0 -media call.ivf  # sender leaf
$ ./bin/peer -call -managed -room mgmt -name leaf-d -upload-kbps 0 -record out.ivf  # receiver leaf
```

`leaf-d`'s log shows `applied pushed topology … relay=false neighbors=[relay]` — it
connects only to the relay — and `out.ivf` is `leaf-b`'s VP8 forwarded through it
(`ffprobe out.ivf` → `codec_name=vp8`). Notes:

- **`-managed` requires `-name`** and is mutually exclusive with `-topology`. A peer
  reports telemetry every few seconds; the coordinator recomputes on join/leave (and a
  node's first report), never on a mere metric wiggle (anti-thrash).
- **Upload budget shapes the tree.** Give two peers high `-upload-kbps` and the builder
  makes a two-level tree under the depth bound; mark a peer `-nat turn` and it is forced
  to a leaf no matter how much upload it claims.
- **~~Additive apply~~ — superseded in Phase 5.** Apply is now a **diff** of *(self, wanted
  tree, current reality)*: edges are added, removed, re-legged and re-created as needed, and
  the baseline is reality, so a partially realized state converges. Mid-call re-parenting
  works; see the failover section above.
- **Deterministic testing.** All the graph/churn logic is exercised media-free by the
  `simnet` harness (`go test ./internal/simnet/...`) — see `docs/testing.md`.

---

### Failover and election — a managed room that heals (Phase 5–6)

Same setup as above, plus `-elect` on the server. Nothing extra on the peers: `-backup`
defaults to `true`, and the coordinator assigns a warm secondary parent to every node it can.

```console
$ make run-server ARGS="-coordinate -elect -log-level debug"
… msg="arbiter starting" coordinate=true elect=true dashboard=true demo=false socket_detection=7s
… msg="coordinator enabled" max_depth=2 stream_kbps=2000 stickiness_ms=25 join_settle=1.5s dwell=10s gone_after=8s
```

**Kill a relay.** Its children do not wait for the coordinator: a child that sees its parent's
`PeerConnection` reach `failed` — or sit `disconnected` for 2 s — opens a session to its
precomputed backup **make-before-break** (the old parent stays open and receiving until the new
one carries media), rewrites RTP continuity on every downstream leg, requests a keyframe, and
only then closes the old session. It reports the promotion; the coordinator **ratifies** it
into its working copy rather than second-guessing it, then rebuilds. Success requires **media**,
not merely ICE.

**Kill the coordinator.** The arbiter notices (its own liveness view, not the coordinator's),
mints `Epoch+1`, and broadcasts the announcement. Every peer adopts it, resets `Rev` to 0, and
sends one immediate out-of-cycle heartbeat and report. The new coordinator accumulates for up
to `RebuildWindow` (3 s), reconstructs a stickiness baseline **from what peers say they have
realized**, validates it, and publishes `Rev = 1` under the new epoch.

What to watch for, and what each thing means:

- `stale_rejected` becoming non-zero at some peer during the window — **that is the fence doing visible work**, not a fault. It is the observable proof that an old-epoch push was refused.
- `announce_repair` on the dashboard — a peer missed the announcement and the arbiter re-sent it verbatim. Expected occasionally; persistent means that peer is barely reachable.
- The meets **list** and the meet **detail** disagreeing for a few seconds — one is REALIZED, one is INTENDED. See `troubleshooting.md`.
- **Media should keep flowing throughout.** The data plane does not need the coordinator; what is suspended for ≤3 s is re-optimization, not the call.

> **Honest status.** These behaviours are covered by the automated suite — deterministic
> simulation of the control plane and real-pion integration tests of the media plane — but a
> **live multi-process run of Phases 5 and 6 has not been recorded** (`DESIGN.md` §9.5). State
> any claim about live failover or live migration with that qualification.

---

## The dashboard

The dashboard is a **separate static site**, not a page served by the arbiter. It is vanilla ES
modules and hand-written CSS with **no build step** — the directory that is committed is
byte-for-byte the directory that is served — and it is published to GitHub Pages by
`.github/workflows/pages.yml` on any push touching `web/`.

It is **read-mostly and eventually consistent**: an operator's lens on the same data the
coordinator uses, not a control surface. A `seq` gap triggers a resync; that is the only
consistency mechanism.

### Three ways to run it

```console
# 1. Straight off the filesystem, against a local server.
$ make run-server ARGS="-coordinate"
$ xdg-open web/index.html          # then type localhost:9000 in the server field

# 2. No server at all — the fixtures.
#    web/js/mockApi.js drives the whole UI from web/fixtures/*.json.

# 3. The published Pages site, pointed at YOUR machine.
#    Open https://sammyurfen.github.io/… and type localhost:9000.
```

### Pointing the Pages site at a local server

This works out of the box, and the reason is worth knowing because it does **not** generalise:

| Arbiter reachable at | From the `https://` Pages site | Why |
|---|---|---|
| `ws://localhost:9000` | **works** | W3C *Secure Contexts* classifies `localhost`, `127.0.0.1` and `[::1]` as potentially trustworthy, and mixed-content blocking exempts them. Every major browser implements this. |
| `ws://192.168.1.5:9000` | **blocked** | The exemption is by **hostname**, not by network. Your own LAN does not count. |
| `wss://arbiter.example.com` | **works** | A real certificate. This is the hosted path. |

So the frontend picks the scheme from *both* what you typed and how the page itself was loaded:
a loopback host stays plaintext regardless; any other host follows the page's own scheme. The
default `-allowed-origins` already lists the Pages origin and any localhost port, so nothing
else is needed. For a `wss://` rehearsal, `deploy/docker-compose.yml` + `Caddyfile` issue a
locally-trusted certificate — see `deploy/README.md`.

If the dashboard shows **`rejected`** and stops retrying, that is an origin refusal: add your
origin to `-allowed-origins`. Note that a page served from **the arbiter's own origin is not
auto-allowed** — self-origin auto-allow is the DNS-rebinding vector.

### The `/api` surface

Mounted at `/api/` when `-dashboard` is on (the default). Every non-2xx carries the same
envelope: `{"api_version":1,"error":{"code":"…","message":"…","details":{}}}`.

| Method + path | Does |
|---|---|
| `GET /api/meets` | List meets — the **REALIZED** view (reconstructed from heartbeats) plus the ended-meet tombstone ring. Also carries `demo_enabled`. |
| `POST /api/meets` | Create a meet. An **empty body is legal** and is the normal call: the arbiter generates an unguessable `crypto/rand` id. `201` + `Location:` + a `join` block. |
| `GET /api/meets/{id}` | One meet, the full **INTENDED** snapshot — the same object the WS stream sends as its first frame. |
| `GET /api/meets/{id}/events` | WebSocket: snapshot, then the live event stream. |
| `POST /api/demo/meets/{id}/evict` | *(`-demo` only)* Terminate a participant's connection. `name` required. `202`. |
| `POST /api/demo/meets/{id}/elect` | *(`-demo` only)* Force an election. `name` **optional** — empty means "the best candidate", the arbiter's own default. `202`. |

```console
$ curl -sS localhost:9000/api/meets
{"api_version":1,"demo_enabled":false,"meets":[],"ended":[]}

$ curl -sS -X POST localhost:9000/api/meets
# 201, and the join block is the point:
#   "join": { "ws_url": "ws://localhost:9000/ws?room=k3f9xq2b",
#             "peer_command": "peer -call -managed -server http://localhost:9000 -room k3f9xq2b -name your-name" }
```

The placeholder is `your-name`, lowercase-with-a-hyphen, because it must itself be a **valid
peer name** — an earlier `YOUR_NAME` produced a copy button that handed out a command rejected
the instant it was pasted unchanged.

**CORS behaves differently from the upgrade, deliberately.** A matching origin is **echoed**,
never `*`. A request with **no** `Origin` (curl, a health checker) is answered normally with no
CORS headers at all. A **non-matching** origin gets the normal response **minus**
`Access-Control-Allow-Origin` — the browser blocks it, and the server does *not* 403, because a
distinguishable error would leak the allow-list to a probing page. The event-stream **upgrade**,
by contrast, *is* a 403.

### Demo controls

With `-demo` on, the meet detail view grows an **Evict** dropdown (pick a member) and a
**Force election** control (pick a member, or leave it empty for "best candidate"). Both are
`202 Accepted` and both **publish a `demo` event to everyone watching the meet**, including the
colleague looking at the same screen — which is why the event carries the caller's remote
address. The panel renders only when `demo_enabled` is true on `GET /api/meets`, derived
server-side from the same nil check that decides whether the routes exist, so the button and
the route cannot disagree. An older server that omits the field is treated as demo-**off**.

---

## The `/healthz` contract

`server` exposes three route groups: `GET /healthz`, `GET /ws`, and `/api/` (when `-dashboard` is on). The first two are wired with Go 1.22+ **method-aware** patterns, so the mux enforces the method and the handler never inspects `r.Method`. The `/api/` handlers deliberately do the opposite — they register without a method and dispatch on `r.Method` themselves — because the contract requires the `{code, message, details}` envelope on **every** non-2xx, and `ServeMux`'s own 404 and 405 responses are plain text.

| Request | Result |
|---|---|
| `GET /healthz` | `200 OK`, `Content-Type: application/json`, body `{"status":"ok","service":"conclave-server"}` |
| `POST` / `PUT` / … `/healthz` | `405 Method Not Allowed`, `Allow: GET, HEAD` — returned by the mux automatically, no handler code |
| any other path | `404 Not Found` |

The 200 body is a small typed struct (`healthResponse{Status, Service}`) with `json` tags fixing the lowercase wire names — the seed of the wire protocol Phase 1 grows.

**Raw `curl`:**

```console
$ curl -i http://localhost:9000/healthz
HTTP/1.1 200 OK
Content-Type: application/json
Date: Sun, 12 Jul 2026 06:01:07 GMT
Content-Length: 44

{"status":"ok","service":"conclave-server"}
```

```console
$ curl -i -X POST http://localhost:9000/healthz
HTTP/1.1 405 Method Not Allowed
Allow: GET, HEAD
Content-Type: text/plain; charset=utf-8
X-Content-Type-Options: nosniff
Content-Length: 19

Method Not Allowed
```

(`HEAD` appears in `Allow` because Go serves `HEAD` for any `GET` route automatically.) `curl -f http://localhost:9000/healthz` is a one-liner liveness check: `-f` makes curl itself exit non-zero on any 4xx/5xx.

---

## Logging: level and format

Both binaries build their logger once, at the edge of `main`, and inject it downward — **no global logger**. Two knobs control it.

### `-log-level` — how much

Records below the chosen level are dropped. Order: `debug < info < warn < error`.

| Level | Effect today | Use when |
|---|---|---|
| `debug` | Mechanism detail: SDP/ICE exchange, per-tick metrics, `forwarded keyframe request upstream`, `meet is settling`, re-parent state transitions. | A call that will not reach `connected`, or a tree that will not settle. |
| `info` (default) | Shows startup, per-request, and probe-outcome lines. | Normal dev. |
| `warn` / `error` | Suppresses the INFO lines above — e.g. `-log-level error` hides `server is healthy` and `http server listening`, surfacing only problems. | Quiet CI, or when you only care about failures. |

### `-log-format` — how it's shaped

| Format | Looks like | Use when |
|---|---|---|
| `text` (default) | `time=… level=INFO msg="…" service=conclave-peer …` | Reading at a terminal during development — easier on human eyes. |
| `json` | `{"time":"…","level":"INFO","msg":"…","service":"conclave-peer",…}` | Piping into a log aggregator or `jq`, or when you'll grep/correlate structured fields across a server + a tree of peers. |

An **unrecognized** `-log-format` falls back to `json` (structured output is the safe default) rather than erroring; an unrecognized `-log-level`, by contrast, is a hard startup error.

---

## Exit codes

Both binaries follow the same thin-`main` → `run() error` shape: `main`'s only job is to translate a returned error into an exit code and a `binary: message` line on stderr.

| Code | Meaning |
|---|---|
| `0` | Success. Server: shut down cleanly. Peer: server was reachable and healthy. |
| `1` | Any error — bad flag, unresolvable/`ftp://` `-server`, connect refused, timeout, non-200 status, undecodable body, or a real server bind failure (e.g. `-addr` port in use). The wrapped cause is printed to **stderr**. |

**Why this matters.** Exit codes are the contract shell, `make`, and CI read:

- `make check` aborts on the first failing target (`SHELL := bash`, `.SHELLFLAGS := -eu -o pipefail`) — a non-zero test/vet run stops the pipeline instead of scrolling past.
- A CI step or health gate can be just `./bin/peer -server "$URL"` — exit 0 means healthy, no output parsing required.
- Shell composition works: `./bin/peer && echo up || echo down`, or `if ./bin/peer -timeout 2s; then …`.

Logs go to **stdout**; error summaries go to **stderr**. That split lets you capture structured logs (`> run.log`) while still seeing failures, or discard logs and keep only the error (`2>&1 >/dev/null`).

---

## Graceful shutdown

`SIGINT` (Ctrl-C) or `SIGTERM` triggers an orderly drain, not an abrupt kill: the server stops accepting new connections, waits for in-flight requests (bounded by a 10s timeout so a stuck client can't block exit), and logs its progress. `http.ErrServerClosed` is treated as the benign "you asked me to stop" sentinel, so a clean stop still exits 0.

```console
$ ./bin/server -addr :7070
time=…+05:30 level=INFO msg="http server listening" service=conclave-server addr=:7070
^C
time=…+05:30 level=INFO msg="shutdown signal received, draining connections" service=conclave-server
time=…+05:30 level=INFO msg="server stopped cleanly" service=conclave-server
$ echo $?
0
```

This is the first appearance of **context-as-lifecycle** (`signal.NotifyContext`): one cancellation source that, from Phase 1 on, fans out to tear down goroutines, WebSockets, and peer connections together.

---

## What this does *not* prove

- **Phases 5 and 6 have no recorded live run.** They are verified by the automated suite — deterministic simulation of the control plane plus real-pion integration tests of the media plane — but not by a live multi-process demonstration of failover and migration (`DESIGN.md` §9.5).
- **Every live run so far has been single-host.** Multiple processes on one machine over loopback: no cross-machine result, no real NAT traversal, no real packet loss, no real congestion control. The "≈6 Mbit/s at 5 peers" figure is arithmetic on a measured per-stream bitrate, not an observed collapse.
- **One telemetry field is still declared, not measured.** `-upload-kbps` is an operator claim, and there is no bandwidth probe: measuring the uplink means saturating it, in a system whose whole thesis is that the uplink is the scarce resource. Everything else is live — CPU from `/proc/stat`, control-link RTT from a WebSocket ping, uplink loss from RTCP receiver reports, pairwise RTT from the nominated ICE candidate pair, and the NAT class from that pair's local candidate type (`DESIGN.md` §8.1).
- **Synthetic media isn't decodable.** Without `-media`, the sender emits opaque bytes — enough to prove RTP flows and `OnTrack` fires, but the recorded `.ivf` won't play.
- **Keyframe *response* is unproven.** File and synthetic sources have no live encoder, so PLI *plumbing* (including the upstream SSRC translation) is proven and "recover on demand" awaits a browser sender.
- **Recorded IVF header dimensions are `ivfwriter` defaults**, not the sender's frame size — the VP8 frames inside still decode at their true resolution.
- **`/healthz` is liveness, not readiness.** It reports "the process is up and routing", not "a call could succeed".
- **No authentication, anywhere.** No token, password, credential, JWT or TLS termination in any non-test file. Anyone who can reach `/ws` can join any meet under any unused name; anyone who can reach `/api` can create meets and read every meet's telemetry. What exists instead is server-stamped identity, the epoch fence, the origin allow-list, unguessable meet ids, and bounded state. Fine for `localhost` bring-up; see `deploy/README.md` before exposing anything.
- **No simulcast and no SVC.** A relay forwards one quality layer to every downstream. TURN and the measured NAT class *are* built, but **the TURN path has not been exercised live** — the relay allocates and the classification logic is pinned by tests, and the two have not yet been shown working together over a real ICE negotiation (`verify-turn.md`).
- **The NAT class is behavioural, not a NAT type.** `turn` means "all of this peer's media paths are relayed", not "this peer is behind a symmetric NAT". A peer that has connected to nobody reports `direct` because it has measured nothing.

For the phase plan see [`ROADMAP.md`](./ROADMAP.md); for why the system is shaped this way, [`DESIGN.md`](./DESIGN.md) is the single best explanation.
