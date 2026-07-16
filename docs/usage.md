# conclave — Usage

How to run and drive the two binaries: the central `server` and the `peer`. Everything below is real, current behavior — every log line and HTTP response in this doc was captured from the actual binaries, not hand-written. Anything not yet built is marked **Planned (Phase N)**.

> **Status — Phase 1 (signaling + 2-peer call).** The `server` answers `GET /healthz` **and** hosts the WebSocket signaling hub at `GET /ws?room=<id>`. The `peer` has two modes: the Phase 0 **probe** (dial `/healthz` once and exit) and **call** (`-call`: join a room and establish a WebRTC call, optionally sending a VP8 track and recording the received one). Media flows peer-to-peer over WebRTC (SRTP/UDP); the server only relays signaling frames.

---

## Prerequisites

| Need | Version / note |
|---|---|
| Go | 1.26.4 (uses Go 1.22+ method-aware routing: `"GET /healthz"`) |
| C compiler | gcc present — only needed because `make test` runs `-race`, which links cgo |
| `make` | optional; every target is just a thin wrapper over `go build`/`go run`/`go test` |

Optional tooling the Makefile *prefers but degrades without*: `golangci-lint` (falls back to `go vet`), `goimports`, `staticcheck`. Not yet installed on this machine.

---

## Quick start

```console
$ make build          # -> ./bin/server and ./bin/peer
$ ./bin/server &      # start the central node (text logs, :9000)
$ ./bin/peer          # probe it once; exits 0 if healthy
```

Or skip the build step and use `go run` via make:

```console
$ make run-server                    # foreground; Ctrl-C to stop cleanly
$ make run-peer                      # in another shell
```

---

## The two binaries

### `server` — central bootstrap / (future) election arbiter

Always-up control node. Today it serves `GET /healthz`; from Phase 1 on it grows room rendezvous, the WebSocket signaling relay, and — the reason it exists — the role of **election arbiter and single source of truth for "who is coordinator."**

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:9000` | TCP address to listen on, `host:port`. `:9000` binds all interfaces on port 9000; `127.0.0.1:9000` binds loopback only. |
| `-log-level` | `info` | Minimum level to emit: `debug` \| `info` \| `warn` \| `error`. Case-insensitive; `warning` is accepted as `warn`. Unknown value → startup error (exit 1). |
| `-log-format` | `text` | `text` (human `key=value`) or `json` (one object per line). Any unrecognized value falls back to `json` rather than erroring. |

### `peer` — participant node (probe + call)

A conclave participant with two modes. **Probe** (default) is the Phase 0 health check that proves the two binaries can reach each other. **Call** (`-call`) is the Phase 1 WebRTC path: join a signaling room and establish a peer-to-peer media connection with the other peer in it.

| Flag | Default | Meaning |
|---|---|---|
| `-server` | `http://localhost:9000` | Base URL of the server. A bare `host:port` is accepted as shorthand for `http://host:port`. In probe mode the scheme must be `http`/`https`; in call mode `ws`/`wss` are also accepted and the target becomes `ws(s)://host/ws`. A missing host or empty value is rejected (exit 1). |
| `-log-level` | `info` | Same semantics as the server. |
| `-log-format` | `text` | Same semantics as the server. |
| `-timeout` | `5s` | **Probe mode only.** Overall deadline for the probe, as a Go duration (`500ms`, `5s`, `2m`). Bounds DNS + connect + read together, so a hung or slow server can't wedge the peer forever. |
| `-call` | `false` | **Call mode switch.** Join a room and establish a WebRTC call instead of probing `/healthz`. Enables the flags below; the call runs until `Ctrl-C`. |
| `-room` | `default` | Room to join (`/ws?room=<id>`). Peers in the same room discover each other and connect. |
| `-send` | `false` | Add an outbound video track — i.e. offer media to the peer. Implied by `-media`. |
| `-media` | `""` | VP8 IVF file to stream (looped). Empty sends synthetic, non-decodable frames that still prove the transport (RTP flows, the far side's `OnTrack` fires). |
| `-record` | `""` | Write the first received track to this IVF file. Empty just counts packets. A real VP8 sender produces a playable file. |
| `-stun` | `""` | STUN server URL, e.g. `stun:stun.l.google.com:19302`. Empty is fine on one host (host candidates connect directly); needed for two machines behind NAT. |

---

## Make targets

Run `make` (or `make help`) for the live list. Pass binary flags through the `ARGS` variable.

| Target | Does |
|---|---|
| `make help` | List targets (default goal). |
| `make build` | Compile both binaries into `./bin/server`, `./bin/peer`. |
| `make run-server` | `go run ./cmd/server $(ARGS)`. |
| `make run-peer` | `go run ./cmd/peer $(ARGS)`. |
| `make test` | `go test -race ./...` — all packages, race detector on. |
| `make cover` | Run tests with a coverage profile and print a per-function report. |
| `make fmt` | `go fmt ./...`. |
| `make vet` | `go vet ./...`. |
| `make tidy` | `go mod tidy`. |
| `make lint` | `golangci-lint run` if installed, else prints an install hint and runs `go vet`. |
| `make check` | `fmt` + `vet` + `test` — the pre-commit gate. |
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

---

## The `/healthz` contract

`server` exposes a single route today, wired with Go 1.22+ **method-aware** patterns (`mux.HandleFunc("GET /healthz", …)`). The mux itself enforces the method, so the handler never inspects `r.Method`.

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
| `debug` | No extra output yet — Phase 0 code emits only INFO/ERROR records. Wired for later. | Future: verbose tracing of signaling/ICE. |
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

## What this does *not* prove yet (Phase 1 limitations)

- **Two peers, not a mesh.** A room can hold more, but each `peer` only wires up one remote peer's media policy cleanly; N-way mesh (and feeling its upload ceiling) is *Phase 2*, and forwarding media *through* a participant — the elected peer-SFU — is *Phase 3*.
- **No relay/SFU, no election, no metrics.** Media is direct peer-to-peer; there is no coordinator, no graph, no telemetry yet. *(Planned: Phases 3–6.)*
- **Synthetic media isn't decodable.** Without `-media`, the sender emits opaque bytes — enough to prove RTP flows and `OnTrack` fires, but the recorded `.ivf` won't play. Use a real VP8 `.ivf` for a watchable result.
- **Recorded IVF header dimensions are `ivfwriter` defaults** (it writes a fixed header), not the sender's frame size — the VP8 frames inside still decode at their true resolution.
- **`/healthz` is liveness, not readiness.** It reports "the process is up and routing," not "a call could succeed."
- **No TLS / auth.** Plain HTTP/WS, no authentication, no origin allow-list configured — fine for `localhost` bring-up, not for anything exposed.
- **`-log-level debug` now traces negotiation** — `sent offer`, `sent answer`, `rolled back local offer`, per-candidate sends — which is exactly what to turn on when a call won't reach `connected` (diff the two peers' state timelines).

For the full phase plan and the architecture rationale (why the control plane is centralized while the data plane is decentralized, why the relay tree stays shallow, why election uses a central arbiter instead of Raft), see `docs/ROADMAP.md`.
