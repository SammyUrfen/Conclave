# Troubleshooting

Failure modes for `conclave`, organized by phase, with a **Symptom → Likely cause → Fix** shape. Phase 0 entries are real and reproducible today. Everything under "Forward-looking" is distilled from `docs/ROADMAP.md` and labeled by the phase that first hits it — recorded now so the trap is already named when you walk into it.

This is a **living doc**: when a new failure mode bites during a phase, add it here with the fix that actually worked.

## Legend

| Marker | Meaning |
|---|---|
| **Now** | Actionable today. Phase 0 code exists; you can hit this on the current tree. |
| **Planned (Phase N)** | Not yet reachable — the code that fails this way lands in Phase N. Written ahead so the symptom is recognizable on first contact. |

---

## Phase 0 — bootstrap, build, run (Now)

### `listen tcp :9000: bind: address already in use`

- **Symptom:** `make run-server` exits immediately; the server log line for a bound address never appears, and stderr shows `conclave-server: ... address already in use`.
- **Likely cause:** Something already owns `:9000` — most often a previous `conclave` server you forgot to `Ctrl-C`, or an unrelated dev service. The default `-addr` is `:9000`.
- **Fix — option A, move out of the way:**
  ```
  make run-server ARGS="-addr :9090"
  ```
  Then point the peer at it: `make run-peer ARGS="-server http://localhost:9090"`.
- **Fix — option B, reclaim the port.** Find who holds it (Fedora ships `ss`):
  ```
  ss -ltnp 'sport = :9000'
  ```
  That prints the PID/`comm` in the `users:(("...",pid=NNNN,...))` field. If it's a stale `conclave` server, stop that PID specifically — **do not** `pkill` broadly (per the project safety rules; a wide pattern can take out unrelated processes). If `ss` isn't handy, `lsof -i :9000` or `fuser 9000/tcp` give the same PID.
- **Why the graceful shutdown matters here:** the server installs `signal.NotifyContext(SIGINT, SIGTERM)` and calls `srv.Shutdown`, so a clean `Ctrl-C` (SIGINT) *should* release the port and log `server stopped cleanly`. If you routinely see the port held after exit, you likely killed it with `SIGKILL` (`kill -9`) — that skips the shutdown path. Prefer SIGINT/SIGTERM so the socket closes.

### `make test` fails to build with a cgo / C-compiler error

- **Symptom:** `make test` never runs any test; it dies in the build with something like `cgo: C compiler "gcc" not found` or `exec: "gcc": executable file not found in $PATH`.
- **Likely cause:** `make test` runs `go test -race ./...`. The **race detector requires cgo**, and cgo requires a C compiler. On a box without `gcc`/`clang`, `-race` can't build.
- **Fix — keep race detection (preferred):** install a C compiler.
  ```
  sudo dnf install gcc
  ```
  (This machine already has `gcc 16`; this entry is for a fresh box or CI image.) Race detection is worth protecting — Phases 2–6 are concurrency-heavy and `-race` is the cheapest way to catch the data races those phases invite.
- **Fix — unblock without a compiler (temporary):** run the plain, non-race build directly, bypassing the Makefile target:
  ```
  go test ./...
  ```
  This loses the race detector. Treat it as a stopgap, not the default — re-enable `-race` (i.e. go back to `make test`) before trusting any concurrency change.

### `golangci-lint: command not found` (but `make lint` still passes)

- **Symptom:** `make lint` prints `golangci-lint: command not found` and then runs `go vet ./...` anyway, finishing green.
- **Likely cause:** `golangci-lint` isn't installed. This is **expected** — the `lint` target is written to **fall back to `go vet`** when the linter is absent, so a missing linter never blocks you; it just gives you thinner coverage.
- **Fix — install the real linter (optional, recommended before a phase wrap-up):**
  ```
  go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
  ```
  While you're at it, the other two tools the repo expects but doesn't ship:
  ```
  go install golang.org/x/tools/cmd/goimports@latest
  go install honnef.co/go/tools/cmd/staticcheck@latest
  ```
  All three land in `$(go env GOPATH)/bin`. If `make lint` still can't find `golangci-lint` after installing, that dir isn't on your `PATH` — add it (`export PATH="$PATH:$(go env GOPATH)/bin"` in your `~/.zshrc`).

### Peer exits 1 with `connection refused`

- **Symptom:** `make run-peer` logs a failed probe and exits non-zero; the error wraps `dial tcp 127.0.0.1:9000: connect: connection refused`.
- **Likely cause (in order of likelihood):**
  1. The server isn't running (nothing is listening at the target).
  2. The peer's `-server` doesn't match where the server actually bound — e.g. you moved the server to `:9090` (see the port-conflict fix above) but left the peer on the `http://localhost:9000` default.
  3. Right host, wrong scheme/port typed into `-server`.
- **Fix:** start the server first (`make run-server`), confirm the address it logged, then run the peer against that exact `host:port`:
  ```
  make run-peer ARGS="-server http://localhost:9090"
  ```
  `healthURLFor` accepts a bare `host:port` (it prepends `http://`) and rebuilds to `scheme://host/healthz`, so `-server localhost:9090` works too — but it **rejects** empty, non-`http(s)`, and hostless values with an error rather than guessing. If you get a parse/validation error instead of `connection refused`, the URL itself is malformed, not the connection.

### Peer exits 1 with `no Host in request URL`, or a `missing host` validation error

- **Symptom:** the peer fails immediately. On an older build it logged `probing server health` with a mangled `url=http://:/healthz` and then died with `peer: GET http://:/healthz: Get "http:///healthz": http: no Host in request URL`. On the current build it never probes and prints, e.g., `peer: invalid -server URL ":9000": missing host (did you mean "localhost:9000"?)`.
- **Likely cause (two that compound):**
  1. **Shell word-splitting inside `ARGS`.** `make run-peer ARGS="-server : http://localhost:9000"` is *three* arguments after the shell splits on spaces — `-server`, `:`, and `http://localhost:9000`. So `-server` consumed the bare `:`, and the real URL became a positional argument that `flag` silently ignores. There must be **no spaces** inside the value.
  2. **A bare `:port` doesn't name a host.** `:9000` mirrors the server's own `-addr :9000` (where an empty host legitimately means "all interfaces"), so it's a natural thing to type — but a *client* needs a host to dial. `url.Parse(":9000")` yields a non-empty `Host` (`":9000"`) but an **empty `Hostname()`**, which is why the earlier `u.Host == ""` guard missed it and built a hostless `http://:/healthz` that only blew up deep inside the HTTP client.
- **Fix:** give `-server` a real host, or omit it — the default is already `http://localhost:9000`:
  ```
  make run-peer                                    # uses the default
  make run-peer ARGS="-server localhost:9000"      # bare host:port is fine (http:// is prepended)
  make run-peer ARGS="-server http://localhost:9000"
  ```
  `healthURLFor` now validates `u.Hostname()` (not `u.Host`), so a hostless value **fails loud at startup** with a message naming the likely fix, instead of surfacing as a cryptic `no Host in request URL` later. Locked in by regression cases in `cmd/peer/main_test.go` (`bare colon`, `port only`, `scheme and port no host`).

### `unknown level "..."` at startup

- **Symptom:** either binary refuses to start; stderr carries an error mentioning an unknown level, sourced from `-log-level`.
- **Likely cause:** `-log-level` got a value `ParseLevel` doesn't recognize. The accepted set (case-insensitive) is exactly: `debug`, `info`, `warn`, `warning`, `error`. Common misses: `verbose`, `trace`, `warning` misspelled, or a stray quote/space.
- **Fix:** pass one of the accepted values.
  ```
  make run-server ARGS="-log-level debug"
  ```
  Note the intended contract: `ParseLevel` returns `LevelInfo` as its fallback value *alongside* the error, so the level itself is safe — it's the surfaced error that stops startup. Fix the flag rather than trying to ignore the message.

### `gofmt` / `go fmt` reports files (or CI would)

- **Symptom:** `make fmt` lists files, or you notice formatting drift — often from habits carried in from C/Java/JS (`var x int = ...` where `:=` reads cleaner, trailing semicolons, hand-aligned blocks).
- **Likely cause:** the file hasn't been run through `gofmt`. Go's formatting isn't a style opinion you can argue with — it's canonical; the toolchain and reviewers assume it.
- **Fix:** just format in place.
  ```
  make fmt
  ```
  Get this into your editor's save hook early so it never shows up in a diff. Once `goimports` is installed (see the lint entry), prefer it over bare `gofmt` — it also adds/removes imports, which matters a lot once real packages (`pion/webrtc`, `coder/websocket`) start moving between files. `make check` (fmt + vet + test) is the right gate before you consider a phase done — it lines up with *"never say done without verification."*

---

## Forward-looking: known hard parts by phase

These are the traps the roadmap already predicts. None are reproducible yet — the failing code lands in the labeled phase. Read the relevant one **before** starting that phase, not while bleeding.

### Phase 1 — ICE/DTLS never reaches `connected` — Now

> **Now real.** `internal/media/session.go` already registers all the state-change
> handlers below and logs each transition. Run either `peer -call` with
> `-log-level debug` to see offers/answers/candidates and the ICE/PC state climb,
> and diff the two peers' timelines. On **one host** ICE connects via host
> candidates (no STUN); for **two machines** pass `-stun stun:stun.l.google.com:19302`
> on both. If a peer never even starts negotiating, check it actually joined the
> room (`msg="joined room"` with the other peer in `peers`).

- **Symptom:** signaling completes (offer/answer exchanged, both sides think they're set up), but no media flows. The `PeerConnection` ICE state climbs to `checking` and stalls, or briefly reaches `connected` then drops to `disconnected`/`failed`. DTLS never finishes, so no SRTP keys, so no frames.
- **Likely cause (ranked):**
  1. A candidate the far side needs was **never relayed through signaling** — a gathered ICE candidate got dropped on the WebSocket path, so the peers never learn a reachable address pair. This is the single most common Phase-1 self-own.
  2. A local firewall (Fedora ships `firewalld` active by default) is dropping the inbound UDP the candidate advertises. ICE checks silently fail.
  3. Trickle-ICE ordering: applying a remote candidate before the remote description is set, so it's discarded.
- **Fix / method:** **log every state transition on both peers** and diff the two timelines. Register handlers for `OnICEConnectionStateChange`, `OnConnectionStateChange`, `OnICEGatheringStateChange`, and `OnSignalingStateChange`, and log each candidate as it's gathered *and* as it's applied. When it hangs, the mismatch is usually obvious: peer A gathered a candidate that never appears in peer B's "applied" log → it was dropped in signaling. If both sides show the candidates but checks still fail, suspect the firewall — test on loopback / same-host first (removes NAT and firewall from the equation), and only then move to two machines. Getting to `connected` reliably on one box is the real acceptance test for Phase 1; don't move on until the state log is clean twice in a row.
- **Go note:** these pion callbacks fire on **pion's own goroutines**, not yours. If a handler touches shared peer state, guard it (`sync.Mutex`) — this is the first place the injected-logger, no-globals discipline pays off, because every goroutine already has a `*slog.Logger` to stamp `peer_id=`/`state=` onto. Same locking discipline you used for the live-bidding race; new syntax, same problem.

### Phase 1 — call connects but `received.ivf` won't play — Now

- **Symptom:** both peers reach `connected` and the receiver logs `recording finished packets=N`, but the `.ivf` won't open in a player, or `ffprobe` shows unexpected dimensions.
- **Likely cause:**
  1. **The sender used synthetic media** (no `-media` flag). Synthetic frames are opaque bytes — perfect for proving transport, not decodable. Re-run the sender with a real VP8 IVF: `-media sample.ivf`.
  2. **IVF header dimensions look wrong** (e.g. 640×480 for a 320×240 clip). `ivfwriter` writes a fixed default container header; the VP8 frames inside carry the true resolution and decode fine. Cosmetic — confirm with `ffprobe -count_frames …` (frames decode) rather than trusting the header width/height.
- **Make a real sample:** `ffmpeg -f lavfi -i testsrc=duration=5:size=320x240:rate=30 -c:v libvpx -f ivf sample.ivf`.

### Phase 2 — mesh peers stuck at `new`, `rollback failed` in the logs — Now

- **Symptom:** two peers connect fine, but a 3rd+ peer joins and some `PeerConnection`s never leave `new`. The logs show `rollback failed … invalid SDP type supplied to SetLocalDescription(): rollback` followed by `set remote description … have-local-offer->SetRemote(offer)`.
- **Cause:** this was **our** bug, and worth recording because it defeats the textbook fix. The MDN "perfect negotiation" pattern resolves offer **glare** (both peers offer at once) by having the *polite* peer roll its local offer back and accept the other's. **pion v4 cannot do that** — its signaling state machine (`signalingstate.go`) has no `SetLocal`+rollback transition from `have-local-offer`, and `SetLocalDescription` rejects an empty-SDP rollback outright. A 2-peer call rarely triggers true glare, so the rollback code looked fine; a full mesh triggers it immediately.
- **Fix:** don't reconcile glare — **avoid it**. Use a **single-offerer** scheme: pick one deterministic offerer per pair (the higher peer id; `offerer=…` in the logs), and have the other *only* answer. No two simultaneous offers ever exist. Both directions still flow over one sendrecv m-line because the answerer adds its outbound track **before** it applies the offer, so pion folds that track into the offered m-line (an SDP answer can't add new m-lines — hence the ordering). An offerer that sends no media adds a recvonly video transceiver so it still has something to offer.
- **Method that found it:** `MESH_DEBUG=1 go test -run TestMesh ./internal/media` prints the offer/answer/rollback/ICE timeline — the `rollback failed` line points straight at the cause. Same "log every transition and diff the timelines" discipline as the ICE section above.

### Phase 3 — black video / frozen first frame on a relayed stream — Planned (Phase 3)

- **Symptom:** the direct-neighbor path works, but a viewer **downstream of a relay** sees a black or frozen frame, or video that only recovers after a long stall. Audio may be fine while video is dead.
- **Likely cause:** the decoder is waiting for a **keyframe (IDR)** it never gets. When a relay (the elected peer-SFU) starts forwarding to a new subscriber mid-stream, that subscriber joins between keyframes and can't decode the inter-frames it's receiving. The browser signals this with a **PLI (Picture Loss Indication)** / FIR RTCP feedback message — but that message has to travel **all the way upstream to the original sender**, which is the only node that can emit a fresh keyframe. If the relay swallows the PLI instead of forwarding it toward the source, the sender never re-encodes an IDR and the picture stays black.
- **Fix:** treat RTCP as a **first-class, bidirectional plane**, not an afterthought. On every relay hop, forward PLI/FIR upstream toward the original sender; do not terminate them at the relay. The sender, on receiving a PLI, asks its encoder for a keyframe. Validate by deliberately subscribing a new viewer several seconds into a stream and confirming a keyframe request propagates hop-by-hop to the source in your logs.
- **Why this is structural, not a bug:** a naive relay copies **RTP forward** and forgets the **RTCP backward** path. The peer-SFU is the novel core of the whole project — its correctness *is* "media survives N hops," and keyframe/PLI plumbing is exactly where a tree of relays diverges from a single direct connection. Budget real time for it.

### Phases 2–5 — goroutine leaks per connection — discipline applied Phase 2, churn-tested Phase 5

> **Phase 2 status:** the discipline below is now in place for the `Router`'s own goroutines — the upload-meter sampler and each per-peer media pump run under a `context` + `sync.WaitGroup`, and `Run` won't return until they've all exited (the mesh test would hang if one leaked). The *stress* case — many rapid join/leave cycles — still awaits the Phase-4 simnet goroutine-count assertion.

- **Symptom:** the process slowly grows memory and goroutine count across a session of peers joining and leaving; behavior degrades or the scheduler thrashes long after the peers that "own" the work have disconnected. Often invisible in a 2-peer test and only obvious once churn starts (Phase 5).
- **Likely cause:** a per-connection goroutine (RTP reader, RTCP handler, metrics ticker, signaling pump) was **started but never joined**. When the peer leaves, nothing tells that goroutine to stop, so it blocks forever on a channel/read that will never complete. Every leave leaks a fixed set of goroutines; over a long session they accumulate.
- **Fix / discipline (adopt this in Phase 2, before it hurts):**
  1. **Every goroutine gets a `context.Context`** derived from a per-connection parent context. When the connection ends, cancel that context; the goroutine's `select { case <-ctx.Done(): return; ... }` is its exit door.
  2. **Every goroutine you start, you join.** Track them in a `sync.WaitGroup` (or an explicit `done chan struct{}`) and `Wg.Wait()` during teardown so leave is *synchronous* — the connection isn't "closed" until its goroutines have actually returned. This is the same lifecycle rigor as your wait-for-graph teardown; the leak is just the un-joined thread you'd never have left dangling in C++.
  3. Pair `defer cancel()` with the context creation so an early return can't skip cleanup.
- **How to catch it:** run `make test` (i.e. `go test -race ./...`) — the race detector often surfaces the shutdown-ordering bugs that accompany leaks. In the simnet harness (Phase 4), **assert on goroutine count**: snapshot `runtime.NumGoroutine()` before a batch of joins, run a full join→leave cycle, and assert it returns to the baseline (allow for a stable pool). A deterministic simnet is the right place to make this a hard test rather than a hope, and it lines up with *"never say done without verification."*

### Phase 5 — the tree thrashes (constant re-optimization) — Planned (Phase 5)

- **Symptom:** under join/leave churn or noisy metrics, the coordinator rebuilds the relay tree repeatedly; peers get reparented over and over, each reparent causing a brief media hiccup (and, per Phase 3, a keyframe request). The overlay never settles even though the underlying network is basically stable.
- **Likely cause:** re-optimization is triggering on **metric wiggle** instead of on **sustained** change. A single high-latency sample, a momentary bandwidth dip, or one flapping peer is enough to knock a peer off the "best parent," and the greedy builder happily produces a different-but-not-better tree. This is a control-loop stability problem, not a graph-algorithm problem.
- **Fix — hysteresis, deliberately tuned:**
  - **Threshold events only.** Re-optimize on join, leave, or *sustained* degradation — never on every metric update. Debounce metrics with a rolling window / EWMA so one bad sample can't trigger a rebuild.
  - **Require a minimum improvement to act.** Only reparent if the new tree beats the current one by more than a margin (e.g. latency must improve by more than X% *and* hold for T seconds). A move that's marginally better isn't worth the reparent glitch it costs.
  - **Cooldown per peer.** After reparenting a peer, refuse to move it again for a cooldown window, so it can't ping-pong between two near-equal parents.
  - **Name every constant with its justification** (the window length, the improvement margin, the cooldown) — these are exactly the reward-shaping-style magic numbers that must not be bare. Expect to *tune* them against the simnet; there's no closed-form right answer, and honest Limitations note that hysteresis trades responsiveness for stability on purpose.

### Phase 6 — coordinator handover ambiguity window — Planned (Phase 6)

- **Symptom:** during an election or a coordinator migration, there's a window where **two nodes both believe they're coordinator**, or peers act on instructions from the **old** coordinator after a new one is elected. Stale graph decisions get applied; the overlay briefly follows two conflicting plans (a mini split-brain), even though the central arbiter is supposed to prevent exactly that.
- **Likely cause:** in-flight messages and state that predate the handover. A message sent by coordinator epoch *N* arrives at a peer after it has already learned about epoch *N+1*; without a way to reject it, the peer honors a stale command. The central server being the single source of truth for *who* is coordinator does **not** by itself make individual in-flight messages safe.
- **Fix — epoch/term fencing (this is the mechanism, not a nicety):**
  - Every coordinator authority is stamped with a **monotonically increasing epoch/term**, issued by the central arbiter. This is the same fencing-token idea from consensus systems (Raft terms, ZooKeeper `zxid`) — you've reasoned about it before; here it's the whole defense.
  - **Peers reject any message carrying an epoch lower than the highest they've seen.** A stale coordinator's commands are simply ignored, which collapses the ambiguity window to "harmless" instead of "split-brain."
  - The arbiter, not the peers, decides the epoch — that's the point of keeping control/bootstrap centralized. You are explicitly **not** running Raft/Paxos among home PCs; the arbiter is the tie-breaker, and epoch fencing is what makes a *single* arbiter enough.
- **How to make this testable:** this is a distributed-timing bug, and you cannot trust it to a manual two-machine run. Reproduce it **deterministically in the simnet** (Phase 4's harness): script the exact adversarial interleaving — old coordinator emits a command, new coordinator is elected, old command is delivered *after* — and assert every peer rejects the stale-epoch message. Handover is the single hardest thing in the project; the acceptance bar is a green simnet test for the ambiguity window, not "it looked fine once."

---

## When you add to this doc

Keep the **Symptom → Likely cause → Fix** shape, mark each entry **Now** or **Planned (Phase N)**, and prefer a concrete command or log line over prose. If a "Planned" trap turns real during its phase and the predicted fix was wrong, correct it here — a troubleshooting doc that lies is worse than none.
