# Troubleshooting

Failure modes for `conclave`, with a **Symptom → Likely cause → Fix** shape. As of **Phase 6**
almost everything here is reproducible on the current tree; the few remaining forward-looking
entries are marked.

This is a **living doc**: when a new failure mode bites, add it here with the fix that actually
worked.

## Legend

| Marker | Meaning |
|---|---|
| **Now** | Actionable today — you can hit this on the current tree. |
| **Planned (Phase N)** | Not reachable yet; the code that fails this way is not written. |

## Start here

Three habits answer most questions faster than this document:

1. **Read the two startup lines.** `cmd/server` logs `arbiter starting` (`coordinate`, `elect`, `dashboard`, `demo`, `arbiter_id`, `allowed_origins`, `socket_detection`) and, when enabled, `coordinator enabled` (`max_depth`, `stream_kbps`, `stickiness_ms`, `join_settle`, `dwell`, `gone_after`). Several knobs resolve to package defaults when a flag is zero, so **the flags you typed do not answer "what is this process actually running with" — those two lines do.**
2. **Error summaries go to stderr, prefixed with the binary name** (`server: …`, `peer: …`); structured logs go to stdout. `2>/dev/null` and `>/dev/null` therefore isolate different things.
3. **A self-contradictory configuration does not start.** Every check below the flag parser runs before the listener opens. If the process is running, its configuration is at least internally consistent.

---

## Build, run, and flags (Now)

### `listen tcp :9000: bind: address already in use`

- **Symptom:** `make run-server` exits immediately; the server log line for a bound address never appears, and stderr shows `server: listen tcp :9000: bind: address already in use`.
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

### `go test ./...` fails in `internal/media` but `make test` is green

- **Symptom:** plain `go test ./...` reports
  ```
  --- FAIL: TestPionRemoveTrackDoesNotRenegotiate (0.21s)
      pionbehavior_test.go:85: pion NOT to fire negotiation-needed for a removal stopped holding
  ```
  while `make test` (`go test -race ./...`) passes every package.
- **Cause:** that test pins a *pion* behaviour inside a real-time window — pion must **not** fire negotiation-needed within 500 ms of a `RemoveTrack`. Race instrumentation slows the binary enough to change which side of that window pion's ops goroutine lands on. It is reproducible in both directions on this machine: 5/5 failures without `-race`, 3/3 passes with it. It is **not** version drift — `pion/webrtc/v4` is `v4.2.16`, exactly the version the finding was measured against.
- **Fix:** use `make test` / `make check`. **The gate the project defines is `-race`**, and CI runs the same thing. Do not "fix" the test by loosening its window — if this pin ever fails *under `-race`*, the correct response is to delete `pendingLocalChange` (pion started renegotiating removals on its own), not to relax the assertion.
- **Why it lives outside the determinism gate:** `internal/media` is deliberately excluded from `make check-determinism` because it runs real pion, real ICE and real DTLS. This is exactly the class of wall-clock-dependent test the control-plane rules exist to keep out of the control plane.

### `make check-determinism` fails

- **Symptom:** `make check` (or CI's `determinism` step) prints one or more `file.go:NN: … time.Now(…)` lines followed by `^ the control plane must use the injected clock.Clock`.
- **Cause:** a package in `internal/overlay`, `internal/simnet`, `internal/coordinator` or `internal/arbiter` — **including its `_test.go` files** — called one of `time.Now|Since|Until|Sleep|After|AfterFunc|Tick|NewTimer|NewTicker`. The list is exhaustive on purpose: *a guard that catches `time.Now` but misses `time.NewTicker` is worse than no guard, because it reads as coverage.*
- **Fix:** take a `clock.Clock` on the surrounding config struct (defaulting to `clock.System()` when nil) and use it. In a test, use `simnet.NewVirtualClock(simnet.DefaultStart)`.
- **If you genuinely need a real-time deadlock guard** — a harness that must not hang forever — use the shape the harnesses already use: a `const` duration plus `context.WithTimeout`, never `time.After`. That gives you a real-time bound without a banned call, and it is why `simnet.SettleTimeout` does not trip the gate.
- **There is no escape hatch.** No `//nolint`, no skip list. If a control-plane package needs the wall clock, the design is wrong, not the gate.

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
  Get this into your editor's save hook early so it never shows up in a diff. Once `goimports` is installed (see the lint entry), prefer it over bare `gofmt` — it also adds/removes imports, which matters a lot once real packages (`pion/webrtc`, `coder/websocket`) start moving between files. `make check` (fmt + vet + check-determinism + test -race) is the right gate before you consider a phase done — it lines up with *"never say done without verification."*

---

## Startup refusals — the server will not run a contradictory config (Now)

Every check below runs inside `run()`, **before the listener opens**, and returns rather than
logging-and-continuing: *a self-contradictory configuration must not produce a running server.*
All of them print one sentence to stderr prefixed `server: ` and exit 1.

| You typed | You get |
|---|---|
| `-log-level trace` | `logging: unknown level "trace" (want debug\|info\|warn\|error)` — accepted set is `debug`, `info`, `warn`, `warning`, `error`, case-insensitive |
| `-log-format tex` | `logging: unknown -log-format "tex" (want text\|json)`. **Note the asymmetry:** `logging.New` itself falls back to JSON for anything unrecognised, so without this explicit parse step a typo would start a server logging JSON and say nothing. |
| `-degraded-after 8s -gone-after 8s` | `-degraded-after (8s) must be strictly less than -gone-after (8s)` |
| any of `-join-settle -dwell -recompute-cooldown -degraded-after -gone-after -election-dwell -min-term` set to `0` or negative | `<flag> must be > 0, got <v>` |
| `-coordinate -stream-kbps 0` | `-stream-kbps must be > 0, got 0` |
| `-coordinate -max-depth 0` | `-max-depth must be >= 1, got 0` |
| `-coordinate -stickiness-ms -1` | `-stickiness-ms must be >= 0, got -1` |
| `-root-change-margin-kbps -1` | `-root-change-margin-kbps must be >= 0, got -1` |
| `-root-change-margin-kbps 3000` | Refused, with the reason: `overlay` froze the margin as the **compile-time constant** `overlay.RootChangeMarginKbps` and exposes no `Constraints` field for it, so only the default value is achievable. A flag that silently did nothing would be worse. |
| `-allowed-origins ""` with `-dashboard` on (the default) | `-allowed-origins is empty while -dashboard is on: the dashboard is a cross-origin browser surface and would refuse every request` |
| `-allowed-origins "http://*.example.com"` | `'*' is only allowed as a trailing port wildcard ("host:*")`. Also rejected: an empty entry, `*` in the scheme, more than one `*`, and the metacharacters `?` `[` `]` `\`. |
| `-public-url` that is not an `http(s)`/`ws(s)` URL | refused by `dashboard.New`'s `deriveBase` |
| `-gone-after` shorter than the Hub's socket-detection window | see the liveness-budget section below |

`cmd/peer` has the same posture (`peer: ` prefix): `-managed and -topology are mutually
exclusive: a pushed tree or a static one, not both`; `-managed requires -name so the
coordinator can place this peer in the tree`; `-topology requires -name so this peer can
locate itself in the tree`; `invalid -nat "nat44": must be direct or turn`; `invalid
-heartbeat 500µs: the wire carries whole milliseconds, so anything under 1ms would be sent as
0 and read back as the 1s default`.

### "I set `-upload-kbps` and nothing changed"

Not an error — a **warning**, and easy to miss:

```
WARN flag has no effect in this mode flag=upload-kbps
```

`-backup`, `-coordinatable`, `-heartbeat`, `-nat` and `-upload-kbps` are managed-mode flags.
Only flags you **explicitly set** are named, so the warning never fires on defaults. The fix
is almost always that you also need `-managed` (and `-name`).

---

## A peer is refused for a bad name or a bad meet id (Now)

**The surprising part first:** the WebSocket upgrade **succeeds**, so the peer logs
`connected to signaling` and looks healthy. The refusal arrives as an application frame and
then the socket is closed *without a closing handshake* — so there is **no WebSocket close
code**; a browser sees `1006`. The observable symptom is a successful connect immediately
followed by a warn and an exit.

- **Symptom (peer log):**
  ```
  INFO connected to signaling …
  WARN signaling error frame  error="peer name is invalid; it must match ^[a-z0-9][a-z0-9_-]{0,63}$"
  ```
  or `meet id "My Room" is invalid; it must match …`, or `name "alice" is already in use in room "standup"`.
- **The rules.** `policy.MeetIDPattern` and `policy.PeerNamePattern` are **identical**: `^[a-z0-9][a-z0-9_-]{0,63}$`. Lowercase letters, digits, `_` and `-`; 1–64 characters (64 is the longest single DNS label); **the first character may not be `-` or `_`, so a name can never read as a command-line flag when pasted.** No uppercase, no spaces, no `.`, no `/`.
- **Two special cases.** An **empty peer name is legal** — it means "unnamed", which is what a plain mesh peer is, and it is exempt from the duplicate-name check. An **empty meet id is not**; `/ws` substitutes the default room when `?room=` is absent. `_server` is a reserved *sender* id and is already excluded by the pattern's first-character rule.
- **Fix:** rename. Callers **reject, never truncate or normalise** — a silently rewritten id would put you in a different meet than the one you asked for, which is far harder to debug than a refusal.
- **Server-side log:** the invalid-name warning records `name_len`, **not the name** — deliberately, so attacker-controlled unbounded text never reaches the log.

### The pasted join command is rejected

`POST /api/meets` returns a ready-to-paste `join.peer_command` containing the placeholder
`your-name`. It is lowercase-with-a-hyphen on purpose: an earlier `YOUR_NAME` failed
`ValidPeerName`, so the copy button handed out a command that was rejected the instant it was
pasted unchanged. If you get a name rejection from a pasted command, either you left the
placeholder in, or you are on an old build.

---

## Origin rejection — a 403, not a close code (Now)

This is the entry most likely to be misdiagnosed, because the three surfaces fail three
different ways **on purpose**.

| Surface | A disallowed origin gets |
|---|---|
| `GET /ws` (the peer hub) | **HTTP 403** before the upgrade, body `origin not allowed` (plain text, not the JSON envelope). Server logs `rejected websocket upgrade from unlisted origin` with `origin` and `host`. |
| `GET /api/meets/{id}/events` (the dashboard stream) | **HTTP 403** before the upgrade, with the `{code, message, details}` envelope, code `forbidden_origin`. |
| `GET/POST /api/…` (REST) | **A normal response with the normal status, minus `Access-Control-Allow-Origin`.** The *browser* blocks it. Deliberately **not** a 403: a distinguishable error would let a probing page enumerate the allow-list. |

**There is no `1008` close code for an origin.** The rejection happens on the handshake, so no
WebSocket is ever established and no close code can be sent. (An earlier revision of the
contract listed 1008 here and was wrong.) Do not "improve" this by accepting the upgrade and
then closing.

- **Browser symptom:** the dashboard shows `rejected` and stops retrying, with a fatal reason of `origin_rejected`. The frontend distinguishes a refused handshake from a real mid-session drop by whether the socket had **ever** opened — a `1006` before any successful open is treated as a permanent origin refusal, because retrying an origin the server will not accept cannot help.
- **Fix:** add your origin to `-allowed-origins`. Default is `http://localhost:*,http://127.0.0.1:*,https://sammyurfen.github.io`. A trailing `*` is a **port** wildcard only, and `http://localhost:*` does **not** match a port-less `http://localhost`.
- **The gotcha worth internalising:** a page served from **the arbiter's own origin is no longer auto-allowed** and must be listed. Self-origin auto-allow *is* the DNS-rebinding vector — `coder/websocket`'s own origin check allows as soon as the `Origin` header's host equals the `Host` header, before it consults `OriginPatterns` and ignoring the scheme, and `Host` is client-supplied. That is why both upgrade paths gate on `policy.Origins.AllowUpgrade` first and then set `InsecureSkipVerify: true` — the reversed-looking line is turning the library's shortcut *off*.
- **A request with no `Origin` header at all is allowed** (Go peers send none, and curl sends none), and REST answers it with no CORS headers, because there is nothing for a non-browser to be protected from.

---

## The server refuses to start on the liveness budget (Now)

- **Symptom:**
  ```
  server: -gone-after=3s is shorter than the WebSocket liveness budget 7s: the control plane
  would declare a peer gone while the Hub still holds its socket, and a peer whose socket never
  closed cannot re-announce itself; raise -gone-after to at least 7s
  ```
- **The relation being checked.** The Hub's worst case for noticing a dead socket is `WSPingInterval + WSPingTimeout` = 5s + 2s = **7s**. The coordinator's "gone" threshold defaults to `GoneBeats(8) × HeartbeatInterval(1s)` = **8s**, or `-gone-after` when set. **The health FSM must be the slower detector.**
- **Why the inequality runs that way.** Reversed, a peer gets ejected from the tree while the Hub still considers it present — and since `TypeJoined` is only sent on a *new* connection, a peer whose socket never closed could never rejoin. It would be permanently ejected from a meet it believes it is still in, heartbeating forever into a coordinator with no record of it, while the dashboard cheerfully shows the meet healthy. That was a real bug.
- **Fix:** raise `-gone-after` to at least the budget the message names, or shorten the ping interval/timeout.
- **The half no flag check can reach.** Because per-node thresholds are derived from the cadence the **peer** declares (`Heartbeat.IntervalMs`), a peer running `peer -heartbeat 500ms` shrinks *its own* threshold to 4s — under the 7s window — from a place the server's flag validation cannot see. The coordinator therefore **floors** every per-node gone threshold at `SocketDetection`. (The floor is deliberately *not* applied to the degraded threshold, because degraded is advisory.) The effective value is logged at startup as `socket_detection`.

---

## The dashboard says `settling`, `relaxed`, or `unbuildable` (Now)

These are the four `coordinator.BuildOutcome` values (with `built` being the normal one). They
are three completely different situations that look similar on a screen.

### `settling` — normal, not a fault, and logged at DEBUG on purpose

- **Meaning:** the meet has not met the eligibility rule yet, so no tree was built. Emitted once per *transition* into ineligibility, never per suppressed event — the latter would be a frame storm at exactly the moment the dashboard is connecting.
- **The three reasons, verbatim:** `waiting for first telemetry from the fleet`; `meet has N named member(s); 2 are needed for a tree`; `rebuilding from peers: waiting for realized topology` (that last one is a coordinator handover in progress).
- **Why DEBUG and not WARN:** *a warning that fires on every healthy startup teaches operators to ignore warnings.*
- **Action: none.** It is bounded by `JoinSettle` (1.5 s) per join episode. If it *persists*, the meet genuinely has fewer than two **named** members — check that your peers passed `-name`.

### `relaxed` — a rare, expensive **success**

- **Meaning:** the stability-preserving build failed, so the coordinator retried once from scratch (`prev = nil`) and that succeeded. **A tree was published.** Expect nearly every peer to re-parent, i.e. a visible hiccup for everyone.
- **Log:** `WARN stability-preserving build failed; published a relaxed tree`, then `WARN published topology`. The event's `reason` carries the **first (sticky) attempt's** error text, because that error is the only explanation of why everyone is about to move.
- **Action: watch, do not act.** One relaxed build is fine. *A meet that goes relaxed repeatedly is telling you the fleet is chronically near its capacity bound* — that is when you raise upload budgets, raise `-max-depth`, or lower `-stream-kbps`.
- It does **not** bypass `-recompute-cooldown`, and there is no "relaxed mode" to get stuck in: the next recompute starts sticky again from the newly published tree.

### `unbuildable` — the one that needs a human

- **Meaning:** no legal tree exists for this fleet. **The previous tree is kept**, so whoever was already connected stays connected — but a new or orphaned peer is receiving nothing. The dashboard renders it as a persistent critical banner.
- **Log:** `WARN no tree exists for this fleet; keeping the previous topology`, with a `reason`.
- **Three distinct causes, told apart by the reason string:**
  1. `no eligible root: no member can serve a child` — nobody advertised enough upload. Short-circuits before either build attempt, because dropping the stability preference does not create capacity.
  2. A `build tree: …` message from the builder, e.g. `cannot attach "leaf-e" — no relay has spare upload within depth 2 (fleet is over-constrained)` or `root "relay" cannot serve any children (… effective of … advertised < stream cost …, or TURN-bound)`. Genuinely over-constrained.
  3. `computed tree failed Validate: validate: …` — **this one is a bug report, not a tuning knob.** The builder produced a tree that fails its own oracle. Logged at ERROR. Never published: *a stale tree still carries media, and an invalid one is the mysterious missing stream this project refuses to ship.*
- **Fix for (1) and (2):** drop a peer, raise `-max-depth`, lower `-stream-kbps`, or give a peer a real `-upload-kbps`. Because the two-attempt build eliminated "stickiness painted us into a corner", `unbuildable` now genuinely means over-constrained.

### Two adjacent states that are *not* faults

- **`degraded` is advisory only.** It colours the dashboard and arms the dwell; **nothing re-parents on it.** It arms only after an unbroken run of bad samples lasting `-dwell` (10 s) on `LossPct ≥ 5`, `RTTServerMs ≥ 400` or `CPUPct ≥ 90` — and since none of those three fields is populated in production, **you will never see it live.** (See `DESIGN.md` §8.1.)
- **A health event where `prev_health == health` is a valid shape, not a bug** — sustained degradation and recovery both ride it.

### `rebuild-from-peers` degraded the handover

Two reasons appear on the first topology of a new term, with the *same* visible symptom
(everyone re-parents) and completely different fixes:

- `rebuild-from-peers: no realized topology was reported; this handover degraded to a full rebuild` — the fix is on the **peer**: its heartbeat is not populating parent/children.
- `rebuild-from-peers: the reported realized topology is not a legal tree` (with the validator's complaint appended) — the fleet really was torn mid-re-parent when the handover landed.

---

## A peer receives nothing and the meet keeps re-publishing (Now)

- **Symptom:** the dashboard shows the peer heartbeating and healthy, the coordinator keeps publishing trees, and that one peer's media never arrives. Its `stale_rejected` count climbs.
- **Cause:** the peer's fence is refusing every push. `Fence.Accept` requires **all** of: the peer has been told who is in charge (`Epoch != 0`); the sender *is* that coordinator; the topology's epoch **exactly equals** the peer's; and its `Rev` is strictly newer. A peer that missed the coordinator announcement sits at epoch 0 and rejects everything — indefinitely, in a static meet, because nothing else would prompt a re-announce.
- **What closes it, and what to look for:** the arbiter re-sends the current announcement **verbatim and unicast** on any heartbeat whose epoch lags, past `RebuildWindow` since the term started — no cap and no backoff, because the condition is indefinite so the repair must be. The dashboard shows this on **transition only**, as a distinct `announce_repair` event kind, deliberately kept out of the election log (whose value is that it lists *real* leadership changes) and deliberately pairable with the peer-side `stale_rejected` symptom so you can correlate cause and effect.
- **If `announce_repair` is not firing either**, the peer is not reaching the arbiter at all — check the socket, not the fence.
- **Note on the counters:** the meet-wide `stale_rejected` and a `stale` event's `data.total` are **different counters**, and the per-event one can legitimately decrease. Do not read one as a running total of the other.

---

## A negotiation ladder wedged (Now)

There are **two** ladders in the Go peer plus one in the browser. Do not conflate them.

### The WebRTC negotiation ladder (`internal/media/session.go`)

Two rungs run in parallel structures:

- **Error ladder** — an attempt that errored (`create offer`, `set local description (offer)`, `send offer`, `resend offer`). `WARN negotiation attempt failed; retrying op=… attempt=0..4`, `NegotiationRetryDelay = 250ms` apart (~1.25 s total).
- **Answer-deadline ladder** — the offer went out and no answer came. `WARN no answer for our offer; re-sending attempt=0..4`, every `NegotiationAnswerTimeout = 2s` (~10 s total). This exists for the **role-inversion window**: during an inversion the far end still holds a session baked as offerer and correctly refuses an inbound offer, so the offer is legitimately dropped and must be re-sent.

Both end at `NegotiationRetries = 5`:

```
ERROR no answer after the full retry ladder; this session can no longer offer  attempts=6
ERROR giving up on negotiation; this session can no longer offer               op=… attempts=6
```

- **What "wedged" actually means.** The `PeerConnection` is left in `have-local-offer`; both guards in the negotiation handler then refuse every future offer; and **a coordinator re-push changes nothing**, because the diff sees that neighbour as live, wanted and same-role, so the session survives untouched and every new forwarded track lands on a pc that will never offer again. *"Giving up is self-healing" is false*, and an earlier comment claimed it.
- **The recovery, and the log line that proves it ran:** exhaustion reports itself, and the Router **re-creates the edge**: `WARN re-creating a session that exhausted its negotiation ladder peer_name=…`. If you see the ERROR without that WARN, check whether the peer is a *pending re-parent target* — in that case the re-parent deadline deliberately owns the outcome instead (`DEBUG negotiation exhausted on a pending new parent; the re-parent deadline owns it`).

### The re-parent ladder (`internal/media/reparent.go`)

Timers: `ParentDisconnectGrace = 2s` (how long `disconnected` is tolerated before treating the
parent as failed), `ReparentConnectTimeout = 5s` (wait for the new parent's session to reach
`Connected`), `ReparentMediaTimeout = 3s` (after `Connected`, wait for **actual media** —
success requires media, not just ICE).

Log lines, in rough order of how much they should worry you:

- `WARN re-parent target not in the roster yet new_parent=…` — transient.
- `WARN re-parent failed new_parent=… reason=…` — the backup is tried once, then the peer falls back to its old parent.
- `WARN parent lost with no usable backup parent=…` — expected for a **direct child of the root**, which has no backup by construction. Otherwise the fleet was too tight to assign one.
- `WARN internal event queue full; dropping` — the 32-deep internal channel overflowed; something upstream is producing events far faster than the state machine can drain them.

A **failed** promotion is not silent: the peer reports `OK: false`, and a stranded peer
**bypasses the recompute cooldown** so the coordinator repairs it promptly rather than waiting
out the cooldown.

### The browser reconnect ladder (`web/js/api.js`)

`500 → 1000 → 2000 → 4000 → 8000 → 15000 ms`, each ±20 % jitter; the index resets only if the
connection that just ended lived **≥ 30 s**.

Close-code policy: `1000`/`1011` reconnect; `1001` (meet ended) and `4404` (no such meet) stop
with a fatal reason and status `gone`; `1008` stops permanently; `1006` **before the socket
ever opened** is treated as an origin refusal (above); `1006` after an open reconnects.

> **A fixed bug worth recognising if you run an old build.** `connectedAt` records *when* the
> connection that just ended was established, not whether you are still inside it — and those
> two facts diverge the instant the socket closes. Without clearing it on close, every failure
> after one 30-second-plus connection computed "survived" forever (the elapsed time only
> grows), so the backoff index reset to 0 on **every** failure and the ladder never climbed: a
> dead server got hammered roughly twice a second, indefinitely. Symptom: a flood of failed
> `GET /api/meets/{id}/events` upgrades from a tab that had been open a while. Post-fix the
> ladder climbs and settles at one attempt per ~15 s.
> `web/tests/reconnect-backoff.test.html` pins the exact sequence — open it in a browser.

---

## The `/api` surface returned an error (Now)

Every non-2xx carries the same envelope, and `details` is never absent (the client
dereferences it unconditionally):

```json
{"api_version":1,"error":{"code":"…","message":"…","details":{}}}
```

| Code | Status | When |
|---|---|---|
| `bad_request` | 400 | unreadable body, invalid JSON, or a missing required `name` on a demo action |
| `invalid_meet_id` | 400 | a `{id}` path segment or a client-supplied id that fails `policy.MeetIDPattern` |
| `forbidden_origin` | 403 | **upgrade only** — the event stream, never REST |
| `meet_not_found` | 404 | no such meet |
| `member_not_found` | 404 | no such member in this meet |
| `not_found` | 404 | no such endpoint (the `/api/` catch-all) — **and what you get for a demo route when `-demo` is off** |
| `method_not_allowed` | 405 | wrong method; the `Allow` header is set |
| `meet_exists` | 409 | that meet already exists |
| `no_candidate` | 409 | no peer in this meet is eligible to coordinate |
| `too_many_meets` | 429 | at the `MaxMeets` (100) limit |
| `internal` | 500 | see below |

Three things to know:

- **The mapping is on error *sentinels* via `errors.Is`, never on message text.**
- **A 500 is deliberately opaque to the caller.** The real error only goes to the server log line `dashboard request failed`, which carries `op` and `error` fields. When a user reports a 500, grep the server log for `op=`.
- **`-demo` off gives you 404, not 403 or 405,** because the routes are **not registered at all**. That is the design: an unregistered route cannot be reached by a bug in a permission check. The dashboard learns whether the buttons should exist from `demo_enabled` on `GET /api/meets`, derived from the same absent-dependency check — so the button and the route cannot disagree.

Event-stream close codes: `4404` invalid/unknown meet · `1000` server shutting down · `1013`
too many viewers (chosen over 1008 because retrying *later* genuinely can help, where 1008
would tell the client to stop forever) · `1008` malformed frame or unknown op · `1001` meet
ended · `1011` snapshot failed.

Success statuses: `200` list/get · `201` + `Location: /api/meets/{id}` on create · `202` on a
demo action · `204` on a CORS preflight.

---

## The dashboard shows two different trees (Now — and this is correct)

- **Symptom:** the meets **list** and the meet **detail** disagree about who is parented to whom.
- **Cause:** they are two different truths, and the UI labels them rather than averaging them. The list endpoint is **REALIZED** — reconstructed from heartbeats, i.e. what peers actually did. The detail endpoint and the WS snapshot are **INTENDED** — the coordinator's last published tree. They genuinely disagree during convergence.
- **Fix: none needed.** The gap is diagnostic. A UI that rendered whichever it fetched last would be confidently wrong at exactly the moments that matter, so `converged`/`diverged` is computed and shown instead. If the divergence *persists*, that is the real signal — look for a fenced-out peer or a wedged edge.
- Related: the per-member fitness value is named `fitness_lower_bound` on the wire, because uptime is unreachable from a member snapshot — *a value quietly 0.15 too low is invisibly wrong*, so the name says so.

---

## The media plane and the classic traps

These were the traps the roadmap predicted. As of Phase 6 all of them are reachable on the current tree; each is annotated with what the build actually did about it.

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

### Black video / frozen first frame on a relayed stream — Now

- **Symptom:** the direct-neighbor path works, but a viewer **downstream of a relay** sees a black or frozen frame, or video that only recovers after a long stall. Audio may be fine while video is dead.
- **Likely cause:** the decoder is waiting for a **keyframe (IDR)** it never gets. When a relay (the elected peer-SFU) starts forwarding to a new subscriber mid-stream, that subscriber joins between keyframes and can't decode the inter-frames it's receiving. The browser signals this with a **PLI (Picture Loss Indication)** / FIR RTCP feedback message — but that message has to travel **all the way upstream to the original sender**, which is the only node that can emit a fresh keyframe. If the relay swallows the PLI instead of forwarding it toward the source, the sender never re-encodes an IDR and the picture stays black.
- **Fix:** treat RTCP as a **first-class, bidirectional plane**, not an afterthought. On every relay hop, forward PLI/FIR upstream toward the original sender; do not terminate them at the relay. The sender, on receiving a PLI, asks its encoder for a keyframe. Validate by deliberately subscribing a new viewer several seconds into a stream and confirming a keyframe request propagates hop-by-hop to the source in your logs.
- **Why this is structural, not a bug:** a naive relay copies **RTP forward** and forgets the **RTCP backward** path. The peer-SFU is the novel core of the whole project — its correctness *is* "media survives N hops," and keyframe/PLI plumbing is exactly where a tree of relays diverges from a single direct connection.

> **What shipped, and the trap inside the trap.** `requestUpstreamKeyframe` does **not** forward the child's PLI verbatim. A child's PLI names **the SSRC the relay assigned it**; forwarding that upstream makes the original source ignore it as an unknown SSRC — black video, no error, no log line. The relay builds a *fresh* PLI carrying the source's own upstream SSRC, throttled at 300 ms **per source, not per child** ("a keyframe is not per-subscriber"). Turn on `-log-level debug` and look for `forwarded keyframe request upstream`.
>
> **Two honest limits.** (1) A **file or synthetic source cannot act on a PLI** — there is no live encoder — so a late joiner decodes from the file's next natural keyframe. The plumbing is proven; keyframe *response* awaits a browser sender. (2) After a **re-parent**, the leg deliberately **drops packets until a VP8 keyframe arrives**, because a continuous-but-undecodable stream is worse than a gap: the decoder renders garbage rather than freezing. The drop window is one PLI round trip. A brief black frame right after a failover is the design, not a fault.

### Goroutine leaks per connection — Now

> **Phase 2 status:** the discipline below is now in place for the `Router`'s own goroutines — the upload-meter sampler and each per-peer media pump run under a `context` + `sync.WaitGroup`, and `Run` won't return until they've all exited (the mesh test would hang if one leaked). The *stress* case — many rapid join/leave cycles — still awaits the Phase-4 simnet goroutine-count assertion.

- **Symptom:** the process slowly grows memory and goroutine count across a session of peers joining and leaving; behavior degrades or the scheduler thrashes long after the peers that "own" the work have disconnected. Often invisible in a 2-peer test and only obvious once churn starts (Phase 5).
- **Likely cause:** a per-connection goroutine (RTP reader, RTCP handler, metrics ticker, signaling pump) was **started but never joined**. When the peer leaves, nothing tells that goroutine to stop, so it blocks forever on a channel/read that will never complete. Every leave leaks a fixed set of goroutines; over a long session they accumulate.
- **Fix / discipline (adopt this in Phase 2, before it hurts):**
  1. **Every goroutine gets a `context.Context`** derived from a per-connection parent context. When the connection ends, cancel that context; the goroutine's `select { case <-ctx.Done(): return; ... }` is its exit door.
  2. **Every goroutine you start, you join.** Track them in a `sync.WaitGroup` (or an explicit `done chan struct{}`) and `Wg.Wait()` during teardown so leave is *synchronous* — the connection isn't "closed" until its goroutines have actually returned. This is the same lifecycle rigor as your wait-for-graph teardown; the leak is just the un-joined thread you'd never have left dangling in C++.
  3. Pair `defer cancel()` with the context creation so an early return can't skip cleanup.
- **How to catch it:** run `make test` (i.e. `go test -race ./...`) — the race detector often surfaces the shutdown-ordering bugs that accompany leaks. In the simnet harness (Phase 4), **assert on goroutine count**: snapshot `runtime.NumGoroutine()` before a batch of joins, run a full join→leave cycle, and assert it returns to the baseline (allow for a stable pool). A deterministic simnet is the right place to make this a hard test rather than a hope, and it lines up with *"never say done without verification."*

### The tree thrashes (constant re-optimization) — Now

- **Symptom:** under join/leave churn or noisy metrics, the coordinator rebuilds the relay tree repeatedly; peers get reparented over and over, each reparent causing a brief media hiccup (and, per Phase 3, a keyframe request). The overlay never settles even though the underlying network is basically stable.
- **Likely cause:** re-optimization is triggering on **metric wiggle** instead of on **sustained** change. A single high-latency sample, a momentary bandwidth dip, or one flapping peer is enough to knock a peer off the "best parent," and the greedy builder happily produces a different-but-not-better tree. This is a control-loop stability problem, not a graph-algorithm problem.
- **Fix — hysteresis, deliberately tuned:**
  - **Threshold events only.** Re-optimize on join, leave, or *sustained* degradation — never on every metric update. Debounce metrics with a rolling window / EWMA so one bad sample can't trigger a rebuild.
  - **Require a minimum improvement to act.** Only reparent if the new tree beats the current one by more than a margin (e.g. latency must improve by more than X% *and* hold for T seconds). A move that's marginally better isn't worth the reparent glitch it costs.
  - **Cooldown per peer.** After reparenting a peer, refuse to move it again for a cooldown window, so it can't ping-pong between two near-equal parents.
  - **Name every constant with its justification** (the window length, the improvement margin, the cooldown) — these are exactly the reward-shaping-style magic numbers that must not be bare. Expect to *tune* them against the simnet; there's no closed-form right answer, and honest Limitations note that hysteresis trades responsiveness for stability on purpose.

> **What shipped.** All four, with the knobs exposed: `-recompute-cooldown` (5 s, the minimum interval between two published trees for one meet), `-join-settle` (1.5 s, re-armed on **every** join so a burst coalesces into one build), `-dwell` (10 s of unbroken bad samples before sustained degradation counts), and `-stickiness-ms` (25 ms — the RTT margin a challenger must beat an incumbent parent by; **`0` makes rebuilds memoryless**, which is Phase 4's behaviour and the defect stickiness exists to fix). The improvement margin lives as **rank 1 of the builder's comparator**, above RTT and above load but below the hard constraints — an incumbent that is gone, TURN-bound, full or too deep is simply not a candidate, so stickiness can never produce an invalid tree.
>
> **If you are actually seeing thrash today, suspect something else.** With `RTTServerMs` unpopulated in production, no challenger can beat an incumbent's unknown RTT, so the anti-thrash property is *stronger* live than in simulation. Repeated re-parenting on the current tree is far more likely to be a **relaxed build** firing repeatedly (a fleet chronically near its capacity bound), a peer flapping in and out of `gone`, or a coordinator handover — all three of which say so in the log.

### Coordinator handover ambiguity window — Now

- **Symptom:** during an election or a coordinator migration, there's a window where **two nodes both believe they're coordinator**, or peers act on instructions from the **old** coordinator after a new one is elected. Stale graph decisions get applied; the overlay briefly follows two conflicting plans (a mini split-brain), even though the central arbiter is supposed to prevent exactly that.
- **Likely cause:** in-flight messages and state that predate the handover. A message sent by coordinator epoch *N* arrives at a peer after it has already learned about epoch *N+1*; without a way to reject it, the peer honors a stale command. The central server being the single source of truth for *who* is coordinator does **not** by itself make individual in-flight messages safe.
- **Fix — epoch/term fencing (this is the mechanism, not a nicety):**
  - Every coordinator authority is stamped with a **monotonically increasing epoch/term**, issued by the central arbiter. This is the same fencing-token idea from consensus systems (Raft terms, ZooKeeper `zxid`) — you've reasoned about it before; here it's the whole defense.
  - **Peers reject any message carrying an epoch lower than the highest they've seen.** A stale coordinator's commands are simply ignored, which collapses the ambiguity window to "harmless" instead of "split-brain."
  - The arbiter, not the peers, decides the epoch — that's the point of keeping control/bootstrap centralized. You are explicitly **not** running Raft/Paxos among home PCs; the arbiter is the tie-breaker, and epoch fencing is what makes a *single* arbiter enough.

> **What shipped, and the correction that matters.** "Reject anything lower than the highest you've seen" is only half the rule, and the half it omits is a privilege escalation: it says nothing about a *higher* epoch, so a peer could promote itself by stamping a bigger number. `Fence.Accept` requires the epoch to match **exactly** — a peer may learn who is in charge only from the arbiter, never from the node claiming the job — and the ordering comparison (`Supersedes`) is a *separate* function that is deliberately never on the apply path.
>
> **Why the residual window is safe rather than merely tolerable.** A peer that has not yet processed the `E+1` announcement may still apply an `E`-stamped topology. That is fine: an `E` tree is a valid tree over the membership the old coordinator knew — it passed `Validate` before it was published — so applying it cannot produce an illegal local state. It can only be **stale**, and staleness self-corrects, because the announcement is already queued on the *same FIFO socket*. The invariant to hold onto: **a peer's realized topology may be STALE, but it is never INCONSISTENT.**
>
> **The observable symptoms, and where to look.** During a handover you should see a non-zero `stale_rejected` at some peer — that is the fence doing visible work, not a fault. If a peer stays dark *after* the handover settles, read the "A peer receives nothing and the meet keeps re-publishing" entry above: it missed the announcement, and `announce_repair` is what fixes it. If the whole meet re-parents on the first tree of a new term, read the two `rebuild-from-peers:` reasons above.
>
> **One hard rule you may not "fix":** on a failed announcement broadcast the epoch is **not** rolled back. Reissuing a consumed epoch is the one thing that could put two coordinators in the same term, which is the only state the fence cannot survive.
- **How to make this testable:** this is a distributed-timing bug, and you cannot trust it to a manual two-machine run. Reproduce it **deterministically in the simnet** (Phase 4's harness): script the exact adversarial interleaving — old coordinator emits a command, new coordinator is elected, old command is delivered *after* — and assert every peer rejects the stale-epoch message. Handover is the single hardest thing in the project; the acceptance bar is a green simnet test for the ambiguity window, not "it looked fine once."

---

## When you add to this doc

Keep the **Symptom → Likely cause → Fix** shape, mark each entry **Now** or **Planned (Phase N)**, and prefer a concrete command or log line over prose. If a "Planned" trap turns real during its phase and the predicted fix was wrong, correct it here — a troubleshooting doc that lies is worse than none.
