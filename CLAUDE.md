# CLAUDE.md — conclave

> Operational index for any Claude Code session in this repo. Thin on purpose:
> it loads into every session's context. Depth lives in `docs/`.

**What this is:** a decentralized, peer-assisted video-meeting platform in Go —
an **SFU (Selective Forwarding Unit) that is *elected* from among the
participants and can *migrate***. Learning/portfolio project, not shipping.

**Module:** `github.com/SammyUrfen/conclave` · **Go:** 1.26 · **Owner:** SammyUrfen

## Read these first
- `docs/DESIGN.md` — **the best single explanation of the system**: architecture,
  end-to-end traces, every load-bearing trade-off with its rejected alternative,
  where the contract was wrong, and a calibrated Limitations section.
- `docs/ROADMAP.md` — phased build order, per-phase Go + systems learning goals,
  architecture decisions (do not relitigate them).
- `README.md` — front door / quickstart.
- `docs/ARCHITECTURE.md` — the original conceptual narrative (why mesh dies, the
  elected-SFU idea, control/data-plane split, election & migration).
- `docs/PLAN.md` — the ~5,000-line Phase 5–6 architecture contract, amended in
  place (§15 log, v2.0 → v2.6). **Historical: the code is the truth.** Where a
  doc, the contract, and the code disagree, verify against the code.
- `docs/structure.md` — file tree → responsibility. `docs/tech-stack.md`,
  `docs/setup.md`, `docs/usage.md`, `docs/testing.md`, `docs/troubleshooting.md`,
  `docs/design-system.md`.

## Phase status
| Phase | Title | Status |
|------:|-------|--------|
| 0 | Repo bootstrap & Go foundations | ✅ done |
| 1 | Signaling + 2-peer WebRTC call | ✅ done |
| 2 | Full mesh to ~4 peers (feel the ceiling) | ✅ done |
| 3 | Static relay tree — the peer SFU ⭐ | ✅ done |
| 4 | Metrics plane + coordinator computes the tree | ✅ done |
| 5 | Join/leave handover + backup parents | ✅ done |
| 6 | Coordinator election + migration | ✅ done |
| 7 | Simulcast/SVC, TURN, polish & demo | 🟡 **partly built** |

Phase 7's *observability* slice was pulled forward and reshaped during Phases 5–6:
the planned `/debug` page shipped as the arbiter's `/api` surface plus a static
GitHub Pages frontend (`web/`, `deploy/`, `.github/workflows/pages.yml`).

**What of the rest is built, item by item — the honest split:**

| Phase 7 item | Status |
|---|---|
| Quality layers: multi-rung publish + per-child selection | ✅ built. **Not** RFC 8853 simulcast — pion v4 writes no MID/RID header extension on the send path, so an origin publishes N ordinary tracks instead. Rejection recorded in `docs/DESIGN.md` §5.14. |
| Adaptive per-edge layer selection | ✅ built (`media.selectLayer`, pure + hysteresis). Loss-driven; no bandwidth estimate. |
| SVC | ⬜ not built. |
| TURN relay + ICE plumbing | ✅ built (`cmd/turn`, on `pion/turn/v5` — not coturn; coturn stays in `deploy/` as the deployment story). |
| NAT class measured, not declared | ✅ built (`media.relayedPath`). A **behavioural** classification — "this peer's media paths are relayed" — not NAT-type discovery. `-nat` survives as an override. |
| Demo script | ⬜ not built. |

**Two Phase 7 claims are NOT proven live, and the docs must keep saying so:**
the TURN path has never carried media with the direct path blocked
(`docs/verify-turn.md` is a *proposed* recipe), and the layer **downgrade** has
never been observed — only the upstream half (three rungs reaching a relay, a
clean child recording the top rung). Both recipes need root.

Update this table and the `docs/` files as each phase lands — they are **living**.

> **What the telemetry now measures, and what still bounds it.**
> `metrics.Report`'s `CPUPct`, `RTTServerMs`, `LossPct` and `PeerRTT` are
> **measured live** as of the sensor work: CPU from `/proc/stat` deltas, server
> RTT from a WebSocket ping, uplink loss from the RTCP receiver reports the relay
> already drains, and pairwise RTT from the nominated ICE candidate pair (pion
> collects **no** RTPSender stats, so `RemoteInboundRTPStreamStats` is a zero
> value — see `TestPionPopulatesSelectedPairRTT`). Voluntary promotion **and**
> demotion have both been observed live. Three things still bound it:
>
> 1. **Demotion needs all three signals.** `Score` is `0.30 cpu + 0.35 rtt +
>    0.20 loss + 0.15 uptime` and `DemoteBelowScore` is `0.35` on a strict `<`,
>    so a settled peer maximally bad on CPU and RTT but clean on loss scores
>    *exactly* 0.35 and cannot be demoted. Pinned by
>    `TestScoreFloorOfAFullyDegradedPeer`.
> 2. **Pairwise RTT is partial.** A peer can only measure a path it has a
>    PeerConnection over, so `Node.RTT` covers current neighbours plus ones it
>    held within `media.RTTMemory` (2 min). A peer never connected to stays
>    unknown, so a first attachment is still RTT-blind.
> 3. **`arbiter.ScoreQuantum` exists because the sensors are real.** Candidate
>    ranking compares *quantized* fitness; ranking on raw score lets microseconds
>    of jitter reorder the top two every second, which restarts the election
>    dwell forever and pins the role. See `TestDwellSurvivesChallengerJitter`.
>
> See `docs/DESIGN.md` §8.1.

## Commands
- `make run-server` · `make run-peer` · `make test` (race)
- `make check` = `fmt` + `vet` + `check-determinism` + `test -race` — the gate
  - ⚠️ **The `-race` gate is ~97% green per run, not 100%.** `internal/media`
    trips a race **inside pion** — two pion goroutines on one SRTP stream, no
    conclave frame on either side — in ~2 of 61 package runs on the pinned
    v4.2.16. There is no setting that disables it and no upstream fix yet. A
    single red `-race` run is not automatically your change; diff the stack
    against `docs/DESIGN.md` §8.6 before you go looking. Anything with a
    conclave frame in it **is** ours.
- `make check-determinism` — fails if `overlay`/`simnet`/`coordinator`/`arbiter`
  reach for the wall clock instead of the injected `clock.Clock`
- `make build` → `./bin/{server,peer,turn}` · `make lint` (golangci-lint if installed, else vet)
- `make cover` · `make fmt` · `make vet` · `make tidy` · `make clean`
- Pass flags: `make run-server ARGS="-addr :9000 -log-format json"`

## Conventions (match the existing code)
- **Package-by-feature** under `internal/` (`signaling`, `overlay`, `metrics`,
  `coordinator`, `media`, `simnet`, `clock`, `policy`, `arbiter`, `dashboard`,
  `meetconfig`, `logging`) — not layer-by-type. Entrypoints in `cmd/`
  (`server`, `peer`, `turn`). The zero-build static
  frontend lives in `web/`; container/proxy files in `deploy/`; CI + Pages in
  `.github/workflows/`.
- **No globals for dependencies.** Construct at the edge (`main`), inject the
  `*slog.Logger` and friends downward. Every binary: thin `main` → `run() error`.
- **Consumer-defined interfaces**, declared at the point of use, so the DAG stays
  acyclic (`coordinator` imports neither `signaling` nor `media`). The two
  deliberate exceptions are `clock` and `policy` — see `docs/DESIGN.md` §3.5.
- **Everything temporal goes through `clock.Clock`.** `check-determinism` is a
  build gate, not a review note.
- **Errors are values, wrapped with `%w`.** Centralized error→exit / error→HTTP
  translation (`internal/dashboard/errors.go` owns the `{code, message, details}`
  envelope). Fail loud on bad config (e.g. unknown log level, an impossible
  liveness budget).
- **Doc comments on every exported identifier; comments explain WHY, not what.**
- **Structured logging via `log/slog`**, one logger per binary, `.With()` for
  context fields (`service=...`). No `fmt.Println` for anything operational.
- **Tests: table-driven + `t.Run` subtests, run with `-race`.** The `simnet`
  harness gives deterministic, media-free tests of the real control plane over a
  virtual clock; `overlay.Validate` / `ValidateLocalRepair` are the independent
  oracles. Write down the mutation each assertion would catch — see
  `docs/testing.md`.
- Tooling welcome: `gofmt`/`goimports`, `go vet`, `staticcheck`, `golangci-lint`.

## Working style here
- **Teach Go inline while building** — the owner is a strong systems engineer but
  new to Go; frame concurrency/interfaces as new syntax for known problems, and
  name trade-offs explicitly (mutex vs channel, greedy vs optimal, arbiter vs Raft).
- **Verify before claiming done** — run it, show output. Never say done without it.
- **Priorities:** Correctness > Reliability > UX > Maintainability > Performance.
- **Commits:** terse, imperative, single sentence, no co-author trailer. Commit
  only when asked.
- **Keep all deliverables local — never publish Artifacts to claude.ai.**
