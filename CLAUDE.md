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
| 7 | Simulcast/SVC, TURN, polish & demo | ⬜ **not built** |

Phase 7's *observability* slice was pulled forward and reshaped during Phases 5–6:
the planned `/debug` page shipped as the arbiter's `/api` surface plus a static
GitHub Pages frontend (`web/`, `deploy/`, `.github/workflows/pages.yml`). The rest
of Phase 7 — simulcast/SVC and TURN/coturn — was **not** built.

Update this table and the `docs/` files as each phase lands — they are **living**.

> **One limitation to know before you claim anything about Phase 6.**
> `metrics.Report`'s `CPUPct`, `LossPct` and `RTTServerMs` are never populated in
> production, so every eligible peer scores 0.85–1.0 in `arbiter.Score`.
> `DemoteBelowScore` can never be crossed and `PromoteMarginScore` can never be
> met: **voluntary promotion/demotion is unreachable live — only bootstrap and
> failover elections fire.** The logic is real and simnet-tested; the sensors are
> not built. See `docs/DESIGN.md` §8.1.

## Commands
- `make run-server` · `make run-peer` · `make test` (race)
- `make check` = `fmt` + `vet` + `check-determinism` + `test -race` — the gate
- `make check-determinism` — fails if `overlay`/`simnet`/`coordinator`/`arbiter`
  reach for the wall clock instead of the injected `clock.Clock`
- `make build` → `./bin/{server,peer}` · `make lint` (golangci-lint if installed, else vet)
- `make cover` · `make fmt` · `make vet` · `make tidy` · `make clean`
- Pass flags: `make run-server ARGS="-addr :9000 -log-format json"`

## Conventions (match the existing code)
- **Package-by-feature** under `internal/` (`signaling`, `overlay`, `metrics`,
  `coordinator`, `media`, `simnet`, `clock`, `policy`, `arbiter`, `dashboard`,
  `logging`) — not layer-by-type. Entrypoints in `cmd/`. The zero-build static
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
