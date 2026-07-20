# CLAUDE.md — conclave

> Operational index for any Claude Code session in this repo. Thin on purpose:
> it loads into every session's context. Depth lives in `docs/`.

**What this is:** a decentralized, peer-assisted video-meeting platform in Go —
an **SFU (Selective Forwarding Unit) that is *elected* from among the
participants and can *migrate***. Learning/portfolio project, not shipping.

**Module:** `github.com/SammyUrfen/conclave` · **Go:** 1.26 · **Owner:** SammyUrfen

## Read these first
- `docs/ROADMAP.md` — **the source of truth**: phased build order, per-phase Go +
  systems learning goals, architecture decisions (do not relitigate them).
- `README.md` — front door / quickstart.
- `ARCHITECTURE.md` — conceptual narrative (why mesh dies, the elected-SFU idea,
  control/data-plane split, election & migration).
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
| 4 | Metrics plane + coordinator computes the tree | ⬜ next |
| 5 | Join/leave handover + backup parents | ⬜ |
| 6 | Coordinator election + migration | ⬜ |
| 7 | Simulcast/SVC, TURN, polish & demo | ⬜ |

Update this table and the `docs/` files as each phase lands — they are **living**.

## Commands
- `make run-server` · `make run-peer` · `make test` (race) · `make check` (fmt+vet+test)
- `make build` → `./bin/{server,peer}` · `make lint` (golangci-lint if installed, else vet)
- Pass flags: `make run-server ARGS="-addr :9000 -log-format json"`

## Conventions (match the existing code)
- **Package-by-feature** under `internal/` (`signaling`, `overlay`, `metrics`,
  `coordinator`, `media`, `simnet`) — not layer-by-type. Entrypoints in `cmd/`.
- **No globals for dependencies.** Construct at the edge (`main`), inject the
  `*slog.Logger` and friends downward. Every binary: thin `main` → `run() error`.
- **Errors are values, wrapped with `%w`.** Centralized error→exit / error→HTTP
  translation as surfaces grow. Fail loud on bad config (e.g. unknown log level).
- **Doc comments on every exported identifier; comments explain WHY, not what.**
- **Structured logging via `log/slog`**, one logger per binary, `.With()` for
  context fields (`service=...`). No `fmt.Println` for anything operational.
- **Tests: table-driven + `t.Run` subtests, run with `-race`.** From Phase 4,
  the `simnet` harness gives deterministic, media-free tests of control logic.
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
