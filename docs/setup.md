# conclave — Setup

Get from a clean machine to a building, testing, self-verifying checkout. Everything here reflects the tree at **Phase 6 complete**. Once you are building, [`usage.md`](./usage.md) is the flag reference and [`DESIGN.md`](./DESIGN.md) explains the system.

Written for **Fedora Linux + zsh** (the owner's daily driver), but nothing is Fedora-specific except the one `dnf` line, which is clearly marked.

---

## 1. Prerequisites

| Tool | Version | Why it's required | Check |
|---|---|---|---|
| **Go** | **1.26+** (repo pins `go 1.26.4`) | The build target. `go.mod` declares `go 1.26.4`; the server also relies on method-aware mux patterns like `"GET /healthz"`, which need Go 1.22+. | `go version` |
| **C compiler** (`gcc` or `clang`) | any recent | Needed for the **`-race` detector** — see below. `make test` and `make check` both run `go test -race`, so this is **not optional** for the normal dev loop. | `gcc --version` |
| **git** | any | Cloning the repo. | `git --version` |

### Why the race detector needs a C compiler

`go test -race` doesn't just add a compiler flag. Go's race detector is a thin front-end over **ThreadSanitizer (TSan)** — the same C/C++ runtime library that ships with LLVM/`compiler-rt`. When you build with `-race`, Go instruments every memory access and then **links your program against that native TSan runtime**. Linking a native library from Go goes through **cgo**, and cgo needs a working **C toolchain** (a C compiler + system linker).

Consequences:

- cgo is enabled by default (`CGO_ENABLED=1`) **only when a C compiler is found on `PATH`**. If none is found, cgo is off and `go build -race` / `go test -race` fail outright.
- So on a box with no `gcc`/`clang`, `go test` (no flag) still works, but `make test`, `make check`, and CI-style `-race` runs will error. Given this project is about goroutines, channels, and an elected coordinator, catching data races early is the whole point — treat the C compiler as a hard prerequisite, not a nicety.

On Fedora, `gcc` is usually already installed (this machine has gcc 16). If it's missing:

```sh
sudo dnf install gcc
```

### Installing Go itself

Go has no `go install` bootstrap (chicken-and-egg). Two options:

- **Recommended — pin the exact version** from <https://go.dev/dl/>. Fedora's `dnf` `golang` package can lag behind a fresh release like 1.26.4, and this repo pins that minor. Grab the tarball for `linux-amd64`, unpack to `/usr/local/go`, and put `/usr/local/go/bin` on `PATH`.
- **Fedora package** (may be an older minor): `sudo dnf install golang`. Fine if `go version` reports **1.26 or newer**; otherwise use the tarball.

Make sure Go's own bin directory is on your `PATH` — you'll need it in §4 for the dev tools. Add to `~/.zshrc`:

```sh
export PATH="$PATH:$(go env GOPATH)/bin"   # default: ~/go/bin
```

Reload with `source ~/.zshrc` (or open a new shell).

---

## 2. Getting the code

The module path is **`github.com/SammyUrfen/conclave`**. Clone it anywhere — Go modules don't care about `GOPATH` layout:

```sh
git clone https://github.com/SammyUrfen/conclave.git
cd conclave
```

Sanity-check that the toolchain sees the module:

```sh
go build ./...   # should exit 0 with no output
```

---

## 3. First build & run

All day-to-day tasks go through the `Makefile`. Run `make` (or `make help`) any time to list targets — it self-documents from the `## ` comments.

### Build

```sh
make build
```

Compiles all three binaries into `./bin/`:

| Binary | What it is |
|---|---|
| `./bin/server` | The **arbiter**: meet rendezvous, the WebSocket signaling relay, epoch minting and election arbitration, and the `/api` dashboard surface. Optionally hosts the coordinator (`-coordinate`). Never in the media path. |
| `./bin/peer` | A **participant**: health probe, mesh call, static tree, or a fully managed peer that reports telemetry, realises the pushed tree, and fails over to a backup parent on its own. |
| `./bin/turn` | A **TURN relay** (Phase 7), for peers with no direct path to each other. ~50 lines over `github.com/pion/turn/v5`, which pion/webrtc already pulls in for the ICE client — so there is **nothing extra to install**, which is the whole reason it exists rather than coturn. Optional: leave it unused on one host. |

`./bin/` is git-ignored — never commit build artifacts.

### Run the server

```sh
make run-server                          # listens on :9000
make run-server ARGS="-addr :9000 -log-level debug -log-format json"
```

`make run-server` is `go run ./cmd/server` — pass flags through the `ARGS="..."` variable. The
three you will use immediately are `-addr` (default `:9000`), `-log-level` (default `info`) and
`-log-format` (default `text`); `-coordinate` turns on the coordinator and `-elect` turns on
election arbitration. [`usage.md`](./usage.md) has the full surface, and the server logs its
**effective** configuration at startup in two lines (`arbiter starting`, `coordinator enabled`)
— read those rather than trusting the flags you typed, because several knobs resolve to package
defaults when a flag is zero.

Verify it's alive from another terminal:

```sh
curl -i localhost:9000/healthz
# 200 OK, body: {"status":"ok","service":"conclave-server"}

curl -i -X POST localhost:9000/healthz
# 405 Method Not Allowed  (the mux pattern is method-aware)
```

Stop it with `Ctrl-C` (SIGINT) — it shuts down gracefully and logs `server stopped cleanly`.

### Run a peer

With the server running, in a second terminal:

```sh
make run-peer                                    # probes http://localhost:9000
make run-peer ARGS="-server http://localhost:9000 -timeout 3s"
```

Probe-mode peer flags: `-server` (default `http://localhost:9000`), `-log-level`, `-log-format`, `-timeout` (default `5s`). The peer exits **0** when the server is healthy and **1** when it can't reach it — a clean way to script "is the server up?". Everything else the peer can do lives behind `-call`; see [`usage.md`](./usage.md).

### Test

```sh
make test    # go test -race ./...  — should print "ok" per package
```

This is the race-instrumented run; it needs the C compiler from §1.

---

## 4. Optional but recommended dev tooling

Three tools sharpen the loop. **None of them is currently installed on this machine**, and nothing hard-depends on them — in particular, `make lint` **degrades gracefully to `go vet`** when `golangci-lint` is absent (it prints the install URL and carries on). Install them when you want the stricter feedback.

All `go install` binaries land in `$(go env GOPATH)/bin` (default `~/go/bin`) — which is why §1 put that directory on your `PATH`.

| Tool | What it buys you | Install |
|---|---|---|
| **goimports** | Superset of `gofmt` that also **adds/removes/orders imports**. Directly fixes two habits carried from C/Java/JS: stray manual import edits and formatting drift. | `go install golang.org/x/tools/cmd/goimports@latest` |
| **staticcheck** | Deep static analysis well beyond `go vet` (dead code, misuse of stdlib, concurrency smells). | `go install honnef.co/go/tools/cmd/staticcheck@latest` |
| **golangci-lint** | Meta-linter that runs many linters (incl. the above ideas) under one config. `make lint` uses it when present. | Follow the official installer — **do not `go install` a pinned version blindly**: <https://golangci-lint.run/welcome/install/> |

> Why the golangci-lint exception: its authors explicitly recommend the versioned installer script / release binaries over `go install`, because a `@latest` build can pull incompatible linter versions. Use their page and let it choose the version.

After installing, confirm they're on `PATH`:

```sh
goimports -h    >/dev/null 2>&1 && echo "goimports ok"
staticcheck -h  >/dev/null 2>&1 && echo "staticcheck ok"
golangci-lint version 2>/dev/null && echo "golangci-lint ok"
```

Then `make lint` will run `golangci-lint run` instead of the `go vet` fallback.

---

## 5. Editor setup — VS Code + the Go extension

The owner already runs VS Code. Install the official **Go** extension (`golang.go`). On first open of a `.go` file it will offer to install its toolset — accept it. The important pieces:

- **gopls** — the Go language server. Powers completion, hover, go-to-definition, rename, and inline diagnostics. The extension installs and manages it for you; no manual step needed.
- **Format on save** wired to `gofmt`/`goimports` — so you never hand-format again. (Nudge: this kills the `var x int = ...` / trailing-semicolon habits automatically; let the formatter be the source of truth.)
- **Organize imports on save** — lets `goimports` add/remove imports as you type.

Add this to your **workspace** `.vscode/settings.json` (or user settings):

```jsonc
{
  "go.useLanguageServer": true,
  "gopls": {
    "formatting.gofumpt": false
  },
  "[go]": {
    "editor.formatOnSave": true,
    "editor.defaultFormatter": "golang.go",
    "editor.codeActionsOnSave": {
      "source.organizeImports": "explicit"
    }
  },
  // Use goimports (superset of gofmt) once installed; falls back safely if not.
  "go.formatTool": "goimports"
}
```

If you haven't installed `goimports` yet (§4), set `"go.formatTool": "gofmt"` for now — formatting still works, you just don't get automatic import management.

---

## 6. Verify your setup

A quick checklist that mirrors the **`make check`** gate — `check` = `fmt` + `vet` + **`check-determinism`** + `test -race`. If all of these pass, you have the same green CI enforces.

- [ ] `go version` → **go1.26 or newer**
- [ ] `gcc --version` (or `clang --version`) succeeds — race detector will link
- [ ] `go build ./...` → exits 0, no output
- [ ] `make build` → produces `./bin/server`, `./bin/peer` and `./bin/turn`
- [ ] `make check` → runs **fmt + vet + check-determinism + test**; the `test` stage is `go test -race ./...` and must be green
- [ ] `make check-determinism` on its own → **silent**, exit 0 (it only prints when it finds a control-plane package reaching for the wall clock)
- [ ] **Manual smoke test:** `make run-server` in one terminal; in another, `curl localhost:9000/healthz` returns `{"status":"ok","service":"conclave-server"}` and `make run-peer` exits **0**. Kill the server with `Ctrl-C` and confirm it logs `server stopped cleanly`.
- [ ] **Dashboard smoke test:** `make run-server ARGS="-coordinate"`, then `curl -sS localhost:9000/api/meets` → `{"api_version":1,"demo_enabled":false,"meets":[],"ended":[]}`. Open `web/index.html` in a browser and type `localhost:9000` in the server field — no build step, no npm, nothing to install.

Optional, and only if you want the TURN path:

- [ ] `./bin/turn -public-ip 127.0.0.1 -users conclave=hunter2 -relay-ports 49160-49200` logs `turn relay listening … relay_ports=49160-49200` and stays up. Nothing else in the repository requires it — no test, no gate — and a single-host run never needs it. See [`verify-turn.md`](./verify-turn.md) for the end-to-end exercise, which does need `nft` and `unshare` (both present on a stock Fedora; `tc` is **not** required and is deliberately not used).

Optional, once §4 tools are installed:

- [ ] `make lint` → runs `golangci-lint run` (not the `go vet` fallback message)
- [ ] `make cover` → prints a per-function coverage report

---

## Notes & limitations

- **Third-party dependencies are real and pinned.** `go.mod` declares **five direct requires** — `github.com/pion/webrtc/v4`, `pion/rtp`, `pion/rtcp`, `pion/interceptor` (media), and `github.com/coder/websocket` (signaling) — plus their transitive set. `make tidy` is **not** a no-op; run it whenever imports move. *(An earlier revision of this file claimed `go.mod` had zero third-party requires. That stopped being true in Phase 1.)*
- **Both invocations are green:** `go test ./...` and `go test -race ./...` each report 13/13 packages `ok`. The gate *specifies* `-race` because a data race in this codebase is a design violation, not because the suite needs it to pass. Expect a large time difference — `internal/media` runs real pion, ICE and DTLS, and takes ~16 s plain against ~82 s instrumented; everything else is a virtual clock and is essentially free either way.
- **A green `make check` proves a lot more than it used to, and still not everything.** It proves the code builds, vets clean, keeps the control plane free of the wall clock, and passes 372 race-tested test functions — including deterministic simulation of churn, failover and coordinator handover, and real-pion integration tests of the media plane. It does **not** prove any *live* multi-process behaviour: Phases 5 and 6 have no recorded live run (`DESIGN.md` §9.5).
- **Nothing here sets up a deployment.** For running the arbiter anywhere other than your own shell — including the `ws://` vs `wss://` decision, which the browser makes for you — see `deploy/README.md`.
- **The source of truth for what is built** is `docs/ROADMAP.md`'s status table; the source of truth for *how it works* is `docs/DESIGN.md`. `docs/PLAN.md` is the frozen Phase 5–6 contract and is historical — where it and the code disagree, the code wins.
