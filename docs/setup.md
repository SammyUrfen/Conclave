# conclave — Setup

Get from a clean machine to a building, testing, self-verifying checkout. Everything here is Phase 0 reality — no future flags or binaries are described except where a section is explicitly labelled **Planned**.

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

Compiles both binaries into `./bin/`:

| Binary | What it is (Phase 0) |
|---|---|
| `./bin/server` | Central bootstrap/signaling/arbiter. Today it serves `GET /healthz` only. |
| `./bin/peer` | A participant node. Today it probes the server's `/healthz` and logs the outcome. |

`./bin/` is git-ignored — never commit build artifacts.

### Run the server

```sh
make run-server                          # listens on :9000
make run-server ARGS="-addr :9000 -log-level debug -log-format json"
```

`make run-server` is `go run ./cmd/server` — pass flags through the `ARGS="..."` variable. Server flags: `-addr` (default `:9000`), `-log-level` (default `info`), `-log-format` (default `text`).

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

Peer flags: `-server` (default `http://localhost:9000`), `-log-level`, `-log-format`, `-timeout` (default `5s`). The peer exits **0** when the server is healthy and **1** when it can't reach it — a clean way to script "is the server up?".

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

A quick checklist that mirrors the **`make check`** gate (`check` = `fmt` + `vet` + `test`). If all of these pass, you're ready to build on Phase 1.

- [ ] `go version` → **go1.26 or newer**
- [ ] `gcc --version` (or `clang --version`) succeeds — race detector will link
- [ ] `go build ./...` → exits 0, no output
- [ ] `make build` → produces `./bin/server` and `./bin/peer`
- [ ] `make check` → runs **fmt + vet + test**; the `test` stage is `go test -race ./...` and must be green
- [ ] **Manual smoke test:** `make run-server` in one terminal; in another, `curl localhost:9000/healthz` returns `{"status":"ok","service":"conclave-server"}` and `make run-peer` exits **0**. Kill the server with `Ctrl-C` and confirm it logs `server stopped cleanly`.

Optional, once §4 tools are installed:

- [ ] `make lint` → runs `golangci-lint run` (not the `go vet` fallback message)
- [ ] `make cover` → prints a per-function coverage report

---

## Notes & limitations

- This document covers **Phase 0** only. Media (WebRTC via `pion/webrtc`) and signaling (`coder/websocket`) dependencies are **Planned (Phase 1+)** and not yet imported — `go.mod` currently has zero third-party requires, so `make tidy` should be a no-op today.
- A green `make check` proves the code builds, vets clean, and passes race-tested unit tests. It does **not** prove any distributed behavior — there is no multi-peer, WebRTC, or election path to exercise yet.
- The source of truth for what's built and what's next is **`docs/ROADMAP.md`**. Read it before starting work on any phase.
