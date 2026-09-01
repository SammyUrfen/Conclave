# conclave — developer task runner.
#
# `make` (or `make help`) lists the targets. These are the Phase 0 targets; the
# file grows with the project. Every target is .PHONY because none of them
# produce a file named after the target (except build, which writes into ./bin).

# Fail fast: any command in a recipe that errors aborts the target.
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

GO ?= go
PKGS := ./...
SERVER_PKG := ./cmd/server
PEER_PKG := ./cmd/peer
BIN_DIR := bin

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Compile both binaries into ./bin.
	$(GO) build -o $(BIN_DIR)/server $(SERVER_PKG)
	$(GO) build -o $(BIN_DIR)/peer $(PEER_PKG)

.PHONY: run-server
run-server: ## Run the central server. Pass flags via ARGS="-addr :9000".
	$(GO) run $(SERVER_PKG) $(ARGS)

.PHONY: run-peer
run-peer: ## Run a peer health probe. Pass flags via ARGS="-server http://...".
	$(GO) run $(PEER_PKG) $(ARGS)

.PHONY: test
test: ## Run all tests with the race detector.
	$(GO) test -race $(PKGS)

.PHONY: cover
cover: ## Run tests and print a per-function coverage report.
	$(GO) test -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out

.PHONY: fmt
fmt: ## Format all code with gofmt.
	$(GO) fmt $(PKGS)

.PHONY: vet
vet: ## Run go vet (catches suspicious constructs the compiler allows).
	$(GO) vet $(PKGS)

.PHONY: tidy
tidy: ## Reconcile go.mod / go.sum with the imports actually used.
	$(GO) mod tidy

.PHONY: lint
lint: ## Run golangci-lint if installed; otherwise fall back to go vet.
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found — install: https://golangci-lint.run/welcome/install/"; \
		echo "falling back to 'go vet' for now"; \
		$(GO) vet $(PKGS); \
	fi

# The determinism gate has TWO TIERS, because docs/PLAN.md §4.7 states two different
# rules and giving both one name is how a reader ends up believing the wrong thing.
# Read the exclusion list at the bottom before concluding anything from a silent run:
# this gate says "these named packages do not reach for the wall clock", NOT "no
# package does".
#
# TIER 1 — REPLAYABLE_PKGS, checked in EVERY file, tests included. These are the pure
# control plane: a scenario must be able to run a whole meet in virtual time and
# replay byte-identically from a seed, so a wall-clock reference in a TEST here is as
# damaging as one in production — it reintroduces the nondeterminism the harness
# exists to eliminate. (§4.7 rule 2.)
REPLAYABLE_PKGS := internal/overlay internal/simnet internal/coordinator internal/arbiter

# TIER 2 — INJECTED_CLOCK_PKGS, checked in PRODUCTION FILES ONLY. These drive every
# temporal behaviour through the injected clock.Clock, which is what lets their
# callers and simnet control time — but their own tests legitimately drive real
# sockets and real goroutines, and polling one with a real deadline is the honest way
# to test it. Holding them to tier 1 would force those tests to fake a socket, which
# would test less. They are gated so that a future edit cannot silently opt them out.
# (§4.7 rule 1.)
INJECTED_CLOCK_PKGS := internal/dashboard internal/signaling internal/metrics internal/policy cmd/server cmd/peer

# DELIBERATELY OUTSIDE BOTH, and why — this list is the honest part of the gate:
#
#   internal/media   — the data plane's cadence IS real time. meter.go paces an upload
#                      sample, source.go paces RTP frame emission; both must track the
#                      wall clock because pion and the network do. Forcing them onto a
#                      virtual clock would not make them deterministic, it would make
#                      them wrong. media's CONTROL logic is tested separately, pure.
#   cmd/server,      — production code in both is clean today, but they are binaries
#   cmd/peer           whose owners have not signed up to the constraint, and startup
#                      or shutdown timing in main is a legitimate wall-clock use. Worth
#                      revisiting: if their owners want the guarantee, move them into
#                      INJECTED_CLOCK_PKGS — it costs nothing at present.
#   internal/logging — no temporal behaviour at all; nothing to guarantee.
#   web/             — not Go.

# WALL_CLOCK_CALLS is every entry point into package time that reads or waits on real
# time. The list is deliberately exhaustive: a guard that catches time.Now but misses
# time.NewTicker is worse than no guard, because it reads as coverage.
WALL_CLOCK_CALLS := time\.(Now|Since|Until|Sleep|After|AfterFunc|Tick|NewTimer|NewTicker)\(

.PHONY: check-determinism
check-determinism: ## Fail if a gated package reaches for the wall clock (see the two tiers above).
	@fail=0; \
	tier1="$(wildcard $(REPLAYABLE_PKGS))"; \
	if [ -n "$$tier1" ] && grep -rnE '$(WALL_CLOCK_CALLS)' --include='*.go' $$tier1; then \
		echo ""; \
		echo "^ REPLAYABLE_PKGS must use the injected clock.Clock in tests TOO (docs/PLAN.md 4.7 rule 2)"; \
		fail=1; \
	fi; \
	tier2="$(wildcard $(INJECTED_CLOCK_PKGS))"; \
	if [ -n "$$tier2" ] && grep -rnE '$(WALL_CLOCK_CALLS)' --include='*.go' --exclude='*_test.go' $$tier2; then \
		echo ""; \
		echo "^ INJECTED_CLOCK_PKGS must take a clock.Clock rather than calling package time (docs/PLAN.md 4.7 rule 1)"; \
		fail=1; \
	fi; \
	exit $$fail

.PHONY: check
check: fmt vet check-determinism test ## Format + vet + determinism + test: the pre-commit gate.

.PHONY: clean
clean: ## Remove build and coverage artifacts.
	rm -rf $(BIN_DIR) coverage.out coverage.html
