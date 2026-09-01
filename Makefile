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

# CONTROL_PLANE_DIRS are the packages that must be deterministically testable: they
# drive every temporal behaviour through the injected clock.Clock, so a scenario can
# run a whole meet in virtual time. Reaching for the wall clock in one of them breaks
# replayability silently, which is why this is a build gate and not a review note.
CONTROL_PLANE_DIRS := internal/overlay internal/simnet internal/coordinator internal/arbiter

# WALL_CLOCK_CALLS is every entry point into package time that reads or waits on real
# time. The list is deliberately exhaustive: a guard that catches time.Now but misses
# time.NewTicker is worse than no guard, because it reads as coverage.
WALL_CLOCK_CALLS := time\.(Now|Since|Until|Sleep|After|AfterFunc|Tick|NewTimer|NewTicker)\(

.PHONY: check-determinism
check-determinism: ## Fail if a control-plane package reaches for the wall clock.
	@dirs="$(wildcard $(CONTROL_PLANE_DIRS))"; \
	if [ -z "$$dirs" ]; then echo "check-determinism: no control-plane packages yet"; exit 0; fi; \
	if grep -rnE '$(WALL_CLOCK_CALLS)' $$dirs; then \
		echo ""; \
		echo "^ the control plane must use the injected clock.Clock (see docs/PLAN.md 4.7)"; \
		exit 1; \
	fi

.PHONY: check
check: fmt vet check-determinism test ## Format + vet + determinism + test: the pre-commit gate.

.PHONY: clean
clean: ## Remove build and coverage artifacts.
	rm -rf $(BIN_DIR) coverage.out coverage.html
