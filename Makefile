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

.PHONY: check
check: fmt vet test ## Format + vet + test: the pre-commit gate.

.PHONY: clean
clean: ## Remove build and coverage artifacts.
	rm -rf $(BIN_DIR) coverage.out coverage.html
