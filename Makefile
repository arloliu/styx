# Makefile for styx
# Usage: make [target]

TEST_TIMEOUT           ?= 5m
BENCH_TIMEOUT          ?= 10m
LINT_TIMEOUT            ?= 3m
LINTER_GOMOD            := -modfile=.linter.go.mod
GOLANGCI_LINT_VERSION   := 2.12.2
BUF_GOMOD               := -modfile=.buf.go.mod
BUF_VERSION             := 1.72.0
BIN_DIR                 := bin
GENERATOR_BIN           := $(BIN_DIR)/protoc-gen-go-styx

.DEFAULT_GOAL := help
.PHONY: help build test test-failpoint bench bench-goplugin bench-goplugin-check vet lint lint-failpoint fmt generate tidy clean \
	linter-update linter-version clean-linter-cache \
	buf-update buf-version ci

## help: Show this help message
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

## build: Build the protoc-gen-go-styx binary
build:
	@mkdir -p $(BIN_DIR)
	go build -o $(GENERATOR_BIN) ./cmd/protoc-gen-go-styx

## test: Run unit tests with the race detector (plus the tagged ring/event ordering proofs)
test:
	go test ./... -race -timeout=$(TEST_TIMEOUT)
	go test -tags ringhook ./internal/ring/... -race -timeout=$(TEST_TIMEOUT)
	go test -tags eventhook ./internal/event/... ./internal/transport/shm/... -race -timeout=$(TEST_TIMEOUT)

## test-failpoint: Run the crash-window tests behind the failpoint build tag
test-failpoint:
	go test -tags failpoint ./internal/transport/shm ./internal/supervisor -race -timeout=$(TEST_TIMEOUT)

## bench: Run the SHM spike benchmark suite (see bench/spike)
bench:
	go test ./bench/... -run='^$$' -bench=. -benchmem -timeout=$(BENCH_TIMEOUT)

## bench-goplugin: Run the go-plugin-fork vs. styx-shm/styx-uds comparison (separate module)
bench-goplugin:
	cd bench/goplugin && go test ./... -run='^$$' -bench=. -benchmem -timeout=$(BENCH_TIMEOUT)

## bench-goplugin-check: Build/vet/test bench/goplugin (no -bench; seconds not minutes) so CI catches it drifting from the public API
bench-goplugin-check:
	cd bench/goplugin && go build ./... && go vet ./... && go test ./...

## vet: Run go vet
vet:
	go vet ./...

## lint: Run golangci-lint (verifies the pinned version first)
lint:
	@INSTALLED=$$(go tool $(LINTER_GOMOD) golangci-lint --version 2>/dev/null | grep -oE 'version [^ ]+' | cut -d' ' -f2 || echo none); \
	if [ "$$INSTALLED" != "$(GOLANGCI_LINT_VERSION)" ]; then \
		echo "golangci-lint version mismatch (have $$INSTALLED, want $(GOLANGCI_LINT_VERSION)); run 'make linter-update'"; exit 1; fi
	go tool $(LINTER_GOMOD) golangci-lint run --timeout=$(LINT_TIMEOUT)

## lint-failpoint: Run golangci-lint with the failpoint build tag (its guarded files, e.g. under internal/transport/shm, only compile with the tag set, so the plain lint pass above never sees them)
lint-failpoint:
	@INSTALLED=$$(go tool $(LINTER_GOMOD) golangci-lint --version 2>/dev/null | grep -oE 'version [^ ]+' | cut -d' ' -f2 || echo none); \
	if [ "$$INSTALLED" != "$(GOLANGCI_LINT_VERSION)" ]; then \
		echo "golangci-lint version mismatch (have $$INSTALLED, want $(GOLANGCI_LINT_VERSION)); run 'make linter-update'"; exit 1; fi
	go tool $(LINTER_GOMOD) golangci-lint run --timeout=$(LINT_TIMEOUT) --build-tags failpoint

## fmt: Format code (gofmt + goimports via golangci-lint fmt)
fmt:
	go tool $(LINTER_GOMOD) golangci-lint fmt

## generate: Regenerate protobuf/Styx code via buf (verifies the pinned buf version first)
generate: build
	@INSTALLED=$$(go tool $(BUF_GOMOD) buf --version 2>/dev/null || echo none); \
	if [ "$$INSTALLED" != "$(BUF_VERSION)" ]; then \
		echo "buf version mismatch (have $$INSTALLED, want $(BUF_VERSION)); run 'make buf-update'"; exit 1; fi
	go tool $(BUF_GOMOD) buf generate

## tidy: Tidy and verify go.mod/go.sum
tidy:
	go mod tidy
	go mod verify

## clean: Remove build artifacts and test cache
clean:
	@rm -rf $(BIN_DIR)/
	go clean -testcache

## linter-update: Install/update the pinned golangci-lint
linter-update:
	go get -tool $(LINTER_GOMOD) github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$(GOLANGCI_LINT_VERSION)
	go mod verify $(LINTER_GOMOD)

## linter-version: Print the installed golangci-lint version
linter-version:
	go tool $(LINTER_GOMOD) golangci-lint --version

## clean-linter-cache: Clear golangci-lint's on-disk cache
clean-linter-cache:
	go tool $(LINTER_GOMOD) golangci-lint cache clean

## buf-update: Install/update the pinned buf
buf-update:
	go get -tool $(BUF_GOMOD) github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)
	go mod verify $(BUF_GOMOD)

## buf-version: Print the installed buf version
buf-version:
	go tool $(BUF_GOMOD) buf --version

## ci: Full local gate (lint, vet, test, failpoint tests)
ci: lint lint-failpoint vet test test-failpoint bench-goplugin-check
