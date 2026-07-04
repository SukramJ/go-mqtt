# SPDX-License-Identifier: MIT
# go-mqtt — developer Makefile
#
# Tabs are required by GNU make. The whitespace rules below pin sane
# shell behaviour so a failing recipe step actually aborts the target
# instead of silently moving on.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

GO            ?= go
GOFUMPT       ?= gofumpt
GOIMPORTS     ?= goimports
GOLANGCI_LINT ?= golangci-lint
GOVULNCHECK   ?= govulncheck

MODULE := github.com/SukramJ/go-mqtt

.PHONY: help
help: ## show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: setup
setup: hooks ## install developer tooling (gofumpt, goimports, golangci-lint, govulncheck) + git hooks
	$(GO) install mvdan.cc/gofumpt@latest
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	$(GO) install golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: hooks
hooks: ## point git at the tracked hooks in .githooks/ (blocks direct commits on main)
	@git config core.hooksPath .githooks
	@echo "git core.hooksPath -> .githooks (direct commits on main/master are now blocked)"

.PHONY: test
test: ## run the full test suite with race detector
	CGO_ENABLED=1 $(GO) test -race -count=1 -timeout=60s ./...

.PHONY: test-cover
test-cover: ## run tests + coverage report
	CGO_ENABLED=1 $(GO) test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -20

.PHONY: vet
vet: ## run go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## format with gofumpt + goimports (writes in place)
	$(GOFUMPT) -w .
	$(GOIMPORTS) -w -local $(MODULE) .

.PHONY: fmt-check
fmt-check: ## fail when sources are not gofumpt-clean
	@diff=$$($(GOFUMPT) -l .); \
	if [ -n "$$diff" ]; then \
	  echo "gofumpt would rewrite:"; echo "$$diff"; exit 1; \
	fi

.PHONY: lint
lint: ## run golangci-lint
	$(GOLANGCI_LINT) run ./...

.PHONY: vuln
vuln: ## scan dependencies + reachable code for known vulnerabilities (govulncheck)
	$(GOVULNCHECK) ./...

.PHONY: tidy
tidy: ## sync go.mod / go.sum
	$(GO) mod tidy

.PHONY: check
check: vet fmt-check lint test ## the pre-commit / pre-push gate

FUZZTIME ?= 5m
COVER_MIN ?= 90

.PHONY: fuzz-smoke
fuzz-smoke: ## run every Fuzz target in ./protocol for 10s each (CI smoke gate)
	@for fn in $$($(GO) test ./protocol -list '^Fuzz' | grep '^Fuzz'); do \
	  echo "== fuzz-smoke: $$fn (10s) =="; \
	  $(GO) test ./protocol -run '^$$' -fuzz "^$$fn$$" -fuzztime=10s; \
	done

.PHONY: fuzz
fuzz: ## run every Fuzz target in ./protocol for FUZZTIME (default 5m)
	@for fn in $$($(GO) test ./protocol -list '^Fuzz' | grep '^Fuzz'); do \
	  echo "== fuzz: $$fn (fuzztime=$(FUZZTIME)) =="; \
	  $(GO) test ./protocol -run '^$$' -fuzz "^$$fn$$" -fuzztime=$(FUZZTIME); \
	done

.PHONY: cover-check
cover-check: ## per-package coverage gate (COVER_MIN percent, default 90); NOT part of `check`
	@status=0; \
	for pkg in $$($(GO) list ./... | grep -v '/e2e'); do \
	  profile=$$(mktemp); \
	  log=$$(mktemp); \
	  if ! CGO_ENABLED=1 $(GO) test -race -covermode=atomic -coverprofile="$$profile" "$$pkg" >"$$log" 2>&1; then \
	    echo "FAIL $$pkg (test failure)"; cat "$$log"; status=1; rm -f "$$profile" "$$log"; continue; \
	  fi; \
	  total=$$($(GO) tool cover -func="$$profile" | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	  rm -f "$$profile" "$$log"; \
	  if awk -v t="$$total" -v m="$(COVER_MIN)" 'BEGIN{exit !(t+0>=m+0)}'; then \
	    echo "ok   $$pkg ($$total% >= $(COVER_MIN)%)"; \
	  else \
	    echo "FAIL $$pkg ($$total% < $(COVER_MIN)%)"; status=1; \
	  fi; \
	done; \
	exit $$status

.PHONY: e2e-certs
e2e-certs: ## generate the e2e CA + server TLS cert (idempotent)
	$(GO) run ./e2e/gencert -out e2e/testdata/certs

.PHONY: e2e-up
e2e-up: e2e-certs ## start the e2e mosquitto + emqx docker containers
	docker rm -f gomqtt-e2e-mosquitto gomqtt-e2e-emqx >/dev/null 2>&1 || true
	docker run --rm -v $(CURDIR)/e2e/testdata:/work eclipse-mosquitto:2 \
	  mosquitto_passwd -b -c /work/passwd e2e e2epass
	docker run -d --name gomqtt-e2e-mosquitto \
	  -p 1883:1883 -p 8883:8883 -p 1884:1884 \
	  -v $(CURDIR)/e2e/testdata:/mosquitto/config:ro \
	  eclipse-mosquitto:2
	docker run -d --name gomqtt-e2e-emqx \
	  -p 2883:1883 \
	  emqx/emqx:5

.PHONY: e2e-down
e2e-down: ## stop and remove the e2e docker containers
	docker rm -f gomqtt-e2e-mosquitto gomqtt-e2e-emqx >/dev/null 2>&1 || true

.PHONY: test-e2e
test-e2e: ## run ./e2e against the containers started by `make e2e-up`
	MQTT_E2E_MOSQUITTO=tcp://127.0.0.1:1883 \
	MQTT_E2E_MOSQUITTO_TLS=tls://127.0.0.1:8883 \
	MQTT_E2E_MOSQUITTO_AUTH=tcp://127.0.0.1:1884 \
	MQTT_E2E_EMQX=tcp://127.0.0.1:2883 \
	MQTT_E2E_CERTS_DIR=$(CURDIR)/e2e/testdata/certs \
	CGO_ENABLED=1 $(GO) test -race -count=1 -timeout=300s ./e2e/...

.PHONY: clean
clean: ## remove build artefacts and generated e2e assets (certs, passwd, coverage)
	rm -f coverage.out coverage.* *.coverprofile profile.cov
	rm -rf e2e/testdata/certs
	rm -f e2e/testdata/passwd
