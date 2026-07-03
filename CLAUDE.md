# CLAUDE.md — AI Assistant Guide for go-mqtt

## Project Overview

**go-mqtt** is a minimal, dependency-free, pure-Go **MQTT 3.1.1 client
library**: a wire codec, a TCP/TLS transport adapter, and a
reconnecting lifecycle wrapper. It is a **library module**, not a
daemon — no `main` package, no binaries, no Docker image, no config
file, nothing to deploy on its own.

The code was extracted from the `go-*2mqtt` bridge family
(`go-mtec2mqtt`, `go-zendure2mqtt`, `go-homeconnect2mqtt`, ...), where
it previously lived four times over, byte-for-byte duplicated, as each
bridge's own `internal/mqtt` package. go-mqtt is the single shared
implementation those bridges now import.

## Key Characteristics

- **Language**: Go 1.26+ (see `go.mod`).
- **Module path**: `github.com/SukramJ/go-mqtt`.
- **License: MIT** (not LGPL — unlike the `go-*2mqtt` bridges that
  consume this module). Provenance is
  [`openccu-loom`](https://github.com/SukramJ/openccu-loom), where this
  client was originally written before being carved out into its own
  module. Every Go source file starts with:
  ```go
  // SPDX-License-Identifier: MIT
  // Copyright (C) 2026 OpenCCU-Loom authors.
  ```
  Keep that header and attribution as-is on files carried over from
  openccu-loom / the bridges — do not switch it to a `SukramJ`
  copyright line or an LGPL header; this module's license lineage is
  independent of the LGPL bridges that import it.
- **No deployment artifacts**: this is a library. There is no `cmd/`,
  no `Makefile build`/`run`/`docker`/`release` target, no add-on
  packaging. The only thing CI produces is a green test/lint run.
- **Zero third-party dependencies** — deliberately. The MQTT 3.1.1
  wire codec and the TCP/TLS transport are hand-rolled against the
  standard library only, so consumers of this module don't inherit any
  transitive dependency tree. `go.mod` has no `require` block.

## Repository Structure

```
client.go               package mqtt (root): Publisher/Subscriber/Client contracts, QoS type
adapter_tcp.go           TCPClient — the concrete Connector+Client implementation
lifecycle.go             Lifecycle — reconnect loop with exponential backoff + jitter around any Connector
tls_config.go            NewClientTLSConfig — safe tls.Config construction (mandatory ServerName)
test_mock_broker.go      in-package (non-_test.go) mock broker used by lifecycle_test.go
protocol/                package protocol: MQTT 3.1.1 packet encode/decode (the wire codec)
protocol/doc.go          package-level doc comment; enumerates exact protocol feature coverage
protocol/codec.go        CONNECT/CONNACK/PUBLISH/PUBACK/SUBSCRIBE/SUBACK/UNSUBSCRIBE/UNSUBACK/PINGREQ/PINGRESP/DISCONNECT
*_test.go                colocated tests, incl. protocol/testdata-free unit tests and adapter-level mock-broker tests
.github/workflows/       ci.yml (lint + test), codeql.yml, dependabot-auto-merge.yml
.githooks/               pre-commit hook blocking direct commits on main/master
```

## Core Components

- **`protocol` package** — pure encode/decode of the MQTT 3.1.1 frames
  the bridges need: CONNECT (with optional LWT + username/password),
  PUBLISH (QoS 0 and QoS 1 only — QoS 2 is rejected at `Publish` time),
  SUBSCRIBE/UNSUBSCRIBE (one topic filter per frame), PINGREQ/PINGRESP,
  DISCONNECT. `ReadFrame` enforces `maxRemainingLength` (1 MiB) so a
  malformed or hostile "remaining length" field can't be used to stall
  or blow up the reader.
- **`TCPClient`** (`adapter_tcp.go`) — dials a `tcp://` or `tls://`
  broker, implements both `Connector` (`Connect`/`Disconnect`) and
  `Client` (`Publisher`+`Subscriber`). On (re)connect it replays every
  previously-registered subscription filter so a `CleanSession=true`
  reconnect doesn't silently drop inbound command topics. A
  **PINGRESP watchdog** in `keepAliveLoop` detects half-open sockets:
  if a PINGREQ goes unanswered by the next keep-alive tick, the
  connection is declared lost even though `readLoop` would otherwise
  block forever waiting on a socket the peer vanished from without a
  FIN/RST. **`ConnectionLost()`** exposes a buffered, non-blocking
  channel so an event-driven reconnect loop reacts immediately instead
  of polling `IsConnected()`. **SUBACK handling**: a rejected filter
  (non-zero return code) is logged via `slog`, since the SUBSCRIBE
  call itself returns as soon as the frame is on the wire and does not
  block on the broker's ack.
- **`Lifecycle`** (`lifecycle.go`) — a transport-agnostic reconnect
  loop around any `Connector`: exponential backoff (`InitialBackoff` →
  `MaxBackoff`, default 1s → 30s) with jitter, an `OnConnect` callback
  hook (bridges use it for "announce online" + resubscribe), and an
  "already connected" idempotency short-circuit so a stray reconnect
  attempt against a still-healthy socket doesn't spam warn logs or
  reset the backoff timer.
- **`MessageHandler` contract**: `func(topic string, payload []byte,
  retained bool)`. Handlers run **synchronously inline** in
  `TCPClient.readLoop` — the same goroutine that also decodes
  PUBACK/PINGRESP and feeds the keep-alive watchdog. A handler that
  blocks stalls PUBACK/PINGRESP processing and can trip a spurious
  `ping_timeout`. The `retained` flag carries the PUBLISH retain bit so a
  side-effecting handler can drop the retained replay the broker
  re-delivers on every (re)connect. This contract is documented on the
  `MessageHandler` type itself — read it before wiring a new consumer.
- **`NewClientTLSConfig`** (`tls_config.go`) — always sets `ServerName`
  explicitly (`tls.Client` does not infer it from the dialed address),
  closing the common "just set InsecureSkipVerify" trap.
  `insecureSkipVerify` must only ever be an explicit, operator-driven
  opt-in from the caller — never a default.

## Development Commands

Defined in the `Makefile` (`make help` lists them):

```sh
make test          # CGO_ENABLED=1 go test -race -count=1 -timeout=60s ./...
make test-cover     # tests + coverage report (coverage.out)
make vet            # go vet ./...
make fmt            # gofumpt -w . && goimports -w -local github.com/SukramJ/go-mqtt .
make fmt-check      # fail if gofumpt would rewrite anything (CI gate)
make lint           # golangci-lint run ./...
make vuln           # govulncheck ./...
make tidy           # go mod tidy
make check          # vet + fmt-check + lint + test — the pre-commit/pre-push gate
make setup          # install gofumpt/goimports/golangci-lint/govulncheck + git hooks
make clean          # remove coverage.out
```

There is no `build`/`run`/`docker`/`release` target — this module
compiles as a library only, and `go build ./...` (which CI's `test`
job exercises implicitly via `go test`) is the only "does it compile"
check that applies.

Run a single package's tests directly, e.g. `go test ./protocol/...`.

## Test Approach

- **`protocol`** tests are pure unit tests against encoded byte
  sequences — no network involved.
- **Root package** tests spin up in-process mock brokers:
  `test_mock_broker.go` (compiled into the package, not `_test.go`, so
  it's usable from `lifecycle_test.go` without visibility issues) is a
  multi-connection mock supporting `InjectTCPReset` (simulated abrupt
  TCP close), `DropPings` (half-open socket simulation, exercises the
  PINGRESP watchdog), and `RejectNextConnect` (non-zero CONNACK,
  exercises backoff). `adapter_tcp_test.go` has its own narrower
  single-connection mock for `TCPClient`-level tests.
- Always run with the race detector (`make test` sets
  `CGO_ENABLED=1`) — the client is concurrency-heavy (read loop,
  keep-alive loop, ack map, subscriber map all guarded by separate
  mutexes/atomics).

## Code Conventions Observed

- **License header** on every `.go` file: `SPDX-License-Identifier:
  MIT` + `Copyright (C) 2026 OpenCCU-Loom authors.` (see above — do
  not change this to an LGPL header or a `SukramJ` copyright line).
- **Formatting**: `gofumpt` (stricter gofmt) + `goimports -local
  github.com/SukramJ/go-mqtt` for import grouping.
- **Structured logging**: `log/slog` (enforced by `sloglint`).
- **`golangci-lint` v2** config (`.golangci.yaml`) — same linter set as
  the `go-*2mqtt` bridges: `bodyclose`, `contextcheck`, `copyloopvar`,
  `errcheck`, `errorlint`, `exhaustive`, `gocritic`, `gosec`, `govet`,
  `intrange`, `makezero`, `nilerr`, `noctx`, `prealloc`, `reassign`,
  `revive`, `sloglint`, `staticcheck`, `thelper`, `tparallel`,
  `unconvert`, `unparam`, `unused`, `usestdlibvars`, `wastedassign`.
- **No third-party dependencies** — this is a design decision, not
  just current state. Before reaching for a library (even a small
  one), re-derive the need against the standard library; the whole
  point of extracting this module was to give the `go-*2mqtt` bridges
  a shared, dependency-free MQTT transport.
- **Git hooks**: `make setup` (or `make hooks`) points
  `core.hooksPath` at `.githooks/`, which blocks direct commits on
  `main`/`master`.
- **Commit style**: Conventional Commits with a scope, e.g.
  `feat(protocol): ...`, `fix(lifecycle): ...`, `chore(deps): ...`.
- **CI** (`.github/workflows/ci.yml`) runs two jobs: `lint` (go vet +
  gofumpt check + golangci-lint), `test` (matrix across
  ubuntu/macos/windows with the race detector). A separate `codeql.yml`
  runs CodeQL SAST on push/PR/weekly schedule; `dependabot-auto-merge.yml`
  auto-merges non-major Dependabot PRs.

## Consumers and Compatibility

This module is imported by (at least) `go-mtec2mqtt`,
`go-zendure2mqtt`, and `go-homeconnect2mqtt`. **A breaking change here
breaks all of them at once.** Practical implications:

- Follow SemVer discipline strictly. Any change to an exported
  signature in `client.go`, `adapter_tcp.go`, `lifecycle.go`,
  `tls_config.go`, or `protocol/` is a breaking change unless it is
  purely additive.
- Before changing behavior (not just signatures) — e.g. the PINGRESP
  watchdog timing, the resubscribe-on-reconnect behavior, or the
  `MessageHandler` synchronous-dispatch contract — consider that every
  consumer bridge relies on the current behavior implicitly, often
  without a direct test of its own for that behavior. Check this
  module's own tests first; they are the closest thing to a
  compatibility contract.
- New functionality should default to being additive (new exported
  function/type/option) rather than changing an existing default.

## When in Doubt

- Read [`README.md`](./README.md) first — it documents the package
  layout and has a runnable usage example (`TCPClient` + `Lifecycle`
  wiring, `tls://` broker setup).
- For MQTT 3.1.1 semantics, the canonical reference is the OASIS MQTT
  3.1.1 specification; `protocol/doc.go` documents exactly which
  subset of the spec this module implements (and, implicitly, which
  parts — QoS 2, retained-message queries, etc. — it deliberately does
  not).
- For how this module is actually consumed in practice, look at
  `internal/mqtt` in a sibling `go-*2mqtt` bridge repo (e.g.
  `go-mtec2mqtt`) — those directories are the pre-extraction copy this
  module replaces, so they show the original call sites and can help
  answer "why does this API look like this."
