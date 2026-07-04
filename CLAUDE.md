# CLAUDE.md — AI Assistant Guide for go-mqtt

## Project Overview

**go-mqtt** is a minimal, dependency-free, pure-Go **MQTT client
library** speaking both **MQTT 5.0 (default) and MQTT 3.1.1**: a
dual-version wire codec, a TCP/TLS transport adapter with QoS 0/1/2,
session resumption and flow control, and a reconnecting lifecycle
wrapper. It is a **library module**, not a daemon — no `main` package,
no binaries, no Docker image (beyond the `e2e/` test harness's use of
`docker run` to stand up throwaway brokers), no config file, nothing to
deploy on its own.

The code was originally extracted from the `go-*2mqtt` bridge family
(`go-mtec2mqtt`, `go-zendure2mqtt`, `go-homeconnect2mqtt`, ...), where
it previously lived four times over, byte-for-byte duplicated, as each
bridge's own `internal/mqtt` package, before being carved out of
`openccu-loom`. go-mqtt is the single shared implementation those
projects import. See [MIGRATION.md](./MIGRATION.md) for the v0.x -> v1.0
upgrade path and [CHANGELOG.md](./CHANGELOG.md) for what changed.

## Key Characteristics

- **Language**: Go 1.26+ (see `go.mod`).
- **Module path**: `github.com/SukramJ/go-mqtt` (unchanged across the
  v1.0 rewrite — no `/v2` suffix; v1.0 is a breaking change accepted by
  every consumer at upgrade time, not a parallel major version).
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
  no `Makefile build`/`run`/`release` target, no add-on packaging. The
  only thing CI produces is a green test/lint/fuzz/e2e run. The `e2e/`
  Make targets (`e2e-up`/`e2e-down`) run `docker run` purely as
  disposable test fixtures for `make test-e2e` — not a deployment path
  for this module.
- **Zero third-party dependencies** — deliberately, including in
  tests. The wire codec and the TCP/TLS transport are hand-rolled
  against the standard library only, so consumers of this module don't
  inherit any transitive dependency tree. `go.mod` has no `require`
  block.

## Repository Structure

```
client.go               package mqtt (root): Publisher/Subscriber/Client contracts, QoS/Message/MessageHandler, LegacyHandler
options.go              PublishOption/SubscribeOption, SubscribeResult, ConnectResult, Will, TCPConfig-adjacent public types
errors.go               sentinel errors (ErrNotConnected, ErrConnectionLost, ...) + *ReasonError
adapter_tcp.go           TCPClient: TCPConfig, link struct, Connect/Disconnect, dial, session (re)establishment
pump.go                  readLoop/keepAliveLoop: frame dispatch, inbound QoS 1/2 handling, topic-alias resolution, dispatch to subscribers
publish.go               Publish/Subscribe/Unsubscribe: ack waiting, flow-control acquire, SUBSCRIBE/UNSUBSCRIBE request/ack plumbing
session.go               SessionStore interface + memStore, idAllocator (packet-id bitmap), quota (send-quota semaphore)
lifecycle.go             Lifecycle + ConnectionNotifier — reconnect loop, event-driven via a Connector's ConnectionLost(), exponential backoff + jitter
tls_config.go            NewClientTLSConfig — safe tls.Config construction (mandatory ServerName)
test_mock_broker.go      in-package (non-_test.go) scripted multi-connection mock broker (v3.1.1 + v5), fault injection knobs
protocol/                package protocol: dual-version (v3.1.1 + v5) MQTT wire codec
protocol/doc.go          package-level doc comment; precise feature coverage + deliberate omissions
protocol/version.go      Version (V311/V50) type
protocol/errors.go       ErrMalformedPacket / ErrProtocolViolation / ErrStringTooLong / ErrFrameTooLarge
protocol/wire.go         bounds-checked cursor decoder, varint, appendString/appendBinary, ReadFrame, Frame/PacketType/ValidateFlags
protocol/reason.go       ReasonCode (all v5 codes) + v3-return-code-to-v5-reason mapping
protocol/properties.go   Properties model + propertySpec allow-table (single source of truth for encode/decode validation)
protocol/connect.go      CONNECT (encode only) + Will, CONNACK (decode only)
protocol/publish.go      PUBLISH (encode+decode), AckPacket (PUBACK/PUBREC/PUBREL/PUBCOMP, one shape)
protocol/subscribe.go    SUBSCRIBE/UNSUBSCRIBE (encode only), SUBACK/UNSUBACK (decode only)
protocol/control.go      PINGREQ/PINGRESP, DISCONNECT (encode+decode), AUTH (decode only)
protocol/topic.go        MatchTopic, ValidateTopicName, ValidateTopicFilter
protocol/fuzz_test.go    native go test Fuzz targets (FuzzReadFrame, FuzzDecodeProperties, FuzzPublishRoundTrip, FuzzPropertiesRoundTrip, FuzzTopicMatch)
e2e/                     env-gated scenario tests against real mosquitto + EMQX brokers over Docker; no build tag (always compiled)
e2e/gencert/             standalone `go run` program generating the e2e CA + server TLS cert
*_test.go                colocated tests (root + protocol), incl. review_round*_test.go (adversarial-review regressions)
.github/workflows/       ci.yml (lint, test matrix, fuzz-smoke, e2e), codeql.yml, release-on-tag.yml, dependabot-auto-merge.yml
.githooks/               pre-commit hook blocking direct commits on main/master
```

## Core Components

- **`protocol` package** — one codebase, both dialects: every packet
  type takes a `Version` argument (`V311`/`V50`); a v5 packet is a
  strict wire superset of its v3.1.1 form (property block and/or reason
  codes appended). CONNECT/CONNACK (with LWT + username/password, v5
  property block), PUBLISH at QoS 0/1/2 with the full PUBACK/PUBREC/
  PUBREL/PUBCOMP handshake (`AckPacket` is one shape for all four),
  SUBSCRIBE/SUBACK/UNSUBSCRIBE/UNSUBACK (multi-filter per frame),
  PINGREQ/PINGRESP, DISCONNECT/AUTH (AUTH decode only — this client
  rejects enhanced auth). Every decoder is bounds-checked via the
  `cursor` type in `wire.go` and **never panics** on arbitrary input —
  this is fuzzed (`protocol/fuzz_test.go`). `ReadFrame` enforces a
  caller-supplied `maxRemainingLength` (wired to
  `TCPConfig.MaximumPacketSize`, default 1 MiB) checked **before**
  allocating a body buffer. `properties.go`'s `propertySpec` table is
  the single source of truth for which property ID is legal on which
  packet type, used identically by the encoder (rejects illegal
  attachment) and the decoder (rejects an inbound violation as
  `ErrMalformedPacket`). See `protocol/doc.go` for the exact,
  currently-accurate feature/omission list — read it before assuming
  something is or isn't implemented.
- **`TCPClient`** (`adapter_tcp.go` + `pump.go` + `publish.go`) — dials
  a `tcp://` or `tls://` broker, implements `Client`
  (`Publisher`+`Subscriber`), `Connector` (`Connect`/`Disconnect`) and
  `ConnectionNotifier` (`ConnectionLost()`). Defaults to MQTT 5.0
  (`TCPConfig.ProtocolVersion` zero value); pin `ProtocolV311` for an
  older broker. Per-connection mutable state lives in a `link` struct
  (`conn`, `r`/`w`, `sendMu`+scratch buffer, `stop`, `aliases`) that
  `Connect` builds fresh and swaps in atomically — `readLoop`/
  `keepAliveLoop` each hold their own `*link` pointer, so there is no
  shared nil-able field for a reconnect to race against (see the lock
  ordering comment on `TCPClient` before touching any of `quota.mu` /
  `ids.mu` / `store.mu` / `waitersMu` / `link.sendMu`). `writeFrame`
  encodes a whole packet into `link.buf` under `sendMu` before a single
  `Write`+`Flush`, so a partial encode can never leave a truncated frame
  on the wire.
  - **QoS 2 + session resumption** (`session.go`): `SessionStore`
    (`Save`/`Delete`/`All`/`Reset`) persists in-flight `StoredPublish`/
    `StoredPubrel` (outbound) and `StoredInboundID` (inbound dedup)
    entries; the shipped `memStore` is in-memory only (the interface
    exists so a persistent store can be added later without an API
    break — not implemented in v1.0). `idAllocator` is a 65536-bit
    bitmap (`Acquire`/`Release`/`Reset`) shared by PUBLISH/SUBSCRIBE/
    UNSUBSCRIBE packet identifiers. On a broker-resumed session
    (`CONNACK Session Present=1`, not `CleanStart`, non-zero Session
    Expiry) stored state replays in `Seq` order before new traffic;
    otherwise `applySession` resets the store, identifiers and quota.
  - **Flow control** (`session.go`'s `quota`): a resizable counting
    semaphore bounding concurrent outbound QoS>0 sends to the broker's
    negotiated Receive Maximum (v5) or `TCPConfig.MaxInflight`
    (v3.1.1). `acquire` is ctx-cancellable; `fail()` unblocks every
    waiter with `ErrConnectionLost` on a drop; `reset(n)` reseeds the
    ceiling on (re)connect, accounting for messages already in flight
    for replay.
  - **Inbound topic aliases**: resolved per-connection
    (`link.aliases`) against `TCPConfig.TopicAliasMaximum`; an invalid
    alias closes the connection with reason `0x94` (outbound aliasing
    is out of scope).
  - **PINGRESP watchdog** in `keepAliveLoop` (`pump.go`) detects
    half-open sockets: `pingTimeoutThreshold = 2` consecutive
    unanswered PINGREQs before declaring the connection lost — rides
    out one delayed/dropped PINGRESP without a spurious reconnect. A
    broker `Server Keep Alive` override reschedules the ping interval
    (spec MUST) even below the client's 30s floor.
  - **Dispatch** (`pump.go`): delivers to **every** matching
    subscription, in registration order (an ordered slice, not a map).
  - **`ConnectionLost()`** exposes a buffered, non-blocking channel so
    `Lifecycle` reacts immediately instead of polling `IsConnected()`.
  - **Fail-fast**: `Publish`/`Subscribe`/`Unsubscribe` return
    `ErrNotConnected` immediately when disconnected; connection loss
    mid-flight fails every waiter immediately with `ErrConnectionLost`
    (no waiting out `AckTimeout` on a link already known dead).
- **`Lifecycle`** (`lifecycle.go`) — a transport-agnostic reconnect
  loop around any `Connector`: exponential backoff (`InitialBackoff` ->
  `MaxBackoff`, default 1s -> 30s) with jitter, an `OnConnect` callback
  hook, `errors.Is(err, ErrAlreadyConnected)` idempotency short-circuit,
  and — new in v1.0 — event-driven reconnect: if the `Connector` also
  implements `ConnectionNotifier` (`TCPClient` does), `loop()` selects
  on its `ConnectionLost()` channel alongside the jittered timer and
  reconnects immediately (backoff reset to `InitialBackoff`) instead of
  only noticing on the next timer tick. `Start`'s `ctx` governs the
  *whole* reconnect loop, not just the first connect — pass a long-lived
  context, not a short one.
- **`MessageHandler` contract**: `func(msg *Message)`. Handlers run
  **synchronously inline** in `TCPClient.readLoop` — the same goroutine
  that also decodes PUBACK/PUBREC/PUBCOMP/PINGRESP and feeds the
  keep-alive watchdog. A handler that blocks stalls ack/PINGRESP
  processing and can trip a spurious `ping_timeout`. `Message.Retain`
  carries the PUBLISH retain bit so a side-effecting handler can drop
  the retained replay the broker re-delivers on every (re)connect;
  `Message` also carries the v5 PUBLISH properties (`ContentType`,
  `UserProperties`, `SubscriptionIdentifiers`, ...), zero-valued on a
  v3.1.1 link. `LegacyHandler(fn)` adapts a v0.x-style
  `func(topic string, payload []byte, retained bool)` handler. This
  contract is documented on the `MessageHandler` type itself — read it
  before wiring a new consumer.
- **`NewClientTLSConfig`** (`tls_config.go`) — always sets `ServerName`
  explicitly (`tls.Client` does not infer it from the dialed address),
  closing the common "just set InsecureSkipVerify" trap.
  `insecureSkipVerify` must only ever be an explicit, operator-driven
  opt-in from the caller — never a default.

## Development Commands

Defined in the `Makefile` (`make help` lists them):

```sh
make test          # CGO_ENABLED=1 go test -race -count=1 -timeout=60s ./...
make test-cover    # tests + coverage report (coverage.out)
make cover-check   # per-package coverage gate (COVER_MIN=90 by default); NOT part of `check`
make vet           # go vet ./...
make fmt           # gofumpt -w . && goimports -w -local github.com/SukramJ/go-mqtt .
make fmt-check     # fail if gofumpt would rewrite anything (CI gate)
make lint          # golangci-lint run ./...
make vuln          # govulncheck ./...
make tidy          # go mod tidy
make check         # vet + fmt-check + lint + test — the pre-commit/pre-push gate
make fuzz-smoke    # 10s per ./protocol Fuzz target (CI smoke gate)
make fuzz          # FUZZTIME per ./protocol Fuzz target (default 5m; local/periodic)
make e2e-certs     # generate the e2e CA + server TLS cert (idempotent, gitignored output)
make e2e-up        # start the e2e mosquitto (plain/TLS/password listeners) + EMQX containers
make e2e-down      # stop and remove them
make test-e2e      # run ./e2e against the containers started by e2e-up (env-var gated, auto-skips without a broker)
make setup         # install gofumpt/goimports/golangci-lint/govulncheck + git hooks
make clean         # remove coverage.out and generated e2e assets (certs, passwd)
```

There is no `build`/`run`/`release` target — this module compiles as a
library only, and `go build ./...` (which CI's `test` job exercises
implicitly via `go test`) is the only "does it compile" check that
applies. `cover-check`, `fuzz`/`fuzz-smoke` and `test-e2e` are
deliberately **not** part of `check` (so iterative work isn't blocked
mid-change) but all run in CI and should be green before a release.

Run a single package's tests directly, e.g. `go test ./protocol/...`.

## Test Approach

- **`protocol`** tests are pure unit tests against encoded byte
  sequences (encode/decode round-trips for every packet type x both
  versions, a property-ID x packet-type matrix, malformed-input
  tables) — no network involved. Native Go fuzzing
  (`protocol/fuzz_test.go`) targets `ReadFrame`, property decoding,
  PUBLISH/property round-trips and topic matching; seed corpora are
  committed under `protocol/testdata/fuzz/`.
- **Root package** tests spin up an in-process scripted mock broker:
  `test_mock_broker.go` (compiled into the package, not `_test.go`, so
  it's usable from every `_test.go` file without visibility issues) is
  a multi-connection, dual-version (v3.1.1/v5) mock supporting, among
  other knobs: `RejectNextConnect` (non-zero CONNACK reason),
  `SetSessionPresent`/`SetConnackProperties` (resumption + negotiated
  limits), `RejectSubscribe`/`GrantQoS` (SUBACK policy),
  `DropNextPuback`/`DropNextPubrec`/`DropNextPubcomp`/
  `DuplicateNextPuback` (QoS 1/2 handshake fault injection at every
  state), `InjectPublish`/`InjectDisconnect`/`InjectAuth`/
  `InjectRawFrame` (server-initiated traffic and malformed frames),
  `InjectTCPReset` (simulated abrupt TCP close), and
  `DropPings`/`DropNextPings` (half-open socket simulation exercising
  the PINGRESP watchdog). `review_round1_test.go`/
  `review_round2_test.go` (root and `protocol/`) are regression tests
  for confirmed adversarial-review findings — read them before
  touching the code paths they cover.
- **E2E** (`e2e/`, no build tag — always compiled and vetted/linted,
  just skipped at runtime): scenario tests against real mosquitto and
  EMQX brokers over Docker, gated by `MQTT_E2E_MOSQUITTO`/
  `MQTT_E2E_MOSQUITTO_TLS`/`MQTT_E2E_MOSQUITTO_AUTH`/`MQTT_E2E_EMQX`/
  `MQTT_E2E_CERTS_DIR` plus a 2s dial probe, `t.Skip`-ping when no
  broker is reachable. Covers both protocol versions, TLS (pinned CA),
  password auth, QoS 0/1/2, retained replay, LWT (incl. v5 will
  properties) after a hard socket kill, session resumption across a
  simulated broker "restart" (an in-test TCP proxy, so broker-side
  session/retained state survives — real `docker restart` is also
  available behind `MQTT_E2E_ALLOW_DOCKER_CONTROL=1`, CI-only), Server
  Keep Alive override, and v5 extras (user properties, message expiry,
  inbound topic alias, Receive Maximum back-pressure).
- **Coverage gate**: `make cover-check` fails if any non-`e2e` package
  drops below `COVER_MIN` (default 90%); it parses `go tool cover
  -func` per package, not a merged total, so one weak package can't
  hide behind a strong one.
- Always run with the race detector (`make test` sets
  `CGO_ENABLED=1`) — the client is concurrency-heavy (read loop,
  keep-alive loop, waiter map, subscription slice, quota and id
  allocator all guarded by separate mutexes/atomics per the lock-order
  comment on `TCPClient`).

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
  just current state, and it applies to tests too (the mock broker,
  fuzz harness and e2e TCP proxy are all hand-rolled). Before reaching
  for a library (even a small one), re-derive the need against the
  standard library.
- **Decoders never panic** on arbitrary input — every protocol decode
  path goes through the bounds-checked `cursor` in `protocol/wire.go`.
  A new decoder must do the same (no direct slice indexing) and should
  get a fuzz target or at least a seed in the existing corpus.
- **Git hooks**: `make setup` (or `make hooks`) points
  `core.hooksPath` at `.githooks/`, which blocks direct commits on
  `main`/`master`.
- **Commit style**: Conventional Commits with a scope, e.g.
  `feat(protocol)!: ...`, `fix(client): ...`, `chore(deps): ...`; a
  trailing `!` (or a `BREAKING CHANGE:` footer) marks an API- or
  behavior-breaking commit.
- **CI** (`.github/workflows/ci.yml`) runs four jobs: `lint` (go vet +
  gofumpt check + golangci-lint), `test` (matrix across
  ubuntu/macos/windows with the race detector), `fuzz-smoke` (10s per
  `./protocol` Fuzz target, ubuntu), `e2e` (ubuntu-only — macOS/Windows
  runners have no Docker — starts mosquitto + EMQX via `make e2e-up`,
  which blocks until both brokers log readiness, runs `make test-e2e`,
  dumps container logs on failure). A separate `codeql.yml` runs CodeQL
  SAST on push/PR/weekly schedule; `dependabot-auto-merge.yml`
  auto-merges non-major Dependabot PRs; `release-on-tag.yml` creates a
  GitHub Release for every pushed tag with the matching `CHANGELOG.md`
  section as its body (and fails if that section is missing) — a
  library release ships notes only, no binaries.

## Non-Goals (v1.0)

Deliberately not implemented — do not add these without a fresh design
discussion, they were explicitly scoped out:

- **Enhanced authentication** (AUTH send / re-auth flows). AUTH is
  decoded only, so the adapter can reject an unexpected one; there is
  no client-initiated AUTH exchange.
- **Outbound topic aliasing.** Inbound alias resolution is implemented;
  the client never assigns or sends an alias of its own on publish.
- **A persistent `SessionStore`.** The interface exists precisely so
  one can be added later without an API break, but v1.0 ships only the
  in-memory `memStore` — no on-disk/DB-backed implementation, no
  config hook to plug one in yet.
- **Offline publish queueing.** A `Publish` on a disconnected client
  fails immediately with `ErrNotConnected` rather than being queued for
  the next reconnect.
- **A WebSocket transport.** Only `tcp://`/`tls://` (`mqtt://`,
  `ssl://`, `mqtts://` aliases) are dialed.
- **A shared-subscription helper.** `$share/...` filter syntax passes
  through the wire codec untouched; there is no client-side API sugar
  for it.

## Consumers and Compatibility

This module is imported by (at least) `go-mtec2mqtt`,
`go-zendure2mqtt`, `go-homeconnect2mqtt`, **and `openccu-loom`** (the
project this client was originally written for and later carved out
of — it consumes the extracted module too, not just the three
bridges). **A breaking change here breaks all four at once.** Practical
implications:

- Follow SemVer discipline strictly. Any change to an exported
  signature in `client.go`, `options.go`, `errors.go`, `adapter_tcp.go`,
  `pump.go`, `publish.go`, `session.go`, `lifecycle.go`, `tls_config.go`,
  or `protocol/` is a breaking change unless it is purely additive.
- Before changing behavior (not just signatures) — e.g. the PINGRESP
  watchdog timing, the resubscribe-on-reconnect behavior, the
  fail-fast-on-disconnect contract, the flow-control quota accounting,
  or the `MessageHandler` synchronous-dispatch contract — consider that
  every consumer relies on the current behavior implicitly, often
  without a direct test of its own for that behavior. Check this
  module's own tests first (especially `review_round*_test.go` and the
  `e2e/` scenarios); they are the closest thing to a compatibility
  contract.
- New functionality should default to being additive (new exported
  function/type/option) rather than changing an existing default.
- v1.0 was itself a breaking rewrite with a documented migration path —
  see [MIGRATION.md](./MIGRATION.md) before assuming a v0.x call site
  still compiles unchanged.

## When in Doubt

- Read [`README.md`](./README.md) first — it documents the feature set
  and has runnable usage examples (v5-default `TCPClient` + `Lifecycle`
  wiring, a v3.1.1-downgrade + `WithUserProperties` variant, `tls://`
  broker setup, and the full test-command list).
- For MQTT 5.0 semantics, the canonical reference is the OASIS
  mqtt-v5.0-os specification; for MQTT 3.1.1, mqtt-v3.1.1-os.
  `protocol/doc.go` documents exactly which subset of each spec this
  module implements and which parts it deliberately does not — read it
  (and the Non-Goals section above) before assuming a gap is a bug.
- [MIGRATION.md](./MIGRATION.md) and [CHANGELOG.md](./CHANGELOG.md)
  describe the v0.x -> v1.0 rewrite in detail: old/new signatures,
  before/after diffs, and every behavior change, including the ten
  robustness/spec issues (data races, wire-corruption, topic-matching,
  fail-fast, flow control, ...) found and fixed during that rewrite.
- For how this module is actually consumed in practice, look at
  `internal/mqtt` in a sibling `go-*2mqtt` bridge repo (e.g.
  `go-mtec2mqtt`) at a pre-v1.0 tag — that directory is the
  pre-extraction copy this module replaces, so it shows the original
  call sites and can help answer "why does this API look like this."
