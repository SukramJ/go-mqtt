# Changelog

All notable changes to this project are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-07-04

Major rewrite: MQTT 5.0 support (now the default protocol), full QoS 2
in both directions, session resumption, flow control, and a bulletproof
pass over every race/robustness issue found in the v0.x implementation.
See [MIGRATION.md](./MIGRATION.md) for the upgrade guide.

### Added

- **MQTT 5.0 support, default protocol.** `protocol` now encodes and
  decodes both MQTT 3.1.1 and MQTT 5.0 for every packet type, selected
  per-connection via `TCPConfig.ProtocolVersion` (zero value = v5). The
  v5 property model (`protocol.Properties`, all 27 property IDs),
  reason codes, and a v3-return-code-to-v5-reason-code mapping are new.
- **QoS 2, both directions.** Full PUBREC/PUBREL/PUBCOMP handshake for
  outbound publishes and the exactly-once receiver state machine
  (method A) for inbound ones, backed by a new `session.go`
  (`SessionStore` interface + in-memory implementation, packet-id
  bitmap allocator).
- **Session resumption.** On a broker-resumed session (CONNACK Session
  Present = 1 with `CleanStart=false`/non-zero Session Expiry),
  unacknowledged outbound QoS 1/2 publishes and the inbound QoS 2 dedup
  set are replayed in original order before new traffic.
- **Flow control.** A counting-semaphore send quota bounds concurrent
  in-flight outbound QoS 1/2 publishes to the broker's negotiated
  Receive Maximum (v5) or the new `TCPConfig.MaxInflight` (v3.1.1);
  `acquire` is context-cancellable and fails fast on connection loss.
- **Inbound topic aliases.** A v5 broker publishing with a topic alias
  is resolved against a per-connection table (`TCPConfig.TopicAliasMaximum`
  advertises the accepted range); a violation closes the connection
  with reason `0x94` (outbound aliasing remains out of scope).
- **`TCPClient.ConnectResult()`** exposes the negotiated CONNACK state:
  session present, assigned client id, server keep-alive override,
  Receive Maximum, Maximum QoS, Retain Available, Maximum Packet Size,
  Topic Alias Maximum, user properties.
- **Event-driven `Lifecycle` reconnect.** `Lifecycle` now recognizes an
  optional `ConnectionNotifier` capability on its `Connector` and
  reconnects immediately (backoff reset to `InitialBackoff`) on a
  detected drop, instead of only noticing on the next backoff timer
  tick.
- **`e2e/` package**: scenario tests against real mosquitto and EMQX
  brokers over Docker (`make e2e-up`/`test-e2e`/`e2e-down`), covering
  both protocol versions, TLS, password auth, retained replay, LWT,
  session resumption over a simulated broker restart, and v5-specific
  behavior (user properties, message expiry, topic aliases, Receive
  Maximum back-pressure).
- **Native fuzzing** (`protocol/fuzz_test.go`): `FuzzReadFrame`,
  `FuzzDecodeProperties`, `FuzzPublishRoundTrip`,
  `FuzzPropertiesRoundTrip`, `FuzzTopicMatch`, wired to `make
  fuzz`/`make fuzz-smoke`.
- New Make targets: `test-cover`, `cover-check` (per-package coverage
  gate), `fuzz`, `fuzz-smoke`, `e2e-certs`, `e2e-up`, `e2e-down`,
  `test-e2e`.

### Changed (BREAKING)

- **`Subscribe` now returns `(SubscribeResult, error)` and blocks until
  the SUBACK** (bounded by `ctx` and `AckTimeout`), instead of
  returning as soon as the frame was written. A broker rejection
  (reason code >= `0x80`) is now a `*ReasonError` instead of a
  fire-and-log warning.
- **`MessageHandler` is now `func(msg *Message)`** (was
  `func(topic string, payload []byte, retained bool)`).
  `mqtt.LegacyHandler(fn)` adapts an old-style handler mechanically.
  `Message` adds the MQTT 5.0 PUBLISH properties (`ContentType`,
  `ResponseTopic`, `CorrelationData`, `MessageExpirySeconds`,
  `PayloadFormatUTF8`, `SubscriptionIdentifiers`, `UserProperties`),
  zero-valued on a v3.1.1 link.
  Dispatch now delivers to **every** matching subscription in
  registration order (previously the first match only, non-deterministic
  under Go's map iteration — see Fixed/B4 below).
- **`TCPConfig.CleanSession` renamed to `CleanStart`** (same wire bit).
- **`TCPConfig.WillTopic`/`WillPayload`/`WillRetain` replaced by
  `TCPConfig.Will *Will`**, which adds a configurable `QoS` (previously
  hardcoded to 0) and, on a v5 link, will properties (delay interval,
  message expiry, content type, response topic, correlation data,
  payload format, user properties).
- **`Publish`/`Subscribe` gain variadic functional options**
  (`PublishOption`/`SubscribeOption`) for MQTT 5.0 PUBLISH properties
  and subscription options; source-compatible for existing
  fixed-arity call sites.
- **Sentinel errors replace ad hoc error strings/timeouts**:
  `ErrNotConnected`, `ErrAlreadyConnected`, `ErrConnectionLost`,
  `ErrPacketTooLarge`, `ErrPacketIDExhausted`, and `*ReasonError` for
  broker-reported v5 failure reason codes. Match with
  `errors.Is`/`errors.As`.
- **Fail-fast instead of riding out `AckTimeout` on a dead link**:
  `Publish`/`Subscribe` return `ErrNotConnected` immediately when
  disconnected, and every in-flight call fails immediately with
  `ErrConnectionLost` the moment a drop is detected (previously it
  waited out the full ack timeout — see Fixed/B5 below).
- **Broker-advertised limits are enforced client-side**: a `Publish`
  above the negotiated Maximum QoS, or a retained `Publish` when
  Retain Available = 0, now fails locally with a wrapped
  `protocol.ErrProtocolViolation` instead of being sent and triggering
  a broker DISCONNECT that tears down the whole connection.
- `protocol` package: `PublishPacket`/`InboundPublish` merged into one
  `PublishPacket`; PUBACK/PUBREC/PUBREL/PUBCOMP unified under one
  `AckPacket` shape; `ReadFrame`'s remaining-length cap is now a
  parameter (wired to `TCPConfig.MaximumPacketSize`) instead of a
  hardcoded constant.

### Fixed

Ten robustness/spec issues found analyzing the v0.x implementation
ahead of this rewrite:

- **Data race and possible nil-pointer panic around the connection
  writer.** `Disconnect` and the read loop's inbound-PUBACK path read
  the shared `c.writer`/`c.reader` fields without holding the lock
  `handleConnectionLost` used to nil them out concurrently. Fixed by
  moving all per-connection state into an immutable-after-construction
  `link` struct that the read and keep-alive loops each hold their own
  pointer to, eliminating the shared nil-able field entirely.
- **Strings/binary fields over 65535 bytes were silently truncated**
  instead of rejected, corrupting the two-byte length prefix on the
  wire. Encoding now returns `ErrStringTooLong`.
- **Topic filter matching was wrong for the parent-level `#` case**
  (`a/#` did not match topic `a`, violating §4.7.1.2) and performed no
  filter validation (`#` only as the last, whole level; `+` as a whole
  level; wildcard filters must never match a `$`-prefixed topic).
  Rewritten in `protocol/topic.go` with `MatchTopic`,
  `ValidateTopicFilter`, `ValidateTopicName`.
- **Dispatch only delivered a message to the first matching
  subscription** when multiple filters overlapped, because it iterated
  a Go map (unordered). Subscriptions are now an ordered slice; dispatch
  delivers to every match in registration order.
- **Pending acknowledgements were not failed on connection loss**, so a
  `Publish`/`Subscribe` in flight when the socket dropped blocked for
  the full `AckTimeout` instead of failing immediately. Connection loss
  now fails every waiter immediately with `ErrConnectionLost`.
- **CONNECT allowed a password without a username**, which MQTT 3.1.1
  §3.1.2.9 forbids (MQTT 5.0 permits it). Validation is now
  version-dependent.
- **Reconnect idempotency was detected by matching an error string**
  (`"already connected"`) instead of a typed error. `Connect` now
  returns a wrapped `ErrAlreadyConnected` sentinel matched with
  `errors.Is`.
- **`Lifecycle` never consumed the client's connection-loss signal**,
  relying purely on timer polling to notice a drop (potentially a full
  `MaxBackoff` late). It now reacts immediately via the optional
  `ConnectionNotifier` capability.
- **A partially-encoded frame could reach the wire.** The old
  `writeFrame` encoded directly into the shared `bufio.Writer`; an
  encode error partway through left a truncated fixed header queued for
  the next write. Frames are now fully encoded into a scratch buffer
  first, then written and flushed in one pass.
- **Packet-identifier allocation did not check for collisions** with
  identifiers still in flight (only relevant once QoS 2 and Receive
  Maximum tracking existed). Replaced with a 65536-bit bitmap allocator
  that skips in-use identifiers.

Two additional issues surfaced after the initial rewrite:

- **Subscription handler registered after the SUBACK round-trip.** A
  broker commonly delivers the retained-message replay for a new
  subscription in the same flush as the SUBACK; registering the handler
  only after `Subscribe`'s ack wait returned lost that race against the
  read loop and silently dropped the first message(s) — caught by the
  e2e retained-replay tests against mosquitto. The handler is now
  registered before the SUBSCRIBE is sent, and rolled back if the
  SUBSCRIBE fails.
- **A pipelined ack read after teardown could corrupt the next
  connection's state.** `readLoop` could still process a buffered
  PUBACK/PUBCOMP from a just-torn-down socket against the *new* link's
  packet-id allocator and send quota after a fast reconnect, risking a
  double-freed identifier or an over-credited quota. The read loop now
  checks the stop signal before dispatching a decoded frame.

Adversarial-review fixes (two rounds, each with regression tests):

- Broker-advertised Maximum QoS / Retain Available were not enforced on
  outbound `Publish` (now checked locally, see Changed above).
- A CONNACK advertising Receive Maximum = 0 or Maximum Packet Size = 0
  (both a §3.2.2.3 Protocol Error) was accepted, seeding a send quota
  that would starve every QoS>0 publish, or a packet-size limit
  silently treated as unlimited. Both are now refused with a
  best-effort DISCONNECT `0x82`.
- The send-quota permit for an in-flight QoS>0 publish was released too
  early (on ctx-cancel/ack-timeout/connection-loss) instead of staying
  held until the terminal acknowledgement, risking an over-commit past
  Receive Maximum; session replay likewise bypassed the quota. Both now
  account correctly across a reconnect.
- Inbound MQTT strings were not validated as well-formed UTF-8 /
  rejected for embedded `U+0000`, as the spec (§1.5.4) requires.
- A malformed inbound PUBLISH was logged and read past instead of
  closing the connection (§4.13.1), which could livelock on a broker's
  unbounded retransmits of an unacknowledgeable QoS 1/2 PUBLISH.
- A concurrent `Subscribe` for the same filter could have its
  successful registration clobbered by another `Subscribe`'s failure
  rollback; registrations now carry a monotonic token so a rollback
  only undoes its own, still-current registration.
- `DecodeSuback` silently accepted an unsupported protocol version
  instead of returning `ErrProtocolViolation` like its sibling decoders.
- The read-loop's per-ack/per-QoS2-PUBLISH store lookup was an
  O(n log n) snapshot-and-sort; the default store now offers an O(1)
  fast path.
- `WithResponseTopic` did not reject wildcard characters
  (`MQTT-3.3.2-14`); a wildcarded Response Topic is now rejected
  locally instead of provoking a broker DISCONNECT that tears down the
  whole connection.

## [0.2.0] - 2026-07-03

Two changes carried back from the `openccu-loom` transport this module
was extracted from, where they still lived after the 0.1.0 carve-out.

### Changed (BREAKING)

- **`MessageHandler` now takes a third `retained bool` argument**
  (`func(topic string, payload []byte, retained bool)`). It carries the
  MQTT PUBLISH retain bit so a handler with side effects can drop the
  retained message the broker re-delivers on every (re)connect —
  without it, a stale `mosquitto_pub -r` command is re-applied to the
  real device each time the consumer restarts. Consumers that don't
  care can ignore the parameter (`func(_ string, _ []byte, _ bool)`).
  This is a source-breaking signature change for every `Subscribe` call
  site.

### Fixed

- **Subscription replay now preserves the QoS each filter was
  subscribed at.** The 0.1.0 reconnect path re-subscribed every filter
  at a hardcoded QoS 1; it now records and replays the caller's
  requested QoS, so a QoS 0 subscription is no longer silently upgraded
  on reconnect.
- **The PINGRESP watchdog no longer trips on a single delayed pong.**
  It now tolerates one unanswered PINGREQ and only declares the socket
  dead after two consecutive misses (≈ one full KeepAlive). A lone
  late/dropped PINGRESP — a GC pause, a scheduler stall on a
  CPU-throttled host, a momentary network blip — previously forced a
  spurious `mqtt.tcp.ping_timeout` + reconnect; a genuinely half-open
  socket is still detected, one ping interval later.

## [0.1.0] - 2026-07-02

Initial release. This module is the shared MQTT 3.1.1 transport layer,
extracted from the `go-*2mqtt` bridges (`go-mtec2mqtt`, `go-daikin2mqtt`,
`go-homeconnect2mqtt`, `go-zendure2mqtt`) where it previously lived four
times over as `internal/mqtt`. Consolidating it here means a fix lands
once and every bridge picks it up via `go get -u`, instead of drifting
across four copies.

### Added

- **`protocol` package** — MQTT 3.1.1 wire codec (CONNECT / CONNACK /
  PUBLISH / PUBACK / SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK /
  PINGREQ / PINGRESP / DISCONNECT), QoS 0 and 1. Includes a 1 MiB
  frame-size cap that rejects an oversized `remaining length` **before**
  allocating a body buffer, closing an OOM/DoS vector against a
  malicious or malfunctioning broker.
- **`mqtt` package** — `TCPClient` for `tcp://` and `tls://` brokers
  with subscription replay on reconnect, a PINGRESP watchdog that
  detects half-open sockets, a `ConnectionLost()` channel for
  event-driven reconnect, and SUBACK return-code handling that surfaces
  broker-rejected subscriptions.
- **`Lifecycle`** — a reconnect loop with exponential backoff and
  jitter that waits for the loop to exit before disconnecting (no
  teardown race).
- **`NewClientTLSConfig`** — always sets `ServerName` and keeps
  certificate verification on unless an explicit opt-in disables it.
- No third-party dependencies — standard library only. MIT licensed
  (openccu-loom provenance).
