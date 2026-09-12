# Changelog

All notable changes to this project are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.5.0] - 2026-09-12

MQTT 5.0 Subscription Identifiers, so a client can tell which of its own
subscriptions a delivered message arrived for. Additive: a caller that
does not ask for one sees exactly the previous behaviour.

### Added

- `WithSubscriptionID(id uint32)` sets the Subscription Identifier
  (§3.8.2.1.2) on a single `Subscribe` call, and `dispatch` then
  delivers a stamped PUBLISH only to the subscription that identifier
  names.

  The measured reason: a broker sends one copy of a PUBLISH **per
  matching subscription** (§3.3.4), and deciding delivery by
  re-matching each copy's topic against every registered filter
  multiplies the two. Two overlapping filters, two copies, both
  handlers on each copy — a handler runs twice per published message.
  That was measured against Mosquitto 2.1.2 on both dialects while
  tracking down a consumer whose command handler performed its write
  twice, with nothing in any log. Identifiers turn the broker's
  fan-out into something the client can attribute.

  Three deliberate refusals, each because the silent alternative is
  worse than an error:
  - An identifier the client never registered is **dropped**, not
    broadened into a topic match. It names a subscription this process
    did not create — a session resumed from a previous run — and
    guessing a handler for it would deliver a message to code that
    never asked for it.
  - The option is **refused on an MQTT 3.1.1 link** rather than
    ignored. That dialect has no property block, so a silently dropped
    identifier would leave a caller believing its deliveries are
    attributable while the client is still re-matching topics — the
    exact failure the option exists to prevent, now invisible.
  - A value outside 1..268435455 is refused **before** the encoder, so
    the error names the value the caller chose instead of a frame it
    did not write.

  The identifier is replayed with its filter on reconnect. A broker
  holds it as part of the subscription and forgets it with the
  session, so a replay that omitted it would leave attribution
  silently working before a drop and silently not after it.

  Note for a client that must be correct on both dialects: identifiers
  are MQTT 5.0 only, so they are an improvement on a v5 link and not a
  substitute for keeping overlapping filters disjoint.

## [1.4.0] - 2026-09-09

Two additive helpers extracted from the consuming bridges, where each
existed in four or five near-identical copies. No behavior changes.

### Added

- `SplitClient(Publisher, Subscriber) Client` joins a decorated
  publisher and an undecorated subscriber into one `Client`. Every
  `go-*2mqtt` bridge carried its own `mqttSession` struct doing exactly
  this, so a `Breaker` could guard the publish path while subscriptions
  went through the raw client.
- `ConnectWithRetry(ctx, Starter, RetryConfig)` retries
  `Lifecycle.Start` with exponential backoff until it succeeds, the
  context is done, or `MaxAttempts` is reached. `Start` deliberately
  makes a single attempt so a caller can treat a missing broker as
  fatal; a daemon almost never wants that, and two bridges had written
  the same `startMQTT` wrapper to say so. `Starter` is an interface, so
  a consumer can substitute a fake without a broker.

### Fixed

- `TestSessionReplayCleanStartDiscardsStore` reconnected without first
  waiting for the read loop to clear the link after `InjectTCPReset()`,
  and could fail with `ErrAlreadyConnected` under load. It now performs
  the same wait as every other reset-then-reconnect test in the suite.
  Test-only; no production code was involved.

## [1.3.0] - 2026-08-16

Audit release: a full-codebase adversarial review (four parallel
reviewers over the wire codec, the connection core, the supporting
components and the test/CI infrastructure, each finding substantiated
by a failing test, a byte trace or a probe, plus read-only verification
against a production mosquitto broker) produced 42 findings — 3 high,
7 medium, 32 low. All are fixed or documented below. Exported API
changes are purely additive.

### Added

- `LifecycleConfig.FlapWindow` (default 10s): a connection that drops
  within this window of coming up counts as flapping and reconnects
  with an exponentially growing delay instead of immediately.
- `ConnectResult.ServerKeepAliveSet`: distinguishes a broker-sent
  Server Keep Alive of 0 (keep-alive disabled, §3.1.2.10 — the client
  now stops pinging entirely) from the property being absent.
- Shared-subscription support in the codec: `protocol.MatchTopic`
  matches a `$share/{ShareName}/{filter}` subscription against the
  real delivery topic (prefix stripped per §4.8.2) — previously such a
  subscription silently dropped every inbound message — and
  `protocol.ValidateTopicFilter` enforces the §4.8.2 structural rules.

### Fixed

- **Teardown/Connect race poisoning a healthy session (high).**
  `teardownLink` cleared the link pointer before failing the shared
  waiters and send quota; a `Connect` landing in that window
  established a healthy session whose quota the stale teardown then
  marked failed — every QoS>0 publish returned `ErrConnectionLost`
  forever while `IsConnected()` reported true, unrepairable by the
  reconnect loop. Shared state is now poisoned before the pointer is
  cleared.
- **Undamped reconnect storm against a flapping broker (high).** The
  event-driven reconnect path had no delay and reset the backoff on
  every loss, redialling a broker that accepts CONNECTs and
  immediately drops the socket at measured ~6000 dials/s. See
  `FlapWindow` above.
- **Circuit breaker tripped by client-side validation errors (high).**
  `protocol.ErrProtocolViolation`/`ErrMalformedPacket`/
  `ErrStringTooLong` raised before any bytes reach the wire (invalid
  topic, QoS above the broker maximum, ...) counted as broker failures,
  so one malformed topic opened the circuit for every healthy publish —
  contrary to the documented contract. They are neutral now.
- **Cross-session release in the read loop (medium).** The terminal-ack
  paths released packet identifiers and quota permits unguarded; a read
  loop stalled inside a store call across a reconnect could free an
  identifier owned by the new session and credit its quota past the
  negotiated Receive Maximum (§4.9). Both now release through the
  generation-checked paths keyed to the link's session.
- **Spurious reconnect after an intentional Disconnect (medium).** The
  read/keep-alive loops ignored the graceful flag, so a broker closing
  the socket in response to our own DISCONNECT signalled a connection
  loss and made the Lifecycle reconnect a session the caller had just
  shut down.
- **Breaker outcomes booked against the wrong state (medium).** Every
  state transition now bumps an epoch and outcomes from a superseded
  epoch are discarded: a straggler publish can no longer free a
  half-open probe slot it never held (admitting more than
  `HalfOpenMax` concurrent probes) or close the circuit on stale
  pre-trip evidence. `OnStateChange` callbacks are additionally
  delivered in transition order.
- **Backoff-collapsing jitter (medium).** `Jitter >= backoff` made a
  large share of reconnect delays non-positive (an immediate-retry
  spin); jittered delays are clamped to a floor of half the nominal
  value, config defaulting handles negative values, and an inverted
  `MaxBackoff < InitialBackoff` pair is raised.
- **CONNACK sanity (low).** Session Present=1 answering CleanStart=1
  refuses the session ([MQTT-3.2.2-4]); a phantom `ConnectResult` is no
  longer published when the connect fails during session replay.
- **QoS 2 recovery (low).** A PUBREC/PUBREL for an unknown packet
  identifier is answered with reason 0x92 (Packet Identifier not
  found) on v5 — previously warn-logged and left unanswered, stranding
  the broker's QoS 2 flow in a retransmit loop.
- **Watchdog and dispatch robustness (low).** The PINGRESP counter
  increments before the PINGREQ write (restoring the documented
  one-lost-PINGRESP tolerance); server-to-client-illegal packets
  (second CONNACK, SUBSCRIBE, PINGREQ, ...) tear the connection down
  per §4.13 instead of being read past; `Lifecycle.Start` drains a
  stale ConnectionLost token buffered before its first connect.
- **Stored-payload aliasing (low).** QoS>0 publishes deep-copy the
  payload (and v5 correlation data) into the session store, so a caller
  reusing its buffer can no longer corrupt the DUP replay after a
  resumed session; the buffer-ownership rule is documented on
  `Publish`.
- **Codec spec conformance (low).** The wire codec now rejects: Topic
  Alias 0, packet identifier 0 on every packet carrying one, DUP=1 on
  QoS 0, non-minimal Variable Byte Integers ([MQTT-1.5.5-1]), more than
  one Subscription Identifier on a SUBSCRIBE, out-of-range property
  values (zero Receive Maximum/Maximum Packet Size, non-boolean flag
  properties, Maximum QoS > 1), Session Present=1 with an error reason
  code, out-of-range SUBACK/UNSUBACK reason codes (previously
  surfaced as a nonsense "granted QoS 3" with a nil error), and
  unsupported protocol versions in every encoder and decoder.
  `ReadFrame` caps the total packet size (§2.1.4) instead of the
  remaining length.
- **Test/CI/infrastructure (low/medium).** ci.yml actions pinned by
  commit SHA and lint tools by version; the release workflow's
  CHANGELOG extraction matches the heading literally instead of via an
  unescaped regex; expired e2e TLS certs are regenerated instead of
  reused; the e2e Server Keep Alive scenario is reachable against the
  harness's own mosquitto; stale documentation (breaker coverage,
  reconnect semantics, a dead e2e env var, copyright headers) aligned
  with reality.

## [1.2.0] - 2026-07-07

Hardening release: a multi-agent adversarial audit across seven
dimensions (concurrency, decoder robustness, resource exhaustion,
spec conformance, error handling, TLS/trust boundaries, test gaps)
produced 28 verified findings, all fixed below. No exported signatures
changed; the behavior changes are confined to inputs/states that were
already broken (malformed frames, misconfiguration, races).

### Fixed

- **Concurrent `Connect`/`Disconnect` TOCTOU.** Both calls are now
  serialised end-to-end by an internal mutex, so two concurrent
  `Connect` calls can no longer establish two live links sharing one
  session state (double read loops, clobbered send quota, spurious
  `ErrConnectionLost` on the healthy link), and `Disconnect` can no
  longer report success while a concurrent `Connect` is mid-handshake.
  `teardownLink` additionally refuses to fail the shared waiters/quota
  when the link being torn down is not the current one.
- **Cross-epoch quota/packet-id releases.** `quota` and the packet-id
  allocator carry a generation counter, bumped on every reset. A
  Publish/Subscribe goroutine that stalls across a teardown+reconnect
  and then unwinds can no longer over-credit the send quota past the
  broker's Receive Maximum or free a packet identifier the new session
  handed to another exchange; its writeFrame-failure cleanup is skipped
  wholesale when the session epoch has advanced (the state now belongs
  to the resumed session's replay).
- **Forged/mismatched acknowledgements.** Ack waiters are typed by
  acknowledgement class (publish-ack / SUBACK / UNSUBACK). A SUBACK
  carrying the packet identifier of an in-flight QoS>0 PUBLISH — a
  hostile-broker data-integrity lie that previously resolved the
  Publish as successful and leaked its quota permit — is now
  warn-logged and ignored; the waiter resolves through the real ack or
  its timeout.
- **Resumed-session replay ordering.** Stored QoS>0 state and
  resubscriptions are replayed *before* the link pointer is published,
  so a concurrent `Publish` can no longer jump ahead of (or interleave
  with) the DUP replay ([MQTT-4.4.0-1] / v5 §4.4). Until replay
  completes, publishes keep failing fast with `ErrNotConnected`.
- **`Lifecycle.Stop` racing `Start`'s first connect.** A `Stop` that
  landed while the synchronous first connect was in flight could
  return success and leave the session connected with no reconnect
  loop. `Start` now detects the intervening `Stop`, disconnects the
  just-established session and returns a "stopped during start" error.
- **`Unsubscribe` clobbering a newer registration.** The local handler
  removal is token-checked (mirroring the Subscribe rollback guard), so
  a concurrent `Subscribe` for the same filter that registered while
  the UNSUBSCRIBE was in flight keeps its live registration.
- **Blocked-handler session poisoning.** A QoS 2 `MessageHandler` that
  blocks across a teardown+reconnect no longer records its inbound
  dedup entry into the next session's freshly reset store (which would
  swallow a future QoS 2 message reusing the identifier as a
  duplicate); QoS 1 acks are likewise skipped on a dead link.
- **Caller-supplied `TLSConfig` with empty `ServerName`.** The config
  is now cloned per connection and an empty `ServerName` is filled from
  the broker URL, so the natural CA-pinning config (`RootCAs` only)
  completes a *verified* handshake instead of failing with an error
  that steers operators toward `InsecureSkipVerify`.
- **Credential leak in the connected log.** `mqtt.tcp.connected` logs
  the redacted broker URL (`url.Redacted()`), so a password embedded in
  the URL userinfo no longer reaches the structured log stream on every
  reconnect.

### Changed

- **Malformed acknowledgements are now fatal (spec §4.13.1).** A
  PUBACK/PUBREC/PUBCOMP/PUBREL/SUBACK/UNSUBACK the codec cannot decode
  closes the connection (DISCONNECT 0x81 on v5) instead of being
  warn-logged and skipped — previously the in-flight exchange leaked
  its stored entry, packet identifier and send-quota permit forever on
  a long-lived connection. Well-formed acks for unknown identifiers are
  still warn-and-continue.
- **Write deadlines on every socket write.** `writeFrame` bounds each
  write/flush with `AckTimeout`, and a failed write tears the link down
  (a deadline can expire mid-frame, poisoning the stream). A zero-window
  or half-open broker can no longer wedge `Publish` — and the keep-alive
  watchdog with it — for the kernel's ~15-minute TCP retransmission
  timeout while holding the send mutex.
- **`DialTimeout` now truly bounds the whole connect.** One absolute
  deadline covers the dial, TLS handshake, CONNECT flush *and* CONNACK
  wait (previously the CONNECT write was unbounded and the CONNACK wait
  added a second `DialTimeout` on top). Cancelling the caller's context
  now aborts an in-flight CONNACK wait promptly — and `Connect` can no
  longer succeed on a context the caller has already abandoned.
- **No DISCONNECT on MQTT 3.1.1 protocol errors.** The v3 DISCONNECT is
  defined as a clean disconnect that discards the Last Will
  ([MQTT-3.14.4-3]); on a protocol-error teardown the client now closes
  the socket abruptly instead, keeping the LWT armed. v5 behavior
  (DISCONNECT 0x81) is unchanged.
- **Inbound PUBLISH validation.** `protocol.DecodePublish` now rejects
  (as `ErrMalformedPacket`, closing the connection) a QoS>0 PUBLISH
  with a zero packet identifier, a topic containing `+`/`#`, and an
  empty topic without a v5 topic alias — spec-malformed frames that
  previously polluted the QoS 2 dedup state or reached handlers.
- **Will topic validation.** `Connect` fails fast with a clear error on
  a Will topic (or v5 Will response topic) that violates topic-name
  rules, instead of emitting a malformed CONNECT that turns into a
  silent reconnect loop.
- **`TCPConfig.TLSConfig` on a non-TLS scheme warns.** A configured
  `TLSConfig` combined with `tcp://`/`mqtt://` is an invisible
  plaintext downgrade; the client now logs
  `mqtt.tcp.tls_config_ignored` (the scheme still selects the
  transport, unchanged).

## [1.1.0] - 2026-07-04

### Added

- **Circuit breaker (`Breaker`)** — a drop-in [Publisher] decorator for
  the degraded-broker case the reconnect loop cannot see: the TCP link
  is up but the broker stops acknowledging, so every QoS >= 1 publish
  blocks for the full `AckTimeout`. After `FailureThreshold`
  consecutive broker-side failures (ack timeouts, connection loss,
  broker rejects) the circuit opens and publishes fail fast with
  `ErrCircuitOpen`; after `RecoveryTimeout` a bounded number of probes
  (`HalfOpenMax`) tests recovery, and one success closes the circuit.
  Local conditions (caller cancellation, packet-size/ID limits) never
  trip it. `OnStateChange` exposes transitions for consumer metrics.

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
- **Encoding a string containing embedded `U+0000` or ill-formed UTF-8
  (e.g. a PUBLISH topic) produced wire bytes this codec's own decoder
  — and any conformant broker — would then reject as a Malformed
  Packet**, an encode/decode asymmetry `FuzzPublishRoundTrip` caught at
  the v1.0 final gate. `appendString` now enforces the same §1.5.4
  well-formedness rules on encode that `readString` already enforced on
  decode, wrapping `ErrProtocolViolation`.

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
