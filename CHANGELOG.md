# Version 0.2.0 (2026-07-03)

## What's Changed

Two changes carried back from the `openccu-loom` transport this module was
extracted from, where they still lived after the 0.1.0 carve-out.

### Changed (BREAKING)

- **`MessageHandler` now takes a third `retained bool` argument**
  (`func(topic string, payload []byte, retained bool)`). It carries the MQTT
  PUBLISH retain bit so a handler with side effects can drop the retained
  message the broker re-delivers on every (re)connect — without it, a stale
  `mosquitto_pub -r` command is re-applied to the real device each time the
  consumer restarts. Consumers that don't care can ignore the parameter
  (`func(_ string, _ []byte, _ bool)`). This is a source-breaking signature
  change for every `Subscribe` call site.

### Fixed

- **Subscription replay now preserves the QoS each filter was subscribed at.**
  The 0.1.0 reconnect path re-subscribed every filter at a hardcoded QoS 1;
  it now records and replays the caller's requested QoS, so a QoS 0
  subscription is no longer silently upgraded on reconnect.
- **The PINGRESP watchdog no longer trips on a single delayed pong.** It now
  tolerates one unanswered PINGREQ and only declares the socket dead after two
  consecutive misses (≈ one full KeepAlive). A lone late/dropped PINGRESP — a
  GC pause, a scheduler stall on a CPU-throttled host, a momentary network
  blip — previously forced a spurious `mqtt.tcp.ping_timeout` + reconnect;
  a genuinely half-open socket is still detected, one ping interval later.

# Version 0.1.0 (2026-07-02)

## What's Changed

Initial release. This module is the shared MQTT 3.1.1 transport layer, extracted
from the `go-*2mqtt` bridges (`go-mtec2mqtt`, `go-daikin2mqtt`,
`go-homeconnect2mqtt`, `go-zendure2mqtt`) where it previously lived four times
over as `internal/mqtt`. Consolidating it here means a fix lands once and every
bridge picks it up via `go get -u`, instead of drifting across four copies.

### Added

- **`protocol` package** — MQTT 3.1.1 wire codec (CONNECT / CONNACK / PUBLISH /
  PUBACK / SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK / PINGREQ / PINGRESP /
  DISCONNECT), QoS 0 and 1. Includes a 1 MiB frame-size cap that rejects an
  oversized `remaining length` **before** allocating a body buffer, closing an
  OOM/DoS vector against a malicious or malfunctioning broker.
- **`mqtt` package** — `TCPClient` for `tcp://` and `tls://` brokers with
  subscription replay on reconnect, a PINGRESP watchdog that detects half-open
  sockets, a `ConnectionLost()` channel for event-driven reconnect, and SUBACK
  return-code handling that surfaces broker-rejected subscriptions.
- **`Lifecycle`** — a reconnect loop with exponential backoff and jitter that
  waits for the loop to exit before disconnecting (no teardown race).
- **`NewClientTLSConfig`** — always sets `ServerName` and keeps certificate
  verification on unless an explicit opt-in disables it.
- No third-party dependencies — standard library only. MIT licensed
  (openccu-loom provenance).
