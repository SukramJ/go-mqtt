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
