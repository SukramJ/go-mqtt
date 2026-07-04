# go-mqtt

[![ci](https://github.com/SukramJ/go-mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/SukramJ/go-mqtt/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/SukramJ/go-mqtt.svg)](https://pkg.go.dev/github.com/SukramJ/go-mqtt)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

Minimal pure-Go MQTT client: a dual-version wire codec (**MQTT 5.0 by
default, MQTT 3.1.1 supported**), a TCP/TLS adapter with QoS 0/1/2,
session resumption and flow control, and a reconnecting lifecycle
wrapper. No third-party dependencies — standard library only, in the
module and in its tests.

This module is the shared transport layer factored out of the
`go-*2mqtt` bridges (`go-mtec2mqtt`, `go-zendure2mqtt`,
`go-homeconnect2mqtt`) and `openccu-loom`, where it previously lived
duplicated as `internal/mqtt`.

> Upgrading from v0.x? See [MIGRATION.md](./MIGRATION.md) — the API,
> defaults and a few behaviors changed in v1.0.

## Features

| Feature | Support |
|---|---|
| MQTT 5.0 | Default protocol (`ProtocolV50`) |
| MQTT 3.1.1 | Supported via `ProtocolVersion: ProtocolV311` |
| QoS | 0, 1 and 2, both directions, full PUBREC/PUBREL/PUBCOMP handshake |
| Session resumption | `CleanStart=false` + Session Expiry; unacked QoS>0 state and inbound QoS 2 dedup state replay in order on a resumed session |
| Flow control | Receive Maximum (v5) / configurable `MaxInflight` (v3.1.1) enforced as a send-quota semaphore; broker Maximum QoS / Retain Available honored locally |
| Inbound topic aliases | v5 topic alias table resolved per connection (outbound aliasing is out of scope) |
| Last Will and Testament | v3.1.1 topic/payload/QoS/retain; v5 adds will properties (delay interval, message expiry, content type, correlation data, user properties) |
| TLS | `NewClientTLSConfig` helper — always sets `ServerName`, never defaults to `InsecureSkipVerify` |
| Reconnect lifecycle | `Lifecycle`: exponential backoff + jitter, event-driven via `ConnectionLost()` (immediate reconnect + backoff reset on a detected drop, not just idle polling) |
| Dependencies | Zero third-party — standard library only, including tests |

## Packages

- `protocol` — CONNECT/CONNACK, PUBLISH + PUBACK/PUBREC/PUBREL/PUBCOMP,
  SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP,
  DISCONNECT/AUTH encoding and decoding for both MQTT 3.1.1 and MQTT
  5.0, plus the v5 property model, reason codes and topic
  matching/validation. See [`protocol/doc.go`](./protocol/doc.go) for
  the exact feature/omission list.
- `mqtt` (module root) — `TCPClient`, a `Connector`/`Client`
  implementation that dials a `tcp://` or `tls://` broker, drives the
  QoS 1/2 state machine and session replay, resolves inbound topic
  aliases, and watches PINGRESP to detect half-open sockets; and
  `Lifecycle`, a reconnect loop around any `Connector`.

## License

MIT. The client was originally written for
[`openccu-loom`](https://github.com/SukramJ/openccu-loom); file
headers keep that provenance.

## Usage

MQTT 5.0 is the default protocol — leave `ProtocolVersion` unset:

```go
client := mqtt.NewTCPClient(mqtt.TCPConfig{
	BrokerURL:  "tcp://broker.example.com:1883",
	ClientID:   "my-bridge",
	CleanStart: true,
	KeepAlive:  30 * time.Second,
	Will: &mqtt.Will{
		Topic:   "bridge/status",
		Payload: []byte("offline"),
		QoS:     mqtt.QoS1,
		Retain:  true,
	},
})

lc := mqtt.NewLifecycle(mqtt.DefaultLifecycle(), client)
lc.OnConnect(func(ctx context.Context) {
	_, _ = client.Subscribe(ctx, "cmd/#", mqtt.QoS1, func(msg *mqtt.Message) {
		// A handler with side effects should skip retained replays
		// (msg.Retain == true) the broker re-delivers on every (re)connect.
		if msg.Retain {
			return
		}
		// handle msg.Topic / msg.Payload — return quickly, see the
		// MessageHandler contract on the type for why.
	})
})

// Start blocks until the first connect succeeds (or ctx is done); ctx
// then governs the WHOLE reconnect loop, not just this call.
if err := lc.Start(context.Background()); err != nil {
	log.Fatal(err)
}
defer lc.Stop(context.Background())

_ = client.Publish(context.Background(), "bridge/status", []byte("online"), mqtt.QoS1, true)
```

For a `tls://` broker, set `TCPConfig.TLSConfig` —
`NewClientTLSConfig` builds a `*tls.Config` with the mandatory
`ServerName` set correctly.

### MQTT 3.1.1 and v5 properties

Pin `ProtocolV311` for a broker that doesn't speak MQTT 5.0 yet. On a
v5 link, `PublishOption`/`SubscribeOption` attach protocol properties
(they are silently no-ops on a v3.1.1 link):

```go
client311 := mqtt.NewTCPClient(mqtt.TCPConfig{
	BrokerURL:       "tcp://legacy-broker.example.com:1883",
	ClientID:        "my-bridge",
	ProtocolVersion: mqtt.ProtocolV311, // downgrade from the v5 default
	MaxInflight:     20,                // v3.1.1's Receive-Maximum equivalent
})

_ = client311.Publish(context.Background(), "bridge/status", []byte("online"),
	mqtt.QoS1, true,
	mqtt.WithUserProperties(mqtt.UserProperty{Key: "source", Value: "my-bridge"}),
)
```

## Testing

```sh
make test          # race-detector unit + integration tests (mock broker), no network
make test-cover     # tests + coverage report
make cover-check    # per-package coverage gate (COVER_MIN=90 by default)
make fuzz-smoke     # 10s per protocol Fuzz target — cheap CI regression net
make fuzz           # FUZZTIME per target (default 5m) — longer local/CI fuzz run
make check          # vet + fmt-check + lint + test — the pre-commit/pre-push gate
```

The `e2e/` package exercises real brokers over Docker (mosquitto and
EMQX, both protocol versions, TLS, auth, QoS 0/1/2, retained replay,
LWT, session resumption, reconnection). It is gated by environment
variables and skips itself when no broker is reachable:

```sh
make e2e-up      # start mosquitto (plain/TLS/password listeners) + EMQX via docker
make test-e2e    # run ./e2e against the containers started above
make e2e-down    # stop and remove them
```

See the [Development Commands](./CLAUDE.md#development-commands)
section of `CLAUDE.md` for the full target list.

## Migrating from v0.x

v1.0 defaults to MQTT 5.0, makes `Subscribe` block until SUBACK, and
changes several config field names and error-handling behaviors. See
[MIGRATION.md](./MIGRATION.md) for the full old→new signature table,
before/after diffs and behavior changes.
