# go-mqtt

[![ci](https://github.com/SukramJ/go-mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/SukramJ/go-mqtt/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/SukramJ/go-mqtt.svg)](https://pkg.go.dev/github.com/SukramJ/go-mqtt)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

Minimal pure-Go MQTT 3.1.1 client: wire codec, a TCP/TLS adapter, and a
reconnecting lifecycle wrapper. No third-party dependencies — standard
library only.

This module is the shared transport layer factored out of the
`go-*2mqtt` bridges (`go-mtec2mqtt`, `go-zendure2mqtt`,
`go-homeconnect2mqtt`, ...), where it previously lived four times over
as `internal/mqtt`.

## Packages

- `protocol` — CONNECT / CONNACK / PUBLISH / PUBACK / SUBSCRIBE /
  SUBACK / UNSUBSCRIBE / UNSUBACK / PINGREQ / PINGRESP / DISCONNECT
  encoding and decoding for MQTT 3.1.1. QoS 0 and QoS 1 only.
- `mqtt` (module root) — `TCPClient`, a `Connector`/`Client`
  implementation that dials a `tcp://` or `tls://` broker, replays
  subscriptions on reconnect, and watches PINGRESP to detect half-open
  sockets; and `Lifecycle`, a reconnect loop with exponential backoff
  and jitter around any `Connector`.

## License

MIT. The client was originally written for
[`openccu-loom`](https://github.com/SukramJ/openccu-loom); file
headers keep that provenance.

## Usage

```go
client := mqtt.NewTCPClient(mqtt.TCPConfig{
	BrokerURL: "tcp://broker.example.com:1883",
	ClientID:  "my-bridge",
	KeepAlive: 30 * time.Second,
})

lc := mqtt.NewLifecycle(mqtt.DefaultLifecycle(), client)
lc.OnConnect(func(ctx context.Context) {
	_ = client.Subscribe(ctx, "cmd/#", mqtt.QoS1, func(topic string, payload []byte) {
		// handle inbound message
	})
})

if err := lc.Start(context.Background()); err != nil {
	log.Fatal(err)
}
defer lc.Stop(context.Background())

_ = client.Publish(context.Background(), "state/online", []byte("1"), mqtt.QoS1, true)
```

For a `tls://` broker, set `TCPConfig.TLSConfig` — `NewClientTLSConfig`
builds a `*tls.Config` with the mandatory `ServerName` set correctly.
