# Migrating from v0.x to v1.0

v1.0 is a breaking rewrite: MQTT 5.0 becomes the default wire
protocol (3.1.1 stays fully supported, opt in explicitly), QoS 2 is
implemented in both directions, and the client API gained functional
options instead of growing more positional parameters. This guide
covers all four consumers of this module: `go-mtec2mqtt`,
`go-zendure2mqtt`, `go-homeconnect2mqtt` and `openccu-loom`.

The module path is unchanged (`github.com/SukramJ/go-mqtt`, no `/v2`
suffix) — `go get -u github.com/SukramJ/go-mqtt@v1` and fix the
compile errors below.

## Signature changes

| v0.x | v1.0 | Notes |
|---|---|---|
| `Publish(ctx, topic, payload, qos, retain) error` | `Publish(ctx, topic, payload, qos, retain, opts ...PublishOption) error` | Source-compatible (trailing variadic); QoS 2 now accepted. Behavior change: fails fast, see below. |
| `Subscribe(ctx, filter, qos, handler) error` | `Subscribe(ctx, filter, qos, handler, opts ...SubscribeOption) (SubscribeResult, error)` | **Now returns two values and blocks until SUBACK.** Every call site needs a `_, err :=` update at minimum. |
| `MessageHandler func(topic string, payload []byte, retained bool)` | `MessageHandler func(msg *Message)` | Use `mqtt.LegacyHandler(fn)` to keep the old handler shape unchanged. |
| `TCPConfig.CleanSession bool` | `TCPConfig.CleanStart bool` | Same wire bit (CONNECT Clean Start/Clean Session), renamed for v5 terminology. |
| `TCPConfig.WillTopic string`, `WillPayload []byte`, `WillRetain bool` | `TCPConfig.Will *Will` | `Will` also adds `QoS` (previously not configurable — always 0) and, on a v5 link, will properties. |
| *(implicit MQTT 3.1.1)* | `TCPConfig.ProtocolVersion ProtocolVersion` | Zero value now means MQTT 5.0. Set `ProtocolVersion: mqtt.ProtocolV311` to keep 3.1.1. |
| *(none)* | `TCPClient.ConnectResult() (ConnectResult, bool)` | New: session/limits snapshot from the CONNACK (session present, assigned client id, negotiated Receive Maximum/Maximum QoS/...). |
| PUBACK timeout error (`fmt.Errorf("...PUBACK timeout...")`) | Sentinel errors: `ErrNotConnected`, `ErrConnectionLost`, `ErrPacketIDExhausted`, `ErrPacketTooLarge`, `*ReasonError` | Match with `errors.Is`/`errors.As` instead of string matching. |

`Connector`, `ConnectionLost()`, `IsConnected()`, `LastConnectedAt()`,
`NewClientTLSConfig`, `Lifecycle`/`NewLifecycle`/`DefaultLifecycle`
are unchanged in shape (`Lifecycle` gained an internal event-driven
fast path — no API change, see Behavior changes below).

## Before / after: typical bridge wiring

### Subscribe: two-value return, and it now blocks until SUBACK

```go
// v0.x
if err := client.Subscribe(ctx, "cmd/#", mqtt.QoS1, handleCmd); err != nil {
	return err
}
```

```go
// v1.0 — Subscribe blocks until the broker's SUBACK (bounded by ctx
// and TCPConfig.AckTimeout); a broker rejection is now a *ReasonError
// instead of being silently logged.
if _, err := client.Subscribe(ctx, "cmd/#", mqtt.QoS1, handleCmd); err != nil {
	return err
}
```

If you need the granted QoS or reason code, keep the first return
value: `res, err := client.Subscribe(...)`.

### MessageHandler: adopt `*Message`, or keep the old shape with `LegacyHandler`

```go
// v0.x
func handleCmd(topic string, payload []byte, retained bool) {
	if retained {
		return
	}
	apply(topic, payload)
}
client.Subscribe(ctx, "cmd/#", mqtt.QoS1, handleCmd)
```

```go
// v1.0 — mechanical, zero-behavior-change migration:
client.Subscribe(ctx, "cmd/#", mqtt.QoS1, mqtt.LegacyHandler(handleCmd))

// Or migrate to the richer *Message (adds v5 properties: ContentType,
// UserProperties, SubscriptionIdentifiers, ...):
client.Subscribe(ctx, "cmd/#", mqtt.QoS1, func(msg *mqtt.Message) {
	if msg.Retain {
		return
	}
	apply(msg.Topic, msg.Payload)
})
```

### CleanSession -> CleanStart

```go
// v0.x
cfg := mqtt.TCPConfig{ /* ... */ CleanSession: true }
```

```go
// v1.0 — same wire bit, renamed
cfg := mqtt.TCPConfig{ /* ... */ CleanStart: true }
```

### Will fields -> `Will` struct

```go
// v0.x
cfg := mqtt.TCPConfig{
	/* ... */
	WillTopic:   "bridge/status",
	WillPayload: []byte("offline"),
	WillRetain:  true,
}
```

```go
// v1.0
cfg := mqtt.TCPConfig{
	/* ... */
	Will: &mqtt.Will{
		Topic:   "bridge/status",
		Payload: []byte("offline"),
		QoS:     mqtt.QoS1, // new: was always QoS 0 in v0.x
		Retain:  true,
		// v5-only, ignored on a ProtocolV311 link:
		// DelayIntervalSeconds, MessageExpirySeconds, ContentType,
		// ResponseTopic, CorrelationData, PayloadFormatUTF8, UserProperties
	},
}
```

### v5 is the default now — pin `ProtocolV311` for a broker that isn't ready

```go
// v0.x always spoke MQTT 3.1.1; v1.0's zero value now means v5. If your
// broker doesn't support MQTT 5.0 yet, pin the old dialect explicitly:
cfg := mqtt.TCPConfig{
	/* ... */
	ProtocolVersion: mqtt.ProtocolV311,
	MaxInflight:     20, // optional: v3.1.1's Receive-Maximum equivalent
}
```

There is no silent downgrade: connecting v5-default against a broker
that rejects protocol level 5 surfaces a `*ReasonError` (or a CONNACK
decode error) from `Connect`/`Lifecycle.Start` naming the problem, so
the fix (set `ProtocolV311`) is discoverable from the error rather than
a silent fallback.

## Behavior changes (no signature change, but the client acts differently)

- **Fail-fast instead of riding out the ack timeout on a dead link.**
  v0.x's `Publish`/`Subscribe` left a pending acknowledgement wait
  running the full `AckTimeout` even after the connection had already
  dropped. v1.0 fails every in-flight `Publish`/`Subscribe` immediately
  with `ErrConnectionLost` the moment the drop is detected, and a call
  made while already disconnected returns `ErrNotConnected`
  immediately — neither blocks for `AckTimeout` on a link known to be
  dead. If your code was relying on the timeout as an implicit "give
  the reconnect a chance" delay, add an explicit retry/backoff around
  the call instead.
- **Event-driven reconnect.** `Lifecycle` now reacts to a `TCPClient`
  connection-loss event immediately (and resets its backoff), instead
  of only discovering the drop on its next timer tick. A reconnect
  after a network blip is typically much faster; no config change is
  needed to get this — it's automatic when the `Connector` is a
  `TCPClient`.
- **`Start`'s `ctx` governs the whole reconnect loop, not just the
  first connect.** This was true in v0.x too but is now documented
  loudly on `Lifecycle.Start`: pass a long-lived context (the
  application's run context), not a short-lived one — cancelling it
  permanently stops reconnection.
- **Broker limits enforced locally instead of provoking a
  DISCONNECT.** A `Publish` above the broker's negotiated Maximum QoS,
  or a retained `Publish` when the broker advertised Retain
  Available = 0, now fails locally with a wrapped
  `protocol.ErrProtocolViolation` instead of being sent and having the
  broker tear down the whole connection in response. Check
  `ConnectResult()` if you want to branch on these limits proactively.
- **Subscription replay on reconnect preserves per-filter options,**
  not just QoS (topic-alias state, No Local, Retain As Published,
  Retain Handling on a v5 link) — nothing to change in caller code,
  behavior only gets more correct.
- **QoS 2 now works end-to-end.** v0.x rejected QoS 2 in `Publish` and
  never implemented it inbound. If a bridge was working around this
  (e.g. downgrading a device's QoS 2 topic to QoS 1), it's safe to
  request QoS 2 directly now.

## What did not change

`Connector` (`Connect`/`Disconnect`), `TCPClient.ConnectionLost()`,
`IsConnected()`, `LastConnectedAt()`, `NewClientTLSConfig`,
`Lifecycle`/`NewLifecycle`/`DefaultLifecycle`/`OnConnect` all keep
their v0.x shape and behavior contract. `BrokerURL`, `ClientID`,
`Username`, `Password`, `KeepAlive`, `DialTimeout`, `AckTimeout`,
`TLSConfig`, `Logger` on `TCPConfig` are unchanged.
