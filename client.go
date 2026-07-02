// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import "context"

// QoS mirrors the MQTT QoS enum.
type QoS byte

// QoS values.
const (
	QoS0 QoS = 0
	QoS1 QoS = 1
	QoS2 QoS = 2
)

// Publisher is the outbound contract the bridge publishes through.
// Adapters wrap any MQTT client (paho, nhave, etc.).
type Publisher interface {
	Publish(ctx context.Context, topic string, payload []byte, qos QoS, retain bool) error
}

// MessageHandler is invoked for every message a subscription receives.
//
// Contract: the [TCPClient] adapter calls the handler synchronously,
// inline in its read loop (see TCPClient.dispatch), which is the same
// goroutine that decodes PUBACK and PINGRESP frames and feeds the
// keep-alive watchdog. The handler MUST return quickly and must not
// block on I/O, locks, or anything else of unbounded duration. If the
// work takes real time, hand it off to a queue or a goroutine instead
// of doing it inline — otherwise PUBACK/PINGRESP processing stalls
// behind it and the keep-alive watchdog can declare the connection
// lost (spurious `ping_timeout`) even though the broker and network
// are fine.
type MessageHandler func(topic string, payload []byte)

// Subscriber is the inbound contract — subscribe to a topic filter
// and route matching messages to a handler. Wiring typically happens
// once at startup and stays active for the broker connection's
// lifetime.
type Subscriber interface {
	Subscribe(ctx context.Context, topicFilter string, qos QoS, handler MessageHandler) error
	Unsubscribe(ctx context.Context, topicFilter string) error
}

// Client is the combined role the Bridge uses. Most real adapters
// satisfy both; the split exists to make testing narrow facades
// easier.
type Client interface {
	Publisher
	Subscriber
}
