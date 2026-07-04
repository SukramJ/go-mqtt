// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

// Package mqtt is a minimal, dependency-free MQTT 3.1.1 and 5.0 client
// library: a reconnecting TCP/TLS adapter over the wire codec in
// [github.com/SukramJ/go-mqtt/protocol]. The root package defines the
// version-agnostic client contracts ([Publisher], [Subscriber], [Client]),
// the inbound [Message] shape and [MessageHandler] dispatch rules, the
// publish/subscribe options, and the sentinel errors consumers match on.
package mqtt

import (
	"context"

	"github.com/SukramJ/go-mqtt/protocol"
)

// QoS mirrors the MQTT Quality of Service enum (§4.3): at-most-once (0),
// at-least-once (1) and exactly-once (2).
type QoS byte

// QoS values.
const (
	QoS0 QoS = 0
	QoS1 QoS = 1
	QoS2 QoS = 2
)

// ProtocolVersion is the MQTT protocol level a client speaks. It aliases
// [protocol.Version] so callers configure the wire dialect without
// importing the codec package directly.
type ProtocolVersion = protocol.Version

// Supported protocol versions.
const (
	// ProtocolV311 selects MQTT 3.1.1 (protocol level 4).
	ProtocolV311 ProtocolVersion = protocol.V311
	// ProtocolV50 selects MQTT 5.0 (protocol level 5) and is the default.
	ProtocolV50 ProtocolVersion = protocol.V50
)

// ReasonCode is a single-byte MQTT 5.0 reason code (§2.4), surfaced on
// results and on [ReasonError]. It aliases [protocol.ReasonCode]. Values
// >= 0x80 denote failure (see [protocol.ReasonCode.IsError]).
type ReasonCode = protocol.ReasonCode

// UserProperty is a single MQTT 5.0 User Property key/value pair. It
// aliases [protocol.UserProperty]. User Properties are repeatable and
// preserve order on both publish and receive.
type UserProperty = protocol.UserProperty

// Message is an inbound application message delivered to a
// [MessageHandler]. Beyond the MQTT 3.1.1 fields (Topic, Payload, QoS,
// Retain, Dup) it carries the MQTT 5.0 PUBLISH properties the broker set;
// on an MQTT 3.1.1 link every v5-only field is zero.
type Message struct {
	// Topic is the topic name the message was published to. For an
	// inbound PUBLISH that used a topic alias, the alias has already been
	// resolved to the original topic name.
	Topic string
	// Payload is the application message bytes.
	Payload []byte
	// QoS is the delivery quality of service of this message.
	QoS QoS
	// Retain is the PUBLISH retain bit (§3.3.1.3): true when the broker
	// re-delivered this as the retained message for the filter on
	// (re)subscribe rather than as a live publish. See [MessageHandler].
	Retain bool
	// Dup is the PUBLISH duplicate-delivery flag (§3.3.1.1).
	Dup bool

	// ContentType is the MQTT 5.0 Content Type property (0x03), empty when
	// absent.
	ContentType string
	// ResponseTopic is the MQTT 5.0 Response Topic property (0x08) for
	// request/response, empty when absent.
	ResponseTopic string
	// CorrelationData is the MQTT 5.0 Correlation Data property (0x09),
	// nil when absent.
	CorrelationData []byte
	// MessageExpirySeconds is the MQTT 5.0 Message Expiry Interval
	// property (0x02), zero when absent.
	MessageExpirySeconds uint32
	// PayloadFormatUTF8 reports the MQTT 5.0 Payload Format Indicator
	// property (0x01): true when the publisher declared the payload a
	// UTF-8 string.
	PayloadFormatUTF8 bool
	// SubscriptionIdentifiers holds the MQTT 5.0 Subscription Identifier
	// property values (0x0B) the broker attached, one per matching
	// subscription that carried an identifier. Nil when absent.
	SubscriptionIdentifiers []uint32
	// UserProperties holds the MQTT 5.0 User Property pairs (0x26) in the
	// order the publisher sent them. Nil when absent.
	UserProperties []UserProperty
}

// MessageHandler is invoked for every message a subscription receives.
//
// The [Message.Retain] flag carries the MQTT PUBLISH "retain" bit
// (§3.3.1.3) so a handler that performs side effects on inbound commands
// can drop retained replays from the broker. On every (re)connect the
// broker re-delivers the last retained message on each subscribed filter;
// without this flag a handler cannot tell that replay apart from a fresh
// command, so an old `mosquitto_pub -r` left over from a previous test or
// automation is re-applied to the real device every time the consumer
// restarts. Consumers that don't care (pure state sinks) can simply
// ignore the flag.
//
// Contract: the [TCPClient] adapter calls the handler synchronously,
// inline in its read loop, which is the same goroutine that decodes
// PUBACK/PUBREC/PUBCOMP and PINGRESP frames and feeds the keep-alive
// watchdog. The handler MUST return quickly and must not block on I/O,
// locks, or anything else of unbounded duration. If the work takes real
// time, hand it off to a queue or a goroutine instead of doing it inline
// — otherwise acknowledgement and PINGRESP processing stalls behind it
// and the keep-alive watchdog can declare the connection lost (spurious
// `ping_timeout`) even though the broker and network are fine. The
// *Message and its slices are owned by the caller for the duration of the
// call only; a handler that retains them past return must copy.
type MessageHandler func(msg *Message)

// Publisher is the outbound contract the bridge publishes through.
// Adapters wrap any MQTT client.
type Publisher interface {
	// Publish sends payload to topic at the given QoS. QoS 0 is
	// fire-and-forget; QoS 1 blocks until PUBACK; QoS 2 blocks until
	// PUBCOMP. The variadic options attach MQTT 5.0 PUBLISH properties
	// (ignored on an MQTT 3.1.1 link).
	Publish(ctx context.Context, topic string, payload []byte, qos QoS, retain bool, opts ...PublishOption) error
}

// Subscriber is the inbound contract — subscribe to a topic filter and
// route matching messages to a handler. Wiring typically happens once at
// startup and stays active for the broker connection's lifetime.
type Subscriber interface {
	// Subscribe registers handler for filter at the requested QoS and
	// blocks until the SUBACK arrives (bounded by ctx and the configured
	// ack timeout). A granted QoS below the requested one is not an error;
	// a broker rejection (reason code >= 0x80) yields a *[ReasonError].
	// The variadic options set MQTT 5.0 subscription options.
	Subscribe(ctx context.Context, filter string, qos QoS, handler MessageHandler, opts ...SubscribeOption) (SubscribeResult, error)
	// Unsubscribe removes the subscription for filter.
	Unsubscribe(ctx context.Context, filter string) error
}

// Client is the combined role a bridge uses. Most real adapters satisfy
// both; the split exists to make testing narrow facades easier.
type Client interface {
	Publisher
	Subscriber
}

// LegacyHandler adapts a v0.x-style handler — the
// func(topic string, payload []byte, retained bool) shape used before the
// v1.0 [Message] type — into a [MessageHandler]. It lets consumers migrate
// call sites incrementally without rewriting every handler at once.
func LegacyHandler(fn func(topic string, payload []byte, retained bool)) MessageHandler {
	return func(msg *Message) {
		fn(msg.Topic, msg.Payload, msg.Retain)
	}
}
