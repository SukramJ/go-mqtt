// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import "time"

// PublishOption attaches an MQTT 5.0 PUBLISH property to a single
// [Publisher.Publish] call. Options have no effect on an MQTT 3.1.1 link
// (that dialect has no property block). Apply them variadically:
//
//	c.Publish(ctx, "t", p, mqtt.QoS1, false,
//		mqtt.WithContentType("application/json"),
//		mqtt.WithMessageExpiry(30))
type PublishOption func(*publishOptions)

// publishOptions accumulates the MQTT 5.0 PUBLISH properties selected by
// the [PublishOption] values passed to a publish call. The adapter reads
// it to build the outbound PUBLISH property block.
type publishOptions struct {
	messageExpiry     *uint32
	contentType       string
	responseTopic     string
	correlationData   []byte
	payloadFormatUTF8 bool
	userProperties    []UserProperty
}

// WithMessageExpiry sets the MQTT 5.0 Message Expiry Interval property
// (0x02): the broker discards the message for any subscriber it has not
// been delivered to within seconds.
func WithMessageExpiry(seconds uint32) PublishOption {
	return func(o *publishOptions) { o.messageExpiry = &seconds }
}

// WithContentType sets the MQTT 5.0 Content Type property (0x03), a
// UTF-8 description of the payload's content (e.g. a MIME type).
func WithContentType(ct string) PublishOption {
	return func(o *publishOptions) { o.contentType = ct }
}

// WithResponseTopic sets the MQTT 5.0 Response Topic property (0x08) used
// in the request/response pattern to tell the receiver where to reply.
func WithResponseTopic(topic string) PublishOption {
	return func(o *publishOptions) { o.responseTopic = topic }
}

// WithCorrelationData sets the MQTT 5.0 Correlation Data property (0x09)
// a responder echoes back so a requester can match a reply to its
// request.
func WithCorrelationData(data []byte) PublishOption {
	return func(o *publishOptions) { o.correlationData = data }
}

// WithPayloadFormatUTF8 sets the MQTT 5.0 Payload Format Indicator
// property (0x01) to "UTF-8 encoded character data", declaring the
// payload a valid UTF-8 string.
func WithPayloadFormatUTF8() PublishOption {
	return func(o *publishOptions) { o.payloadFormatUTF8 = true }
}

// WithUserProperties appends MQTT 5.0 User Property pairs (0x26) to the
// PUBLISH. Repeated calls accumulate; order is preserved on the wire.
func WithUserProperties(props ...UserProperty) PublishOption {
	return func(o *publishOptions) { o.userProperties = append(o.userProperties, props...) }
}

// RetainHandling selects how a broker delivers retained messages for a
// filter at subscribe time (MQTT 5.0 §3.8.3.1).
type RetainHandling byte

// Retain-handling options for [WithRetainHandling].
const (
	// SendRetained delivers retained messages when the subscription is
	// established (the MQTT 3.1.1 behaviour, and the default).
	SendRetained RetainHandling = 0
	// SendRetainedIfNew delivers retained messages only if the
	// subscription did not already exist.
	SendRetainedIfNew RetainHandling = 1
	// DontSendRetained never delivers retained messages at subscribe
	// time.
	DontSendRetained RetainHandling = 2
)

// SubscribeOption sets an MQTT 5.0 subscription option on a single
// [Subscriber.Subscribe] call. Options have no effect on an MQTT 3.1.1
// link (that dialect carries only QoS in the options byte).
type SubscribeOption func(*subscribeOptions)

// subscribeOptions accumulates the MQTT 5.0 subscription options selected
// by the [SubscribeOption] values passed to a subscribe call. The adapter
// reads it to build the per-filter options byte.
type subscribeOptions struct {
	noLocal           bool
	retainAsPublished bool
	retainHandling    RetainHandling
}

// WithNoLocal sets the MQTT 5.0 No Local option: the broker does not
// forward a message back to the connection that published it.
func WithNoLocal() SubscribeOption {
	return func(o *subscribeOptions) { o.noLocal = true }
}

// WithRetainAsPublished sets the MQTT 5.0 Retain As Published option: the
// broker keeps the original PUBLISH retain flag instead of clearing it
// when forwarding to this subscription.
func WithRetainAsPublished() SubscribeOption {
	return func(o *subscribeOptions) { o.retainAsPublished = true }
}

// WithRetainHandling sets the MQTT 5.0 Retain Handling option controlling
// whether retained messages are delivered at subscribe time.
func WithRetainHandling(h RetainHandling) SubscribeOption {
	return func(o *subscribeOptions) { o.retainHandling = h }
}

// SubscribeResult is the outcome of a [Subscriber.Subscribe] call, decoded
// from the SUBACK. A non-error result may still carry a GrantedQoS below
// the requested one (the broker is permitted to downgrade).
type SubscribeResult struct {
	// GrantedQoS is the maximum QoS the broker granted for the filter.
	GrantedQoS QoS
	// ReasonCode is the SUBACK reason code for the filter (a success
	// grant; failure codes are returned as a *[ReasonError] instead).
	ReasonCode ReasonCode
}

// ConnectResult is the negotiated session state decoded from the CONNACK.
// The MQTT 5.0 fields reflect the broker's advertised limits; on an MQTT
// 3.1.1 link they carry their protocol defaults.
type ConnectResult struct {
	// SessionPresent reports whether the broker resumed an existing
	// session (§3.2.2.1.1).
	SessionPresent bool
	// ReasonCode is the CONNACK reason code (Success on a normal accept).
	ReasonCode ReasonCode
	// AssignedClientID is the client identifier the broker assigned when
	// the client connected with an empty one (MQTT 5.0 property 0x12);
	// empty otherwise.
	AssignedClientID string
	// ServerKeepAlive is the keep-alive interval the broker imposed
	// (MQTT 5.0 property 0x13); zero when the broker did not override the
	// requested value.
	ServerKeepAlive time.Duration
	// ReceiveMaximum is the number of unacknowledged QoS 1/2 publishes the
	// broker will accept concurrently (MQTT 5.0 property 0x21); defaults
	// to 65535.
	ReceiveMaximum uint16
	// MaximumQoS is the highest QoS the broker supports (MQTT 5.0 property
	// 0x24); defaults to QoS 2.
	MaximumQoS QoS
	// RetainAvailable reports whether the broker supports retained
	// messages (MQTT 5.0 property 0x25); defaults to true.
	RetainAvailable bool
	// MaximumPacketSize is the largest packet the broker will accept
	// (MQTT 5.0 property 0x27); zero when the broker set no limit.
	MaximumPacketSize uint32
	// TopicAliasMaximum is the highest topic alias the broker accepts on
	// outbound publishes (MQTT 5.0 property 0x22); zero disables aliases.
	TopicAliasMaximum uint16
	// UserProperties holds any MQTT 5.0 User Property pairs (0x26) the
	// broker returned in the CONNACK.
	UserProperties []UserProperty
}

// Will is the client's Last Will and Testament: the message the broker
// publishes on the client's behalf if the connection closes without a
// clean DISCONNECT. The MQTT 5.0 fields (DelayIntervalSeconds and the
// property-backed values) are ignored on an MQTT 3.1.1 link.
type Will struct {
	// Topic is the will message topic.
	Topic string
	// ContentType is the will Content Type property (0x03).
	ContentType string
	// ResponseTopic is the will Response Topic property (0x08).
	ResponseTopic string
	// Payload is the will message payload.
	Payload []byte
	// CorrelationData is the will Correlation Data property (0x09).
	CorrelationData []byte
	// QoS is the will message QoS.
	QoS QoS
	// Retain sets the retain flag on the will message.
	Retain bool
	// PayloadFormatUTF8 sets the will Payload Format Indicator property
	// (0x01).
	PayloadFormatUTF8 bool
	// DelayIntervalSeconds is the MQTT 5.0 Will Delay Interval property
	// (0x18): the broker waits this long after the connection closes
	// before publishing the will.
	DelayIntervalSeconds uint32
	// MessageExpirySeconds is the will Message Expiry Interval property
	// (0x02).
	MessageExpirySeconds uint32
	// UserProperties holds MQTT 5.0 User Property pairs (0x26) attached to
	// the will message.
	UserProperties []UserProperty
}

// SessionExpiryNever is the MQTT 5.0 Session Expiry Interval value that
// requests a session which never expires (0xFFFFFFFF, §3.1.2.11.2).
const SessionExpiryNever uint32 = 0xFFFFFFFF
