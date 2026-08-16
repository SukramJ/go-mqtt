// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// SubscribeOptions is the per-subscription option set encoded in the
// options byte that follows each topic filter in a SUBSCRIBE payload. In
// MQTT 3.1.1 only QoS is carried (the remaining bits are reserved and must
// be zero); MQTT 5.0 adds No Local, Retain As Published and Retain
// Handling (MQTT 5.0 §3.8.3.1).
type SubscribeOptions struct {
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

// subscribeOptionsByte encodes o into the wire options byte for version v.
// QoS above 2, or a Retain Handling above 2 (its value 3 is reserved),
// yields [ErrProtocolViolation]. For [V311] only the QoS bits are set; the
// No Local, Retain As Published and Retain Handling fields are ignored so
// the reserved bits stay zero.
func subscribeOptionsByte(v Version, o SubscribeOptions) (byte, error) {
	if o.QoS > 2 {
		return 0, fmt.Errorf("%w: subscription QoS %d", ErrProtocolViolation, o.QoS)
	}
	b := o.QoS
	if v == V50 {
		if o.NoLocal {
			b |= 0x04
		}
		if o.RetainAsPublished {
			b |= 0x08
		}
		if o.RetainHandling > 2 {
			return 0, fmt.Errorf("%w: retain handling %d", ErrProtocolViolation, o.RetainHandling)
		}
		b |= o.RetainHandling << 4
	}
	return b, nil
}

// Subscription pairs a topic filter with its options.
type Subscription struct {
	Filter  string
	Options SubscribeOptions
}

// SubscribePacket is a client SUBSCRIBE request. This package never decodes
// SUBSCRIBE (a client only sends it). The fixed header carries reserved
// flags 0x02. For [V50] a property block (subscription identifier, user
// properties) follows the packet identifier.
type SubscribePacket struct {
	Version       Version
	PacketID      uint16
	Subscriptions []Subscription
	Properties    *Properties
}

// Encode writes the SUBSCRIBE packet to w. It must carry a supported
// [Version], a non-zero packet identifier ([MQTT-2.2.1-3]) and at least one
// subscription (MQTT 5.0 §3.8.3); otherwise [ErrProtocolViolation]. Illegal
// options or properties — including more than one Subscription Identifier
// (§3.8.2.1.2) — surface [ErrProtocolViolation] from their encoders.
func (p *SubscribePacket) Encode(w io.Writer) error {
	if !p.Version.Valid() {
		return fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(p.Version))
	}
	if p.PacketID == 0 {
		return fmt.Errorf("%w: SUBSCRIBE with zero packet identifier", ErrProtocolViolation)
	}
	if len(p.Subscriptions) == 0 {
		return fmt.Errorf("%w: SUBSCRIBE with no subscriptions", ErrProtocolViolation)
	}

	var body bytes.Buffer
	var id [2]byte
	binary.BigEndian.PutUint16(id[:], p.PacketID)
	body.Write(id[:])

	if p.Version == V50 {
		if err := p.Properties.encode(&body, tgSubscribe); err != nil {
			return err
		}
	}

	for _, sub := range p.Subscriptions {
		if err := appendString(&body, sub.Filter); err != nil {
			return err
		}
		opt, err := subscribeOptionsByte(p.Version, sub.Options)
		if err != nil {
			return err
		}
		body.WriteByte(opt)
	}

	return writePacket(w, byte(Subscribe)<<4|0x02, body.Bytes())
}

// SubackPacket is a decoded server SUBACK. Each reason code is the grant
// (Granted QoS 0/1/2) or a failure code for the subscription at the same
// index in the originating SUBSCRIBE. In MQTT 3.1.1 the return codes
// (0x00/0x01/0x02 granted, 0x80 failure) map one-to-one onto the reason
// code byte; Properties is nil for 3.1.1.
type SubackPacket struct {
	PacketID    uint16
	ReasonCodes []ReasonCode
	Properties  *Properties
}

// subackReasonV311 is the closed set of MQTT 3.1.1 SUBACK return codes
// (§3.9.3): granted QoS 0/1/2 and the single failure code 0x80. Anything
// else on the wire is a Malformed Packet, not a mysterious "granted QoS 3".
var subackReasonV311 = map[byte]bool{
	0x00: true, // Success - Maximum QoS 0
	0x01: true, // Success - Maximum QoS 1
	0x02: true, // Success - Maximum QoS 2
	0x80: true, // Failure
}

// subackReasonV50 is the closed set of MQTT 5.0 SUBACK reason codes
// (§3.9.3): the three grants plus the failure codes a server may return
// for a subscription.
var subackReasonV50 = map[byte]bool{
	0x00: true, // Granted QoS 0
	0x01: true, // Granted QoS 1
	0x02: true, // Granted QoS 2
	0x80: true, // Unspecified error
	0x83: true, // Implementation specific error
	0x87: true, // Not authorized
	0x8F: true, // Topic Filter invalid
	0x91: true, // Packet Identifier in use
	0x97: true, // Quota exceeded
	0x9E: true, // Shared Subscriptions not supported
	0xA1: true, // Subscription Identifiers not supported
	0xA2: true, // Wildcard Subscriptions not supported
}

// unsubackReasonV50 is the closed set of MQTT 5.0 UNSUBACK reason codes
// (§3.11.3). MQTT 3.1.1 UNSUBACK carries no reason codes at all.
var unsubackReasonV50 = map[byte]bool{
	0x00: true, // Success
	0x11: true, // No subscription existed
	0x80: true, // Unspecified error
	0x83: true, // Implementation specific error
	0x87: true, // Not authorized
	0x8F: true, // Topic Filter invalid
	0x91: true, // Packet Identifier in use
}

// DecodeSuback decodes a SUBACK body for protocol version v: a packet
// identifier, a [V50]-only property block, then one reason code byte per
// subscription. At least one reason code is required (MQTT 5.0 §3.9.3), the
// packet identifier must be non-zero ([MQTT-2.2.1-3]) and every reason code
// must be one the version defines (see [subackReasonV311] /
// [subackReasonV50]). Any truncation, illegal property, out-of-range reason
// code or empty payload yields an error wrapping [ErrMalformedPacket];
// decoding never panics.
func DecodeSuback(v Version, body []byte) (*SubackPacket, error) {
	p := &SubackPacket{}
	c := newCursor(body)

	pid, err := c.readUint16()
	if err != nil {
		return nil, err
	}
	if pid == 0 {
		return nil, wrapMalformed("SUBACK with zero packet identifier")
	}
	p.PacketID = pid

	switch v {
	case V311:
		// MQTT 3.1.1 SUBACK carries no property block; the return codes
		// follow the packet identifier directly.
	case V50:
		props, err := decodeProperties(c, tgSuback)
		if err != nil {
			return nil, err
		}
		p.Properties = props
	default:
		return nil, fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(v))
	}

	if c.remaining() == 0 {
		return nil, wrapMalformed("SUBACK with no reason codes")
	}
	allowed := subackReasonV50
	if v == V311 {
		allowed = subackReasonV311
	}
	for c.remaining() > 0 {
		rc, err := c.readByte()
		if err != nil {
			return nil, err
		}
		if !allowed[rc] {
			return nil, fmt.Errorf("%w: SUBACK reason code 0x%02X undefined for %s", ErrMalformedPacket, rc, v)
		}
		p.ReasonCodes = append(p.ReasonCodes, ReasonCode(rc))
	}
	return p, nil
}

// UnsubscribePacket is a client UNSUBSCRIBE request carrying one or more
// topic filters. This package never decodes UNSUBSCRIBE. The fixed header
// carries reserved flags 0x02. For [V50] a property block (user properties
// only) follows the packet identifier.
type UnsubscribePacket struct {
	Version    Version
	PacketID   uint16
	Filters    []string
	Properties *Properties
}

// Encode writes the UNSUBSCRIBE packet to w. It must carry a supported
// [Version], a non-zero packet identifier ([MQTT-2.2.1-3]) and at least one
// filter (MQTT 5.0 §3.10.3); otherwise [ErrProtocolViolation]. Illegal
// properties surface [ErrProtocolViolation] from the property encoder.
func (p *UnsubscribePacket) Encode(w io.Writer) error {
	if !p.Version.Valid() {
		return fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(p.Version))
	}
	if p.PacketID == 0 {
		return fmt.Errorf("%w: UNSUBSCRIBE with zero packet identifier", ErrProtocolViolation)
	}
	if len(p.Filters) == 0 {
		return fmt.Errorf("%w: UNSUBSCRIBE with no filters", ErrProtocolViolation)
	}

	var body bytes.Buffer
	var id [2]byte
	binary.BigEndian.PutUint16(id[:], p.PacketID)
	body.Write(id[:])

	if p.Version == V50 {
		if err := p.Properties.encode(&body, tgUnsubscribe); err != nil {
			return err
		}
	}

	for _, filter := range p.Filters {
		if err := appendString(&body, filter); err != nil {
			return err
		}
	}

	return writePacket(w, byte(Unsubscribe)<<4|0x02, body.Bytes())
}

// UnsubackPacket is a decoded server UNSUBACK. In MQTT 5.0 it carries one
// reason code per unsubscribed filter; MQTT 3.1.1 UNSUBACK has no payload
// (an empty body, ReasonCodes nil).
type UnsubackPacket struct {
	PacketID    uint16
	ReasonCodes []ReasonCode
	Properties  *Properties
}

// DecodeUnsuback decodes an UNSUBACK body for protocol version v. A [V311]
// UNSUBACK is exactly the two-byte packet identifier (no reason codes). A
// [V50] UNSUBACK adds a property block and one reason code byte per filter
// (at least one), each of which must be defined for UNSUBACK (see
// [unsubackReasonV50]). Any truncation, trailing byte, illegal property,
// zero packet identifier ([MQTT-2.2.1-3]), out-of-range reason code or
// empty [V50] payload yields an error wrapping [ErrMalformedPacket];
// decoding never panics.
func DecodeUnsuback(v Version, body []byte) (*UnsubackPacket, error) {
	p := &UnsubackPacket{}
	c := newCursor(body)

	pid, err := c.readUint16()
	if err != nil {
		return nil, err
	}
	if pid == 0 {
		return nil, wrapMalformed("UNSUBACK with zero packet identifier")
	}
	p.PacketID = pid

	switch v {
	case V311:
		if c.remaining() != 0 {
			return nil, wrapMalformed("trailing bytes after v3 UNSUBACK")
		}
		return p, nil
	case V50:
		props, err := decodeProperties(c, tgUnsuback)
		if err != nil {
			return nil, err
		}
		p.Properties = props
		if c.remaining() == 0 {
			return nil, wrapMalformed("UNSUBACK with no reason codes")
		}
		for c.remaining() > 0 {
			rc, err := c.readByte()
			if err != nil {
				return nil, err
			}
			if !unsubackReasonV50[rc] {
				return nil, fmt.Errorf("%w: UNSUBACK reason code 0x%02X undefined", ErrMalformedPacket, rc)
			}
			p.ReasonCodes = append(p.ReasonCodes, ReasonCode(rc))
		}
		return p, nil
	default:
		return nil, fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(v))
	}
}
