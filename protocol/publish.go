// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// PublishPacket is an application message. DUP, QoS and RETAIN travel in
// the fixed-header flag nibble; the packet identifier is present only for
// QoS 1 and QoS 2. Version selects the wire dialect: [V50] carries a
// PUBLISH property block after the packet identifier (topic alias,
// subscription identifiers, content type, ...); [V311] carries none.
type PublishPacket struct {
	Version    Version
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Dup        bool
	PacketID   uint16
	Properties *Properties
}

// Encode writes the PUBLISH packet to w. An unsupported [Version], a QoS
// above 2, a QoS 1 or 2 message with a zero packet identifier
// ([MQTT-2.2.1-3]) and DUP set on a QoS 0 message ([MQTT-3.3.1-2]) all
// yield [ErrProtocolViolation]. Properties illegal for PUBLISH — including
// a Topic Alias of 0 (§3.3.2.3.4) — surface [ErrProtocolViolation] from the
// property encoder. For [V311] no property block is written.
func (p *PublishPacket) Encode(w io.Writer) error {
	if !p.Version.Valid() {
		return fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(p.Version))
	}
	if p.QoS > 2 {
		return fmt.Errorf("%w: PUBLISH QoS %d", ErrProtocolViolation, p.QoS)
	}
	if p.QoS > 0 && p.PacketID == 0 {
		return fmt.Errorf("%w: PUBLISH QoS %d with zero packet identifier", ErrProtocolViolation, p.QoS)
	}
	// [MQTT-3.3.1-2]: DUP is meaningless at QoS 0 (nothing is redelivered)
	// and MUST be 0 there — refuse to put header 0x38 on the wire, which
	// this package's own decoder (and any conformant peer) rejects.
	if p.QoS == 0 && p.Dup {
		return fmt.Errorf("%w: PUBLISH with DUP set at QoS 0", ErrProtocolViolation)
	}

	header := byte(Publish) << 4
	if p.Dup {
		header |= 0x08
	}
	header |= p.QoS << 1
	if p.Retain {
		header |= 0x01
	}

	var body bytes.Buffer
	if err := appendString(&body, p.Topic); err != nil {
		return err
	}
	if p.QoS > 0 {
		var id [2]byte
		binary.BigEndian.PutUint16(id[:], p.PacketID)
		body.Write(id[:])
	}
	if p.Version == V50 {
		if err := p.Properties.encode(&body, tgPublish); err != nil {
			return err
		}
	}
	body.Write(p.Payload)

	return writePacket(w, header, body.Bytes())
}

// DecodePublish decodes a PUBLISH packet for protocol version v. header is
// the fixed-header byte (DUP/QoS/RETAIN live in its low nibble); body is
// the remaining bytes. A QoS of 3, DUP set at QoS 0 ([MQTT-3.3.1-2]), a
// QoS 1/2 packet with a zero packet identifier ([MQTT-2.2.1-2]), a topic
// name containing a wildcard character ([MQTT-3.3.2-2]) and an empty topic
// name (legal only on [V50] when a Topic Alias property resolves it,
// §3.3.2.1) are all illegal and yield [ErrMalformedPacket]. For [V50] a
// property block follows the packet identifier; an unsupported version
// yields [ErrProtocolViolation] rather than silently decoding v5 property
// bytes as payload. The payload is whatever bytes remain after the
// variable header. Any truncation or illegal property yields an error
// wrapping [ErrMalformedPacket]; decoding never panics.
func DecodePublish(v Version, header byte, body []byte) (*PublishPacket, error) {
	flags := header & 0x0F
	qos := (flags >> 1) & 0x03
	if qos == 3 {
		return nil, wrapMalformed("PUBLISH with QoS 3")
	}
	// [MQTT-3.3.1-2]: DUP MUST be 0 for a QoS 0 PUBLISH. Frame.ValidateFlags
	// catches this on the read hot path; repeated here for callers that
	// decode a body without going through it.
	if qos == 0 && flags&0x08 != 0 {
		return nil, wrapMalformed("PUBLISH with DUP set at QoS 0")
	}

	p := &PublishPacket{
		Version: v,
		QoS:     qos,
		Dup:     flags&0x08 != 0,
		Retain:  flags&0x01 != 0,
	}

	c := newCursor(body)
	topic, err := c.readString()
	if err != nil {
		return nil, err
	}
	if strings.ContainsAny(topic, "+#") {
		return nil, wrapMalformed("PUBLISH topic contains a wildcard character")
	}
	p.Topic = topic

	if qos > 0 {
		id, err := c.readUint16()
		if err != nil {
			return nil, err
		}
		if id == 0 {
			return nil, wrapMalformed("PUBLISH QoS >0 with zero packet identifier")
		}
		p.PacketID = id
	}

	switch v {
	case V311:
		// MQTT 3.1.1 PUBLISH carries no property block; the payload follows
		// the packet identifier directly.
	case V50:
		props, err := decodeProperties(c, tgPublish)
		if err != nil {
			return nil, err
		}
		p.Properties = props
	default:
		return nil, fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(v))
	}

	// §3.3.2.1: an empty topic name is legal only on a [V50] link that
	// resolves it through a Topic Alias. decodeProperties has already
	// rejected an alias of 0 (§3.3.2.3.4), so a non-nil TopicAlias here is
	// necessarily a usable (1..65535) alias — presence is the whole test.
	if topic == "" && (v != V50 || p.Properties == nil || p.Properties.TopicAlias == nil) {
		return nil, wrapMalformed("PUBLISH with empty topic and no topic alias")
	}

	p.Payload = c.readRest()
	return p, nil
}

// readRest returns a copy of the cursor's remaining bytes and advances it
// to the end. Used for the PUBLISH payload, which occupies the rest of the
// packet body. The copy avoids aliasing the frame buffer.
func (c *cursor) readRest() []byte {
	out := make([]byte, c.remaining())
	copy(out, c.buf[c.pos:])
	c.pos = len(c.buf)
	return out
}

// AckPacket is one shape for the four acknowledgement packets that share a
// wire layout: PUBACK, PUBREC, PUBREL and PUBCOMP. Type selects which.
// PUBREL is the only one whose fixed header carries reserved flags 0x02.
// In MQTT 3.1.1 the body is just the packet identifier; in MQTT 5.0 an
// optional reason code and property block may follow.
type AckPacket struct {
	Version    Version
	Type       PacketType
	PacketID   uint16
	ReasonCode ReasonCode
	Properties *Properties
}

// ackTarget maps an acknowledgement packet type to its property-block
// allow-list target, reporting false for any non-acknowledgement type.
func ackTarget(t PacketType) (propTarget, bool) {
	switch t {
	case Puback:
		return tgPuback, true
	case Pubrec:
		return tgPubrec, true
	case Pubrel:
		return tgPubrel, true
	case Pubcomp:
		return tgPubcomp, true
	default:
		return 0, false
	}
}

// EncodeAck writes the acknowledgement packet to w. Type must be one of
// PUBACK/PUBREC/PUBREL/PUBCOMP; any other yields [ErrProtocolViolation], as
// do an unsupported [Version] and a zero packet identifier
// ([MQTT-2.2.1-3]). For [V311] the body is exactly the packet identifier.
// For [V50] the reason code and property block are omitted when the reason
// code is 0x00 (Success) and there are no properties (the spec short form);
// a non-zero reason with no properties writes a three-byte body; properties
// force the full form. PUBREL is written with fixed-header flags 0x02.
func (p *AckPacket) EncodeAck(w io.Writer) error {
	if !p.Version.Valid() {
		return fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(p.Version))
	}
	target, ok := ackTarget(p.Type)
	if !ok {
		return fmt.Errorf("%w: %s is not an acknowledgement packet", ErrProtocolViolation, p.Type)
	}
	// [MQTT-2.2.1-3]: every packet with a packet identifier must carry a
	// non-zero one — an acknowledgement always does.
	if p.PacketID == 0 {
		return fmt.Errorf("%w: %s with zero packet identifier", ErrProtocolViolation, p.Type)
	}

	header := byte(p.Type) << 4
	if p.Type == Pubrel {
		header |= 0x02
	}

	var body bytes.Buffer
	var id [2]byte
	binary.BigEndian.PutUint16(id[:], p.PacketID)
	body.Write(id[:])

	if p.Version == V50 && (p.ReasonCode != 0 || p.Properties != nil) {
		body.WriteByte(byte(p.ReasonCode))
		if p.Properties != nil {
			if err := p.Properties.encode(&body, target); err != nil {
				return err
			}
		}
	}

	return writePacket(w, header, body.Bytes())
}

// DecodeAck decodes an acknowledgement packet body for protocol version v
// and packet type t (one of PUBACK/PUBREC/PUBREL/PUBCOMP). It accepts the
// [V311] two-byte form (packet identifier only) and all three [V50] forms:
// two bytes (reason 0x00, no properties), three bytes (reason code, no
// properties), and the full form with a property block. Any truncation,
// trailing byte, illegal property, zero packet identifier ([MQTT-2.2.1-3])
// or non-acknowledgement type yields an error wrapping
// [ErrMalformedPacket]; decoding never panics.
func DecodeAck(v Version, t PacketType, body []byte) (*AckPacket, error) {
	target, ok := ackTarget(t)
	if !ok {
		return nil, fmt.Errorf("%w: %s is not an acknowledgement packet", ErrMalformedPacket, t)
	}

	p := &AckPacket{Version: v, Type: t, ReasonCode: Success}
	c := newCursor(body)

	pid, err := c.readUint16()
	if err != nil {
		return nil, err
	}
	if pid == 0 {
		return nil, wrapMalformed("acknowledgement with zero packet identifier")
	}
	p.PacketID = pid

	switch v {
	case V311:
		if c.remaining() != 0 {
			return nil, wrapMalformed("trailing bytes after v3 acknowledgement")
		}
		return p, nil
	case V50:
		if c.remaining() == 0 {
			return p, nil
		}
		rc, err := c.readByte()
		if err != nil {
			return nil, err
		}
		p.ReasonCode = ReasonCode(rc)
		if c.remaining() == 0 {
			return p, nil
		}
		props, err := decodeProperties(c, target)
		if err != nil {
			return nil, err
		}
		p.Properties = props
		if c.remaining() != 0 {
			return nil, wrapMalformed("trailing bytes after acknowledgement properties")
		}
		return p, nil
	default:
		return nil, fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(v))
	}
}
