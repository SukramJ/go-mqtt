// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// UserProperty is a single MQTT 5.0 User Property: a UTF-8 string pair
// (property identifier 0x26). User Properties are repeatable and their
// order is preserved on both encode and decode.
type UserProperty struct {
	Key   string
	Value string
}

// Properties is the full set of MQTT 5.0 properties this client encodes
// or decodes. Numeric properties use pointers because zero is a
// meaningful value distinct from "absent"; string and binary properties
// treat their zero value (empty) as absent. Which properties are legal on
// which control packet is decided solely by [propertySpec]; a caller that
// populates a field illegal for the packet being encoded gets an
// [ErrProtocolViolation] from [Properties.encode].
type Properties struct {
	PayloadFormat           *byte          // 0x01 Payload Format Indicator
	MessageExpiryInterval   *uint32        // 0x02 Message Expiry Interval
	ContentType             string         // 0x03 Content Type
	ResponseTopic           string         // 0x08 Response Topic
	CorrelationData         []byte         // 0x09 Correlation Data
	SubscriptionIdentifiers []uint32       // 0x0B Subscription Identifier (repeatable, varint)
	SessionExpiryInterval   *uint32        // 0x11 Session Expiry Interval
	AssignedClientID        string         // 0x12 Assigned Client Identifier
	ServerKeepAlive         *uint16        // 0x13 Server Keep Alive
	AuthMethod              string         // 0x15 Authentication Method
	AuthData                []byte         // 0x16 Authentication Data
	RequestProblemInfo      *byte          // 0x17 Request Problem Information
	WillDelayInterval       *uint32        // 0x18 Will Delay Interval
	RequestResponseInfo     *byte          // 0x19 Request Response Information
	ResponseInfo            string         // 0x1A Response Information
	ServerReference         string         // 0x1C Server Reference
	ReasonString            string         // 0x1F Reason String
	ReceiveMaximum          *uint16        // 0x21 Receive Maximum
	TopicAliasMaximum       *uint16        // 0x22 Topic Alias Maximum
	TopicAlias              *uint16        // 0x23 Topic Alias
	MaximumQoS              *byte          // 0x24 Maximum QoS
	RetainAvailable         *byte          // 0x25 Retain Available
	UserProperties          []UserProperty // 0x26 User Property (repeatable)
	MaximumPacketSize       *uint32        // 0x27 Maximum Packet Size
	WildcardSubAvailable    *byte          // 0x28 Wildcard Subscription Available
	SubIDAvailable          *byte          // 0x29 Subscription Identifier Available
	SharedSubAvailable      *byte          // 0x2A Shared Subscription Available
}

// propTarget is a bitmask of the contexts in which a property is legal.
// Bit i (for a control [PacketType] i) means the property may appear in
// that packet's property block; [willBit] is a pseudo-slot for the
// CONNECT Will Properties block, which is governed by its own allow-list
// even though it travels inside a CONNECT packet.
type propTarget uint32

// Per-context bits for [propTarget]. Bit i corresponds to [PacketType] i;
// PINGREQ and PINGRESP carry no properties and therefore have no bit.
const (
	tgConnect     propTarget = 1 << Connect
	tgConnack     propTarget = 1 << Connack
	tgPublish     propTarget = 1 << Publish
	tgPuback      propTarget = 1 << Puback
	tgPubrec      propTarget = 1 << Pubrec
	tgPubrel      propTarget = 1 << Pubrel
	tgPubcomp     propTarget = 1 << Pubcomp
	tgSubscribe   propTarget = 1 << Subscribe
	tgSuback      propTarget = 1 << Suback
	tgUnsubscribe propTarget = 1 << Unsubscribe
	tgUnsuback    propTarget = 1 << Unsuback
	tgDisconnect  propTarget = 1 << Disconnect
	tgAuth        propTarget = 1 << Auth

	// willBit is the pseudo-slot (bit 16, outside the 1..15 packet-type
	// range) for the CONNECT Will Properties block. Encoders of the will
	// property block pass this as their target.
	willBit propTarget = 1 << 16
)

// userPropTargets is the set of contexts that admit User Property (0x26):
// every packet that carries a property block plus the will block. It is a
// spelled-out union so [propertySpec] stays a single, greppable source of
// truth.
const userPropTargets = tgConnect | tgConnack | tgPublish | willBit |
	tgPuback | tgPubrec | tgPubrel | tgPubcomp |
	tgSubscribe | tgSuback | tgUnsubscribe | tgUnsuback |
	tgDisconnect | tgAuth

// propertySpec is the single source of truth for which property
// identifier is legal in which context (MQTT 5.0 §2.2.2 / §3.*). Both
// encode and decode consult it: encode rejects a populated-but-illegal
// property with [ErrProtocolViolation]; decode rejects an
// unknown-or-illegal identifier with [ErrMalformedPacket].
var propertySpec = map[byte]propTarget{
	0x01: tgPublish | willBit,                                                                                    // Payload Format Indicator
	0x02: tgPublish | willBit,                                                                                    // Message Expiry Interval
	0x03: tgPublish | willBit,                                                                                    // Content Type
	0x08: tgPublish | willBit,                                                                                    // Response Topic
	0x09: tgPublish | willBit,                                                                                    // Correlation Data
	0x0B: tgPublish | tgSubscribe,                                                                                // Subscription Identifier
	0x11: tgConnect | tgConnack | tgDisconnect,                                                                   // Session Expiry Interval
	0x12: tgConnack,                                                                                              // Assigned Client Identifier
	0x13: tgConnack,                                                                                              // Server Keep Alive
	0x15: tgConnect | tgConnack | tgAuth,                                                                         // Authentication Method
	0x16: tgConnect | tgConnack | tgAuth,                                                                         // Authentication Data
	0x17: tgConnect,                                                                                              // Request Problem Information
	0x18: willBit,                                                                                                // Will Delay Interval
	0x19: tgConnect,                                                                                              // Request Response Information
	0x1A: tgConnack,                                                                                              // Response Information
	0x1C: tgConnack | tgDisconnect,                                                                               // Server Reference
	0x1F: tgConnack | tgPuback | tgPubrec | tgPubrel | tgPubcomp | tgSuback | tgUnsuback | tgDisconnect | tgAuth, // Reason String
	0x21: tgConnect | tgConnack,                                                                                  // Receive Maximum
	0x22: tgConnect | tgConnack,                                                                                  // Topic Alias Maximum
	0x23: tgPublish,                                                                                              // Topic Alias
	0x24: tgConnack,                                                                                              // Maximum QoS
	0x25: tgConnack,                                                                                              // Retain Available
	0x26: userPropTargets,                                                                                        // User Property
	0x27: tgConnect | tgConnack,                                                                                  // Maximum Packet Size
	0x28: tgConnack,                                                                                              // Wildcard Subscription Available
	0x29: tgConnack,                                                                                              // Subscription Identifier Available
	0x2A: tgConnack,                                                                                              // Shared Subscription Available
}

// checkEncodeAllowed reports whether property id may be written for the
// given target, returning [ErrProtocolViolation] if not.
func checkEncodeAllowed(id byte, target propTarget) error {
	if propertySpec[id]&target == 0 {
		return fmt.Errorf("%w: property 0x%02X illegal in this packet", ErrProtocolViolation, id)
	}
	return nil
}

// encode writes p as an MQTT 5.0 property block (a variable-byte-integer
// length prefix followed by the property bytes) to buf. Properties are
// emitted in ascending identifier order so the output is deterministic.
// A nil receiver, or one with no populated properties, encodes to the
// single length byte 0x00. Every populated property is validated against
// target before any byte is written to buf; an illegal property yields
// [ErrProtocolViolation] and leaves buf unchanged. MQTT 3.1.1 has no
// property blocks and never calls this.
func (p *Properties) encode(buf *bytes.Buffer, target propTarget) error {
	if p == nil {
		buf.WriteByte(0x00)
		return nil
	}

	var body bytes.Buffer

	writeByteProp := func(id byte, v *byte) error {
		if v == nil {
			return nil
		}
		if err := checkEncodeAllowed(id, target); err != nil {
			return err
		}
		body.WriteByte(id)
		body.WriteByte(*v)
		return nil
	}
	writeU16Prop := func(id byte, v *uint16) error {
		if v == nil {
			return nil
		}
		if err := checkEncodeAllowed(id, target); err != nil {
			return err
		}
		body.WriteByte(id)
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], *v)
		body.Write(b[:])
		return nil
	}
	writeU32Prop := func(id byte, v *uint32) error {
		if v == nil {
			return nil
		}
		if err := checkEncodeAllowed(id, target); err != nil {
			return err
		}
		body.WriteByte(id)
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], *v)
		body.Write(b[:])
		return nil
	}
	writeStrProp := func(id byte, s string) error {
		if s == "" {
			return nil
		}
		if err := checkEncodeAllowed(id, target); err != nil {
			return err
		}
		body.WriteByte(id)
		return appendString(&body, s)
	}
	writeBinProp := func(id byte, v []byte) error {
		if len(v) == 0 {
			return nil
		}
		if err := checkEncodeAllowed(id, target); err != nil {
			return err
		}
		body.WriteByte(id)
		return appendBinary(&body, v)
	}

	if err := writeByteProp(0x01, p.PayloadFormat); err != nil {
		return err
	}
	if err := writeU32Prop(0x02, p.MessageExpiryInterval); err != nil {
		return err
	}
	if err := writeStrProp(0x03, p.ContentType); err != nil {
		return err
	}
	if err := writeStrProp(0x08, p.ResponseTopic); err != nil {
		return err
	}
	if err := writeBinProp(0x09, p.CorrelationData); err != nil {
		return err
	}
	if len(p.SubscriptionIdentifiers) > 0 {
		if err := checkEncodeAllowed(0x0B, target); err != nil {
			return err
		}
		for _, id := range p.SubscriptionIdentifiers {
			if id == 0 || id > maxVarint {
				return fmt.Errorf("%w: subscription identifier %d out of range", ErrProtocolViolation, id)
			}
			body.WriteByte(0x0B)
			appendVarint(&body, id)
		}
	}
	if err := writeU32Prop(0x11, p.SessionExpiryInterval); err != nil {
		return err
	}
	if err := writeStrProp(0x12, p.AssignedClientID); err != nil {
		return err
	}
	if err := writeU16Prop(0x13, p.ServerKeepAlive); err != nil {
		return err
	}
	if err := writeStrProp(0x15, p.AuthMethod); err != nil {
		return err
	}
	if err := writeBinProp(0x16, p.AuthData); err != nil {
		return err
	}
	if err := writeByteProp(0x17, p.RequestProblemInfo); err != nil {
		return err
	}
	if err := writeU32Prop(0x18, p.WillDelayInterval); err != nil {
		return err
	}
	if err := writeByteProp(0x19, p.RequestResponseInfo); err != nil {
		return err
	}
	if err := writeStrProp(0x1A, p.ResponseInfo); err != nil {
		return err
	}
	if err := writeStrProp(0x1C, p.ServerReference); err != nil {
		return err
	}
	if err := writeStrProp(0x1F, p.ReasonString); err != nil {
		return err
	}
	if err := writeU16Prop(0x21, p.ReceiveMaximum); err != nil {
		return err
	}
	if err := writeU16Prop(0x22, p.TopicAliasMaximum); err != nil {
		return err
	}
	if err := writeU16Prop(0x23, p.TopicAlias); err != nil {
		return err
	}
	if err := writeByteProp(0x24, p.MaximumQoS); err != nil {
		return err
	}
	if err := writeByteProp(0x25, p.RetainAvailable); err != nil {
		return err
	}
	if len(p.UserProperties) > 0 {
		if err := checkEncodeAllowed(0x26, target); err != nil {
			return err
		}
		for _, up := range p.UserProperties {
			body.WriteByte(0x26)
			if err := appendString(&body, up.Key); err != nil {
				return err
			}
			if err := appendString(&body, up.Value); err != nil {
				return err
			}
		}
	}
	if err := writeU32Prop(0x27, p.MaximumPacketSize); err != nil {
		return err
	}
	if err := writeByteProp(0x28, p.WildcardSubAvailable); err != nil {
		return err
	}
	if err := writeByteProp(0x29, p.SubIDAvailable); err != nil {
		return err
	}
	if err := writeByteProp(0x2A, p.SharedSubAvailable); err != nil {
		return err
	}

	if body.Len() > maxVarint {
		return fmt.Errorf("%w: property block %d bytes", ErrFrameTooLarge, body.Len())
	}
	appendVarint(buf, uint32(body.Len())) //nolint:gosec // body.Len() bounded by maxVarint check above
	buf.Write(body.Bytes())
	return nil
}

// decodeProperties reads an MQTT 5.0 property block from c: a
// variable-byte-integer length prefix followed by that many bytes of
// properties, which are decoded through a sub-cursor bounded to exactly
// the advertised length. It rejects a length that overruns the remaining
// packet, an unknown identifier, an identifier illegal for target, a
// duplicate of any non-repeatable property (every property except User
// Property 0x26 and Subscription Identifier 0x0B), and any truncated
// value — always with an error wrapping [ErrMalformedPacket], never a
// panic. A zero-length block yields a nil *Properties.
func decodeProperties(c *cursor, target propTarget) (*Properties, error) {
	length, err := c.readVarint()
	if err != nil {
		return nil, err
	}
	n := int(length) //nolint:gosec // length <= maxVarint (28-bit) per cursor.readVarint, fits int
	if n > c.remaining() {
		return nil, wrapMalformed("property length overruns packet")
	}
	sub := newCursor(c.buf[c.pos : c.pos+n])
	c.pos += n
	if n == 0 {
		return nil, nil
	}

	p := &Properties{}
	var seen uint64 // bit id set once a non-repeatable property has appeared
	for sub.remaining() > 0 {
		id, err := sub.readByte()
		if err != nil {
			return nil, err
		}
		allowed, known := propertySpec[id]
		if !known {
			return nil, fmt.Errorf("%w: unknown property 0x%02X", ErrMalformedPacket, id)
		}
		if allowed&target == 0 {
			return nil, fmt.Errorf("%w: property 0x%02X illegal in this packet", ErrMalformedPacket, id)
		}
		if id != 0x26 && id != 0x0B {
			bit := uint64(1) << id
			if seen&bit != 0 {
				return nil, fmt.Errorf("%w: duplicate property 0x%02X", ErrMalformedPacket, id)
			}
			seen |= bit
		}
		if err := p.decodeOne(sub, id); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// decodeOne reads the single value for property id from c and stores it on
// p. id has already been validated as known and legal by
// [decodeProperties]. Every read goes through the bounds-checked cursor,
// so a truncated value surfaces as [ErrMalformedPacket] rather than a
// panic.
func (p *Properties) decodeOne(c *cursor, id byte) error {
	switch id {
	case 0x01:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.PayloadFormat = &v
	case 0x02:
		v, err := c.readUint32()
		if err != nil {
			return err
		}
		p.MessageExpiryInterval = &v
	case 0x03:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ContentType = s
	case 0x08:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ResponseTopic = s
	case 0x09:
		b, err := c.readBinary()
		if err != nil {
			return err
		}
		p.CorrelationData = b
	case 0x0B:
		v, err := c.readVarint()
		if err != nil {
			return err
		}
		if v == 0 {
			return wrapMalformed("subscription identifier 0")
		}
		p.SubscriptionIdentifiers = append(p.SubscriptionIdentifiers, v)
	case 0x11:
		v, err := c.readUint32()
		if err != nil {
			return err
		}
		p.SessionExpiryInterval = &v
	case 0x12:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.AssignedClientID = s
	case 0x13:
		v, err := c.readUint16()
		if err != nil {
			return err
		}
		p.ServerKeepAlive = &v
	case 0x15:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.AuthMethod = s
	case 0x16:
		b, err := c.readBinary()
		if err != nil {
			return err
		}
		p.AuthData = b
	case 0x17:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.RequestProblemInfo = &v
	case 0x18:
		v, err := c.readUint32()
		if err != nil {
			return err
		}
		p.WillDelayInterval = &v
	case 0x19:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.RequestResponseInfo = &v
	case 0x1A:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ResponseInfo = s
	case 0x1C:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ServerReference = s
	case 0x1F:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ReasonString = s
	case 0x21:
		v, err := c.readUint16()
		if err != nil {
			return err
		}
		p.ReceiveMaximum = &v
	case 0x22:
		v, err := c.readUint16()
		if err != nil {
			return err
		}
		p.TopicAliasMaximum = &v
	case 0x23:
		v, err := c.readUint16()
		if err != nil {
			return err
		}
		p.TopicAlias = &v
	case 0x24:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.MaximumQoS = &v
	case 0x25:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.RetainAvailable = &v
	case 0x26:
		k, err := c.readString()
		if err != nil {
			return err
		}
		val, err := c.readString()
		if err != nil {
			return err
		}
		p.UserProperties = append(p.UserProperties, UserProperty{Key: k, Value: val})
	case 0x27:
		v, err := c.readUint32()
		if err != nil {
			return err
		}
		p.MaximumPacketSize = &v
	case 0x28:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.WildcardSubAvailable = &v
	case 0x29:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.SubIDAvailable = &v
	case 0x2A:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.SharedSubAvailable = &v
	default:
		// Unreachable: decodeProperties validates id against propertySpec
		// before dispatching here. Kept as a defensive guard.
		return fmt.Errorf("%w: unhandled property 0x%02X", ErrMalformedPacket, id)
	}
	return nil
}
