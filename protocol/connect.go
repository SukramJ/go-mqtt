// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Will is the wire-level Last Will and Testament carried in a CONNECT
// packet. Topic and Payload are the will message; QoS and Retain travel
// in the CONNECT flags byte; Properties (MQTT 5.0 only) is the will
// property block that precedes the will topic in the payload. A nil
// Properties encodes as an empty (0x00-length) block.
type Will struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties *Properties
}

// ConnectPacket is a client CONNECT request. This package never decodes
// CONNECT (a client only sends it), so only [ConnectPacket.Encode] exists.
// Version selects the wire dialect: [V311] writes protocol name "MQTT",
// level 4 and no property blocks; [V50] writes level 5, a CONNECT property
// block after the keep-alive, and a will property block before the will
// topic.
type ConnectPacket struct {
	Version    Version
	ClientID   string
	KeepAlive  uint16
	Username   string
	Password   string
	CleanStart bool
	Will       *Will
	Properties *Properties
}

// Encode writes the CONNECT packet to w. For [V311] a password without a
// username is rejected with [ErrProtocolViolation] (MQTT 3.1.1 §3.1.2.9);
// MQTT 5.0 permits it. A will QoS above 2, or an unsupported version,
// likewise yields [ErrProtocolViolation]. Properties illegal for the
// CONNECT or will context surface [ErrProtocolViolation] from the property
// encoder before any byte reaches w.
func (p *ConnectPacket) Encode(w io.Writer) error {
	if !p.Version.Valid() {
		return fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(p.Version))
	}
	hasUser := p.Username != ""
	hasPass := p.Password != ""
	if p.Version == V311 && hasPass && !hasUser {
		return fmt.Errorf("%w: CONNECT password without username", ErrProtocolViolation)
	}

	var body bytes.Buffer
	if err := appendString(&body, "MQTT"); err != nil {
		return err
	}
	body.WriteByte(byte(p.Version))

	var flags byte
	if hasUser {
		flags |= 0x80
	}
	if hasPass {
		flags |= 0x40
	}
	if p.Will != nil {
		if p.Will.QoS > 2 {
			return fmt.Errorf("%w: will QoS %d", ErrProtocolViolation, p.Will.QoS)
		}
		flags |= 0x04
		flags |= p.Will.QoS << 3
		if p.Will.Retain {
			flags |= 0x20
		}
	}
	if p.CleanStart {
		flags |= 0x02
	}
	body.WriteByte(flags)

	var ka [2]byte
	binary.BigEndian.PutUint16(ka[:], p.KeepAlive)
	body.Write(ka[:])

	if p.Version == V50 {
		if err := p.Properties.encode(&body, tgConnect); err != nil {
			return err
		}
	}

	if err := appendString(&body, p.ClientID); err != nil {
		return err
	}

	if p.Will != nil {
		if p.Version == V50 {
			if err := p.Will.Properties.encode(&body, willBit); err != nil {
				return err
			}
		}
		if err := appendString(&body, p.Will.Topic); err != nil {
			return err
		}
		if err := appendBinary(&body, p.Will.Payload); err != nil {
			return err
		}
	}

	if hasUser {
		if err := appendString(&body, p.Username); err != nil {
			return err
		}
	}
	if hasPass {
		if err := appendBinary(&body, []byte(p.Password)); err != nil {
			return err
		}
	}

	return writePacket(w, byte(Connect)<<4, body.Bytes())
}

// ConnackPacket is a decoded server CONNACK. In MQTT 3.1.1 the return code
// is mapped onto the equivalent MQTT 5.0 [ReasonCode] (see [reason.go]);
// Properties is always nil for 3.1.1.
type ConnackPacket struct {
	SessionPresent bool
	ReasonCode     ReasonCode
	Properties     *Properties
}

// DecodeConnack decodes a CONNACK body for protocol version v. The first
// byte is the Connect Acknowledge Flags: bit 0 is Session Present and bits
// 1-7 are reserved and must be zero (MQTT 5.0 §3.2.2.1 / 3.1.1 §3.2.2.1).
// For [V311] the body is exactly two bytes and the return code is mapped
// to its v5-equivalent reason code (unknown non-zero codes become
// [UnspecifiedError]). For [V50] the reason code is followed by a CONNACK
// property block. Any truncation, reserved-flag violation or trailing byte
// yields an error wrapping [ErrMalformedPacket]; decoding never panics.
func DecodeConnack(v Version, body []byte) (*ConnackPacket, error) {
	c := newCursor(body)

	ackFlags, err := c.readByte()
	if err != nil {
		return nil, err
	}
	if ackFlags&0xFE != 0 {
		return nil, wrapMalformed("CONNACK reserved acknowledge-flag bits set")
	}
	p := &ConnackPacket{SessionPresent: ackFlags&0x01 != 0}

	rc, err := c.readByte()
	if err != nil {
		return nil, err
	}

	switch v {
	case V311:
		switch rc {
		case 0:
			p.ReasonCode = Success
		default:
			if code, ok := v3ConnackReason[rc]; ok {
				p.ReasonCode = code
			} else {
				p.ReasonCode = UnspecifiedError
			}
		}
		if c.remaining() != 0 {
			return nil, wrapMalformed("trailing bytes after v3 CONNACK")
		}
	case V50:
		p.ReasonCode = ReasonCode(rc)
		props, err := decodeProperties(c, tgConnack)
		if err != nil {
			return nil, err
		}
		p.Properties = props
		if c.remaining() != 0 {
			return nil, wrapMalformed("trailing bytes after CONNACK properties")
		}
	default:
		return nil, fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(v))
	}

	return p, nil
}
