// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"fmt"
	"io"
)

// EncodePingReq writes a PINGREQ packet (fixed header only, empty body) to
// w. PINGREQ is identical in MQTT 3.1.1 and 5.0.
func EncodePingReq(w io.Writer) error {
	return writePacket(w, byte(Pingreq)<<4, nil)
}

// EncodePingResp writes a PINGRESP packet (fixed header only, empty body)
// to w. Provided for symmetry and mock-broker use; a client only receives
// PINGRESP.
func EncodePingResp(w io.Writer) error {
	return writePacket(w, byte(Pingresp)<<4, nil)
}

// DisconnectPacket is a DISCONNECT notification. In MQTT 3.1.1 it carries
// no variable header (empty body). In MQTT 5.0 it carries a reason code and
// an optional property block, both of which may be omitted when the reason
// is 0x00 (Normal disconnection) and there are no properties.
type DisconnectPacket struct {
	Version    Version
	ReasonCode ReasonCode
	Properties *Properties
}

// Encode writes the DISCONNECT packet to w. For [V311] the body is always
// empty. For [V50] a reason code of 0x00 with no properties encodes as an
// empty body (the spec short form); otherwise the reason code byte is
// followed by a property block. An unsupported version yields
// [ErrProtocolViolation].
func (p *DisconnectPacket) Encode(w io.Writer) error {
	switch p.Version {
	case V311:
		return writePacket(w, byte(Disconnect)<<4, nil)
	case V50:
		if p.ReasonCode == 0 && p.Properties == nil {
			return writePacket(w, byte(Disconnect)<<4, nil)
		}
		var body bytes.Buffer
		body.WriteByte(byte(p.ReasonCode))
		if err := p.Properties.encode(&body, tgDisconnect); err != nil {
			return err
		}
		return writePacket(w, byte(Disconnect)<<4, body.Bytes())
	default:
		return fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(p.Version))
	}
}

// DecodeDisconnect decodes a DISCONNECT body for protocol version v. A
// [V311] DISCONNECT must have an empty body. A [V50] DISCONNECT has three
// forms: an empty body (reason 0x00, no properties), a single reason-code
// byte (no properties), or a reason code followed by a property block. Any
// truncation, illegal property or trailing byte yields an error wrapping
// [ErrMalformedPacket]; decoding never panics.
func DecodeDisconnect(v Version, body []byte) (*DisconnectPacket, error) {
	p := &DisconnectPacket{Version: v}

	switch v {
	case V311:
		if len(body) != 0 {
			return nil, wrapMalformed("v3 DISCONNECT with non-empty body")
		}
		p.ReasonCode = NormalDisconnection
		return p, nil
	case V50:
		c := newCursor(body)
		if c.remaining() == 0 {
			p.ReasonCode = NormalDisconnection
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
		props, err := decodeProperties(c, tgDisconnect)
		if err != nil {
			return nil, err
		}
		p.Properties = props
		if c.remaining() != 0 {
			return nil, wrapMalformed("trailing bytes after DISCONNECT properties")
		}
		return p, nil
	default:
		return nil, fmt.Errorf("%w: unsupported protocol version %d", ErrProtocolViolation, byte(v))
	}
}

// AuthPacket is a decoded AUTH packet (MQTT 5.0 only). This client does not
// participate in enhanced authentication, so only decoding is provided;
// AUTH is decoded solely to react to it (the adapter rejects it).
type AuthPacket struct {
	ReasonCode ReasonCode
	Properties *Properties
}

// DecodeAuth decodes an AUTH body. Like DISCONNECT it has three forms: an
// empty body (reason 0x00 Success, no properties), a single reason-code
// byte, or a reason code followed by a property block. Any truncation,
// illegal property or trailing byte yields an error wrapping
// [ErrMalformedPacket]; decoding never panics.
func DecodeAuth(body []byte) (*AuthPacket, error) {
	p := &AuthPacket{}
	c := newCursor(body)

	if c.remaining() == 0 {
		p.ReasonCode = Success
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
	props, err := decodeProperties(c, tgAuth)
	if err != nil {
		return nil, err
	}
	p.Properties = props
	if c.remaining() != 0 {
		return nil, wrapMalformed("trailing bytes after AUTH properties")
	}
	return p, nil
}
