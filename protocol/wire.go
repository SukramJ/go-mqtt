// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// PacketType is the MQTT control packet type: the high nibble of the
// fixed header's first byte.
type PacketType byte

// MQTT control packet types (MQTT 5.0 §2.1.2, MQTT 3.1.1 §2.2.1).
const (
	// Connect is the client request to connect to a server.
	Connect PacketType = 1
	// Connack is the server's connect acknowledgment.
	Connack PacketType = 2
	// Publish transports an application message.
	Publish PacketType = 3
	// Puback acknowledges a QoS 1 PUBLISH.
	Puback PacketType = 4
	// Pubrec is the first response to a QoS 2 PUBLISH (publish received).
	Pubrec PacketType = 5
	// Pubrel is the second QoS 2 handshake packet (publish release).
	Pubrel PacketType = 6
	// Pubcomp is the final QoS 2 handshake packet (publish complete).
	Pubcomp PacketType = 7
	// Subscribe is the client subscribe request.
	Subscribe PacketType = 8
	// Suback is the server's subscribe acknowledgment.
	Suback PacketType = 9
	// Unsubscribe is the client unsubscribe request.
	Unsubscribe PacketType = 10
	// Unsuback is the server's unsubscribe acknowledgment.
	Unsuback PacketType = 11
	// Pingreq is the client keep-alive ping request.
	Pingreq PacketType = 12
	// Pingresp is the server keep-alive ping response.
	Pingresp PacketType = 13
	// Disconnect notifies the peer that the connection is closing.
	Disconnect PacketType = 14
	// Auth carries enhanced-authentication exchange (MQTT 5.0 only).
	Auth PacketType = 15
)

// String returns the packet type's spec name.
func (t PacketType) String() string {
	switch t {
	case Connect:
		return "CONNECT"
	case Connack:
		return "CONNACK"
	case Publish:
		return "PUBLISH"
	case Puback:
		return "PUBACK"
	case Pubrec:
		return "PUBREC"
	case Pubrel:
		return "PUBREL"
	case Pubcomp:
		return "PUBCOMP"
	case Subscribe:
		return "SUBSCRIBE"
	case Suback:
		return "SUBACK"
	case Unsubscribe:
		return "UNSUBSCRIBE"
	case Unsuback:
		return "UNSUBACK"
	case Pingreq:
		return "PINGREQ"
	case Pingresp:
		return "PINGRESP"
	case Disconnect:
		return "DISCONNECT"
	case Auth:
		return "AUTH"
	default:
		return fmt.Sprintf("PacketType(%d)", byte(t))
	}
}

// maxVarint is the largest value the MQTT variable byte integer encoding
// can represent (four bytes): 268,435,455.
const maxVarint = 0x0FFF_FFFF

// maxStringLen is the largest byte length a two-byte-prefixed MQTT UTF-8
// string or binary field can carry.
const maxStringLen = 0xFFFF

// Frame is a decoded fixed-header byte plus the raw remaining bytes. The
// body is version-agnostic; a per-packet decoder interprets it.
type Frame struct {
	Header byte
	Body   []byte
}

// PacketType returns the control packet type carried in the fixed header.
func (f Frame) PacketType() PacketType { return PacketType(f.Header >> 4) }

// ValidateFlags checks the reserved fixed-header flag bits (the low
// nibble) against the packet type, per MQTT 5.0 §2.1.3 / 3.1.1 §2.2.2.
// PUBREL, SUBSCRIBE and UNSUBSCRIBE require the bit pattern 0b0010;
// PUBLISH carries DUP/QoS/RETAIN bits but QoS 3 is illegal; every other
// packet requires all flag bits clear. A malformed nibble (or an
// unknown/reserved packet type) yields an error wrapping
// [ErrMalformedPacket].
func (f Frame) ValidateFlags() error {
	flags := f.Header & 0x0F
	switch f.PacketType() {
	case Publish:
		if (flags>>1)&0x03 == 0x03 {
			return wrapMalformed("PUBLISH with QoS 3")
		}
		return nil
	case Pubrel, Subscribe, Unsubscribe:
		if flags != 0x02 {
			return fmt.Errorf("%w: %s with reserved flags %#02x", ErrMalformedPacket, f.PacketType(), flags)
		}
		return nil
	case Connect, Connack, Puback, Pubrec, Pubcomp, Suback, Unsuback,
		Pingreq, Pingresp, Disconnect, Auth:
		if flags != 0x00 {
			return fmt.Errorf("%w: %s with reserved flags %#02x", ErrMalformedPacket, f.PacketType(), flags)
		}
		return nil
	default:
		return fmt.Errorf("%w: reserved packet type %d", ErrMalformedPacket, byte(f.PacketType()))
	}
}

// ReadFrame reads exactly one MQTT packet from r. maxRemainingLength
// caps the advertised remaining length (the client wires this to the
// negotiated MaximumPacketSize); a frame exceeding it fails with
// [ErrFrameTooLarge] before any body buffer is allocated, so a hostile
// length field cannot force a large allocation. A short/erroring reader
// surfaces the reader's error (io.EOF / io.ErrUnexpectedEOF).
func ReadFrame(r io.Reader, maxRemainingLength uint32) (Frame, error) {
	var head [1]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return Frame{}, err
	}
	length, err := readVarintFrom(r)
	if err != nil {
		return Frame{}, err
	}
	if length > maxRemainingLength {
		return Frame{}, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, length, maxRemainingLength)
	}
	body := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return Frame{}, err
		}
	}
	return Frame{Header: head[0], Body: body}, nil
}

// writePacket writes a complete control packet: the fixed-header byte,
// the variable-byte-integer remaining length, then the body. The header
// and length are buffered and written in a single call so a partial
// write never leaves a truncated fixed header on the wire.
func writePacket(w io.Writer, header byte, body []byte) error {
	if len(body) > maxVarint {
		return fmt.Errorf("%w: body %d bytes", ErrFrameTooLarge, len(body))
	}
	var hdr bytes.Buffer
	hdr.WriteByte(header)
	appendVarint(&hdr, uint32(len(body))) //nolint:gosec // len(body) bounded by maxVarint check above
	if _, err := w.Write(hdr.Bytes()); err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	_, err := w.Write(body)
	return err
}

// readVarintFrom decodes an MQTT variable byte integer directly from a
// stream (used for the fixed-header remaining length). It reads at most
// four bytes; a fifth continuation bit is malformed.
func readVarintFrom(r io.Reader) (uint32, error) {
	var value uint32
	var b [1]byte
	for i := range 4 {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		value |= uint32(b[0]&0x7F) << (7 * i)
		if b[0]&0x80 == 0 {
			return value, nil
		}
	}
	return 0, fmt.Errorf("%w: variable byte integer too long", ErrMalformedPacket)
}

// appendVarint appends v to buf as an MQTT variable byte integer. v must
// be <= maxVarint (callers that accept untrusted lengths check first).
func appendVarint(buf *bytes.Buffer, v uint32) {
	for {
		digit := byte(v & 0x7F)
		v >>= 7
		if v > 0 {
			digit |= 0x80
		}
		buf.WriteByte(digit)
		if v == 0 {
			return
		}
	}
}

// appendString appends s to buf as an MQTT UTF-8 string (two-byte length
// prefix + bytes). It returns [ErrStringTooLong] rather than silently
// truncating a string longer than 65535 bytes.
func appendString(buf *bytes.Buffer, s string) error {
	if len(s) > maxStringLen {
		return fmt.Errorf("%w: %d bytes", ErrStringTooLong, len(s))
	}
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s))) //nolint:gosec // len bounded by maxStringLen check above
	buf.Write(l[:])
	buf.WriteString(s)
	return nil
}

// appendBinary appends b to buf as MQTT binary data (two-byte length
// prefix + bytes). It returns [ErrStringTooLong] for data longer than
// 65535 bytes.
func appendBinary(buf *bytes.Buffer, b []byte) error {
	if len(b) > maxStringLen {
		return fmt.Errorf("%w: %d bytes", ErrStringTooLong, len(b))
	}
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(b))) //nolint:gosec // len bounded by maxStringLen check above
	buf.Write(l[:])
	buf.Write(b)
	return nil
}

// cursor is a bounds-checked reader over a packet body. Every read
// validates the remaining length and returns an error wrapping
// [ErrMalformedPacket] instead of panicking, so decoders driven by
// arbitrary (fuzzed) input can never index out of range.
type cursor struct {
	buf []byte
	pos int
}

// newCursor returns a cursor positioned at the start of b.
func newCursor(b []byte) *cursor { return &cursor{buf: b} }

// remaining reports how many unread bytes are left.
func (c *cursor) remaining() int { return len(c.buf) - c.pos }

// readByte reads one byte.
func (c *cursor) readByte() (byte, error) {
	if c.remaining() < 1 {
		return 0, wrapMalformed("truncated byte")
	}
	b := c.buf[c.pos]
	c.pos++
	return b, nil
}

// readUint16 reads a big-endian two-byte integer.
func (c *cursor) readUint16() (uint16, error) {
	if c.remaining() < 2 {
		return 0, wrapMalformed("truncated uint16")
	}
	v := binary.BigEndian.Uint16(c.buf[c.pos : c.pos+2])
	c.pos += 2
	return v, nil
}

// readUint32 reads a big-endian four-byte integer.
func (c *cursor) readUint32() (uint32, error) {
	if c.remaining() < 4 {
		return 0, wrapMalformed("truncated uint32")
	}
	v := binary.BigEndian.Uint32(c.buf[c.pos : c.pos+4])
	c.pos += 4
	return v, nil
}

// readVarint reads an MQTT variable byte integer (property length,
// subscription identifier). It consumes at most four bytes.
func (c *cursor) readVarint() (uint32, error) {
	var value uint32
	for i := range 4 {
		b, err := c.readByte()
		if err != nil {
			return 0, err
		}
		value |= uint32(b&0x7F) << (7 * i)
		if b&0x80 == 0 {
			return value, nil
		}
	}
	return 0, wrapMalformed("variable byte integer too long")
}

// readString reads a two-byte-prefixed MQTT UTF-8 string and enforces the
// well-formedness rules every MQTT UTF-8 encoded string must satisfy
// (§1.5.4): the bytes MUST be well-formed UTF-8 [MQTT-1.5.4-1] and MUST NOT
// contain U+0000 [MQTT-1.5.4-2]. A receiver treats a violation as a
// Malformed Packet (§4.13), so both are surfaced wrapping
// [ErrMalformedPacket] rather than handed on as a corrupt topic/property.
func (c *cursor) readString() (string, error) {
	n, err := c.readUint16()
	if err != nil {
		return "", err
	}
	if c.remaining() < int(n) {
		return "", wrapMalformed("truncated string")
	}
	s := string(c.buf[c.pos : c.pos+int(n)])
	c.pos += int(n)
	if !utf8.ValidString(s) {
		return "", wrapMalformed("string is not well-formed UTF-8")
	}
	if strings.IndexByte(s, 0) >= 0 {
		return "", wrapMalformed("string contains U+0000")
	}
	return s, nil
}

// readBinary reads a two-byte-prefixed MQTT binary field. It returns a
// fresh copy so the result does not alias the frame body.
func (c *cursor) readBinary() ([]byte, error) {
	n, err := c.readUint16()
	if err != nil {
		return nil, err
	}
	if c.remaining() < int(n) {
		return nil, wrapMalformed("truncated binary data")
	}
	out := make([]byte, n)
	copy(out, c.buf[c.pos:c.pos+int(n)])
	c.pos += int(n)
	return out, nil
}
