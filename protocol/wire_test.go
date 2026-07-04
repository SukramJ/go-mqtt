// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// errReader yields header bytes, then errors on any read past them.
type errReader struct {
	header []byte
	read   int
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.read < len(r.header) {
		n := copy(p, r.header[r.read:])
		r.read += n
		return n, nil
	}
	return 0, errors.New("body read error")
}

// failWriter accepts up to failAfter bytes, then errors.
type failWriter struct {
	failAfter int
	written   int
}

func (w *failWriter) Write(p []byte) (int, error) {
	if w.written >= w.failAfter {
		return 0, errors.New("write error")
	}
	n := len(p)
	if w.written+n > w.failAfter {
		n = w.failAfter - w.written
	}
	w.written += n
	return n, nil
}

func TestVarintRoundTrip(t *testing.T) {
	t.Parallel()
	for _, n := range []uint32{0, 127, 128, 16383, 16384, 2097151, maxVarint} {
		var buf bytes.Buffer
		appendVarint(&buf, n)

		got, err := readVarintFrom(bytes.NewReader(buf.Bytes()))
		if err != nil || got != n {
			t.Fatalf("readVarintFrom(%d): got=%d err=%v", n, got, err)
		}

		gotC, err := newCursor(buf.Bytes()).readVarint()
		if err != nil || gotC != n {
			t.Fatalf("cursor.readVarint(%d): got=%d err=%v", n, gotC, err)
		}
	}
}

func TestVarintLengths(t *testing.T) {
	t.Parallel()
	cases := []struct {
		value uint32
		bytes int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{2097151, 3},
		{2097152, 4},
		{maxVarint, 4},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		appendVarint(&buf, tc.value)
		if buf.Len() != tc.bytes {
			t.Errorf("appendVarint(%d) used %d bytes, want %d", tc.value, buf.Len(), tc.bytes)
		}
	}
}

func TestReadVarintFromMalformed(t *testing.T) {
	t.Parallel()
	// Four continuation bytes with no terminator -> malformed.
	_, err := readVarintFrom(bytes.NewReader([]byte{0x80, 0x80, 0x80, 0x80}))
	if !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("got %v, want ErrMalformedPacket", err)
	}
}

func TestReadVarintFromShort(t *testing.T) {
	t.Parallel()
	// Continuation bit set but stream ends -> reader error surfaces.
	_, err := readVarintFrom(bytes.NewReader([]byte{0x80}))
	if err == nil {
		t.Fatal("expected error for truncated varint stream")
	}
}

func TestCursorReadVarintMalformed(t *testing.T) {
	t.Parallel()
	_, err := newCursor([]byte{0x80, 0x80, 0x80, 0x80}).readVarint()
	if !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("got %v, want ErrMalformedPacket", err)
	}
}

func TestReadFrameRoundTrip(t *testing.T) {
	t.Parallel()
	body := []byte("hello world")
	var buf bytes.Buffer
	if err := writePacket(&buf, byte(Publish)<<4|0x01, body); err != nil {
		t.Fatalf("writePacket: %v", err)
	}
	f, err := ReadFrame(&buf, 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if f.PacketType() != Publish {
		t.Fatalf("PacketType = %v, want PUBLISH", f.PacketType())
	}
	if f.Header&0x01 == 0 {
		t.Fatalf("retain bit lost: header=%#02x", f.Header)
	}
	if !bytes.Equal(f.Body, body) {
		t.Fatalf("body = %q, want %q", f.Body, body)
	}
}

func TestReadFrameEmptyBody(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := writePacket(&buf, byte(Pingreq)<<4, nil); err != nil {
		t.Fatalf("writePacket: %v", err)
	}
	f, err := ReadFrame(&buf, 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if f.PacketType() != Pingreq || len(f.Body) != 0 {
		t.Fatalf("f = %+v", f)
	}
}

func TestReadFrameEOF(t *testing.T) {
	t.Parallel()
	_, err := ReadFrame(bytes.NewReader(nil), 1<<20)
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want EOF-like", err)
	}
}

func TestReadFrameBodyReadError(t *testing.T) {
	t.Parallel()
	// Fixed header 0x30 (PUBLISH) + remaining length 5, then read errors.
	r := &errReader{header: []byte{0x30, 0x05}}
	_, err := ReadFrame(r, 1<<20)
	if err == nil {
		t.Fatal("expected error when body read fails")
	}
}

func TestReadFrameRejectsOversizedBeforeAlloc(t *testing.T) {
	t.Parallel()
	const limit = 1 << 20
	// Encode a remaining length one over the limit. The errReader errors
	// the instant anything reads past the fixed header + length bytes, so
	// if ReadFrame tried to allocate/read the body we would see that
	// error rather than ErrFrameTooLarge.
	var lenBytes bytes.Buffer
	appendVarint(&lenBytes, limit+1)
	header := append([]byte{byte(Publish) << 4}, lenBytes.Bytes()...)
	_, err := ReadFrame(&errReader{header: header}, limit)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameAtLimit(t *testing.T) {
	t.Parallel()
	const limit = 8
	body := []byte("12345678") // exactly the limit
	var buf bytes.Buffer
	if err := writePacket(&buf, byte(Publish)<<4, body); err != nil {
		t.Fatalf("writePacket: %v", err)
	}
	f, err := ReadFrame(&buf, limit)
	if err != nil {
		t.Fatalf("ReadFrame at limit: %v", err)
	}
	if !bytes.Equal(f.Body, body) {
		t.Fatalf("body = %q", f.Body)
	}
}

func TestWritePacketHeaderWriteError(t *testing.T) {
	t.Parallel()
	err := writePacket(&failWriter{failAfter: 0}, byte(Pingreq)<<4, nil)
	if err == nil {
		t.Fatal("expected error when fixed-header write fails")
	}
}

func TestWritePacketBodyWriteError(t *testing.T) {
	t.Parallel()
	// Allow the 2-byte fixed header through, fail on the body write.
	err := writePacket(&failWriter{failAfter: 2}, byte(Publish)<<4, []byte("payload"))
	if err == nil {
		t.Fatal("expected error when body write fails")
	}
}

func TestAppendStringRoundTrip(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"", "a", "topic/level/leaf", strings.Repeat("x", maxStringLen)} {
		var buf bytes.Buffer
		if err := appendString(&buf, s); err != nil {
			t.Fatalf("appendString(len=%d): %v", len(s), err)
		}
		got, err := newCursor(buf.Bytes()).readString()
		if err != nil || got != s {
			t.Fatalf("round trip len=%d: got len=%d err=%v", len(s), len(got), err)
		}
	}
}

func TestAppendStringOverflow(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := appendString(&buf, strings.Repeat("x", maxStringLen+1))
	if !errors.Is(err, ErrStringTooLong) {
		t.Fatalf("got %v, want ErrStringTooLong", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("buffer written despite overflow: %d bytes", buf.Len())
	}
}

func TestAppendBinaryRoundTrip(t *testing.T) {
	t.Parallel()
	for _, b := range [][]byte{nil, {0x00}, []byte("\x00\x01\x02\xff")} {
		var buf bytes.Buffer
		if err := appendBinary(&buf, b); err != nil {
			t.Fatalf("appendBinary: %v", err)
		}
		got, err := newCursor(buf.Bytes()).readBinary()
		if err != nil {
			t.Fatalf("readBinary: %v", err)
		}
		if !bytes.Equal(got, b) {
			t.Fatalf("got %x, want %x", got, b)
		}
	}
}

func TestAppendBinaryOverflow(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := appendBinary(&buf, bytes.Repeat([]byte{0x01}, maxStringLen+1))
	if !errors.Is(err, ErrStringTooLong) {
		t.Fatalf("got %v, want ErrStringTooLong", err)
	}
}

func TestWritePacketBodyTooLarge(t *testing.T) {
	t.Parallel()
	// A body larger than the variable byte integer range is rejected
	// without attempting a write.
	oversize := make([]byte, maxVarint+1)
	w := &failWriter{failAfter: 0}
	err := writePacket(w, byte(Publish)<<4, oversize)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
	if w.written != 0 {
		t.Fatalf("wrote %d bytes for oversize body", w.written)
	}
}

// --- cursor bounds: every read type against short input ---

func TestCursorReadByteBounds(t *testing.T) {
	t.Parallel()
	if _, err := newCursor(nil).readByte(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("empty readByte: %v", err)
	}
	c := newCursor([]byte{0xAB})
	b, err := c.readByte()
	if err != nil || b != 0xAB {
		t.Fatalf("readByte: b=%#x err=%v", b, err)
	}
	if _, err := c.readByte(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("readByte past end: %v", err)
	}
}

func TestCursorReadUint16Bounds(t *testing.T) {
	t.Parallel()
	if _, err := newCursor([]byte{0x00}).readUint16(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("short readUint16: %v", err)
	}
	v, err := newCursor([]byte{0x12, 0x34}).readUint16()
	if err != nil || v != 0x1234 {
		t.Fatalf("readUint16: v=%#x err=%v", v, err)
	}
}

func TestCursorReadUint32Bounds(t *testing.T) {
	t.Parallel()
	if _, err := newCursor([]byte{0x00, 0x00, 0x00}).readUint32(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("short readUint32: %v", err)
	}
	v, err := newCursor([]byte{0x12, 0x34, 0x56, 0x78}).readUint32()
	if err != nil || v != 0x12345678 {
		t.Fatalf("readUint32: v=%#x err=%v", v, err)
	}
}

func TestCursorReadStringBounds(t *testing.T) {
	t.Parallel()
	// Missing length prefix.
	if _, err := newCursor([]byte{0x00}).readString(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("short header: %v", err)
	}
	// Length says 4 but only one byte of content.
	if _, err := newCursor([]byte{0x00, 0x04, 'a'}).readString(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("short body: %v", err)
	}
	s, err := newCursor([]byte{0x00, 0x03, 'a', '/', 'b'}).readString()
	if err != nil || s != "a/b" {
		t.Fatalf("readString: s=%q err=%v", s, err)
	}
}

func TestCursorReadBinaryBounds(t *testing.T) {
	t.Parallel()
	if _, err := newCursor([]byte{0x00}).readBinary(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("short header: %v", err)
	}
	if _, err := newCursor([]byte{0x00, 0x02, 0x01}).readBinary(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("short body: %v", err)
	}
}

func TestCursorReadVarintBounds(t *testing.T) {
	t.Parallel()
	if _, err := newCursor(nil).readVarint(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("empty readVarint: %v", err)
	}
	// Continuation bit set but no following byte.
	if _, err := newCursor([]byte{0x80}).readVarint(); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("truncated readVarint: %v", err)
	}
}

func TestFramePacketType(t *testing.T) {
	t.Parallel()
	f := Frame{Header: byte(Subscribe)<<4 | 0x02}
	if f.PacketType() != Subscribe {
		t.Fatalf("PacketType = %v, want SUBSCRIBE", f.PacketType())
	}
}

func TestValidateFlags(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		header byte
		ok     bool
	}{
		{"publish flags zero", byte(Publish) << 4, true},
		{"publish dup+qos1+retain", byte(Publish)<<4 | 0x0B, true},
		{"publish qos2", byte(Publish)<<4 | 0x04, true},
		{"publish qos3 invalid", byte(Publish)<<4 | 0x06, false},
		{"publish qos3 with dup+retain", byte(Publish)<<4 | 0x0F, false},
		{"pubrel ok", byte(Pubrel)<<4 | 0x02, true},
		{"pubrel bad", byte(Pubrel) << 4, false},
		{"subscribe ok", byte(Subscribe)<<4 | 0x02, true},
		{"subscribe bad", byte(Subscribe) << 4, false},
		{"unsubscribe ok", byte(Unsubscribe)<<4 | 0x02, true},
		{"unsubscribe bad", byte(Unsubscribe)<<4 | 0x0F, false},
		{"connect ok", byte(Connect) << 4, true},
		{"connect bad", byte(Connect)<<4 | 0x01, false},
		{"connack ok", byte(Connack) << 4, true},
		{"puback ok", byte(Puback) << 4, true},
		{"puback bad", byte(Puback)<<4 | 0x02, false},
		{"pubrec ok", byte(Pubrec) << 4, true},
		{"pubcomp ok", byte(Pubcomp) << 4, true},
		{"suback ok", byte(Suback) << 4, true},
		{"unsuback ok", byte(Unsuback) << 4, true},
		{"pingreq ok", byte(Pingreq) << 4, true},
		{"pingreq bad", byte(Pingreq)<<4 | 0x01, false},
		{"pingresp ok", byte(Pingresp) << 4, true},
		{"disconnect ok", byte(Disconnect) << 4, true},
		{"auth ok", byte(Auth) << 4, true},
		{"auth bad", byte(Auth)<<4 | 0x08, false},
		{"reserved type zero", 0x00, false},
		{"reserved type zero with flags", 0x0F, false},
	}
	for _, tc := range cases {
		err := (Frame{Header: tc.header}).ValidateFlags()
		if tc.ok && err != nil {
			t.Errorf("%s: header=%#02x unexpected error %v", tc.name, tc.header, err)
		}
		if !tc.ok {
			if err == nil {
				t.Errorf("%s: header=%#02x expected error", tc.name, tc.header)
			} else if !errors.Is(err, ErrMalformedPacket) {
				t.Errorf("%s: header=%#02x error %v not ErrMalformedPacket", tc.name, tc.header, err)
			}
		}
	}
}

func TestPacketTypeString(t *testing.T) {
	t.Parallel()
	cases := map[PacketType]string{
		Connect:     "CONNECT",
		Publish:     "PUBLISH",
		Pubrec:      "PUBREC",
		Pubrel:      "PUBREL",
		Pubcomp:     "PUBCOMP",
		Unsubscribe: "UNSUBSCRIBE",
		Auth:        "AUTH",
	}
	for pt, want := range cases {
		if got := pt.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", byte(pt), got, want)
		}
	}
	if got := PacketType(0).String(); !strings.Contains(got, "PacketType(0)") {
		t.Errorf("unknown type String = %q", got)
	}
}

func TestVersion(t *testing.T) {
	t.Parallel()
	if !V311.Valid() || !V50.Valid() {
		t.Fatal("V311/V50 should be valid")
	}
	if Version(0).Valid() || Version(3).Valid() {
		t.Fatal("unsupported versions should be invalid")
	}
	if V311.String() != "MQTT 3.1.1" || V50.String() != "MQTT 5.0" {
		t.Fatalf("v311=%q v50=%q", V311.String(), V50.String())
	}
	if !strings.Contains(Version(9).String(), "level 9") {
		t.Fatalf("unknown version String = %q", Version(9).String())
	}
}
