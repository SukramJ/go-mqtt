// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodePing(t *testing.T) {
	t.Parallel()
	var req bytes.Buffer
	if err := EncodePingReq(&req); err != nil {
		t.Fatalf("EncodePingReq: %v", err)
	}
	if !bytes.Equal(req.Bytes(), []byte{0xC0, 0x00}) {
		t.Fatalf("PINGREQ = %x", req.Bytes())
	}
	var resp bytes.Buffer
	if err := EncodePingResp(&resp); err != nil {
		t.Fatalf("EncodePingResp: %v", err)
	}
	if !bytes.Equal(resp.Bytes(), []byte{0xD0, 0x00}) {
		t.Fatalf("PINGRESP = %x", resp.Bytes())
	}
}

func TestEncodeDisconnectV3(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := (&DisconnectPacket{Version: V311}).Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0xE0, 0x00}) {
		t.Fatalf("v3 DISCONNECT = %x", buf.Bytes())
	}
}

func TestEncodeDisconnectV5(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		pkt  *DisconnectPacket
		want []byte
	}{
		{
			"normal short form",
			&DisconnectPacket{Version: V50, ReasonCode: NormalDisconnection},
			[]byte{0xE0, 0x00},
		},
		{
			"reason no props",
			&DisconnectPacket{Version: V50, ReasonCode: SessionTakenOver},
			[]byte{0xE0, 0x02, 0x8E, 0x00},
		},
		{
			"reason zero with reason string",
			&DisconnectPacket{Version: V50, ReasonCode: NormalDisconnection, Properties: &Properties{ReasonString: "bye"}},
			[]byte{0xE0, 0x08, 0x00, 0x06, 0x1F, 0x00, 0x03, 'b', 'y', 'e'},
		},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		if err := tc.pkt.Encode(&buf); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !bytes.Equal(buf.Bytes(), tc.want) {
			t.Fatalf("%s:\n got %x\nwant %x", tc.name, buf.Bytes(), tc.want)
		}
	}
}

// TestEncodeDisconnectIllegalProperty rejects a property illegal for
// DISCONNECT (Topic Alias is PUBLISH-only) once the non-empty-body path is
// taken.
func TestEncodeDisconnectIllegalProperty(t *testing.T) {
	t.Parallel()
	pkt := &DisconnectPacket{Version: V50, ReasonCode: SessionTakenOver, Properties: &Properties{TopicAlias: u16ptr(1)}}
	if err := pkt.Encode(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestEncodeDisconnectBadVersion(t *testing.T) {
	t.Parallel()
	if err := (&DisconnectPacket{Version: Version(3)}).Encode(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestDecodeDisconnect(t *testing.T) {
	t.Parallel()
	// v3 empty body.
	p, err := DecodeDisconnect(V311, nil)
	if err != nil || p.ReasonCode != NormalDisconnection {
		t.Fatalf("v3 empty: p=%+v err=%v", p, err)
	}

	// v5 forms.
	v5 := []struct {
		name   string
		body   []byte
		code   ReasonCode
		hasStr bool
	}{
		{"empty", []byte{}, NormalDisconnection, false},
		{"reason only", []byte{0x8E}, SessionTakenOver, false},
		{"reason + zero props", []byte{0x8E, 0x00}, SessionTakenOver, false},
		{"reason + reason string", []byte{0x00, 0x06, 0x1F, 0x00, 0x03, 'b', 'y', 'e'}, NormalDisconnection, true},
	}
	for _, tc := range v5 {
		p, err := DecodeDisconnect(V50, tc.body)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if p.ReasonCode != tc.code {
			t.Fatalf("%s: code=%v", tc.name, p.ReasonCode)
		}
		if tc.hasStr && (p.Properties == nil || p.Properties.ReasonString != "bye") {
			t.Fatalf("%s: props=%+v", tc.name, p.Properties)
		}
		if !tc.hasStr && p.Properties != nil {
			t.Fatalf("%s: unexpected props %+v", tc.name, p.Properties)
		}
	}
}

func TestDecodeDisconnectRoundTrip(t *testing.T) {
	t.Parallel()
	orig := &DisconnectPacket{Version: V50, ReasonCode: SessionTakenOver, Properties: &Properties{ReasonString: "gone"}}
	var buf bytes.Buffer
	if err := orig.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	f, err := ReadFrame(&buf, 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if f.PacketType() != Disconnect {
		t.Fatalf("type = %v", f.PacketType())
	}
	got, err := DecodeDisconnect(V50, f.Body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ReasonCode != SessionTakenOver || got.Properties == nil || got.Properties.ReasonString != "gone" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestDecodeDisconnectErrors(t *testing.T) {
	t.Parallel()
	if _, err := DecodeDisconnect(V311, []byte{0x00}); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("v3 non-empty: %v", err)
	}
	v5bad := map[string][]byte{
		"prop len overrun": {0x00, 0x05, 0x1F},
		"illegal property": {0x00, 0x03, 0x23, 0x00, 0x01}, // Topic Alias illegal in DISCONNECT
		"trailing":         {0x00, 0x00, 0xFF},
	}
	for name, body := range v5bad {
		if _, err := DecodeDisconnect(V50, body); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("%s: got %v, want ErrMalformedPacket", name, err)
		}
	}
	if _, err := DecodeDisconnect(Version(3), nil); err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

func TestDecodeAuth(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		body    []byte
		code    ReasonCode
		hasAuth bool
	}{
		{"empty", []byte{}, Success, false},
		{"reason only", []byte{0x18}, ContinueAuthentication, false},
		{"reason + zero props", []byte{0x18, 0x00}, ContinueAuthentication, false},
		{"reason + auth method", []byte{0x18, 0x07, 0x15, 0x00, 0x04, 'S', 'C', 'R', 'A'}, ContinueAuthentication, true},
	}
	for _, tc := range cases {
		p, err := DecodeAuth(tc.body)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if p.ReasonCode != tc.code {
			t.Fatalf("%s: code=%v", tc.name, p.ReasonCode)
		}
		if tc.hasAuth && (p.Properties == nil || p.Properties.AuthMethod != "SCRA") {
			t.Fatalf("%s: props=%+v", tc.name, p.Properties)
		}
	}
}

func TestDecodeAuthErrors(t *testing.T) {
	t.Parallel()
	cases := map[string][]byte{
		"prop len overrun": {0x18, 0x05, 0x15},
		"illegal property": {0x18, 0x03, 0x23, 0x00, 0x01}, // Topic Alias illegal in AUTH
		"trailing":         {0x18, 0x00, 0xFF},
	}
	for name, body := range cases {
		if _, err := DecodeAuth(body); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("%s: got %v, want ErrMalformedPacket", name, err)
		}
	}
}
