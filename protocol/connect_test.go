// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"errors"
	"testing"
)

// TestEncodeConnectV3Golden checks a full MQTT 3.1.1 CONNECT with a will,
// username and password against a hand-computed byte vector.
func TestEncodeConnectV3Golden(t *testing.T) {
	t.Parallel()
	pkt := &ConnectPacket{
		Version:    V311,
		ClientID:   "cid",
		KeepAlive:  60,
		Username:   "user",
		Password:   "pass",
		CleanStart: true,
		Will: &Will{
			Topic:   "wt",
			Payload: []byte("wp"),
			QoS:     1,
			Retain:  true,
		},
	}
	want := []byte{
		0x10, 0x23, // CONNECT, remaining length 35
		0x00, 0x04, 'M', 'Q', 'T', 'T', // protocol name
		0x04,       // level 4
		0xEE,       // flags: user|pass|willRetain|willQoS1|will|clean
		0x00, 0x3C, // keep alive 60
		0x00, 0x03, 'c', 'i', 'd', // client id
		0x00, 0x02, 'w', 't', // will topic
		0x00, 0x02, 'w', 'p', // will payload
		0x00, 0x04, 'u', 's', 'e', 'r', // username
		0x00, 0x04, 'p', 'a', 's', 's', // password
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v3 CONNECT bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

// TestEncodeConnectV5PropsGolden checks an MQTT 5.0 CONNECT carrying a
// CONNECT property block (Session Expiry Interval) against a hand-computed
// vector.
func TestEncodeConnectV5PropsGolden(t *testing.T) {
	t.Parallel()
	sei := uint32(10)
	pkt := &ConnectPacket{
		Version:    V50,
		ClientID:   "c",
		KeepAlive:  30,
		CleanStart: true,
		Properties: &Properties{SessionExpiryInterval: &sei},
	}
	want := []byte{
		0x10, 0x13, // CONNECT, remaining length 19
		0x00, 0x04, 'M', 'Q', 'T', 'T',
		0x05,       // level 5
		0x02,       // flags: clean start
		0x00, 0x1E, // keep alive 30
		0x05, 0x11, 0x00, 0x00, 0x00, 0x0A, // props: len 5, SessionExpiry=10
		0x00, 0x01, 'c', // client id
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v5 CONNECT bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

// TestEncodeConnectV5WillProps checks that a v5 will property block is
// written before the will topic, against a hand-computed vector.
func TestEncodeConnectV5WillProps(t *testing.T) {
	t.Parallel()
	delay := uint32(5)
	pkt := &ConnectPacket{
		Version:    V50,
		ClientID:   "c",
		CleanStart: true,
		Will: &Will{
			Topic:      "t",
			Payload:    []byte{0x01},
			QoS:        2,
			Properties: &Properties{WillDelayInterval: &delay},
		},
	}
	want := []byte{
		0x10, 0x1A, // CONNECT, remaining length 26
		0x00, 0x04, 'M', 'Q', 'T', 'T',
		0x05,       // level 5
		0x16,       // flags: clean|will|willQoS2
		0x00, 0x00, // keep alive 0
		0x00,            // CONNECT props length 0
		0x00, 0x01, 'c', // client id
		0x05, 0x18, 0x00, 0x00, 0x00, 0x05, // will props: len 5, WillDelay=5
		0x00, 0x01, 't', // will topic
		0x00, 0x01, 0x01, // will payload
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v5 CONNECT+will bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

func TestEncodeConnectV3PasswordWithoutUsername(t *testing.T) {
	t.Parallel()
	pkt := &ConnectPacket{Version: V311, ClientID: "c", Password: "pw"}
	err := pkt.Encode(&bytes.Buffer{})
	if !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestEncodeConnectV5PasswordWithoutUsernameAllowed(t *testing.T) {
	t.Parallel()
	pkt := &ConnectPacket{Version: V50, ClientID: "c", Password: "pw"}
	if err := pkt.Encode(&bytes.Buffer{}); err != nil {
		t.Fatalf("v5 password without username should be allowed: %v", err)
	}
}

func TestEncodeConnectWillQoS3(t *testing.T) {
	t.Parallel()
	pkt := &ConnectPacket{Version: V50, ClientID: "c", Will: &Will{Topic: "t", QoS: 3}}
	if err := pkt.Encode(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestEncodeConnectBadVersion(t *testing.T) {
	t.Parallel()
	pkt := &ConnectPacket{Version: Version(3), ClientID: "c"}
	if err := pkt.Encode(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

// TestEncodeConnectIllegalProperty rejects a property illegal for CONNECT
// (Assigned Client Identifier is CONNACK-only).
func TestEncodeConnectIllegalProperty(t *testing.T) {
	t.Parallel()
	pkt := &ConnectPacket{Version: V50, ClientID: "c", Properties: &Properties{AssignedClientID: "x"}}
	if err := pkt.Encode(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestDecodeConnackV3(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		body    []byte
		present bool
		code    ReasonCode
	}{
		{"accepted", []byte{0x00, 0x00}, false, Success},
		{"session present", []byte{0x01, 0x00}, true, Success},
		{"unsupported version", []byte{0x00, 0x01}, false, UnsupportedProtocolVersion},
		{"bad credentials", []byte{0x00, 0x04}, false, BadUserNameOrPassword},
		{"not authorized", []byte{0x00, 0x05}, false, NotAuthorized},
		{"unknown code", []byte{0x00, 0x7F}, false, UnspecifiedError},
	}
	for _, tc := range cases {
		p, err := DecodeConnack(V311, tc.body)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if p.SessionPresent != tc.present || p.ReasonCode != tc.code || p.Properties != nil {
			t.Fatalf("%s: got %+v", tc.name, p)
		}
	}
}

func TestDecodeConnackV3Errors(t *testing.T) {
	t.Parallel()
	cases := map[string][]byte{
		"empty":        {},
		"one byte":     {0x00},
		"bad ack flag": {0x02, 0x00},
		"trailing":     {0x00, 0x00, 0x00},
	}
	for name, body := range cases {
		if _, err := DecodeConnack(V311, body); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("%s: got %v, want ErrMalformedPacket", name, err)
		}
	}
}

func TestDecodeConnackV5(t *testing.T) {
	t.Parallel()
	// ack flags 0, reason 0, no properties.
	p, err := DecodeConnack(V50, []byte{0x00, 0x00, 0x00})
	if err != nil {
		t.Fatalf("no-props: %v", err)
	}
	if p.SessionPresent || p.ReasonCode != Success || p.Properties != nil {
		t.Fatalf("no-props: got %+v", p)
	}

	// session present, reason 0, Session Expiry Interval = 10.
	p, err = DecodeConnack(V50, []byte{0x01, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x0A})
	if err != nil {
		t.Fatalf("with-props: %v", err)
	}
	if !p.SessionPresent || p.ReasonCode != Success {
		t.Fatalf("with-props flags: %+v", p)
	}
	if p.Properties == nil || p.Properties.SessionExpiryInterval == nil || *p.Properties.SessionExpiryInterval != 10 {
		t.Fatalf("with-props properties: %+v", p.Properties)
	}

	// error reason code, no properties.
	p, err = DecodeConnack(V50, []byte{0x00, 0x86, 0x00})
	if err != nil {
		t.Fatalf("error-reason: %v", err)
	}
	if p.ReasonCode != BadUserNameOrPassword || !p.ReasonCode.IsError() {
		t.Fatalf("error-reason: got %+v", p)
	}
}

func TestDecodeConnackV5Errors(t *testing.T) {
	t.Parallel()
	cases := map[string][]byte{
		"empty":             {},
		"one byte":          {0x00},
		"missing prop len":  {0x00, 0x00},
		"bad ack flag":      {0x02, 0x00, 0x00},
		"prop len overrun":  {0x00, 0x00, 0x05, 0x11},
		"trailing after ps": {0x00, 0x00, 0x00, 0xFF},
	}
	for name, body := range cases {
		if _, err := DecodeConnack(V50, body); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("%s: got %v, want ErrMalformedPacket", name, err)
		}
	}
}

func TestDecodeConnackBadVersion(t *testing.T) {
	t.Parallel()
	if _, err := DecodeConnack(Version(3), []byte{0x00, 0x00}); err == nil {
		t.Fatal("expected error for unsupported version")
	}
}
