// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestEncodePublishV3Golden checks a QoS 1 PUBLISH with DUP and RETAIN set
// against a hand-computed byte vector.
func TestEncodePublishV3Golden(t *testing.T) {
	t.Parallel()
	pkt := &PublishPacket{
		Version:  V311,
		Topic:    "a/b",
		Payload:  []byte("hi"),
		QoS:      1,
		Retain:   true,
		Dup:      true,
		PacketID: 10,
	}
	want := []byte{
		0x3B, 0x09, // PUBLISH, DUP|QoS1|RETAIN, remaining length 9
		0x00, 0x03, 'a', '/', 'b', // topic
		0x00, 0x0A, // packet id 10
		'h', 'i', // payload
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v3 PUBLISH bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

// TestEncodePublishV5PropsGolden checks a QoS 1 v5 PUBLISH carrying a
// content type, a subscription identifier and a topic alias against a
// hand-computed vector (properties emitted in ascending identifier order).
func TestEncodePublishV5PropsGolden(t *testing.T) {
	t.Parallel()
	alias := uint16(7)
	pkt := &PublishPacket{
		Version:  V50,
		Topic:    "t",
		Payload:  []byte("x"),
		QoS:      1,
		PacketID: 5,
		Properties: &Properties{
			ContentType:             "j",
			SubscriptionIdentifiers: []uint32{3},
			TopicAlias:              &alias,
		},
	}
	want := []byte{
		0x32, 0x10, // PUBLISH QoS1, remaining length 16
		0x00, 0x01, 't', // topic
		0x00, 0x05, // packet id 5
		0x09,                  // property length 9
		0x03, 0x00, 0x01, 'j', // Content Type "j"
		0x0B, 0x03, // Subscription Identifier 3
		0x23, 0x00, 0x07, // Topic Alias 7
		'x', // payload
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v5 PUBLISH bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

// roundTripPublish encodes in, reads the frame back and decodes it for the
// same version, returning the decoded packet.
func roundTripPublish(t *testing.T, in *PublishPacket) *PublishPacket {
	t.Helper()
	var buf bytes.Buffer
	if err := in.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	f, err := ReadFrame(&buf, 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if f.PacketType() != Publish {
		t.Fatalf("packet type = %v", f.PacketType())
	}
	if err := f.ValidateFlags(); err != nil {
		t.Fatalf("ValidateFlags: %v", err)
	}
	out, err := DecodePublish(in.Version, f.Header, f.Body)
	if err != nil {
		t.Fatalf("DecodePublish: %v", err)
	}
	return out
}

func TestPublishRoundTrip(t *testing.T) {
	t.Parallel()
	sei := uint32(30)
	alias := uint16(9)
	cases := []*PublishPacket{
		{Version: V311, Topic: "q/0", Payload: []byte("zero"), QoS: 0},
		{Version: V311, Topic: "q/1", Payload: []byte("one"), QoS: 1, PacketID: 1, Dup: true},
		{Version: V311, Topic: "q/2", Payload: nil, QoS: 2, PacketID: 2, Retain: true},
		{Version: V50, Topic: "q/0", Payload: []byte("z"), QoS: 0, Retain: true},
		{Version: V50, Topic: "q/1", Payload: []byte("o"), QoS: 1, PacketID: 3, Dup: true},
		{
			Version: V50, Topic: "q/2", Payload: []byte("props"), QoS: 2, PacketID: 4,
			Properties: &Properties{
				MessageExpiryInterval:   &sei,
				TopicAlias:              &alias,
				SubscriptionIdentifiers: []uint32{1, 2, 3},
				ContentType:             "application/json",
				UserProperties:          []UserProperty{{Key: "k", Value: "v"}},
			},
		},
	}
	for _, in := range cases {
		out := roundTripPublish(t, in)
		if out.Topic != in.Topic || out.QoS != in.QoS || out.PacketID != in.PacketID {
			t.Fatalf("%s: header mismatch got %+v", in.Topic, out)
		}
		if out.Dup != in.Dup || out.Retain != in.Retain {
			t.Fatalf("%s: DUP/RETAIN mismatch got dup=%v retain=%v", in.Topic, out.Dup, out.Retain)
		}
		if !bytes.Equal(out.Payload, in.Payload) {
			t.Fatalf("%s: payload got %x want %x", in.Topic, out.Payload, in.Payload)
		}
	}
}

func TestPublishRoundTripProperties(t *testing.T) {
	t.Parallel()
	sei := uint32(30)
	alias := uint16(9)
	in := &PublishPacket{
		Version: V50, Topic: "t", Payload: []byte("p"), QoS: 1, PacketID: 4,
		Properties: &Properties{
			MessageExpiryInterval:   &sei,
			TopicAlias:              &alias,
			SubscriptionIdentifiers: []uint32{1, 2, 3},
			ContentType:             "application/json",
		},
	}
	out := roundTripPublish(t, in)
	p := out.Properties
	if p == nil {
		t.Fatal("properties lost")
	}
	if p.MessageExpiryInterval == nil || *p.MessageExpiryInterval != 30 {
		t.Fatalf("message expiry: %+v", p.MessageExpiryInterval)
	}
	if p.TopicAlias == nil || *p.TopicAlias != 9 {
		t.Fatalf("topic alias: %+v", p.TopicAlias)
	}
	if len(p.SubscriptionIdentifiers) != 3 || p.SubscriptionIdentifiers[2] != 3 {
		t.Fatalf("subscription identifiers: %v", p.SubscriptionIdentifiers)
	}
	if p.ContentType != "application/json" {
		t.Fatalf("content type: %q", p.ContentType)
	}
}

func TestEncodePublishErrors(t *testing.T) {
	t.Parallel()
	cases := map[string]*PublishPacket{
		"qos 3":            {Version: V311, Topic: "t", QoS: 3},
		"qos1 zero id":     {Version: V311, Topic: "t", QoS: 1},
		"qos2 zero id":     {Version: V50, Topic: "t", QoS: 2},
		"illegal property": {Version: V50, Topic: "t", Properties: &Properties{AssignedClientID: "x"}},
	}
	for name, pkt := range cases {
		if err := pkt.Encode(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("%s: got %v, want ErrProtocolViolation", name, err)
		}
	}
}

// TestEncodePublishTopicTooLong exercises the topic-length guard PUBLISH
// shares with every other appendString call site.
func TestEncodePublishTopicTooLong(t *testing.T) {
	t.Parallel()
	pkt := &PublishPacket{Version: V311, Topic: strings.Repeat("x", maxStringLen+1)}
	if err := pkt.Encode(&bytes.Buffer{}); !errors.Is(err, ErrStringTooLong) {
		t.Fatalf("got %v, want ErrStringTooLong", err)
	}
}

func TestDecodePublishMalformed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		v      Version
		header byte
		body   []byte
	}{
		{"qos 3", V311, 0x36, []byte{0x00, 0x01, 't'}},
		{"truncated topic len", V311, 0x30, []byte{0x00}},
		{"truncated topic", V311, 0x30, []byte{0x00, 0x05, 't'}},
		{"missing packet id", V311, 0x32, []byte{0x00, 0x01, 't'}},
		{"v5 prop overrun", V50, 0x30, []byte{0x00, 0x01, 't', 0x05, 0x01}},
		{"v5 bad property", V50, 0x30, []byte{0x00, 0x01, 't', 0x02, 0x12, 0x00}},
	}
	for _, tc := range cases {
		if _, err := DecodePublish(tc.v, tc.header, tc.body); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("%s: got %v, want ErrMalformedPacket", tc.name, err)
		}
	}
}

// roundTripAck encodes in, reads the frame back and decodes it, returning
// the decoded packet.
func roundTripAck(t *testing.T, in *AckPacket) *AckPacket {
	t.Helper()
	var buf bytes.Buffer
	if err := in.EncodeAck(&buf); err != nil {
		t.Fatalf("EncodeAck: %v", err)
	}
	f, err := ReadFrame(&buf, 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if err := f.ValidateFlags(); err != nil {
		t.Fatalf("ValidateFlags: %v", err)
	}
	out, err := DecodeAck(in.Version, in.Type, f.Body)
	if err != nil {
		t.Fatalf("DecodeAck: %v", err)
	}
	return out
}

func TestAckRoundTrip(t *testing.T) {
	t.Parallel()
	for _, typ := range []PacketType{Puback, Pubrec, Pubrel, Pubcomp} {
		forms := []*AckPacket{
			{Version: V311, Type: typ, PacketID: 42},
			{Version: V50, Type: typ, PacketID: 42},                            // 2-byte short form
			{Version: V50, Type: typ, PacketID: 42, ReasonCode: QuotaExceeded}, // 3-byte form
			{Version: V50, Type: typ, PacketID: 42, ReasonCode: NoMatchingSubscribers, Properties: &Properties{ReasonString: "why"}}, // full
		}
		for i, in := range forms {
			out := roundTripAck(t, in)
			if out.PacketID != in.PacketID {
				t.Fatalf("%s form %d: packet id %d", typ, i, out.PacketID)
			}
			if out.ReasonCode != in.ReasonCode {
				t.Fatalf("%s form %d: reason %v want %v", typ, i, out.ReasonCode, in.ReasonCode)
			}
			if in.Properties != nil {
				if out.Properties == nil || out.Properties.ReasonString != in.Properties.ReasonString {
					t.Fatalf("%s form %d: reason string lost", typ, i)
				}
			}
		}
	}
}

// TestEncodeAckPubrelFlag verifies PUBREL carries fixed-header flags 0x02
// while the other three acknowledgement packets carry none.
func TestEncodeAckPubrelFlag(t *testing.T) {
	t.Parallel()
	cases := map[PacketType]byte{
		Puback:  0x40,
		Pubrec:  0x50,
		Pubrel:  0x62,
		Pubcomp: 0x70,
	}
	for typ, wantHeader := range cases {
		var buf bytes.Buffer
		pkt := &AckPacket{Version: V311, Type: typ, PacketID: 1}
		if err := pkt.EncodeAck(&buf); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		if got := buf.Bytes()[0]; got != wantHeader {
			t.Fatalf("%s: header %#02x want %#02x", typ, got, wantHeader)
		}
	}
}

// TestEncodeAckV5ShortForm verifies a v5 acknowledgement with reason 0 and
// no properties encodes as a two-byte body (the spec short form).
func TestEncodeAckV5ShortForm(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	pkt := &AckPacket{Version: V50, Type: Puback, PacketID: 258}
	if err := pkt.EncodeAck(&buf); err != nil {
		t.Fatalf("EncodeAck: %v", err)
	}
	want := []byte{0x40, 0x02, 0x01, 0x02}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("short form\n got %x\nwant %x", buf.Bytes(), want)
	}
}

func TestEncodeAckBadType(t *testing.T) {
	t.Parallel()
	pkt := &AckPacket{Version: V50, Type: Publish, PacketID: 1}
	if err := pkt.EncodeAck(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

// TestEncodeAckIllegalProperty rejects a property illegal for the
// acknowledgement's target (Assigned Client Identifier is CONNACK-only).
func TestEncodeAckIllegalProperty(t *testing.T) {
	t.Parallel()
	pkt := &AckPacket{Version: V50, Type: Puback, PacketID: 1, Properties: &Properties{AssignedClientID: "x"}}
	if err := pkt.EncodeAck(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestDecodeAckBadVersion(t *testing.T) {
	t.Parallel()
	if _, err := DecodeAck(Version(3), Puback, []byte{0x00, 0x01}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestDecodeAckMalformed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    Version
		t    PacketType
		body []byte
	}{
		{"empty", V311, Puback, []byte{}},
		{"one byte", V311, Puback, []byte{0x00}},
		{"v3 trailing", V311, Puback, []byte{0x00, 0x01, 0x00}},
		{"v5 prop overrun", V50, Pubrec, []byte{0x00, 0x01, 0x10, 0x05, 0x1F}},
		{"v5 trailing", V50, Pubcomp, []byte{0x00, 0x01, 0x00, 0x00, 0xFF}},
		{"not an ack", V50, Publish, []byte{0x00, 0x01}},
	}
	for _, tc := range cases {
		if _, err := DecodeAck(tc.v, tc.t, tc.body); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("%s: got %v, want ErrMalformedPacket", tc.name, err)
		}
	}
}
