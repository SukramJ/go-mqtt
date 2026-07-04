// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestSubscribeOptionsByte(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    Version
		o    SubscribeOptions
		want byte
	}{
		{"v3 qos0", V311, SubscribeOptions{QoS: 0}, 0x00},
		{"v3 qos1", V311, SubscribeOptions{QoS: 1}, 0x01},
		{"v3 qos2 ignores extras", V311, SubscribeOptions{QoS: 2, NoLocal: true, RetainHandling: 2}, 0x02},
		{"v5 nolocal", V50, SubscribeOptions{QoS: 1, NoLocal: true}, 0x05},
		{"v5 retain as published", V50, SubscribeOptions{QoS: 0, RetainAsPublished: true}, 0x08},
		{"v5 retain handling 1", V50, SubscribeOptions{QoS: 2, RetainHandling: 1}, 0x12},
		{"v5 retain handling 2", V50, SubscribeOptions{QoS: 0, RetainHandling: 2}, 0x20},
		{"v5 all", V50, SubscribeOptions{QoS: 1, NoLocal: true, RetainAsPublished: true, RetainHandling: 2}, 0x2D},
	}
	for _, tc := range cases {
		got, err := subscribeOptionsByte(tc.v, tc.o)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: got %#02x want %#02x", tc.name, got, tc.want)
		}
	}
}

func TestSubscribeOptionsByteErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    Version
		o    SubscribeOptions
	}{
		{"v3 qos3", V311, SubscribeOptions{QoS: 3}},
		{"v5 qos3", V50, SubscribeOptions{QoS: 3}},
		{"v5 retain handling 3", V50, SubscribeOptions{QoS: 0, RetainHandling: 3}},
	}
	for _, tc := range cases {
		if _, err := subscribeOptionsByte(tc.v, tc.o); !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("%s: got %v, want ErrProtocolViolation", tc.name, err)
		}
	}
}

func TestEncodeSubscribeV3Golden(t *testing.T) {
	t.Parallel()
	pkt := &SubscribePacket{
		Version:       V311,
		PacketID:      1,
		Subscriptions: []Subscription{{Filter: "a", Options: SubscribeOptions{QoS: 1}}},
	}
	want := []byte{
		0x82, 0x06, // SUBSCRIBE (flags 0x02), remaining length 6
		0x00, 0x01, // packet id 1
		0x00, 0x01, 'a', // filter
		0x01, // options QoS1
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v3 SUBSCRIBE bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

func TestEncodeSubscribeV5Golden(t *testing.T) {
	t.Parallel()
	pkt := &SubscribePacket{
		Version:       V50,
		PacketID:      1,
		Subscriptions: []Subscription{{Filter: "a", Options: SubscribeOptions{QoS: 1, NoLocal: true}}},
	}
	want := []byte{
		0x82, 0x07, // SUBSCRIBE (flags 0x02), remaining length 7
		0x00, 0x01, // packet id 1
		0x00,            // property length 0
		0x00, 0x01, 'a', // filter
		0x05, // options QoS1|NoLocal
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v5 SUBSCRIBE bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

func TestEncodeSubscribeEmpty(t *testing.T) {
	t.Parallel()
	pkt := &SubscribePacket{Version: V50, PacketID: 1}
	if err := pkt.Encode(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestEncodeUnsubscribeV3Golden(t *testing.T) {
	t.Parallel()
	pkt := &UnsubscribePacket{Version: V311, PacketID: 2, Filters: []string{"a", "b"}}
	want := []byte{
		0xA2, 0x08, // UNSUBSCRIBE (flags 0x02), remaining length 8
		0x00, 0x02, // packet id 2
		0x00, 0x01, 'a', // filter a
		0x00, 0x01, 'b', // filter b
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v3 UNSUBSCRIBE bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

func TestEncodeUnsubscribeV5Golden(t *testing.T) {
	t.Parallel()
	pkt := &UnsubscribePacket{Version: V50, PacketID: 2, Filters: []string{"a"}}
	want := []byte{
		0xA2, 0x06, // UNSUBSCRIBE (flags 0x02), remaining length 6
		0x00, 0x02, // packet id 2
		0x00,            // property length 0
		0x00, 0x01, 'a', // filter a
	}
	var buf bytes.Buffer
	if err := pkt.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("v5 UNSUBSCRIBE bytes\n got %x\nwant %x", buf.Bytes(), want)
	}
}

func TestEncodeUnsubscribeEmpty(t *testing.T) {
	t.Parallel()
	pkt := &UnsubscribePacket{Version: V50, PacketID: 1}
	if err := pkt.Encode(&bytes.Buffer{}); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

func TestDecodeSuback(t *testing.T) {
	t.Parallel()
	// v3: granted QoS 0/1 and a failure.
	p, err := DecodeSuback(V311, []byte{0x00, 0x0A, 0x00, 0x01, 0x80})
	if err != nil {
		t.Fatalf("v3: %v", err)
	}
	if p.PacketID != 10 || p.Properties != nil {
		t.Fatalf("v3 header: %+v", p)
	}
	wantCodes := []ReasonCode{GrantedQoS0, GrantedQoS1, UnspecifiedError}
	if len(p.ReasonCodes) != len(wantCodes) {
		t.Fatalf("v3 codes: %v", p.ReasonCodes)
	}
	for i, c := range wantCodes {
		if p.ReasonCodes[i] != c {
			t.Fatalf("v3 code %d: got %v want %v", i, p.ReasonCodes[i], c)
		}
	}

	// v5 with no properties.
	p, err = DecodeSuback(V50, []byte{0x00, 0x0A, 0x00, 0x02})
	if err != nil {
		t.Fatalf("v5: %v", err)
	}
	if p.PacketID != 10 || p.Properties != nil || len(p.ReasonCodes) != 1 || p.ReasonCodes[0] != GrantedQoS2 {
		t.Fatalf("v5: %+v", p)
	}

	// v5 with a Reason String property.
	p, err = DecodeSuback(V50, []byte{0x00, 0x0A, 0x04, 0x1F, 0x00, 0x01, 'x', 0x00})
	if err != nil {
		t.Fatalf("v5 props: %v", err)
	}
	if p.Properties == nil || p.Properties.ReasonString != "x" {
		t.Fatalf("v5 props: %+v", p.Properties)
	}
	if len(p.ReasonCodes) != 1 || p.ReasonCodes[0] != GrantedQoS0 {
		t.Fatalf("v5 props codes: %v", p.ReasonCodes)
	}
}

func TestDecodeSubackMalformed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    Version
		body []byte
	}{
		{"v3 empty", V311, []byte{}},
		{"v3 truncated id", V311, []byte{0x00}},
		{"v3 no codes", V311, []byte{0x00, 0x01}},
		{"v5 empty", V50, []byte{}},
		{"v5 missing prop len", V50, []byte{0x00, 0x01}},
		{"v5 no codes", V50, []byte{0x00, 0x01, 0x00}},
		{"v5 prop overrun", V50, []byte{0x00, 0x01, 0x05, 0x1F}},
	}
	for _, tc := range cases {
		if _, err := DecodeSuback(tc.v, tc.body); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("%s: got %v, want ErrMalformedPacket", tc.name, err)
		}
	}
}

func TestDecodeUnsuback(t *testing.T) {
	t.Parallel()
	// v3: no payload.
	p, err := DecodeUnsuback(V311, []byte{0x00, 0x07})
	if err != nil {
		t.Fatalf("v3: %v", err)
	}
	if p.PacketID != 7 || len(p.ReasonCodes) != 0 || p.Properties != nil {
		t.Fatalf("v3: %+v", p)
	}

	// v5: one reason code per filter.
	p, err = DecodeUnsuback(V50, []byte{0x00, 0x07, 0x00, 0x11})
	if err != nil {
		t.Fatalf("v5: %v", err)
	}
	if p.PacketID != 7 || len(p.ReasonCodes) != 1 || p.ReasonCodes[0] != NoSubscriptionExisted {
		t.Fatalf("v5: %+v", p)
	}
}

func TestDecodeUnsubackMalformed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    Version
		body []byte
	}{
		{"v3 truncated id", V311, []byte{0x00}},
		{"v3 trailing", V311, []byte{0x00, 0x07, 0x00}},
		{"v5 empty", V50, []byte{}},
		{"v5 missing prop len", V50, []byte{0x00, 0x07}},
		{"v5 no codes", V50, []byte{0x00, 0x07, 0x00}},
		{"v5 prop overrun", V50, []byte{0x00, 0x07, 0x05, 0x1F}},
	}
	for _, tc := range cases {
		if _, err := DecodeUnsuback(tc.v, tc.body); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("%s: got %v, want ErrMalformedPacket", tc.name, err)
		}
	}
}
