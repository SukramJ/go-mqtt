// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Self-test for mockBroker (test_mock_broker.go): it drives the broker
// directly over a raw TCP connection, playing the client role by hand
// with the protocol package's own encoders/decoders, so it exercises
// every scripting knob without depending on TCPClient (not written yet).
// Higher-level lifecycle/adapter tests build on top of this same double.

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// mockTestClient is a hand-rolled MQTT peer used only to poke at
// mockBroker from the other end of the wire.
type mockTestClient struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func dialMock(t *testing.T, b *mockBroker) *mockTestClient {
	t.Helper()
	addr := b.listener.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &mockTestClient{t: t, conn: conn, br: bufio.NewReader(conn)}
}

func (c *mockTestClient) connect(v protocol.Version, clientID string, clean bool) *protocol.ConnackPacket {
	c.t.Helper()
	pkt := &protocol.ConnectPacket{Version: v, ClientID: clientID, KeepAlive: 30, CleanStart: clean}
	if err := pkt.Encode(c.conn); err != nil {
		c.t.Fatalf("encode CONNECT: %v", err)
	}
	frame := c.readFrame()
	if frame.PacketType() != protocol.Connack {
		c.t.Fatalf("expected CONNACK, got %s", frame.PacketType())
	}
	ack, err := protocol.DecodeConnack(v, frame.Body)
	if err != nil {
		c.t.Fatalf("decode CONNACK: %v", err)
	}
	return ack
}

func (c *mockTestClient) readFrame() protocol.Frame {
	c.t.Helper()
	frame, err := protocol.ReadFrame(c.br, mockMaxRemainingLength)
	if err != nil {
		c.t.Fatalf("read frame: %v", err)
	}
	return frame
}

// readFrameTimeout reads one frame within d, reporting ok=false on
// timeout — used to assert a reply was *not* sent (dropped ack/ping).
func (c *mockTestClient) readFrameTimeout(d time.Duration) (protocol.Frame, bool) {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	frame, err := protocol.ReadFrame(c.br, mockMaxRemainingLength)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return protocol.Frame{}, false
		}
		c.t.Fatalf("read frame: %v", err)
	}
	return frame, true
}

func TestMockBrokerConnectV50WithProperties(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	b.SetSessionPresent(true)
	rm := uint16(50)
	b.SetConnackProperties(&protocol.Properties{ReceiveMaximum: &rm, AssignedClientID: "srv-assigned"})

	c := dialMock(t, b)
	ack := c.connect(protocol.V50, "cid-1", true)

	if !ack.SessionPresent {
		t.Fatal("expected SessionPresent")
	}
	if ack.ReasonCode != protocol.Success {
		t.Fatalf("reason = %v", ack.ReasonCode)
	}
	if ack.Properties == nil || ack.Properties.ReceiveMaximum == nil || *ack.Properties.ReceiveMaximum != 50 {
		t.Fatalf("properties = %+v", ack.Properties)
	}
	if ack.Properties.AssignedClientID != "srv-assigned" {
		t.Fatalf("assigned client id = %q", ack.Properties.AssignedClientID)
	}

	if got := b.ProtocolVersion(); got != protocol.V50 {
		t.Fatalf("ProtocolVersion = %v", got)
	}
	if !b.CleanStart() {
		t.Fatal("CleanStart = false, want true")
	}
	if b.ConnCount() != 1 {
		t.Fatalf("ConnCount = %d", b.ConnCount())
	}
}

func TestMockBrokerConnectV311RejectNext(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	b.RejectNextConnect(0x05)

	c := dialMock(t, b)
	ack := c.connect(protocol.V311, "cid-2", true)
	if ack.ReasonCode != protocol.NotAuthorized {
		t.Fatalf("reason = %v, want NotAuthorized (mapped from v3 code 5)", ack.ReasonCode)
	}

	// Rejection is one-shot: a second connection is accepted normally.
	c2 := dialMock(t, b)
	ack2 := c2.connect(protocol.V311, "cid-3", true)
	if ack2.ReasonCode != protocol.Success {
		t.Fatalf("second connect reason = %v, want Success", ack2.ReasonCode)
	}
}

func TestMockBrokerSubscribeUnsubscribe(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := dialMock(t, b)
	c.connect(protocol.V50, "cid-sub", true)

	sub := &protocol.SubscribePacket{
		PacketID: 7,
		Version:  protocol.V50,
		Subscriptions: []protocol.Subscription{
			{Filter: "a/b", Options: protocol.SubscribeOptions{QoS: 1}},
			{Filter: "c/d", Options: protocol.SubscribeOptions{QoS: 2, NoLocal: true}},
		},
	}
	if err := sub.Encode(c.conn); err != nil {
		t.Fatalf("encode SUBSCRIBE: %v", err)
	}
	frame := c.readFrame()
	suback, err := protocol.DecodeSuback(protocol.V50, frame.Body)
	if err != nil {
		t.Fatalf("decode SUBACK: %v", err)
	}
	if len(suback.ReasonCodes) != 2 || suback.ReasonCodes[0] != 1 || suback.ReasonCodes[1] != 2 {
		t.Fatalf("reason codes = %v, want echoed QoS [1 2]", suback.ReasonCodes)
	}
	if b.SubscribeCount() != 1 {
		t.Fatalf("SubscribeCount = %d", b.SubscribeCount())
	}
	recorded := b.Subscriptions()
	if len(recorded) != 2 || recorded[0].Filter != "a/b" || recorded[1].Filter != "c/d" || !recorded[1].Options.NoLocal {
		t.Fatalf("Subscriptions = %+v", recorded)
	}

	// GrantQoS overrides the echoed QoS for subsequent SUBACKs.
	b.GrantQoS(2)
	sub.PacketID = 8
	sub.Subscriptions = []protocol.Subscription{{Filter: "e/f", Options: protocol.SubscribeOptions{QoS: 0}}}
	_ = sub.Encode(c.conn)
	frame = c.readFrame()
	suback, _ = protocol.DecodeSuback(protocol.V50, frame.Body)
	if suback.ReasonCodes[0] != 2 {
		t.Fatalf("granted QoS = %v, want overridden to 2", suback.ReasonCodes[0])
	}

	// RejectSubscribe overrides with a failure code.
	b.RejectSubscribe(0x87)
	sub.PacketID = 9
	_ = sub.Encode(c.conn)
	frame = c.readFrame()
	suback, _ = protocol.DecodeSuback(protocol.V50, frame.Body)
	if suback.ReasonCodes[0] != 0x87 {
		t.Fatalf("rejected reason = %#x, want 0x87", suback.ReasonCodes[0])
	}

	unsub := &protocol.UnsubscribePacket{PacketID: 10, Version: protocol.V50, Filters: []string{"a/b", "c/d"}}
	if err := unsub.Encode(c.conn); err != nil {
		t.Fatalf("encode UNSUBSCRIBE: %v", err)
	}
	frame = c.readFrame()
	unsuback, err := protocol.DecodeUnsuback(protocol.V50, frame.Body)
	if err != nil {
		t.Fatalf("decode UNSUBACK: %v", err)
	}
	if len(unsuback.ReasonCodes) != 2 || unsuback.ReasonCodes[0] != protocol.Success {
		t.Fatalf("UNSUBACK reason codes = %v", unsuback.ReasonCodes)
	}
}

func TestMockBrokerInboundPublishQoS0(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := dialMock(t, b)
	c.connect(protocol.V50, "cid-pub0", true)

	pkt := &protocol.PublishPacket{Version: protocol.V50, Topic: "t/0", Payload: []byte("hi"), QoS: 0}
	if err := pkt.Encode(c.conn); err != nil {
		t.Fatalf("encode PUBLISH: %v", err)
	}
	if _, ok := c.readFrameTimeout(200 * time.Millisecond); ok {
		t.Fatal("QoS 0 PUBLISH must not be acked")
	}
	got := b.Published()
	if len(got) != 1 || got[0].Topic != "t/0" || string(got[0].Payload) != "hi" {
		t.Fatalf("Published = %+v", got)
	}
}

func TestMockBrokerInboundPublishQoS1DropAndDuplicate(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := dialMock(t, b)
	c.connect(protocol.V50, "cid-pub1", true)

	publish := func(id uint16) {
		pkt := &protocol.PublishPacket{Version: protocol.V50, Topic: "t/1", Payload: []byte("x"), QoS: 1, PacketID: id}
		if err := pkt.Encode(c.conn); err != nil {
			t.Fatalf("encode PUBLISH: %v", err)
		}
	}

	b.DropNextPuback(1)
	publish(1)
	if _, ok := c.readFrameTimeout(200 * time.Millisecond); ok {
		t.Fatal("PUBACK should have been dropped")
	}

	b.DuplicateNextPuback()
	publish(2)
	frame1 := c.readFrame()
	frame2 := c.readFrame()
	if frame1.PacketType() != protocol.Puback || frame2.PacketType() != protocol.Puback {
		t.Fatalf("want two PUBACKs, got %s and %s", frame1.PacketType(), frame2.PacketType())
	}
}

func TestMockBrokerInboundPublishQoS2DropRecAndComp(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := dialMock(t, b)
	c.connect(protocol.V50, "cid-pub2", true)

	publish := func(id uint16) {
		pkt := &protocol.PublishPacket{Version: protocol.V50, Topic: "t/2", Payload: []byte("x"), QoS: 2, PacketID: id}
		if err := pkt.Encode(c.conn); err != nil {
			t.Fatalf("encode PUBLISH: %v", err)
		}
	}
	pubrel := func(id uint16) {
		rel := &protocol.AckPacket{Version: protocol.V50, Type: protocol.Pubrel, PacketID: id}
		if err := rel.EncodeAck(c.conn); err != nil {
			t.Fatalf("encode PUBREL: %v", err)
		}
	}

	b.DropNextPubrec(1)
	publish(1)
	if _, ok := c.readFrameTimeout(200 * time.Millisecond); ok {
		t.Fatal("PUBREC should have been dropped")
	}

	publish(2)
	frame := c.readFrame()
	if frame.PacketType() != protocol.Pubrec {
		t.Fatalf("expected PUBREC, got %s", frame.PacketType())
	}

	b.DropNextPubcomp(1)
	pubrel(2)
	if _, ok := c.readFrameTimeout(200 * time.Millisecond); ok {
		t.Fatal("PUBCOMP should have been dropped")
	}
	pubrel(2)
	frame = c.readFrame()
	if frame.PacketType() != protocol.Pubcomp {
		t.Fatalf("expected PUBCOMP, got %s", frame.PacketType())
	}
}

func TestMockBrokerInjectPublishQoS1(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := dialMock(t, b)
	c.connect(protocol.V50, "cid-inj1", true)

	done := make(chan error, 1)
	go func() { done <- b.InjectPublish("cmd/x", []byte("payload"), 1, false, nil) }()

	frame := c.readFrame()
	pub, err := protocol.DecodePublish(protocol.V50, frame.Header, frame.Body)
	if err != nil {
		t.Fatalf("decode injected PUBLISH: %v", err)
	}
	if pub.Topic != "cmd/x" || string(pub.Payload) != "payload" || pub.QoS != 1 {
		t.Fatalf("injected PUBLISH = %+v", pub)
	}
	ack := &protocol.AckPacket{Version: protocol.V50, Type: protocol.Puback, PacketID: pub.PacketID}
	if err := ack.EncodeAck(c.conn); err != nil {
		t.Fatalf("encode PUBACK: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}
}

func TestMockBrokerInjectPublishQoS2(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := dialMock(t, b)
	c.connect(protocol.V50, "cid-inj2", true)

	done := make(chan error, 1)
	go func() { done <- b.InjectPublish("cmd/y", []byte("p2"), 2, true, nil) }()

	frame := c.readFrame()
	pub, err := protocol.DecodePublish(protocol.V50, frame.Header, frame.Body)
	if err != nil {
		t.Fatalf("decode injected PUBLISH: %v", err)
	}
	if !pub.Retain || pub.QoS != 2 {
		t.Fatalf("injected PUBLISH = %+v", pub)
	}

	rec := &protocol.AckPacket{Version: protocol.V50, Type: protocol.Pubrec, PacketID: pub.PacketID}
	if err := rec.EncodeAck(c.conn); err != nil {
		t.Fatalf("encode PUBREC: %v", err)
	}

	frame = c.readFrame()
	if frame.PacketType() != protocol.Pubrel {
		t.Fatalf("expected broker PUBREL, got %s", frame.PacketType())
	}

	comp := &protocol.AckPacket{Version: protocol.V50, Type: protocol.Pubcomp, PacketID: pub.PacketID}
	if err := comp.EncodeAck(c.conn); err != nil {
		t.Fatalf("encode PUBCOMP: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}
}

func TestMockBrokerInjectDisconnectAuthRawFrame(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := dialMock(t, b)
	c.connect(protocol.V50, "cid-inj3", true)

	if err := b.InjectAuth(); err != nil {
		t.Fatalf("InjectAuth: %v", err)
	}
	frame := c.readFrame()
	if frame.PacketType() != protocol.Auth {
		t.Fatalf("expected AUTH, got %s", frame.PacketType())
	}
	authPkt, err := protocol.DecodeAuth(frame.Body)
	if err != nil {
		t.Fatalf("decode AUTH: %v", err)
	}
	if authPkt.ReasonCode != protocol.ContinueAuthentication {
		t.Fatalf("AUTH reason = %v", authPkt.ReasonCode)
	}

	raw := []byte{0xFF, 0x01, 0x02}
	if err := b.InjectRawFrame(raw); err != nil {
		t.Fatalf("InjectRawFrame: %v", err)
	}
	got := make([]byte, len(raw))
	if _, err := io.ReadFull(c.br, got); err != nil {
		t.Fatalf("read raw frame: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("raw frame = %v, want %v", got, raw)
	}

	if err := b.InjectDisconnect(protocol.ServerShuttingDown); err != nil {
		t.Fatalf("InjectDisconnect: %v", err)
	}
	frame = c.readFrame()
	if frame.PacketType() != protocol.Disconnect {
		t.Fatalf("expected DISCONNECT, got %s", frame.PacketType())
	}
	discPkt, err := protocol.DecodeDisconnect(protocol.V50, frame.Body)
	if err != nil {
		t.Fatalf("decode DISCONNECT: %v", err)
	}
	if discPkt.ReasonCode != protocol.ServerShuttingDown {
		t.Fatalf("DISCONNECT reason = %v", discPkt.ReasonCode)
	}
}

func TestMockBrokerPingAndReset(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := dialMock(t, b)
	c.connect(protocol.V50, "cid-ping", true)

	ping := func() {
		if err := protocol.EncodePingReq(c.conn); err != nil {
			t.Fatalf("encode PINGREQ: %v", err)
		}
	}

	ping()
	frame := c.readFrame()
	if frame.PacketType() != protocol.Pingresp {
		t.Fatalf("expected PINGRESP, got %s", frame.PacketType())
	}

	b.DropNextPings(1)
	ping()
	if _, ok := c.readFrameTimeout(200 * time.Millisecond); ok {
		t.Fatal("PINGRESP should have been dropped")
	}
	ping()
	frame = c.readFrame()
	if frame.PacketType() != protocol.Pingresp {
		t.Fatalf("expected PINGRESP after the dropped one, got %s", frame.PacketType())
	}

	if got := b.PingCount(); got != 3 {
		t.Fatalf("PingCount = %d, want 3", got)
	}

	b.InjectTCPReset()
	if _, err := c.br.ReadByte(); err == nil {
		t.Fatal("expected the reset connection to be closed")
	}
}
