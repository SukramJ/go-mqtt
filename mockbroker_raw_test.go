// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Malformed/unexpected-frame resilience in pump.go's ack handlers: a
// SUBACK/UNSUBACK/PUBLISH the codec cannot decode, and a SUBACK/UNSUBACK
// for a packet identifier nobody is waiting on. All are warn-logged and
// ignored — the connection must stay up. Frames are hand-assembled with
// mockBroker's own (unexported, same-package) wire helpers so no
// dependency on a second broker double is needed.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// assertStillConnected gives the read loop a moment to process the last
// injected frame, then confirms it did not tear the connection down.
func assertStillConnected(t *testing.T, c *TCPClient) {
	t.Helper()
	time.Sleep(150 * time.Millisecond)
	if !c.IsConnected() {
		t.Fatal("client dropped the connection reacting to the injected frame")
	}
}

func TestUnknownSubackIDLogsWarnAndIgnores(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "suback-unknown"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	body := b.buildSuback(protocol.V50, 4242, []protocol.Subscription{{Filter: "x", Options: protocol.SubscribeOptions{QoS: 1}}})
	var buf bytes.Buffer
	if err := writeMockFrame(&buf, byte(protocol.Suback)<<4, body); err != nil {
		t.Fatalf("writeMockFrame: %v", err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("InjectRawFrame: %v", err)
	}
	assertStillConnected(t, c)
}

func TestUnknownUnsubackIDLogsWarnAndIgnores(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "unsuback-unknown"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	body := buildMockUnsuback(protocol.V50, 4242, 1)
	var buf bytes.Buffer
	if err := writeMockFrame(&buf, byte(protocol.Unsuback)<<4, body); err != nil {
		t.Fatalf("writeMockFrame: %v", err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("InjectRawFrame: %v", err)
	}
	assertStillConnected(t, c)
}

func TestMalformedSubackLogsWarnAndIgnores(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "suback-malformed"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	var buf bytes.Buffer
	if err := writeMockFrame(&buf, byte(protocol.Suback)<<4, nil); err != nil { // too short: no packet id
		t.Fatalf("writeMockFrame: %v", err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("InjectRawFrame: %v", err)
	}
	assertStillConnected(t, c)
}

func TestMalformedUnsubackLogsWarnAndIgnores(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "unsuback-malformed"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	var buf bytes.Buffer
	if err := writeMockFrame(&buf, byte(protocol.Unsuback)<<4, nil); err != nil {
		t.Fatalf("writeMockFrame: %v", err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("InjectRawFrame: %v", err)
	}
	assertStillConnected(t, c)
}

// TestMalformedPublishTearsDownConnection proves an inbound PUBLISH whose
// body the codec cannot decode (a topic length prefix that overruns the
// remaining bytes) is treated as a fatal Malformed Packet (§4.13.1): the
// client tears the connection down (signalling ConnectionLost) rather than
// logging and reading on — which for a QoS 1/2 malformed PUBLISH would
// livelock on the broker's unbounded retransmits, since the packet id was
// never decoded and so no PUBACK/PUBREC is ever sent.
func TestMalformedPublishTearsDownConnection(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "publish-malformed"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	// QoS 1 PUBLISH flags (0x02); body claims a 5-byte topic but supplies
	// only 1.
	body := []byte{0x00, 0x05, 'a'}
	var buf bytes.Buffer
	if err := writeMockFrame(&buf, byte(protocol.Publish)<<4|0x02, body); err != nil {
		t.Fatalf("writeMockFrame: %v", err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("InjectRawFrame: %v", err)
	}

	select {
	case <-c.ConnectionLost():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not tear down the connection for a malformed PUBLISH")
	}
	if !lcPoll(time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("client still reports connected after a malformed PUBLISH")
	}
}
