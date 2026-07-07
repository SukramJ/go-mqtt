// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Malformed/unexpected-frame handling in pump.go's read loop. Two regimes,
// per MQTT 5.0 §4.13.1:
//
//   - A frame the codec cannot decode (malformed PUBLISH, PUBACK, PUBREL,
//     SUBACK, UNSUBACK, or invalid fixed-header flags) is FATAL: the client
//     closes the connection (DISCONNECT 0x81 on v5 only) instead of reading
//     on against a confused peer — swallowing it would leak the in-flight
//     exchange's stored entry, packet identifier and send-quota permit.
//   - A well-formed acknowledgement for a packet identifier nobody is
//     waiting on is warn-logged and ignored — the connection stays up
//     (legitimately reachable via resumed-session and timing races).
//
// Frames are hand-assembled with mockBroker's own (unexported,
// same-package) wire helpers so no dependency on a second broker double is
// needed.

import (
	"bytes"
	"context"
	"errors"
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

// assertTearsDown injects raw frame bytes and asserts the client detects a
// lost connection and drops the link.
func assertTearsDown(t *testing.T, b *mockBroker, c *TCPClient, header byte, body []byte, what string) {
	t.Helper()
	var buf bytes.Buffer
	if err := writeMockFrame(&buf, header, body); err != nil {
		t.Fatalf("writeMockFrame: %v", err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("InjectRawFrame: %v", err)
	}
	select {
	case <-c.ConnectionLost():
	case <-time.After(2 * time.Second):
		t.Fatalf("client did not tear down the connection for %s", what)
	}
	if !lcPoll(time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatalf("client still reports connected after %s", what)
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

// TestMalformedSubackTearsDownConnection: a SUBACK the codec cannot decode
// (no packet identifier) is a fatal Malformed Packet — the in-flight
// Subscribe would otherwise idle out its full AckTimeout against a peer
// already known to be confused. (Inverted from the pre-hardening
// warn-and-ignore behavior.)
func TestMalformedSubackTearsDownConnection(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "suback-malformed"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	assertTearsDown(t, b, c, byte(protocol.Suback)<<4, nil, "a malformed SUBACK")
}

// TestMalformedUnsubackTearsDownConnection mirrors the SUBACK case for
// UNSUBACK.
func TestMalformedUnsubackTearsDownConnection(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "unsuback-malformed"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	assertTearsDown(t, b, c, byte(protocol.Unsuback)<<4, nil, "a malformed UNSUBACK")
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
	assertTearsDown(t, b, c, byte(protocol.Publish)<<4|0x02, []byte{0x00, 0x05, 'a'}, "a malformed PUBLISH")
}

// TestMalformedPubackTearsDownAndKeepsSessionState: a PUBACK whose body the
// codec rejects (1-byte body: the packet identifier alone needs 2) while a
// QoS 1 publish is in flight must (a) tear the connection down and (b)
// leave the stored entry intact — it is the replay state a resumed session
// needs to complete the exchange. Pre-hardening, the frame was swallowed
// and the exchange leaked its send-quota permit and packet id forever.
func TestMalformedPubackTearsDownAndKeepsSessionState(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	b.DropNextPuback(1)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "puback-malformed"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	pubErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pubErr <- c.Publish(ctx, "t/1", []byte("x"), QoS1, false)
	}()

	// Wait until the PUBLISH is in flight (stored entry present).
	if !lcPoll(2*time.Second, func() bool {
		msgs, _ := c.store.All()
		return len(msgs) == 1
	}) {
		t.Fatal("in-flight QoS 1 publish never reached the store")
	}

	assertTearsDown(t, b, c, byte(protocol.Puback)<<4, []byte{0x09}, "a malformed PUBACK")

	if err := <-pubErr; !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Publish err = %v, want ErrConnectionLost", err)
	}
	msgs, err := c.store.All()
	if err != nil || len(msgs) != 1 || msgs[0].Kind != StoredPublish {
		t.Fatalf("store after malformed PUBACK = %v (err %v), want the in-flight StoredPublish kept for replay", msgs, err)
	}
}

// TestMalformedPubrelTearsDownConnection: an undecodable PUBREL (1-byte
// body) is fatal; reading on would leave the peer's QoS 2 flow — and any
// dedup entry of ours — stuck forever. The frame carries fixed-header
// flags 0x02 so it passes ValidateFlags and exercises the DecodeAck error
// branch specifically.
func TestMalformedPubrelTearsDownConnection(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "pubrel-malformed"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	assertTearsDown(t, b, c, byte(protocol.Pubrel)<<4|0x02, []byte{0x09}, "a malformed PUBREL")
}

// TestInvalidFixedHeaderFlagsTearsDownConnection: a decodable packet type
// with reserved fixed-header flag bits set wrong (PUBREL with flags 0x00;
// the spec requires 0x02) fails ValidateFlags and must be a fatal protocol
// error, not a warn-and-continue.
func TestInvalidFixedHeaderFlagsTearsDownConnection(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "flags-invalid"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	assertTearsDown(t, b, c, byte(protocol.Pubrel)<<4, []byte{0x00, 0x01}, "invalid fixed-header flags")
}

// TestProtocolErrorDisconnectByVersion: on MQTT 5.0 a protocol-error
// teardown is preceded by a DISCONNECT (0x81); on MQTT 3.1.1 it must NOT
// be — the v3 DISCONNECT has no reason code and is defined as a CLEAN
// disconnect that makes the broker discard the Last Will without
// publishing it ([MQTT-3.14.4-3]). Closing the socket abruptly is the
// conformant v3 error signal and keeps the LWT armed.
func TestProtocolErrorDisconnectByVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		version         ProtocolVersion
		wantDisconnects int
	}{
		{name: "v5 sends DISCONNECT", version: ProtocolV50, wantDisconnects: 1},
		{name: "v311 closes without DISCONNECT", version: ProtocolV311, wantDisconnects: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newMockBroker(t)
			cfg := newIntegrationConfig(b.URL(), "proto-err-"+tc.name)
			cfg.ProtocolVersion = tc.version
			c := NewTCPClient(cfg)
			mustConnect(t, c)
			defer func() { _ = c.Disconnect(context.Background()) }()

			assertTearsDown(t, b, c, byte(protocol.Pubrel)<<4, []byte{0x00, 0x01}, "invalid fixed-header flags")

			// Give the broker a moment to consume anything the client
			// flushed before closing.
			time.Sleep(150 * time.Millisecond)
			if got := b.DisconnectCount(); got != tc.wantDisconnects {
				t.Fatalf("broker received %d DISCONNECT(s), want %d", got, tc.wantDisconnects)
			}
		})
	}
}

// TestForgedSubackDoesNotCompletePublish: ack waiters are typed by
// acknowledgement class, so a broker sending a SUBACK carrying the packet
// identifier of an in-flight QoS 1 PUBLISH must not resolve that Publish
// as successful. The real PUBACK still completes it afterwards.
func TestForgedSubackDoesNotCompletePublish(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	b.DropNextPuback(1)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "forged-suback"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	pubDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pubDone <- c.Publish(ctx, "t/forged", []byte("x"), QoS1, false)
	}()

	// Wait for the PUBLISH to be in flight and capture its packet id.
	var id uint16
	if !lcPoll(2*time.Second, func() bool {
		msgs, _ := c.store.All()
		if len(msgs) != 1 {
			return false
		}
		id = msgs[0].ID
		return true
	}) {
		t.Fatal("in-flight QoS 1 publish never reached the store")
	}

	// Forge a successful SUBACK with the publish's packet identifier.
	body := b.buildSuback(protocol.V50, id, []protocol.Subscription{{Filter: "x", Options: protocol.SubscribeOptions{QoS: 1}}})
	var buf bytes.Buffer
	if err := writeMockFrame(&buf, byte(protocol.Suback)<<4, body); err != nil {
		t.Fatalf("writeMockFrame: %v", err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("InjectRawFrame: %v", err)
	}

	// The Publish must NOT complete on the forged SUBACK.
	select {
	case err := <-pubDone:
		t.Fatalf("Publish resolved by a forged SUBACK (err = %v)", err)
	case <-time.After(300 * time.Millisecond):
	}

	// The real PUBACK still completes the exchange.
	injectAck(t, b, protocol.Puback, id)
	select {
	case err := <-pubDone:
		if err != nil {
			t.Fatalf("Publish after real PUBACK: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish never completed after the real PUBACK")
	}
}
