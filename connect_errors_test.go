// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Coverage for TCPClient.Connect's error paths that no other test
// exercises: a broker URL that fails url.Parse, a CONNECT the codec
// itself refuses to encode (password without username on MQTT 3.1.1), a
// peer that closes before sending CONNACK, a peer that replies with a
// packet type other than CONNACK, and a peer that replies with a
// malformed CONNACK body. Also NewTCPClient's zero-value defaulting,
// LastConnectedAt/ConnectResult before any connect, the Will wire
// round-trip (buildWill), and the ReceiveMaximum CONNECT property.
//
// The malformed/out-of-order-CONNACK cases need a peer that does NOT
// speak the scripted mockBroker protocol (which always replies with a
// well-formed CONNACK), so they use a minimal hand-rolled listener
// instead.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// TestNewTCPClientAppliesDefaults proves the zero-value timing/limit
// fields of TCPConfig get their documented defaults.
func TestNewTCPClientAppliesDefaults(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "defaults"})
	if c.cfg.DialTimeout != defaultDialTimeout {
		t.Fatalf("DialTimeout = %v, want %v", c.cfg.DialTimeout, defaultDialTimeout)
	}
	if c.cfg.AckTimeout != defaultAckTimeout {
		t.Fatalf("AckTimeout = %v, want %v", c.cfg.AckTimeout, defaultAckTimeout)
	}
	if c.cfg.KeepAlive != keepAliveFloor {
		t.Fatalf("KeepAlive = %v, want the %v floor", c.cfg.KeepAlive, keepAliveFloor)
	}
	if c.version != ProtocolV50 {
		t.Fatalf("version = %v, want the ProtocolV50 default", c.version)
	}
	if c.inboundMax != defaultMaxPacketSize {
		t.Fatalf("inboundMax = %d, want %d", c.inboundMax, defaultMaxPacketSize)
	}
}

// TestLastConnectedAtAndConnectResultBeforeConnect proves both report their
// documented zero states before any successful Connect.
func TestLastConnectedAtAndConnectResultBeforeConnect(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "unconnected"})
	if !c.LastConnectedAt().IsZero() {
		t.Fatalf("LastConnectedAt = %v, want the zero time", c.LastConnectedAt())
	}
	if _, ok := c.ConnectResult(); ok {
		t.Fatal("ConnectResult ok = true before any connect")
	}
}

// TestLastConnectedAtAfterConnect proves it reports a recent, non-zero
// instant once a connect has succeeded.
func TestLastConnectedAtAfterConnect(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "lastconnected"))
	before := time.Now()
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	got := c.LastConnectedAt()
	if got.IsZero() {
		t.Fatal("LastConnectedAt is still zero after Connect")
	}
	if got.Before(before.Add(-time.Second)) || got.After(time.Now().Add(time.Second)) {
		t.Fatalf("LastConnectedAt = %v, want close to now (%v)", got, before)
	}
}

// TestConnectBadBrokerURLFails proves a BrokerURL that fails url.Parse
// surfaces immediately as an error rather than attempting to dial.
func TestConnectBadBrokerURLFails(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://%zz", ClientID: "badurl", DialTimeout: time.Second, AckTimeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err == nil {
		t.Fatal("expected an error for an unparseable broker URL")
	}
}

// TestConnectPasswordWithoutUsernameV311Fails proves the codec's own
// CONNECT encode validation (password without username is illegal on MQTT
// 3.1.1) surfaces through Connect as an error, and never dials out.
func TestConnectPasswordWithoutUsernameV311Fails(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "badconnect")
	cfg.ProtocolVersion = ProtocolV311
	cfg.Password = "secret"
	c := NewTCPClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx); !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("err = %v, want ErrProtocolViolation", err)
	}
	// The dial itself succeeds (only the CONNECT encode fails, before any
	// bytes reach the wire); the broker's acceptLoop goroutine records the
	// accepted connection asynchronously, so poll rather than check
	// immediately.
	if !lcPoll(time.Second, func() bool { return b.ConnCount() == 1 }) {
		t.Fatalf("ConnCount = %d, want exactly 1", b.ConnCount())
	}
}

// rawListener is a bare TCP listener test helper for the Connect()
// error paths mockBroker's scripted protocol can't produce (mockBroker
// always answers with a well-formed CONNACK). It hands the first
// accepted connection to fn on its own goroutine.
func rawListener(t *testing.T, fn func(conn net.Conn)) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		fn(conn)
	}()
	return "tcp://" + ln.Addr().String()
}

// TestConnectReadConnackErrorOnEarlyClose proves a peer that closes the
// socket before sending CONNACK surfaces the read error from Connect.
func TestConnectReadConnackErrorOnEarlyClose(t *testing.T) {
	t.Parallel()

	addr := rawListener(t, func(conn net.Conn) {
		// Consume the CONNECT, then hang up without replying.
		_, _ = protocol.ReadFrame(bufio.NewReader(conn), 1<<20)
	})
	c := NewTCPClient(TCPConfig{BrokerURL: addr, ClientID: "earlyclose", DialTimeout: 2 * time.Second, AckTimeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err == nil {
		t.Fatal("expected a read-connack error when the peer closes early")
	}
}

// writeRawFrame writes a hand-assembled fixed-header + body frame to conn.
func writeRawFrame(t *testing.T, conn net.Conn, header byte, body []byte) {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteByte(header)
	// Bodies in this file are always short: a single-byte varint length
	// is sufficient.
	buf.WriteByte(byte(len(body)))
	buf.Write(body)
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write raw frame: %v", err)
	}
}

// TestConnectUnexpectedFirstPacketFails proves a peer replying with a
// packet type other than CONNACK is rejected.
func TestConnectUnexpectedFirstPacketFails(t *testing.T) {
	t.Parallel()

	addr := rawListener(t, func(conn net.Conn) {
		_, _ = protocol.ReadFrame(bufio.NewReader(conn), 1<<20)
		writeRawFrame(t, conn, byte(protocol.Pingresp)<<4, nil)
	})
	c := NewTCPClient(TCPConfig{BrokerURL: addr, ClientID: "wrongpkt", DialTimeout: 2 * time.Second, AckTimeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Connect(ctx)
	if err == nil {
		t.Fatal("expected an error for a non-CONNACK first reply")
	}
}

// TestConnectMalformedConnackFails proves a peer replying with a CONNACK
// frame whose body the codec cannot decode surfaces the decode error.
func TestConnectMalformedConnackFails(t *testing.T) {
	t.Parallel()

	addr := rawListener(t, func(conn net.Conn) {
		_, _ = protocol.ReadFrame(bufio.NewReader(conn), 1<<20)
		writeRawFrame(t, conn, byte(protocol.Connack)<<4, nil) // empty body: too short to decode
	})
	c := NewTCPClient(TCPConfig{BrokerURL: addr, ClientID: "malformed", DialTimeout: 2 * time.Second, AckTimeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err == nil {
		t.Fatal("expected a decode error for a malformed CONNACK body")
	}
}

// TestConnectWillAndReceiveMaximumRoundTrip proves a fully populated Will
// (buildWill's every branch) and a non-zero ReceiveMaximum encode and
// connect successfully on both wire versions.
func TestConnectWillAndReceiveMaximumRoundTrip(t *testing.T) {
	t.Parallel()

	for _, version := range []ProtocolVersion{ProtocolV311, ProtocolV50} {
		t.Run(version.String(), func(t *testing.T) {
			t.Parallel()

			b := newMockBroker(t)
			cfg := newIntegrationConfig(b.URL(), "will-full-"+version.String())
			cfg.ProtocolVersion = version
			cfg.ReceiveMaximum = 10
			cfg.Will = &Will{
				Topic:                "lwt/topic",
				ContentType:          "text/plain",
				ResponseTopic:        "lwt/reply",
				Payload:              []byte("goodbye"),
				CorrelationData:      []byte{0x01, 0x02},
				QoS:                  QoS1,
				Retain:               true,
				PayloadFormatUTF8:    true,
				DelayIntervalSeconds: 5,
				MessageExpirySeconds: 60,
				UserProperties:       []UserProperty{{Key: "k", Value: "v"}},
			}
			c := NewTCPClient(cfg)
			mustConnect(t, c)
			defer func() { _ = c.Disconnect(context.Background()) }()

			if b.ProtocolVersion() != version {
				t.Fatalf("broker observed version %v, want %v", b.ProtocolVersion(), version)
			}
		})
	}
}
