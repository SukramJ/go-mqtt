// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// QoS 0/1/2 publish round trips (both wire versions) and the QoS 2
// resilience scenarios: a stalled PUBREC/PUBCOMP leg fails fast on a
// connection reset, a duplicated PUBACK is harmless, a duplicated inbound
// QoS 2 PUBLISH is delivered exactly once, and an unknown-id PUBREL still
// gets a PUBCOMP. Shared helpers (newIntegrationConfig, mustConnect,
// injectAck) live in adapter_integration_test.go; polling uses lcPoll
// (lifecycle_unit_test.go).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

// TestQoSPublishRoundTrip publishes at every QoS on both wire versions and
// checks the broker observes exactly the topic/payload/QoS/retain it was
// given, with the ack sequence (PUBACK, or PUBREC/PUBREL/PUBCOMP) driven
// entirely by mockBroker's default auto-ack behavior.
func TestQoSPublishRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		version protocol.Version
		qos     QoS
	}{
		{protocol.V311, QoS0},
		{protocol.V311, QoS1},
		{protocol.V311, QoS2},
		{protocol.V50, QoS0},
		{protocol.V50, QoS1},
		{protocol.V50, QoS2},
	}
	for _, tt := range cases {
		t.Run(fmt.Sprintf("%s/QoS%d", tt.version, tt.qos), func(t *testing.T) {
			t.Parallel()

			b := newMockBroker(t)
			cfg := newIntegrationConfig(b.URL(), "qos-rt")
			cfg.ProtocolVersion = tt.version
			c := NewTCPClient(cfg)
			mustConnect(t, c)
			defer func() { _ = c.Disconnect(context.Background()) }()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			payload := []byte("payload-" + tt.version.String())
			retain := tt.qos == QoS2
			if err := c.Publish(ctx, "qos/topic", payload, tt.qos, retain); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if !lcPoll(2*time.Second, func() bool { return len(b.Published()) == 1 }) {
				t.Fatal("broker never observed the PUBLISH")
			}
			got := b.Published()[0]
			if got.Topic != "qos/topic" || !bytes.Equal(got.Payload, payload) ||
				got.QoS != byte(tt.qos) || got.Retain != retain || got.Dup {
				t.Fatalf("Published = %+v", got)
			}
		})
	}
}

// TestQoSPayloadTopicFidelity covers payload/topic edge cases (empty
// payload, binary payload with embedded NUL, a non-ASCII topic) across
// distinct publishes on the same connection.
func TestQoSPayloadTopicFidelity(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "qos-fidelity"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	cases := []struct {
		name    string
		topic   string
		payload []byte
		qos     QoS
		retain  bool
	}{
		{"empty-payload", "fidelity/empty", []byte{}, QoS1, false},
		{"binary-payload", "fidelity/binary", []byte{0x00, 0xFF, 0x10, 0x00}, QoS2, true},
		{"unicode-topic", "fidelity/üñî", []byte("hello"), QoS1, true},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	for _, tt := range cases {
		if err := c.Publish(ctx, tt.topic, tt.payload, tt.qos, tt.retain); err != nil {
			t.Fatalf("%s: Publish: %v", tt.name, err)
		}
	}
	if !lcPoll(2*time.Second, func() bool { return len(b.Published()) == len(cases) }) {
		t.Fatalf("broker observed %d publishes, want %d", len(b.Published()), len(cases))
	}
	got := b.Published()
	for i, tt := range cases {
		if got[i].Topic != tt.topic || !bytes.Equal(got[i].Payload, tt.payload) || got[i].Retain != tt.retain {
			t.Fatalf("case %s: got %+v", tt.name, got[i])
		}
	}
}

// ---------------------------------------------------------------------------
// QoS 2 resilience
// ---------------------------------------------------------------------------

// TestQoS2DropPubrecThenResetFailsFast proves a QoS 2 publish stuck
// waiting for a (dropped) PUBREC fails immediately with ErrConnectionLost
// once the connection resets, well under AckTimeout.
func TestQoS2DropPubrecThenResetFailsFast(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "qos2-droprec")
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	b.DropNextPubrec(1)
	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errCh <- c.Publish(ctx, "qos2/droprec", []byte("x"), QoS2, false)
	}()

	if !lcPoll(2*time.Second, func() bool { return len(b.Published()) == 1 }) {
		t.Fatal("broker never observed the PUBLISH")
	}
	b.InjectTCPReset()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConnectionLost) {
			t.Fatalf("Publish error = %v, want ErrConnectionLost", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("Publish took %v to fail, want well under AckTimeout", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return after the connection reset")
	}
}

// TestQoS2DropPubcompThenResetFailsFast is the PUBREL-leg analog: PUBREC
// succeeds, PUBCOMP is dropped, and the reset must fail the still-blocked
// Publish fast.
func TestQoS2DropPubcompThenResetFailsFast(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "qos2-dropcomp")
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	b.DropNextPubcomp(1)
	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errCh <- c.Publish(ctx, "qos2/dropcomp", []byte("y"), QoS2, false)
	}()

	if !lcPoll(2*time.Second, func() bool {
		msgs, _ := c.store.All()
		for _, m := range msgs {
			if m.Kind == StoredPubrel {
				return true
			}
		}
		return false
	}) {
		t.Fatal("client never reached the PUBREL state")
	}
	b.InjectTCPReset()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConnectionLost) {
			t.Fatalf("Publish error = %v, want ErrConnectionLost", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("Publish took %v to fail, want well under AckTimeout", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return after the connection reset")
	}
}

// TestQoS1DuplicatePubackHarmless proves a broker-duplicated PUBACK does
// not wedge the client: the publish it belongs to still completes exactly
// once, and a subsequent publish on the same connection is unaffected.
func TestQoS1DuplicatePubackHarmless(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "qos1-duppuback"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	b.DuplicateNextPuback()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Publish(ctx, "qos1/dup", []byte("z"), QoS1, false); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := c.Publish(ctx, "qos1/dup2", []byte("z2"), QoS1, false); err != nil {
		t.Fatalf("second Publish after a duplicate ack: %v", err)
	}
}

// TestQoS2InboundDuplicateDeliveredOnce injects the same QoS 2 PUBLISH
// identifier twice (the second flagged DUP, mirroring a broker retransmit)
// and proves the handler runs exactly once — the Method A receiver dedup.
//
// Both copies are written as a single concatenated raw frame rather than
// two separate InjectRawFrame calls with a wait in between: mockBroker's
// serve loop treats any inbound PUBREC as the ack for its own outbound
// QoS 2 flow and immediately fires back an (unsolicited, from this test's
// point of view) PUBREL — which would delete the client's dedup record
// between the two deliveries and defeat the very thing under test. Landing
// both PUBLISHes in the client's read buffer before that round trip can
// happen keeps the test deterministic.
func TestQoS2InboundDuplicateDeliveredOnce(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "qos2-inbound-dup"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	var delivered atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Subscribe(ctx, "qos2/dup", QoS2, func(*Message) { delivered.Add(1) }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const id = uint16(4242)
	frame := func(dup bool) []byte {
		var buf bytes.Buffer
		pkt := &protocol.PublishPacket{
			Version: protocol.V50, Topic: "qos2/dup", Payload: []byte("once"),
			QoS: 2, PacketID: id, Dup: dup,
		}
		if err := pkt.Encode(&buf); err != nil {
			t.Fatalf("encode PUBLISH: %v", err)
		}
		return buf.Bytes()
	}

	var combined bytes.Buffer
	combined.Write(frame(false))
	combined.Write(frame(true))
	if err := b.InjectRawFrame(combined.Bytes()); err != nil {
		t.Fatalf("inject PUBLISH pair: %v", err)
	}

	if !lcPoll(time.Second, func() bool { return delivered.Load() >= 1 }) {
		t.Fatal("handler never fired for the original PUBLISH")
	}
	// Give the duplicate every chance to (wrongly) re-dispatch, then assert
	// it did not.
	time.Sleep(150 * time.Millisecond)
	if got := delivered.Load(); got != 1 {
		t.Fatalf("handler fired %d times, want exactly 1 for a duplicate delivery", got)
	}
}

// TestPubrelUnknownIDStillAnswersPubcomp proves a PUBREL for an identifier
// the client never dispatched (no matching StoredInboundID) is still
// answered with a PUBCOMP, so the peer is released either way.
func TestPubrelUnknownIDStillAnswersPubcomp(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "qos2-unknown-pubrel"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	const id = uint16(9001)
	l := b.currentLink()
	if l == nil {
		t.Fatal("no active mock connection")
	}
	wait := l.register(l.pubcomps, id)

	var buf bytes.Buffer
	rel := &protocol.AckPacket{Version: protocol.V50, Type: protocol.Pubrel, PacketID: id}
	if err := rel.EncodeAck(&buf); err != nil {
		t.Fatalf("encode PUBREL: %v", err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("inject PUBREL: %v", err)
	}

	select {
	case <-wait:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not answer PUBCOMP for an unknown PUBREL identifier")
	}
}
