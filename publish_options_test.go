// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Coverage for the MQTT 5.0 PUBLISH property plumbing that smoke_test.go
// and qos_test.go never touch: every PublishOption (options.go,
// buildPublishProps in publish.go), the inbound property lift in
// toMessage (pump.go), LegacyHandler (client.go) and ReasonError.Error's
// two forms (errors.go).

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// TestPublishOptionsRoundTripToBroker exercises every PublishOption and
// checks the broker observed exactly the properties requested.
func TestPublishOptionsRoundTripToBroker(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "pub-opts"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	corr := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	err := c.Publish(
		ctx, "opts/all", []byte("payload"), QoS0, false,
		WithMessageExpiry(30),
		WithContentType("application/json"),
		WithResponseTopic("reply/here"),
		WithCorrelationData(corr),
		WithPayloadFormatUTF8(),
		WithUserProperties(UserProperty{Key: "k1", Value: "v1"}, UserProperty{Key: "k2", Value: "v2"}),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if !lcPoll(time.Second, func() bool { return len(b.Published()) == 1 }) {
		t.Fatal("broker never observed the PUBLISH")
	}
	got := b.Published()[0]
	p := got.Properties
	if p == nil {
		t.Fatal("Properties = nil, want the requested property block")
	}
	if p.MessageExpiryInterval == nil || *p.MessageExpiryInterval != 30 {
		t.Fatalf("MessageExpiryInterval = %v, want 30", p.MessageExpiryInterval)
	}
	if p.ContentType != "application/json" {
		t.Fatalf("ContentType = %q", p.ContentType)
	}
	if p.ResponseTopic != "reply/here" {
		t.Fatalf("ResponseTopic = %q", p.ResponseTopic)
	}
	if !bytes.Equal(p.CorrelationData, corr) {
		t.Fatalf("CorrelationData = %v, want %v", p.CorrelationData, corr)
	}
	if p.PayloadFormat == nil || *p.PayloadFormat != 1 {
		t.Fatalf("PayloadFormat = %v, want 1", p.PayloadFormat)
	}
	if len(p.UserProperties) != 2 || p.UserProperties[0].Key != "k1" || p.UserProperties[1].Value != "v2" {
		t.Fatalf("UserProperties = %+v", p.UserProperties)
	}
}

// TestPublishNoOptionsOmitsPropertyBlock proves a plain Publish call (no
// options) attaches no PUBLISH properties at all — buildPublishProps must
// return nil rather than an all-zero-value block.
func TestPublishNoOptionsOmitsPropertyBlock(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "pub-noopts"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Publish(ctx, "opts/none", []byte("x"), QoS0, false); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !lcPoll(time.Second, func() bool { return len(b.Published()) == 1 }) {
		t.Fatal("broker never observed the PUBLISH")
	}
	if p := b.Published()[0].Properties; p != nil {
		t.Fatalf("Properties = %+v, want nil for a Publish call with no options", p)
	}
}

// TestPublishOptionsV311Ignored proves PublishOptions are silently ignored
// on an MQTT 3.1.1 link (no property block exists on that wire dialect).
func TestPublishOptionsV311Ignored(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "pub-opts-v311")
	cfg.ProtocolVersion = ProtocolV311
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Publish(ctx, "opts/v311", []byte("x"), QoS0, false, WithContentType("text/plain")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !lcPoll(time.Second, func() bool { return len(b.Published()) == 1 }) {
		t.Fatal("broker never observed the PUBLISH")
	}
	if p := b.Published()[0].Properties; p != nil {
		t.Fatalf("Properties = %+v, want nil on an MQTT 3.1.1 link", p)
	}
}

// TestInboundMessagePropertiesLifted proves an inbound PUBLISH carrying
// MessageExpiryInterval and PayloadFormat properties lifts them into the
// typed [Message] fields (toMessage in pump.go).
func TestInboundMessagePropertiesLifted(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "inbound-props"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	received := make(chan *Message, 1)
	if _, err := c.Subscribe(ctx, "inbound/props", QoS0, func(msg *Message) { received <- msg }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	expiry := uint32(45)
	utf8 := byte(1)
	props := &protocol.Properties{MessageExpiryInterval: &expiry, PayloadFormat: &utf8}
	if err := b.InjectPublish("inbound/props", []byte("hi"), 0, false, props); err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}

	select {
	case msg := <-received:
		if msg.MessageExpirySeconds != 45 {
			t.Fatalf("MessageExpirySeconds = %d, want 45", msg.MessageExpirySeconds)
		}
		if !msg.PayloadFormatUTF8 {
			t.Fatal("PayloadFormatUTF8 = false, want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message received")
	}
}

// TestLegacyHandlerAdaptsToMessageHandler proves LegacyHandler forwards
// Topic/Payload/Retain to the wrapped v0.x-style function unchanged.
func TestLegacyHandlerAdaptsToMessageHandler(t *testing.T) {
	t.Parallel()

	var gotTopic string
	var gotPayload []byte
	var gotRetained bool
	h := LegacyHandler(func(topic string, payload []byte, retained bool) {
		gotTopic, gotPayload, gotRetained = topic, payload, retained
	})
	h(&Message{Topic: "a/b", Payload: []byte("x"), Retain: true})
	if gotTopic != "a/b" || string(gotPayload) != "x" || !gotRetained {
		t.Fatalf("got topic=%q payload=%q retained=%v", gotTopic, gotPayload, gotRetained)
	}
}

// TestReasonErrorMessageWithAndWithoutReason exercises both branches of
// ReasonError.Error().
func TestReasonErrorMessageWithAndWithoutReason(t *testing.T) {
	t.Parallel()

	withReason := &ReasonError{Packet: "PUBLISH", Code: protocol.QuotaExceeded, Reason: "too many in flight"}
	if got := withReason.Error(); got == "" {
		t.Fatal("Error() returned empty string")
	}
	var asErr error = withReason
	var re *ReasonError
	if !errors.As(asErr, &re) || re.Reason == "" {
		t.Fatalf("errors.As failed or lost the reason: %+v", re)
	}

	noReason := &ReasonError{Packet: "SUBSCRIBE", Code: protocol.NotAuthorized}
	got := noReason.Error()
	if got == "" {
		t.Fatal("Error() returned empty string")
	}
	if withReason.Error() == noReason.Error() {
		t.Fatal("the with-reason and without-reason forms must differ")
	}
}
