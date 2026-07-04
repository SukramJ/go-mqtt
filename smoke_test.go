// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Smoke coverage for TCPClient wired against mockBroker end to end: this is
// the one thing no other Phase B test exercises yet (test_mock_broker_test.go
// drives mockBroker by hand with the protocol package, never through
// TCPClient; lifecycle_unit_test.go only uses stub Connectors). Phase C owns
// the exhaustive adapter/session/lifecycle suites — this file just proves
// Connect/Subscribe/Publish/Disconnect actually wire together.

import (
	"context"
	"testing"
	"time"
)

func TestSmokeConnectSubscribePublishDisconnect(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)

	c := NewTCPClient(TCPConfig{
		BrokerURL:   b.URL(),
		ClientID:    "smoke-1",
		CleanStart:  true,
		DialTimeout: 2 * time.Second,
		AckTimeout:  2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Disconnect(context.Background()) }()

	if !c.IsConnected() {
		t.Fatal("IsConnected = false after Connect")
	}
	if res, ok := c.ConnectResult(); !ok || res.ReasonCode.IsError() {
		t.Fatalf("ConnectResult = %+v, ok=%v", res, ok)
	}

	received := make(chan *Message, 1)
	subRes, err := c.Subscribe(ctx, "smoke/topic", QoS1, func(msg *Message) {
		received <- msg
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if subRes.ReasonCode.IsError() {
		t.Fatalf("Subscribe rejected: %v", subRes.ReasonCode)
	}

	if err := c.Publish(ctx, "smoke/topic", []byte("hello"), QoS1, false); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	published := b.Published()
	if len(published) != 1 || published[0].Topic != "smoke/topic" || string(published[0].Payload) != "hello" {
		t.Fatalf("broker Published() = %+v", published)
	}

	if err := b.InjectPublish("smoke/topic", []byte("world"), 0, false, nil); err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}
	select {
	case msg := <-received:
		if msg.Topic != "smoke/topic" || string(msg.Payload) != "world" {
			t.Fatalf("received message = %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for injected PUBLISH to reach the handler")
	}

	if err := c.Unsubscribe(ctx, "smoke/topic"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	if err := c.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if c.IsConnected() {
		t.Fatal("IsConnected = true after Disconnect")
	}
}
