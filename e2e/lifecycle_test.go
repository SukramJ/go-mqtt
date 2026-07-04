// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package e2e

import (
	"bytes"
	"context"
	"testing"
	"time"

	mqtt "github.com/SukramJ/go-mqtt"
)

// TestLifecycleReconnectAndResubscribe drives a [mqtt.TCPClient] through
// a [mqtt.Lifecycle] against the in-test proxy: severing the connection
// must trigger an event-driven reconnect (via [mqtt.TCPClient]'s
// ConnectionLost channel) fast — well under the configured backoff floor
// — rather than waiting out a timer, and the adapter's own
// resubscribe-on-reconnect must still deliver messages afterward without
// the application re-subscribing itself.
func TestLifecycleReconnectAndResubscribe(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)
	p := newProxy(t, brokerHostPort(t, brokerAddr))

	topic := uniqueTopicPrefix(t) + "/lifecycle-resub"
	coll := newMsgCollector(4)

	client := mqtt.NewTCPClient(newTCPConfig(t, p.URL(), mqtt.ProtocolV50))
	// A deliberately large backoff floor: the assertion below only holds
	// if the reconnect is driven by the ConnectionLost signal, not by
	// this timer expiring.
	lc := mqtt.NewLifecycle(mqtt.LifecycleConfig{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     30 * time.Second,
	}, client)

	// The ctx handed to Start governs the WHOLE reconnect loop, not just
	// the first connect — cancelling it kills the event-driven reconnect
	// this test exists to prove. Keep it alive until cleanup.
	runCtx, cancelRun := context.WithCancel(context.Background())
	if err := lc.Start(runCtx); err != nil {
		cancelRun()
		t.Fatalf("Lifecycle.Start: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = lc.Stop(ctx)
	})

	// Registered once, directly through the client rather than an
	// app-level OnConnect hook: the adapter itself must replay this on
	// every reconnect (TCPClient's resubscribe-on-reconnect guarantee),
	// which is exactly what this test exercises.
	if _, err := client.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	before := time.Now()
	p.Sever()

	if !pollUntil(2*time.Second, func() bool {
		return client.IsConnected() && client.LastConnectedAt().After(before)
	}) {
		t.Fatal("Lifecycle did not reconnect within 2s of Sever (event-driven reconnect), well under the 5s backoff floor")
	}

	pub := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pub.Publish(ctx, topic, []byte("after-reconnect"), mqtt.QoS1, false); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := coll.Next(t, 10*time.Second)
	if msg.Topic != topic || string(msg.Payload) != "after-reconnect" {
		t.Fatalf("message after reconnect = %+v, want topic=%q payload=after-reconnect", msg, topic)
	}
}

// TestLifecycleLastWillOnAbruptDrop connects a client with a Last Will
// and Testament through the proxy, then severs its connection abruptly
// (no clean DISCONNECT) so the broker publishes the will — a second,
// independent client observes it.
func TestLifecycleLastWillOnAbruptDrop(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)
	p := newProxy(t, brokerHostPort(t, brokerAddr))

	willTopic := uniqueTopicPrefix(t) + "/will"
	willPayload := []byte("gone-dark")

	observer := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	coll := newMsgCollector(4)
	if _, err := observer.Subscribe(context.Background(), willTopic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	willClient := newClient(t, p.URL(), mqtt.ProtocolV50, func(cfg *mqtt.TCPConfig) {
		cfg.Will = &mqtt.Will{
			Topic:   willTopic,
			Payload: willPayload,
			QoS:     mqtt.QoS1,
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := willClient.Connect(ctx); err != nil {
		cancel()
		t.Fatalf("Connect: %v", err)
	}
	cancel()

	p.Sever()

	msg := coll.Next(t, 15*time.Second)
	if msg.Topic != willTopic || !bytes.Equal(msg.Payload, willPayload) {
		t.Fatalf("will message = %+v, want topic=%q payload=%q", msg, willTopic, willPayload)
	}
}
