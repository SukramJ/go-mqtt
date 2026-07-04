// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	mqtt "github.com/SukramJ/go-mqtt"
)

// TestV5MessageExpiry publishes a QoS 1 message with a 1-second Message
// Expiry Interval while its subscriber is offline (persistent session),
// waits past the expiry, then reconnects and asserts the message is NOT
// delivered — the broker must discard an expired queued message
// (§3.3.2.3.3) rather than deliver it late.
func TestV5MessageExpiry(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)

	topic := uniqueTopicPrefix(t) + "/expiry"
	coll := newMsgCollector(4)

	sub := newClient(t, brokerAddr, mqtt.ProtocolV50, func(cfg *mqtt.TCPConfig) {
		cfg.CleanStart = false
		cfg.SessionExpirySeconds = 300
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := sub.Connect(ctx); err != nil {
		cancel()
		t.Fatalf("initial Connect: %v", err)
	}
	cancel()
	if _, err := sub.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// A graceful Disconnect still leaves a persistent (CleanStart=false,
	// non-zero Session Expiry) session's subscriptions and offline-queue
	// behaviour in place — the same broker-side path an abrupt drop would
	// exercise — so there is no need for the in-test proxy here.
	if err := sub.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	pub := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := pub.Publish(pctx, topic, []byte("stale"), mqtt.QoS1, false, mqtt.WithMessageExpiry(1))
	pcancel()
	if err != nil {
		t.Fatalf("Publish with expiry while subscriber offline: %v", err)
	}

	// Past the 1s expiry, comfortably clear of clock/scheduling slack.
	time.Sleep(2 * time.Second)

	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = sub.Connect(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if res, ok := sub.ConnectResult(); !ok || !res.SessionPresent {
		t.Fatalf("ConnectResult after reconnect = %+v ok=%v, want SessionPresent=true", res, ok)
	}

	if msg, got := coll.NextOrNone(2 * time.Second); got {
		t.Fatalf("received a message that should have expired while queued: %+v", msg)
	}
}

// TestV5InboundTopicAlias configures a non-zero TopicAliasMaximum on the
// subscribing client and round-trips several publishes to the same
// topic.
//
// Tolerance note: whether a broker actually chooses to compress the
// topic with a Topic Alias (§3.3.2.3.4) on the way to a given subscriber
// is entirely at the sender's discretion — the spec never requires it —
// and neither mosquitto nor emqx are known to do so unprompted for
// ordinary forwarded publishes; there is no broker-facing knob this
// client exposes to force it, and reliably observing whether an alias
// was used on the wire would need a raw packet decoder woven into the
// proxy relay, which risks wedging the byte pipe on any decode hiccup for
// no corresponding test value. So this test asserts only what is actually
// guaranteed: advertising TopicAliasMaximum does not break normal
// delivery, and the resolved Message.Topic is always the real topic name
// regardless of whether the broker used an alias underneath.
func TestV5InboundTopicAlias(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)

	topic := uniqueTopicPrefix(t) + "/alias"
	coll := newMsgCollector(8)

	sub := newClient(t, brokerAddr, mqtt.ProtocolV50, func(cfg *mqtt.TCPConfig) {
		cfg.TopicAliasMaximum = 10
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := sub.Connect(ctx); err != nil {
		cancel()
		t.Fatalf("Connect: %v", err)
	}
	cancel()
	if _, err := sub.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	pub := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	const n = 5
	for i := range n {
		pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pub.Publish(pctx, topic, fmt.Appendf(nil, "msg-%d", i), mqtt.QoS1, false)
		pcancel()
		if err != nil {
			t.Fatalf("Publish #%d: %v", i, err)
		}
	}

	for i := range n {
		msg := coll.Next(t, 10*time.Second)
		want := fmt.Sprintf("msg-%d", i)
		if msg.Topic != topic {
			t.Errorf("message #%d Topic = %q, want %q (must resolve to the real topic whether or not an alias was used)", i, msg.Topic, topic)
		}
		if string(msg.Payload) != want {
			t.Errorf("message #%d Payload = %q, want %q", i, msg.Payload, want)
		}
	}
}

// TestV5ReceiveMaximumBurst publishes a burst of QoS 1 messages
// concurrently and asserts every one of them completes without error —
// the client's own outbound flow-control quota (session.go's quota,
// sized from ConnectResult.ReceiveMaximum) must throttle sends to
// whatever the broker advertised transparently, never deadlock or fail a
// send outright just because the burst exceeds it.
func TestV5ReceiveMaximumBurst(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)

	c := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	res, ok := c.ConnectResult()
	if !ok {
		t.Fatal("ConnectResult ok=false after a successful Connect")
	}
	t.Logf("broker ReceiveMaximum = %d", res.ReceiveMaximum)

	topic := uniqueTopicPrefix(t) + "/burst"
	coll := newMsgCollector(32)
	if _, err := c.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const burst = 10
	var wg sync.WaitGroup
	errs := make([]error, burst)
	for i := range burst {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			errs[i] = c.Publish(ctx, topic, fmt.Appendf(nil, "burst-%d", i), mqtt.QoS1, false)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Publish #%d: %v", i, err)
		}
	}
	for range burst {
		coll.Next(t, 10*time.Second)
	}
}

// TestV5ServerKeepAliveOverride checks whether the broker returned a
// Server Keep Alive override (property 0x13, §3.2.2.3.14) in the CONNACK.
// Neither mosquitto nor emqx override a client's requested keep-alive
// under this harness's default config (no `max_keepalive` directive is
// set for mosquitto; see e2e/testdata/mosquitto.conf), so this is a
// best-effort observation: when the broker doesn't exercise the override
// the test skips gracefully instead of asserting a broker config this
// harness doesn't actually provision.
func TestV5ServerKeepAliveOverride(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)

	c := connectClient(t, brokerAddr, mqtt.ProtocolV50, func(cfg *mqtt.TCPConfig) {
		cfg.KeepAlive = 45 * time.Second
	})
	res, ok := c.ConnectResult()
	if !ok {
		t.Fatal("ConnectResult ok=false after a successful Connect")
	}
	if res.ServerKeepAlive == 0 {
		t.Skip("broker did not override the requested keep-alive (ServerKeepAlive absent from CONNACK) — not observable with this broker's default config")
	}
	t.Logf("broker overrode keep-alive to %s", res.ServerKeepAlive)
}
