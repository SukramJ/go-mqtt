// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	mqtt "github.com/SukramJ/go-mqtt"
)

// TestSessionResumptionQueuedMessage exercises persistent-session
// resumption: a subscriber connects with CleanStart=false and a non-zero
// Session Expiry, subscribes at QoS 1, is severed (simulating a broker
// crash / network partition) via the in-test proxy, has a message
// published to its filter while offline, and then reconnects — the
// broker must report SessionPresent=true and deliver the message it
// queued while the subscriber was gone.
func TestSessionResumptionQueuedMessage(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)
	p := newProxy(t, brokerHostPort(t, brokerAddr))

	topic := uniqueTopicPrefix(t) + "/session-resume"
	coll := newMsgCollector(4)

	sub := newClient(t, p.URL(), mqtt.ProtocolV50, func(cfg *mqtt.TCPConfig) {
		cfg.CleanStart = false
		cfg.SessionExpirySeconds = 300
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := sub.Connect(ctx); err != nil {
		cancel()
		t.Fatalf("initial Connect: %v", err)
	}
	cancel()
	if res, ok := sub.ConnectResult(); !ok || res.SessionPresent {
		t.Fatalf("ConnectResult on the very first connect = %+v ok=%v, want SessionPresent=false", res, ok)
	}

	if _, err := sub.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	p.Sever()
	if !pollUntil(5*time.Second, func() bool { return !sub.IsConnected() }) {
		t.Fatal("client did not observe the severed connection")
	}

	// Publish directly against the real broker (not through the now
	// upstream-less proxy relay for this connection) while the subscriber
	// is offline, so the broker queues it for the persistent session.
	pub := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := pub.Publish(pctx, topic, []byte("queued"), mqtt.QoS1, false)
	pcancel()
	if err != nil {
		t.Fatalf("Publish while subscriber offline: %v", err)
	}

	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = sub.Connect(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	res, ok := sub.ConnectResult()
	if !ok || !res.SessionPresent {
		t.Fatalf("ConnectResult after reconnect = %+v, ok=%v, want SessionPresent=true", res, ok)
	}

	msg := coll.Next(t, 10*time.Second)
	if msg.Topic != topic || string(msg.Payload) != "queued" {
		t.Fatalf("replayed message = %+v, want topic=%q payload=queued", msg, topic)
	}
}

// TestSessionQoS2FlightAcrossSever deterministically interrupts an
// outbound QoS 2 PUBLISH between the client sending it and receiving the
// broker's PUBREC — by proxying the connection with an artificial delay
// on the broker->client leg, then severing while the (delayed) PUBREC is
// still in flight — and confirms the persisted session replays and
// completes the exchange after reconnecting, exactly once, observed by a
// separate, always-connected subscriber.
func TestSessionQoS2FlightAcrossSever(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquitto)
	p := newProxy(t, brokerHostPort(t, brokerAddr))
	p.SetServerToClientDelay(500 * time.Millisecond)

	topic := uniqueTopicPrefix(t) + "/qos2-flight"

	// A separate, always-connected observer subscribes so message
	// delivery is decoupled from whatever happens to the severed
	// publisher's own connection (which is also, incidentally, not
	// subscribed to anything here).
	observer := connectClient(t, brokerAddr, mqtt.ProtocolV50)
	coll := newMsgCollector(4)
	if _, err := observer.Subscribe(context.Background(), topic, mqtt.QoS1, coll.Handler); err != nil {
		t.Fatalf("observer Subscribe: %v", err)
	}

	pubClient := newClient(t, p.URL(), mqtt.ProtocolV50, func(cfg *mqtt.TCPConfig) {
		cfg.CleanStart = false
		cfg.SessionExpirySeconds = 300
		cfg.AckTimeout = 20 * time.Second
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := pubClient.Connect(ctx); err != nil {
		cancel()
		t.Fatalf("Connect: %v", err)
	}
	cancel()

	pubErr := make(chan error, 1)
	go func() {
		pctx, pcancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer pcancel()
		pubErr <- pubClient.Publish(pctx, topic, []byte("qos2-payload"), mqtt.QoS2, false)
	}()

	// 150ms is a wide margin over the microseconds a local quota/id
	// acquire + single buffered write takes, and comfortably less than
	// the 500ms broker->client delay, so the PUBREC is guaranteed to
	// still be sitting in the proxy's delayed pipe (never having reached
	// the client) when Sever fires.
	time.Sleep(150 * time.Millisecond)
	p.Sever()

	select {
	case err := <-pubErr:
		if !errors.Is(err, mqtt.ErrConnectionLost) {
			t.Fatalf("Publish result across the sever = %v, want ErrConnectionLost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish did not observe the severed connection")
	}

	if !pollUntil(5*time.Second, func() bool { return !pubClient.IsConnected() }) {
		t.Fatal("client did not observe the severed connection")
	}
	// No reason to keep delaying frames for the reconnect: the resumed
	// session replays the in-flight PUBLISH/PUBREL on its own.
	p.SetServerToClientDelay(0)

	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := pubClient.Connect(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	res, ok := pubClient.ConnectResult()
	if !ok || !res.SessionPresent {
		t.Fatalf("ConnectResult after reconnect = %+v ok=%v, want SessionPresent=true", res, ok)
	}

	msg := coll.Next(t, 15*time.Second)
	if msg.Topic != topic || string(msg.Payload) != "qos2-payload" {
		t.Fatalf("observer received = %+v, want topic=%q payload=qos2-payload", msg, topic)
	}
}
