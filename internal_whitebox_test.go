// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Direct, same-package calls into a few TCPClient internals whose failure
// branches are otherwise only reachable through hard-to-win network
// races: replaySubscriptions' identifier-exhaustion and oversized-frame
// paths, awaitResubscribe's "connection already stopping" race, a
// disabled (zero-interval) keepAliveLoop, and addSubscription's
// replace-in-place branch. Calling the unexported methods directly with
// hand-built minimal state is deterministic where orchestrating the
// equivalent through a live reconnect would be flaky.

import (
	"bufio"
	"context"
	"io"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// TestReplaySubscriptionsIDExhaustedSkipsWithWarn proves a saturated
// identifier space makes replaySubscriptions log and skip a filter
// instead of panicking or blocking.
func TestReplaySubscriptionsIDExhaustedSkipsWithWarn(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "replay-exhausted"})
	c.subs = []subscription{{filter: "a/b", options: protocol.SubscribeOptions{QoS: 1}}}
	for i := range c.ids.used {
		c.ids.used[i] = ^uint64(0)
	}

	done := make(chan struct{})
	go func() {
		c.replaySubscriptions(nil) // never reaches the link: Acquire fails first
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replaySubscriptions never returned with every identifier exhausted")
	}
}

// TestReplaySubscriptionsWriteFrameTooLargeSkipsWithWarn proves a
// subscription whose replayed SUBSCRIBE exceeds the negotiated outbound
// size limit is skipped (identifier released) rather than wedging the
// loop.
func TestReplaySubscriptionsWriteFrameTooLargeSkipsWithWarn(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "replay-toolarge"})
	c.subs = []subscription{{filter: "topic/too/long/to/fit", options: protocol.SubscribeOptions{QoS: 0}}}
	l := &link{w: bufio.NewWriter(io.Discard), outboundMax: 5, stop: make(chan struct{})}

	done := make(chan struct{})
	go func() {
		c.replaySubscriptions(l)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replaySubscriptions never returned for an oversized replay frame")
	}

	// The identifier must have been released on the failure path: it is
	// acquirable again.
	if _, _, err := c.ids.Acquire(); err != nil {
		t.Fatalf("Acquire after a failed replay: %v", err)
	}
}

// TestAwaitResubscribeReturnsWhenStopAlreadyClosed proves awaitResubscribe
// does not block forever when the link's stop channel is already closed
// before the SUBACK arrives (a connection torn down while a replayed
// subscription is still in flight).
func TestAwaitResubscribeReturnsWhenStopAlreadyClosed(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "await-stopped"})
	l := &link{stop: make(chan struct{})}
	close(l.stop)

	id, gen, err := c.ids.Acquire()
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	ch := c.registerWaiter(id, ackClassSuback)
	l.wg.Add(1)

	done := make(chan struct{})
	go func() {
		c.awaitResubscribe(l, id, gen, "a/b", ch)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("awaitResubscribe never returned once l.stop was already closed")
	}

	// The waiter must have been cleaned up too.
	c.waitersMu.Lock()
	_, stillRegistered := c.waiters[id]
	c.waitersMu.Unlock()
	if stillRegistered {
		t.Fatal("waiter for the identifier was not removed")
	}
}

// TestKeepAliveLoopDisabledWhenPingIntervalNonPositive proves keepAliveLoop
// with a non-positive ping interval sends no PINGREQs and exits promptly
// once the link's stop channel closes.
func TestKeepAliveLoopDisabledWhenPingIntervalNonPositive(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "keepalive-disabled"})
	l := &link{stop: make(chan struct{})}
	l.wg.Add(1)

	done := make(chan struct{})
	go func() {
		c.keepAliveLoop(l)
		close(done)
	}()

	// Give a (wrongly) enabled loop a chance to misbehave before closing
	// stop.
	time.Sleep(30 * time.Millisecond)
	close(l.stop)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("keepAliveLoop with a zero ping interval never exited on stop")
	}
}

// TestAddSubscriptionReplacesInPlace proves re-subscribing the same filter
// updates its handler/options in place rather than appending a second
// entry.
func TestAddSubscriptionReplacesInPlace(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "resub-inplace"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Subscribe(ctx, "same/filter", QoS0, func(*Message) {}); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	if _, err := c.Subscribe(ctx, "same/filter", QoS2, func(*Message) {}); err != nil {
		t.Fatalf("second Subscribe: %v", err)
	}

	c.subsMu.RLock()
	n := len(c.subs)
	qos := c.subs[0].options.QoS
	c.subsMu.RUnlock()
	if n != 1 {
		t.Fatalf("subs = %d entries, want 1 (replace in place)", n)
	}
	if qos != byte(QoS2) {
		t.Fatalf("options.QoS = %d, want %d (the second Subscribe's options)", qos, QoS2)
	}
}
