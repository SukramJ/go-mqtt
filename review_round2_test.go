// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Regression tests for the round-2 adversarial-review findings that live in
// the root package: PUBLISH Response Topic wildcard rejection
// ([MQTT-3.3.2-14]), the read loop dropping a buffered frame once its link
// has been torn down (so a reconnect's shared allocator/quota/store are not
// corrupted), and requestAck removing the waiter before releasing the packet
// id on the ack-timeout path.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// ---------------------------------------------------------------------------
// Finding: PUBLISH Response Topic must not contain wildcards ([MQTT-3.3.2-14])
// ---------------------------------------------------------------------------

// TestPublishRejectsWildcardResponseTopic proves a wildcarded WithResponseTopic
// value fails locally (ErrProtocolViolation) instead of transmitting a PUBLISH
// a conformant broker answers with a connection-tearing DISCONNECT, while a
// valid Response Topic still publishes.
func TestPublishRejectsWildcardResponseTopic(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "resp-topic"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, bad := range []string{"clients/+/reply", "clients/#"} {
		err := c.Publish(ctx, "cmd", []byte("x"), QoS1, false, WithResponseTopic(bad))
		if !errors.Is(err, protocol.ErrProtocolViolation) {
			t.Fatalf("Publish(WithResponseTopic(%q)) err = %v, want ErrProtocolViolation", bad, err)
		}
	}
	if got := len(b.Published()); got != 0 {
		t.Fatalf("broker observed %d publishes, want 0 (wildcard response topic rejected locally)", got)
	}

	// A valid Response Topic is unaffected.
	if err := c.Publish(ctx, "cmd", []byte("y"), QoS1, false, WithResponseTopic("clients/c1/reply")); err != nil {
		t.Fatalf("Publish with a valid response topic: %v", err)
	}
	if !lcPoll(time.Second, func() bool { return len(b.Published()) == 1 }) {
		t.Fatalf("broker observed %d publishes, want 1 (the valid one)", len(b.Published()))
	}
}

// ---------------------------------------------------------------------------
// Finding: the old link's read loop must not process buffered frames after
// teardown (they would corrupt the reconnect's shared ids/quota/store)
// ---------------------------------------------------------------------------

// TestReadLoopDropsBufferedFrameAfterLinkStopped proves readLoop bails on a
// frame surfaced from a link whose stop channel is already closed, so a
// pipelined PUBACK left in the bufio.Reader of a dead socket does not run
// completeOutbound against the (shared) allocator/quota/store a reconnect may
// have already re-seeded.
func TestReadLoopDropsBufferedFrameAfterLinkStopped(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "readloop-stopped", ProtocolVersion: ProtocolV50})

	// Seed an in-flight QoS 1 PUBLISH (id 7) as if it were still awaiting its
	// PUBACK, and give the quota slack so a stray release() is observable.
	const id = uint16(7)
	if err := c.store.Save(StoredMessage{ID: id, Kind: StoredPublish, Publish: &protocol.PublishPacket{Version: protocol.V50, Topic: "a", QoS: 1, PacketID: id}}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	c.quota.reset(3)
	before := quotaAvail(c.quota)

	// A link whose reader already holds a pipelined PUBACK for id 7 and whose
	// stop channel is already closed (teardownLink ran; a reconnect may have
	// swapped in a new link sharing c.ids/c.quota/c.store).
	var buf bytes.Buffer
	ack := &protocol.AckPacket{Version: protocol.V50, Type: protocol.Puback, PacketID: id}
	if err := ack.EncodeAck(&buf); err != nil {
		t.Fatalf("encode puback: %v", err)
	}
	l := &link{
		r:          bufio.NewReader(&buf),
		stop:       make(chan struct{}),
		inboundMax: c.inboundMax,
		aliases:    map[uint16]string{},
	}
	close(l.stop)

	l.wg.Add(1)
	c.readLoop(l) // must bail after reading the frame, before handleFrame

	if !c.storeContains(id, StoredPublish) {
		t.Fatal("readLoop processed a PUBACK from a stopped link: the in-flight PUBLISH was deleted")
	}
	if got := quotaAvail(c.quota); got != before {
		t.Fatalf("readLoop released a quota permit from a stopped link: avail %d -> %d", before, got)
	}
}

// ---------------------------------------------------------------------------
// Finding: requestAck must remove the waiter before releasing the packet id on
// the ack-timeout path
// ---------------------------------------------------------------------------

// TestRequestAckTimeoutReleasesIDAndWaiter proves the ack-timeout cleanup path
// both removes the waiter and frees the packet id. With removeWaiter ordered
// before ids.Release, a concurrent Acquire of the reused id cannot have its
// fresh waiter clobbered by this cleanup.
func TestRequestAckTimeoutReleasesIDAndWaiter(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "reqack-timeout", ProtocolVersion: ProtocolV50})
	c.cfg.AckTimeout = 50 * time.Millisecond
	l := &link{w: bufio.NewWriter(io.Discard), stop: make(chan struct{})}

	_, err := c.requestAck(context.Background(), l, "SUBSCRIBE", ackClassSuback, func(id uint16) frameEncoder {
		return &protocol.SubscribePacket{
			Version:       protocol.V50,
			PacketID:      id,
			Subscriptions: []protocol.Subscription{{Filter: "a/b", Options: protocol.SubscribeOptions{QoS: 0}}},
		}
	})
	if !errors.Is(err, errAckTimeout) {
		t.Fatalf("requestAck err = %v, want errAckTimeout", err)
	}

	c.waitersMu.Lock()
	n := len(c.waiters)
	c.waitersMu.Unlock()
	if n != 0 {
		t.Fatalf("waiters = %d after a timed-out requestAck, want 0 (waiter removed)", n)
	}
	if _, _, aerr := c.ids.Acquire(); aerr != nil {
		t.Fatalf("Acquire after a timed-out requestAck: %v (id not released)", aerr)
	}
}
