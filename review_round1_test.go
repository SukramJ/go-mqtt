// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Regression tests for the round-1 adversarial-review findings that live in
// the root package: broker Maximum QoS / Retain Available enforcement on
// outbound PUBLISH, refusal of a CONNACK advertising a §3.2.2.3 Protocol
// Error (Receive Maximum / Maximum Packet Size of 0), the send-quota permit
// staying held across an ack timeout (Receive Maximum honoured), the
// resumed-session quota seeding that keeps replayed PUBLISHes accounted, the
// concurrent-Subscribe rollback lost-update guard, and memStore.Contains.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// quotaAvail reads the send quota's currently-available permit count.
func quotaAvail(q *quota) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.avail
}

// ---------------------------------------------------------------------------
// Finding 1: negotiated Maximum QoS / Retain Available enforced on PUBLISH
// ---------------------------------------------------------------------------

// TestPublishRejectsQoSAboveBrokerMaximum proves a broker CONNACK Maximum
// QoS of 1 makes a QoS 2 Publish fail locally (ErrProtocolViolation) instead
// of transmitting a PUBLISH the broker would answer with DISCONNECT 0x9B.
func TestPublishRejectsQoSAboveBrokerMaximum(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	maxQoS := byte(1)
	b.SetConnackProperties(&protocol.Properties{MaximumQoS: &maxQoS})
	c := NewTCPClient(newIntegrationConfig(b.URL(), "maxqos"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Publish(ctx, "cap/topic", []byte("x"), QoS2, false)
	if !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("Publish err = %v, want ErrProtocolViolation", err)
	}
	// Nothing may have reached the broker.
	if got := len(b.Published()); got != 0 {
		t.Fatalf("broker observed %d publishes, want 0 (rejected locally)", got)
	}
	// QoS 1 (at the ceiling) still succeeds.
	if err := c.Publish(ctx, "cap/topic", []byte("y"), QoS1, false); err != nil {
		t.Fatalf("QoS 1 Publish at the ceiling: %v", err)
	}
}

// TestPublishRejectsRetainWhenUnavailable proves a broker CONNACK Retain
// Available = 0 makes a retained Publish fail locally instead of
// transmitting a retained PUBLISH the broker would answer with 0x9A.
func TestPublishRejectsRetainWhenUnavailable(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	retain := byte(0)
	b.SetConnackProperties(&protocol.Properties{RetainAvailable: &retain})
	c := NewTCPClient(newIntegrationConfig(b.URL(), "noretain"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Publish(ctx, "cap/topic", []byte("x"), QoS0, true)
	if !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("retained Publish err = %v, want ErrProtocolViolation", err)
	}
	if got := len(b.Published()); got != 0 {
		t.Fatalf("broker observed %d publishes, want 0 (rejected locally)", got)
	}
	// A non-retained publish is unaffected.
	if err := c.Publish(ctx, "cap/topic", []byte("y"), QoS0, false); err != nil {
		t.Fatalf("non-retained Publish: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Finding 2/10: CONNACK Receive Maximum / Maximum Packet Size of 0 refused
// ---------------------------------------------------------------------------

// TestConnectRejectsZeroReceiveMaximum proves a CONNACK advertising Receive
// Maximum = 0 (a §3.2.2.3.3 Protocol Error) fails the connect instead of
// seeding a zero send quota that would hang every QoS>0 Publish.
func TestConnectRejectsZeroReceiveMaximum(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	zero := uint16(0)
	b.SetConnackProperties(&protocol.Properties{ReceiveMaximum: &zero})
	c := NewTCPClient(newIntegrationConfig(b.URL(), "rcvmax0"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx); !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("Connect err = %v, want ErrProtocolViolation", err)
	}
	if c.IsConnected() {
		t.Fatal("client reports connected after refusing a zero Receive Maximum")
	}
}

// TestConnectRejectsZeroMaximumPacketSize proves a CONNACK advertising
// Maximum Packet Size = 0 (a §3.2.2.3.6 Protocol Error) fails the connect.
func TestConnectRejectsZeroMaximumPacketSize(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	zero := uint32(0)
	b.SetConnackProperties(&protocol.Properties{MaximumPacketSize: &zero})
	c := NewTCPClient(newIntegrationConfig(b.URL(), "maxpkt0"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Connect(ctx); !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("Connect err = %v, want ErrProtocolViolation", err)
	}
	if c.IsConnected() {
		t.Fatal("client reports connected after refusing a zero Maximum Packet Size")
	}
}

// ---------------------------------------------------------------------------
// Finding 3/7: send-quota permit held while an ack-timed-out PUBLISH is still
// in flight (no Receive Maximum over-commit)
// ---------------------------------------------------------------------------

// TestPublishHoldsQuotaPermitOnAckTimeout proves that when a QoS 1 Publish
// times out waiting for its (dropped) PUBACK, the message stays in flight and
// keeps its send-quota permit: with a negotiated Receive Maximum of 1 a
// second Publish then cannot be admitted (it blocks and its context fires),
// so the broker never sees two concurrently-unacknowledged QoS 1 PUBLISHes.
func TestPublishHoldsQuotaPermitOnAckTimeout(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	one := uint16(1)
	b.SetConnackProperties(&protocol.Properties{ReceiveMaximum: &one})
	cfg := newIntegrationConfig(b.URL(), "hold-permit")
	cfg.AckTimeout = 250 * time.Millisecond
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	b.DropNextPuback(1) // A's PUBACK never arrives -> A ack-times-out, stays in flight.
	ctxA, cancelA := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelA()
	if err := c.Publish(ctxA, "hold/a", []byte("a"), QoS1, false); !errors.Is(err, errAckTimeout) {
		t.Fatalf("Publish A err = %v, want errAckTimeout", err)
	}

	// The permit must still be held by the in-flight A: quota is empty.
	if got := quotaAvail(c.quota); got != 0 {
		t.Fatalf("quota avail = %d after an ack timeout, want 0 (permit held for the in-flight PUBLISH)", got)
	}

	// A second Publish cannot acquire a permit: it blocks and its own ctx
	// fires. Crucially the broker must never observe B.
	ctxB, cancelB := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelB()
	if err := c.Publish(ctxB, "hold/b", []byte("b"), QoS1, false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish B err = %v, want context.DeadlineExceeded (blocked on the saturated quota)", err)
	}
	if got := len(b.Published()); got != 1 {
		t.Fatalf("broker observed %d publishes, want exactly 1 (only A; B must not over-commit Receive Maximum)", got)
	}
}

// ---------------------------------------------------------------------------
// Finding 6/8: resumed-session quota seeded with Receive Maximum minus the
// still-in-flight replay set
// ---------------------------------------------------------------------------

// TestApplySessionSeedsQuotaMinusInflight proves a resumed session seeds the
// send quota with the negotiated ceiling minus the outstanding QoS>0 messages
// (StoredPublish + StoredPubrel), so the replayed PUBLISHes stay accounted
// against Receive Maximum; a reset session starts at the full ceiling.
func TestApplySessionSeedsQuotaMinusInflight(t *testing.T) {
	t.Parallel()

	newClient := func(clean bool) *TCPClient {
		return NewTCPClient(TCPConfig{
			BrokerURL:            "tcp://127.0.0.1:1",
			ClientID:             "seed",
			ProtocolVersion:      ProtocolV50,
			CleanStart:           clean,
			SessionExpirySeconds: 3600,
		})
	}
	seedInflight := func(c *TCPClient) {
		if err := c.store.Save(StoredMessage{ID: 1, Kind: StoredPublish, Publish: &protocol.PublishPacket{Version: protocol.V50, Topic: "a", QoS: 1, PacketID: 1}}); err != nil {
			t.Fatalf("save publish: %v", err)
		}
		if err := c.store.Save(StoredMessage{ID: 2, Kind: StoredPubrel}); err != nil {
			t.Fatalf("save pubrel: %v", err)
		}
		// Inbound receiver-side state must not count against the send quota.
		if err := c.store.Save(StoredMessage{ID: 3, Kind: StoredInboundID}); err != nil {
			t.Fatalf("save inbound: %v", err)
		}
	}
	result := ConnectResult{SessionPresent: true, ReceiveMaximum: 2, MaximumQoS: QoS2, RetainAvailable: true}

	// Resumed session: 2 outbound in flight -> quota = 2 - 2 = 0.
	resumed := newClient(false)
	seedInflight(resumed)
	resumed.applySession(result)
	if got := quotaAvail(resumed.quota); got != 0 {
		t.Fatalf("resumed quota avail = %d, want 0 (RcvMax 2 - 2 in flight)", got)
	}

	// Clean start discards the store first -> quota = full ceiling.
	clean := newClient(true)
	seedInflight(clean)
	clean.applySession(result)
	if got := quotaAvail(clean.quota); got != 2 {
		t.Fatalf("clean-start quota avail = %d, want 2 (full ceiling, store reset)", got)
	}
}

// ---------------------------------------------------------------------------
// Finding 9: concurrent-Subscribe rollback must not clobber a succeeding
// registration for the same filter
// ---------------------------------------------------------------------------

// TestConcurrentSubscribeRollbackKeepsSucceedingRegistration reproduces the
// interleaving A.snapshot(none)+add(h1); B.snapshot(h1)+add(h2); B succeeds;
// A fails and rolls back — and proves A's rollback leaves B's live
// registration intact rather than deleting it (which would silently drop
// inbound messages the broker keeps delivering to B).
func TestConcurrentSubscribeRollbackKeepsSucceedingRegistration(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "sub-race"})

	// A registers first (no prior entry).
	prevA, replA := c.snapshotSubscription("f")
	tokA := c.addSubscription("f", protocol.SubscribeOptions{QoS: 1}, func(*Message) {})

	// B registers next, superseding A, and its SUBACK succeeds.
	c.snapshotSubscription("f")
	tokB := c.addSubscription("f", protocol.SubscribeOptions{QoS: 2}, func(*Message) {})
	if tokB == tokA {
		t.Fatal("addSubscription did not bump the token")
	}

	// A's SUBSCRIBE fails and rolls back — must be a no-op against B.
	c.restoreSubscription("f", prevA, replA, tokA)

	c.subsMu.RLock()
	defer c.subsMu.RUnlock()
	if len(c.subs) != 1 {
		t.Fatalf("subs = %d entries, want 1 (B's registration must survive A's rollback)", len(c.subs))
	}
	if c.subs[0].token != tokB {
		t.Fatalf("surviving token = %d, want B's %d", c.subs[0].token, tokB)
	}
	if c.subs[0].options.QoS != byte(QoS2) {
		t.Fatalf("surviving options QoS = %d, want 2 (B's)", c.subs[0].options.QoS)
	}
}

// TestSubscribeRollbackRemovesWhenStillCurrent proves the normal rollback
// path is unchanged: when no concurrent Subscribe supersedes it, a failed
// first-time SUBSCRIBE removes its provisional registration entirely.
func TestSubscribeRollbackRemovesWhenStillCurrent(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "sub-rollback"})
	prev, repl := c.snapshotSubscription("f")
	tok := c.addSubscription("f", protocol.SubscribeOptions{QoS: 1}, func(*Message) {})
	c.restoreSubscription("f", prev, repl, tok)

	c.subsMu.RLock()
	defer c.subsMu.RUnlock()
	if len(c.subs) != 0 {
		t.Fatalf("subs = %d entries, want 0 (provisional registration rolled back)", len(c.subs))
	}
}

// ---------------------------------------------------------------------------
// Finding 11: memStore fast-membership path
// ---------------------------------------------------------------------------

// TestMemStoreContains proves the O(1) Contains membership test the read loop
// uses agrees with the store's contents and keys on (id, kind).
func TestMemStoreContains(t *testing.T) {
	t.Parallel()

	s := newMemStore()
	if s.Contains(5, StoredPublish) {
		t.Fatal("empty store reports Contains(5, publish) = true")
	}
	if err := s.Save(StoredMessage{ID: 5, Kind: StoredPublish}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if !s.Contains(5, StoredPublish) {
		t.Fatal("Contains(5, publish) = false after Save")
	}
	if s.Contains(5, StoredInboundID) {
		t.Fatal("Contains ignored the kind: (5, inbound-id) reported present")
	}
	if err := s.Delete(5, StoredPublish); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if s.Contains(5, StoredPublish) {
		t.Fatal("Contains(5, publish) = true after Delete")
	}
}
