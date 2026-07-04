// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Root-package integration tests driving TCPClient against mockBroker for
// the behaviors smoke_test.go only touches in passing: session replay on a
// resumed reconnect, fail-fast error semantics, Receive Maximum flow
// control, and the Subscribe/Unsubscribe/resubscribe-replay contract.
// Shared helpers here (newIntegrationConfig, mustConnect, injectAck) are
// also used by qos_test.go and dispatch_test.go. Polling uses lcPoll
// (lifecycle_unit_test.go), already package-visible.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// newIntegrationConfig returns a baseline TCPConfig against a mockBroker
// listening at url, with short timeouts suitable for fast, deterministic
// tests. Callers mutate the returned value before NewTCPClient.
func newIntegrationConfig(url, clientID string) TCPConfig {
	return TCPConfig{
		BrokerURL:   url,
		ClientID:    clientID,
		CleanStart:  true,
		DialTimeout: 2 * time.Second,
		AckTimeout:  3 * time.Second,
	}
}

// mustConnect connects c, failing the test immediately on error.
func mustConnect(t *testing.T, c *TCPClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
}

// injectAck builds a success-reason MQTT 5.0 PUBACK/PUBREC/PUBREL/PUBCOMP
// for id and injects it as if sent by the broker, bypassing mockBroker's
// own automatic (and, in these tests, deliberately suppressed) acking so a
// test can complete a specific in-flight exchange on demand.
func injectAck(t *testing.T, b *mockBroker, typ protocol.PacketType, id uint16) {
	t.Helper()
	var buf bytes.Buffer
	ack := &protocol.AckPacket{Version: protocol.V50, Type: typ, PacketID: id}
	if err := ack.EncodeAck(&buf); err != nil {
		t.Fatalf("encode %s: %v", typ, err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("inject %s: %v", typ, err)
	}
}

// ---------------------------------------------------------------------------
// Session replay on a resumed reconnect
// ---------------------------------------------------------------------------

// TestSessionReplayResumedSessionResendsPublishAndPubrel drives one
// outbound exchange into each resumable state (StoredPublish awaiting
// PUBACK, StoredPublish awaiting PUBREC, StoredPubrel awaiting PUBCOMP),
// severs the connection, then reconnects with the broker scripted to
// report Session Present = 1: the two StoredPublish entries must be
// resent with DUP=1, and the StoredPubrel entry must be resent as a bare
// PUBREL (not a re-sent PUBLISH).
func TestSessionReplayResumedSessionResendsPublishAndPubrel(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "session-replay")
	cfg.CleanStart = false
	cfg.SessionExpirySeconds = 3600
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	// q2a: PUBREC dropped, stays StoredPublish.
	b.DropNextPubrec(1)
	errQ2a := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		errQ2a <- c.Publish(ctx, "sess/q2a", []byte("q2a"), QoS2, false)
	}()
	if !lcPoll(time.Second, func() bool { return len(b.Published()) >= 1 }) {
		t.Fatal("q2a PUBLISH never reached the broker")
	}

	// q1: PUBACK dropped, stays StoredPublish.
	b.DropNextPuback(1)
	errQ1 := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		errQ1 <- c.Publish(ctx, "sess/q1", []byte("q1"), QoS1, false)
	}()
	if !lcPoll(time.Second, func() bool { return len(b.Published()) >= 2 }) {
		t.Fatal("q1 PUBLISH never reached the broker")
	}

	// q2b: PUBREC succeeds (no drop armed for it), PUBCOMP dropped, so it
	// advances to StoredPubrel.
	b.DropNextPubcomp(1)
	errQ2b := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		errQ2b <- c.Publish(ctx, "sess/q2b", []byte("q2b"), QoS2, false)
	}()

	if !lcPoll(2*time.Second, func() bool {
		msgs, _ := c.store.All()
		if len(msgs) != 3 {
			return false
		}
		pubrels := 0
		for _, m := range msgs {
			if m.Kind == StoredPubrel {
				pubrels++
			}
		}
		return pubrels == 1
	}) {
		t.Fatal("session state never settled to 2 StoredPublish + 1 StoredPubrel")
	}

	start := time.Now()
	b.InjectTCPReset()

	for name, ch := range map[string]chan error{"q1": errQ1, "q2a": errQ2a, "q2b": errQ2b} {
		select {
		case err := <-ch:
			if !errors.Is(err, ErrConnectionLost) {
				t.Fatalf("publish %s: err = %v, want ErrConnectionLost", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("publish %s never returned after the reset", name)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waiters failed slowly after reset: %v", elapsed)
	}

	msgs, _ := c.store.All()
	if len(msgs) != 3 {
		t.Fatalf("store lost entries across teardown: %d entries, want 3", len(msgs))
	}

	beforeReconnect := len(b.Published())
	b.SetSessionPresent(true)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	if !lcPoll(2*time.Second, func() bool { return len(b.Published()) >= beforeReconnect+2 }) {
		t.Fatal("replay never resent the two StoredPublish entries")
	}
	byTopic := make(map[string]mockPublished)
	for _, m := range b.Published()[beforeReconnect:] {
		byTopic[m.Topic] = m
	}
	for _, topic := range []string{"sess/q1", "sess/q2a"} {
		m, ok := byTopic[topic]
		if !ok {
			t.Fatalf("replay missing a resend for %s", topic)
		}
		if !m.Dup {
			t.Fatalf("replayed %s: Dup = false, want true", topic)
		}
	}
	if _, ok := byTopic["sess/q2b"]; ok {
		t.Fatal("a StoredPubrel entry must be replayed as a bare PUBREL, not a re-sent PUBLISH")
	}

	if !lcPoll(2*time.Second, func() bool {
		msgs, _ := c.store.All()
		return len(msgs) == 0
	}) {
		t.Fatal("session state never drained once the replayed exchanges completed")
	}
}

// TestSessionReplayCleanStartDiscardsStore proves CleanStart=true discards
// the stored QoS>0 state on reconnect instead of replaying it.
func TestSessionReplayCleanStartDiscardsStore(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "session-cleanstart")
	cfg.CleanStart = true
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	b.DropNextPuback(1)
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		errCh <- c.Publish(ctx, "clean/q1", []byte("x"), QoS1, false)
	}()
	if !lcPoll(time.Second, func() bool {
		msgs, _ := c.store.All()
		return len(msgs) == 1
	}) {
		t.Fatal("publish never reached the in-flight state")
	}

	start := time.Now()
	b.InjectTCPReset()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConnectionLost) {
			t.Fatalf("err = %v, want ErrConnectionLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publish never returned after the reset")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waiter failed slowly after reset: %v", elapsed)
	}

	msgs, _ := c.store.All()
	if len(msgs) != 1 {
		t.Fatalf("store entry lost before reconnect: %d entries, want 1", len(msgs))
	}

	beforeReconnect := len(b.Published())
	b.SetSessionPresent(false)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	msgs, _ = c.store.All()
	if len(msgs) != 0 {
		t.Fatalf("CleanStart=true must discard stored session state, got %d entries", len(msgs))
	}
	// Give an (incorrect) replay a chance to happen, then confirm it did not.
	time.Sleep(150 * time.Millisecond)
	if got := len(b.Published()); got != beforeReconnect {
		t.Fatalf("Published() grew by %d after a CleanStart reconnect, want no replay", got-beforeReconnect)
	}
}

// ---------------------------------------------------------------------------
// Fail-fast error semantics
// ---------------------------------------------------------------------------

// TestFailFastNotConnectedBeforeConnect proves Publish/Subscribe/
// Unsubscribe fail immediately (well under AckTimeout) with ErrNotConnected
// when no session has ever been established.
func TestFailFastNotConnectedBeforeConnect(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "failfast-preconnect")
	c := NewTCPClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	if err := c.Publish(ctx, "x/y", []byte("z"), QoS1, false); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Publish before Connect: err = %v, want ErrNotConnected", err)
	}
	if _, err := c.Subscribe(ctx, "x/y", QoS1, func(*Message) {}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Subscribe before Connect: err = %v, want ErrNotConnected", err)
	}
	if err := c.Unsubscribe(ctx, "x/y"); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Unsubscribe before Connect: err = %v, want ErrNotConnected", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("fail-fast took %v, want near-instant", elapsed)
	}
}

// TestFailFastAfterConnectionReset proves that once a connection has been
// torn down, fresh Publish/Subscribe calls fail immediately too (the link
// pointer is nil, so this is ErrNotConnected rather than ErrConnectionLost
// — that sentinel is reserved for calls already in flight when the drop
// happens; see the QoS 2 resilience tests in qos_test.go).
func TestFailFastAfterConnectionReset(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "failfast-postreset")
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	b.InjectTCPReset()
	if !lcPoll(time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("client never observed the reset")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := c.Publish(ctx, "x/y", []byte("z"), QoS1, false); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Publish after reset: err = %v, want ErrNotConnected", err)
	}
	if _, err := c.Subscribe(ctx, "x/y", QoS1, func(*Message) {}); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Subscribe after reset: err = %v, want ErrNotConnected", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("fail-fast took %v, want near-instant", elapsed)
	}
}

// ---------------------------------------------------------------------------
// Flow control (Receive Maximum)
// ---------------------------------------------------------------------------

// TestFlowControlCapsInFlightAndDrains scripts a CONNACK Receive Maximum of
// 2 and starts 5 concurrent QoS 1 publishes with every automatic PUBACK
// suppressed: at most 2 may ever be admitted (hold a StoredPublish entry)
// at once. Manually acking the admitted ones (via a raw injected PUBACK)
// lets the rest drain through the quota, proving both the cap and eventual
// completion.
func TestFlowControlCapsInFlightAndDrains(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	receiveMax := uint16(2)
	b.SetConnackProperties(&protocol.Properties{ReceiveMaximum: &receiveMax})

	cfg := newIntegrationConfig(b.URL(), "flow-cap")
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	const n = 5
	b.DropNextPuback(n) // every automatic ack is suppressed; acked manually below.

	results := make(chan error, n)
	for i := range n {
		go func(i int) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			results <- c.Publish(ctx, fmt.Sprintf("flow/%d", i), []byte("x"), QoS1, false)
		}(i)
	}

	if !lcPoll(time.Second, func() bool {
		msgs, _ := c.store.All()
		return len(msgs) == int(receiveMax)
	}) {
		t.Fatal("in-flight count never reached the ReceiveMaximum ceiling of 2")
	}
	// Stability check: the ceiling must hold, not just be reached transiently.
	for range 20 {
		msgs, _ := c.store.All()
		if len(msgs) > int(receiveMax) {
			t.Fatalf("in-flight count exceeded ReceiveMaximum: %d", len(msgs))
		}
		time.Sleep(5 * time.Millisecond)
	}

	acked := make(map[uint16]bool)
	completed := 0
	deadline := time.Now().Add(3 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for completed < n && time.Now().Before(deadline) {
		msgs, _ := c.store.All()
		for _, m := range msgs {
			if m.Kind == StoredPublish && !acked[m.ID] {
				acked[m.ID] = true
				injectAck(t, b, protocol.Puback, m.ID)
			}
		}
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("Publish: %v", err)
			}
			completed++
		case <-ticker.C:
		}
	}
	if completed != n {
		t.Fatalf("only %d/%d publishes completed before the deadline", completed, n)
	}
}

// TestFlowControlCtxCancelWhileBlocked proves a Publish parked on a
// saturated quota returns ctx.Canceled promptly once its context is
// cancelled, without disturbing the publish still holding the permit.
func TestFlowControlCtxCancelWhileBlocked(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	receiveMax := uint16(1)
	b.SetConnackProperties(&protocol.Properties{ReceiveMaximum: &receiveMax})

	cfg := newIntegrationConfig(b.URL(), "flow-cancel")
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	b.DropNextPuback(1)
	firstErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		firstErr <- c.Publish(ctx, "flow/first", []byte("x"), QoS1, false)
	}()
	if !lcPoll(time.Second, func() bool {
		msgs, _ := c.store.All()
		return len(msgs) == 1
	}) {
		t.Fatal("first publish never took the single permit")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	blockedErr := make(chan error, 1)
	start := time.Now()
	go func() { blockedErr <- c.Publish(ctx2, "flow/second", []byte("y"), QoS1, false) }()
	cancel2()

	select {
	case err := <-blockedErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("cancel took %v to take effect", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Publish never returned after its context was cancelled")
	}

	// Drain the still-in-flight first publish so no goroutine outlives the
	// test: ack it directly since its automatic PUBACK was suppressed.
	msgs, _ := c.store.All()
	for _, m := range msgs {
		if m.Kind == StoredPublish {
			injectAck(t, b, protocol.Puback, m.ID)
		}
	}
	select {
	case err := <-firstErr:
		if err != nil {
			t.Fatalf("first publish: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first publish never completed")
	}
}

// ---------------------------------------------------------------------------
// Subscribe / Unsubscribe / resubscribe replay
// ---------------------------------------------------------------------------

// TestSubscribeGrantedQoSAndDowngrade checks the default (echoed) grant and
// a broker-scripted downgrade both surface on SubscribeResult without being
// treated as an error.
func TestSubscribeGrantedQoSAndDowngrade(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "sub-granted"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := c.Subscribe(ctx, "sub/echo", QoS2, func(*Message) {})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if res.GrantedQoS != QoS2 || res.ReasonCode != protocol.GrantedQoS2 {
		t.Fatalf("res = %+v, want the requested QoS2 echoed back", res)
	}

	b.GrantQoS(1)
	res, err = c.Subscribe(ctx, "sub/downgrade", QoS2, func(*Message) {})
	if err != nil {
		t.Fatalf("Subscribe with a broker downgrade: %v", err)
	}
	if res.GrantedQoS != QoS1 {
		t.Fatalf("GrantedQoS = %v, want downgraded to QoS1 (not an error)", res.GrantedQoS)
	}
}

// TestSubscribeRejectedYieldsReasonError proves a SUBACK failure code
// surfaces as a *ReasonError matched via errors.As, not a generic error.
func TestSubscribeRejectedYieldsReasonError(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "sub-reject"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	b.RejectSubscribe(0x87)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := c.Subscribe(ctx, "sub/denied", QoS1, func(*Message) {})
	var re *ReasonError
	if !errors.As(err, &re) {
		t.Fatalf("err = %v, want *ReasonError", err)
	}
	if re.Code != 0x87 || re.Packet != "SUBSCRIBE" {
		t.Fatalf("ReasonError = %+v", re)
	}
}

// TestUnsubscribeStopsDelivery proves a message published after Unsubscribe
// no longer reaches the handler.
func TestUnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "unsub"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	got := make(chan struct{}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Subscribe(ctx, "unsub/t", QoS0, func(*Message) { got <- struct{}{} }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := b.InjectPublish("unsub/t", []byte("1"), 0, false, nil); err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("handler never fired before unsubscribe")
	}

	if err := c.Unsubscribe(ctx, "unsub/t"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if err := b.InjectPublish("unsub/t", []byte("2"), 0, false, nil); err != nil {
		t.Fatalf("InjectPublish after unsubscribe: %v", err)
	}
	select {
	case <-got:
		t.Fatal("handler fired again after Unsubscribe")
	case <-time.After(150 * time.Millisecond):
	}
}

// TestResubscribeReplayPreservesOptionsAndOrder proves the fire-and-log
// resubscribe on reconnect resends every filter in its original
// registration order with its original wire options intact.
func TestResubscribeReplayPreservesOptionsAndOrder(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "resub"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	filters := []struct {
		filter string
		qos    QoS
		opts   []SubscribeOption
	}{
		{"resub/a", QoS0, nil},
		{"resub/b", QoS2, []SubscribeOption{WithNoLocal()}},
		{"resub/c", QoS1, []SubscribeOption{WithRetainAsPublished(), WithRetainHandling(DontSendRetained)}},
	}
	for _, f := range filters {
		if _, err := c.Subscribe(ctx, f.filter, f.qos, func(*Message) {}, f.opts...); err != nil {
			t.Fatalf("Subscribe(%s): %v", f.filter, err)
		}
	}

	before := b.Subscriptions()
	if len(before) != len(filters) {
		t.Fatalf("initial Subscriptions() = %d entries, want %d", len(before), len(filters))
	}

	b.InjectTCPReset()
	if !lcPoll(time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("client never observed the reset")
	}
	b.SetSessionPresent(false)
	mustConnect(t, c)

	if !lcPoll(2*time.Second, func() bool { return len(b.Subscriptions()) >= 2*len(filters) }) {
		t.Fatalf("resubscribe replay never landed: got %d frames", len(b.Subscriptions()))
	}
	replayed := b.Subscriptions()[len(filters):]
	for i, f := range filters {
		got := replayed[i]
		if got.Filter != f.filter {
			t.Fatalf("replay[%d].Filter = %q, want %q (order not preserved)", i, got.Filter, f.filter)
		}
		if got.Options.QoS != byte(f.qos) {
			t.Fatalf("replay[%d].QoS = %d, want %d", i, got.Options.QoS, f.qos)
		}
	}
	if !replayed[1].Options.NoLocal {
		t.Fatal("replay lost the NoLocal option")
	}
	if !replayed[2].Options.RetainAsPublished || replayed[2].Options.RetainHandling != byte(DontSendRetained) {
		t.Fatalf("replay lost RetainAsPublished/RetainHandling: %+v", replayed[2].Options)
	}
}
