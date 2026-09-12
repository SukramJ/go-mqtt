// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Dispatch-path tests: overlapping subscription filters all firing in
// registration order, the retained flag passing through untouched,
// inbound MQTT 5.0 topic aliasing (registration, alias-only resolution,
// and the over-maximum protocol violation), and the client's reaction to
// a broker DISCONNECT or an inbound AUTH. Shared helpers
// (newIntegrationConfig, mustConnect) live in adapter_integration_test.go;
// polling uses lcPoll (lifecycle_unit_test.go).
//
// A note on scope: the mockBroker double discards the content of a
// client-sent DISCONNECT (its serve loop just returns on that packet type)
// without recording the reason code, so the topic-alias-violation and
// inbound-AUTH tests below assert the fully observable consequence — the
// connection is torn down and ConnectionLost fires — rather than the exact
// 0x94/0x82 reason byte on the wire. See openIssues in the reporting agent
// for the suggested mockBroker hook that would close this gap.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// TestDispatchOverlappingFiltersFireInRegistrationOrder subscribes three
// filters that all match the same topic ("a/#", "a/+", the exact "a/b")
// and proves every one of them fires, in the order they were registered.
func TestDispatchOverlappingFiltersFireInRegistrationOrder(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "dispatch-order"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	var mu sync.Mutex
	var order []string
	record := func(name string) MessageHandler {
		return func(*Message) {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	filters := []string{"a/#", "a/+", "a/b"}
	for _, f := range filters {
		if _, err := c.Subscribe(ctx, f, QoS0, record(f)); err != nil {
			t.Fatalf("Subscribe(%s): %v", f, err)
		}
	}

	if err := b.InjectPublish("a/b", []byte("x"), 0, false, nil); err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}
	if !lcPoll(time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == len(filters)
	}) {
		mu.Lock()
		got := len(order)
		mu.Unlock()
		t.Fatalf("only %d/%d handlers fired", got, len(filters))
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	for i, f := range filters {
		if got[i] != f {
			t.Fatalf("dispatch order = %v, want %v", got, filters)
		}
	}
}

// TestDispatchRetainedFlagPassthrough proves the PUBLISH retain bit
// reaches the handler unmodified for both a retained and a live delivery.
func TestDispatchRetainedFlagPassthrough(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "dispatch-retain"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, retain := range []bool{true, false} {
		received := make(chan *Message, 1)
		filter := fmt.Sprintf("dispatch/retain/%v", retain)
		if _, err := c.Subscribe(ctx, filter, QoS0, func(msg *Message) { received <- msg }); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := b.InjectPublish(filter, []byte("v"), 0, retain, nil); err != nil {
			t.Fatalf("InjectPublish: %v", err)
		}
		select {
		case msg := <-received:
			if msg.Retain != retain {
				t.Fatalf("Retain = %v, want %v", msg.Retain, retain)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no message received for retain=%v", retain)
		}
	}
}

// TestDispatchInboundTopicAliasRegistersAndResolves proves the first
// PUBLISH on an alias (which carries both the topic and the alias)
// registers it, and a later alias-only PUBLISH (empty topic) resolves
// through the client's per-link alias table.
func TestDispatchInboundTopicAliasRegistersAndResolves(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "dispatch-alias")
	cfg.TopicAliasMaximum = 10
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	received := make(chan *Message, 2)
	if _, err := c.Subscribe(ctx, "alias/#", QoS0, func(msg *Message) { received <- msg }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	alias := uint16(5)
	if err := b.InjectPublish("alias/first", []byte("1"), 0, false, &protocol.Properties{TopicAlias: &alias}); err != nil {
		t.Fatalf("InjectPublish (register): %v", err)
	}
	select {
	case msg := <-received:
		if msg.Topic != "alias/first" {
			t.Fatalf("Topic = %q, want %q", msg.Topic, "alias/first")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message for the registering publish")
	}

	if err := b.InjectPublish("", []byte("2"), 0, false, &protocol.Properties{TopicAlias: &alias}); err != nil {
		t.Fatalf("InjectPublish (alias-only): %v", err)
	}
	select {
	case msg := <-received:
		if msg.Topic != "alias/first" {
			t.Fatalf("resolved Topic = %q, want %q", msg.Topic, "alias/first")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message for the alias-only publish")
	}
}

// TestDispatchTopicAliasExceedsMaximumDropsConnection proves an inbound
// topic alias above the client's advertised TopicAliasMaximum is a
// protocol violation that tears the connection down (the client is
// documented to respond with DISCONNECT reason 0x94 Topic Alias Invalid
// before doing so; see the package note on why that exact byte is not
// independently asserted here).
func TestDispatchTopicAliasExceedsMaximumDropsConnection(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "dispatch-alias-max")
	cfg.TopicAliasMaximum = 2
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	alias := uint16(5) // exceeds TopicAliasMaximum of 2
	if err := b.InjectPublish("alias/over", []byte("x"), 0, false, &protocol.Properties{TopicAlias: &alias}); err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}

	select {
	case <-c.ConnectionLost():
	case <-time.After(2 * time.Second):
		t.Fatal("client never dropped the connection for an out-of-range topic alias")
	}
	if !lcPoll(time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("client still reports connected after the alias violation")
	}
}

// TestServerDisconnectTriggersConnectionLost proves a broker-initiated
// DISCONNECT is treated as a lost connection so a Lifecycle reconnects.
func TestServerDisconnectTriggersConnectionLost(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "server-disconnect"))
	mustConnect(t, c)

	if err := b.InjectDisconnect(protocol.ServerShuttingDown); err != nil {
		t.Fatalf("InjectDisconnect: %v", err)
	}
	select {
	case <-c.ConnectionLost():
	case <-time.After(2 * time.Second):
		t.Fatal("a server DISCONNECT never surfaced as a connection loss")
	}
	if !lcPoll(time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("client still connected after a server DISCONNECT")
	}
}

// TestInboundAuthTriggersProtocolErrorAndDrop proves this client, which
// does not participate in enhanced authentication, reacts to a
// server-initiated AUTH as a protocol error and drops the connection (the
// client is documented to respond with DISCONNECT reason 0x82 Protocol
// Error before doing so; see the package note on why that exact byte is
// not independently asserted here).
func TestInboundAuthTriggersProtocolErrorAndDrop(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "inbound-auth"))
	mustConnect(t, c)

	if err := b.InjectAuth(); err != nil {
		t.Fatalf("InjectAuth: %v", err)
	}
	select {
	case <-c.ConnectionLost():
	case <-time.After(2 * time.Second):
		t.Fatal("an inbound AUTH never surfaced as a connection loss")
	}
	if !lcPoll(time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("client still connected after an inbound AUTH")
	}
}

// TestSubscriptionIDsAttributeTheBrokersFanOut is the measured reason
// [WithSubscriptionID] exists.
//
// A broker sends one copy of a PUBLISH per matching subscription (§3.3.4).
// Deciding delivery by re-matching each copy's topic against every filter
// the client holds therefore multiplies: two overlapping filters, two
// copies, both handlers on each copy — a handler runs twice per published
// message. That was measured against Mosquitto 2.1.2 on both dialects, and
// for a handler that performs a write it means the write happens twice with
// nothing in any log.
//
// With identifiers, each copy reaches exactly the subscription the broker
// forwarded it for. This test drives both copies the way a broker sends
// them, so the assertion is on the number of handler runs rather than on
// the shape of the matcher.
func TestSubscriptionIDsAttributeTheBrokersFanOut(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "dispatch-subid"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	var mu sync.Mutex
	runs := map[string]int{}
	record := func(name string) MessageHandler {
		return func(*Message) {
			mu.Lock()
			runs[name]++
			mu.Unlock()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Two filters that overlap on ccu/1/PRESS_SHORT/set — the granularity a
	// consumer reaches for when it wants one subscription per command shape.
	if _, err := c.Subscribe(ctx, "ccu/+/+/set", QoS0, record("shape"), WithSubscriptionID(7)); err != nil {
		t.Fatalf("Subscribe(shape): %v", err)
	}
	if _, err := c.Subscribe(ctx, "ccu/+/PRESS_SHORT/set", QoS0, record("press"), WithSubscriptionID(9)); err != nil {
		t.Fatalf("Subscribe(press): %v", err)
	}

	// The broker's fan-out: one copy per matching subscription, each
	// stamped with the identifier of the subscription it was sent for.
	for _, id := range []uint32{7, 9} {
		props := &protocol.Properties{SubscriptionIdentifiers: []uint32{id}}
		if err := b.InjectPublish("ccu/1/PRESS_SHORT/set", []byte("PRESS"), 0, false, props); err != nil {
			t.Fatalf("InjectPublish(id=%d): %v", id, err)
		}
	}

	if !lcPoll(3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs["shape"] >= 1 && runs["press"] >= 1
	}) {
		t.Fatalf("handlers did not both run: %v", runs)
	}

	mu.Lock()
	defer mu.Unlock()
	if runs["shape"] != 1 || runs["press"] != 1 {
		t.Errorf("handler runs = %v, want exactly one each: two copies reaching "+
			"two handlers apiece is the double-dispatch this option prevents", runs)
	}
}

// TestUnknownSubscriptionIDIsNotBroadenedIntoATopicMatch pins that a
// stamped message naming a subscription this process never registered is
// dropped rather than falling back to matching its topic.
//
// The case is a session resumed from a previous run: the broker still holds
// a subscription with an identifier, and the client that registered it is
// gone. Falling back to a topic match would hand the message to whichever
// handler happens to match, i.e. to code that never asked for it.
func TestUnknownSubscriptionIDIsNotBroadenedIntoATopicMatch(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "dispatch-subid-unknown"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	var mu sync.Mutex
	got := 0
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Subscribe(ctx, "ccu/#", QoS0, func(*Message) {
		mu.Lock()
		got++
		mu.Unlock()
	}, WithSubscriptionID(4)); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	props := &protocol.Properties{SubscriptionIdentifiers: []uint32{99}}
	if err := b.InjectPublish("ccu/1/state", []byte("x"), 0, false, props); err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}
	// Then a stamped message that IS ours, so the test proves the client is
	// still delivering rather than merely asleep.
	props = &protocol.Properties{SubscriptionIdentifiers: []uint32{4}}
	if err := b.InjectPublish("ccu/1/state", []byte("x"), 0, false, props); err != nil {
		t.Fatalf("InjectPublish(own): %v", err)
	}

	if !lcPoll(3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got >= 1
	}) {
		t.Fatal("the client's own stamped message never arrived")
	}

	mu.Lock()
	defer mu.Unlock()
	if got != 1 {
		t.Errorf("handler runs = %d, want 1: an identifier this process never "+
			"registered must not be broadened into a topic match", got)
	}
}

// TestSubscriptionIDIsRefusedOnV311 pins that the option fails loudly on a
// dialect that cannot carry it, rather than being dropped.
//
// MQTT 3.1.1 has no property block, so a silently ignored identifier would
// leave a caller believing its deliveries are attributable while the client
// is in fact still re-matching topics — which is the failure the option
// exists to prevent, now invisible.
func TestSubscriptionIDIsRefusedOnV311(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "dispatch-subid-v311")
	cfg.ProtocolVersion = ProtocolV311
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := c.Subscribe(ctx, "ccu/#", QoS0, func(*Message) {}, WithSubscriptionID(3))
	if err == nil {
		t.Fatal("Subscribe accepted a subscription identifier on an MQTT 3.1.1 link")
	}
	if !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Errorf("err = %v, want ErrProtocolViolation", err)
	}
}

// TestSubscriptionIDRangeIsRefused pins the §3.8.2.1.2 bound. The property
// is a variable byte integer, so it caps at four bytes; a caller's value is
// refused here rather than at the encoder, so the error names the value the
// caller chose instead of a frame it did not write.
func TestSubscriptionIDRangeIsRefused(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "dispatch-subid-range"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := c.Subscribe(ctx, "ccu/#", QoS0, func(*Message) {}, WithSubscriptionID(maxSubscriptionID+1))
	if err == nil {
		t.Fatal("Subscribe accepted an out-of-range subscription identifier")
	}
	if !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Errorf("err = %v, want ErrProtocolViolation", err)
	}
}

// TestUnstampedPublishSkipsStampedSubscriptions closes a hole an
// adversarial review measured in the identifier routing.
//
// §3.3.4 requires a server to include the identifier of *every*
// subscription it forwarded a PUBLISH for. So a message arriving with no
// identifier was forwarded for no stamped subscription — and matching it
// by topic against one delivers a copy the broker never sent for it.
// That is the doubling identifiers exist to remove, reintroduced through
// the fallback: with one stamped overlapping route and one unstamped
// broad subscription on the same client, a single published message was
// measured running a stamped handler twice.
//
// Failing closed is the deliberate choice. A doubled command is worse
// than a dropped one, because the doubling is invisible while the drop
// is a warning line.
func TestUnstampedPublishSkipsStampedSubscriptions(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "dispatch-unstamped"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	var mu sync.Mutex
	runs := map[string]int{}
	record := func(name string) MessageHandler {
		return func(*Message) {
			mu.Lock()
			runs[name]++
			mu.Unlock()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Subscribe(ctx, "ccu/+/+/set", QoS0, record("stamped"), WithSubscriptionID(5)); err != nil {
		t.Fatalf("Subscribe(stamped): %v", err)
	}
	// A second subscription on the same client with no identifier — the
	// shape a consumer produces by adding a broad diagnostic subscribe
	// beside an attributing command router.
	if _, err := c.Subscribe(ctx, "ccu/#", QoS0, record("broad")); err != nil {
		t.Fatalf("Subscribe(broad): %v", err)
	}

	// The broker's copy for the unstamped subscription carries no
	// identifier, exactly as §3.3.4 prescribes.
	if err := b.InjectPublish("ccu/1/PRESS_SHORT/set", []byte("PRESS"), 0, false, nil); err != nil {
		t.Fatalf("InjectPublish(unstamped): %v", err)
	}

	if !lcPoll(3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs["broad"] >= 1
	}) {
		t.Fatal("the unstamped subscription never received its own copy")
	}

	mu.Lock()
	defer mu.Unlock()
	if runs["broad"] != 1 {
		t.Errorf("broad handler ran %d times, want 1", runs["broad"])
	}
	if runs["stamped"] != 0 {
		t.Errorf("stamped handler ran %d times for a message carrying no identifier, want 0: "+
			"a topic match against a stamped subscription is the doubling identifiers remove",
			runs["stamped"])
	}
}
