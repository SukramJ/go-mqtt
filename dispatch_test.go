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
