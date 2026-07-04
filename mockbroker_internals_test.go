// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Direct, same-package unit tests for mockBroker's own wire-decoding
// internals (test_mock_broker.go): decodeMockProperty/decodeMockProperties
// for every MQTT 5.0 property id the mock recognises (most never appear
// in a real client's CONNECT/SUBSCRIBE/UNSUBSCRIBE — the codec's own
// propertySpec restricts what TCPClient ever emits there — so the only
// way to exercise them is to hand-craft the property block and call the
// mock's decoder directly, exactly as test_mock_broker_test.go already
// does for the higher-level packet types), decodeMockConnect's malformed
// inputs, decodeMockSubscribe/decodeMockUnsubscribe's malformed inputs,
// the Inject* helpers' "nothing connected yet" guard, and allocID's
// wraparound.
//
// This is test infrastructure testing test infrastructure — legitimate
// here because test_mock_broker.go is compiled into the package proper
// (not a _test.go file, see its own header comment) and therefore counts
// toward the root package's statement coverage like any other source
// file.

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/SukramJ/go-mqtt/protocol"
)

// TestDecodeMockPropertiesAllKnownIDs builds one property block containing
// every property id decodeMockProperty recognises and checks every field
// lands in the right place. A single well-formed pass through the switch
// exercises the assignment statement for property ids no real CONNECT/
// SUBSCRIBE/UNSUBSCRIBE ever carries (those are broker-to-client-only
// properties like AssignedClientID or MaximumQoS; the mock is lenient and
// decodes them anyway).
func TestDecodeMockPropertiesAllKnownIDs(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	body.WriteByte(0x01)
	body.WriteByte(1)
	body.WriteByte(0x02)
	writeMockU32(&body, 30)
	body.WriteByte(0x03)
	writeMockString(&body, "text/plain")
	body.WriteByte(0x08)
	writeMockString(&body, "reply/topic")
	body.WriteByte(0x09)
	writeMockU16(&body, 2)
	body.Write([]byte{0xAA, 0xBB})
	body.WriteByte(0x0B)
	body.Write(encodeMockVarint(7))
	body.WriteByte(0x11)
	writeMockU32(&body, 3600)
	body.WriteByte(0x12)
	writeMockString(&body, "assigned-id")
	body.WriteByte(0x13)
	writeMockU16(&body, 60)
	body.WriteByte(0x15)
	writeMockString(&body, "method")
	body.WriteByte(0x16)
	writeMockU16(&body, 1)
	body.WriteByte(0xCC)
	body.WriteByte(0x17)
	body.WriteByte(1)
	body.WriteByte(0x18)
	writeMockU32(&body, 5)
	body.WriteByte(0x19)
	body.WriteByte(1)
	body.WriteByte(0x1A)
	writeMockString(&body, "info")
	body.WriteByte(0x1C)
	writeMockString(&body, "other-server")
	body.WriteByte(0x1F)
	writeMockString(&body, "reason text")
	body.WriteByte(0x21)
	writeMockU16(&body, 100)
	body.WriteByte(0x22)
	writeMockU16(&body, 10)
	body.WriteByte(0x23)
	writeMockU16(&body, 5)
	body.WriteByte(0x24)
	body.WriteByte(1)
	body.WriteByte(0x25)
	body.WriteByte(1)
	body.WriteByte(0x26)
	writeMockString(&body, "k")
	writeMockString(&body, "v")
	body.WriteByte(0x27)
	writeMockU32(&body, 1024)
	body.WriteByte(0x28)
	body.WriteByte(1)
	body.WriteByte(0x29)
	body.WriteByte(1)
	body.WriteByte(0x2A)
	body.WriteByte(1)

	var outer bytes.Buffer
	outer.Write(encodeMockVarint(body.Len()))
	outer.Write(body.Bytes())

	c := newMCursor(outer.Bytes())
	props, err := decodeMockProperties(c)
	if err != nil {
		t.Fatalf("decodeMockProperties: %v", err)
	}
	if props == nil {
		t.Fatal("props = nil")
	}
	checks := []struct {
		name string
		ok   bool
	}{
		{"PayloadFormat", props.PayloadFormat != nil && *props.PayloadFormat == 1},
		{"MessageExpiryInterval", props.MessageExpiryInterval != nil && *props.MessageExpiryInterval == 30},
		{"ContentType", props.ContentType == "text/plain"},
		{"ResponseTopic", props.ResponseTopic == "reply/topic"},
		{"CorrelationData", bytes.Equal(props.CorrelationData, []byte{0xAA, 0xBB})},
		{"SubscriptionIdentifiers", len(props.SubscriptionIdentifiers) == 1 && props.SubscriptionIdentifiers[0] == 7},
		{"SessionExpiryInterval", props.SessionExpiryInterval != nil && *props.SessionExpiryInterval == 3600},
		{"AssignedClientID", props.AssignedClientID == "assigned-id"},
		{"ServerKeepAlive", props.ServerKeepAlive != nil && *props.ServerKeepAlive == 60},
		{"AuthMethod", props.AuthMethod == "method"},
		{"AuthData", bytes.Equal(props.AuthData, []byte{0xCC})},
		{"RequestProblemInfo", props.RequestProblemInfo != nil && *props.RequestProblemInfo == 1},
		{"WillDelayInterval", props.WillDelayInterval != nil && *props.WillDelayInterval == 5},
		{"RequestResponseInfo", props.RequestResponseInfo != nil && *props.RequestResponseInfo == 1},
		{"ResponseInfo", props.ResponseInfo == "info"},
		{"ServerReference", props.ServerReference == "other-server"},
		{"ReasonString", props.ReasonString == "reason text"},
		{"ReceiveMaximum", props.ReceiveMaximum != nil && *props.ReceiveMaximum == 100},
		{"TopicAliasMaximum", props.TopicAliasMaximum != nil && *props.TopicAliasMaximum == 10},
		{"TopicAlias", props.TopicAlias != nil && *props.TopicAlias == 5},
		{"MaximumQoS", props.MaximumQoS != nil && *props.MaximumQoS == 1},
		{"RetainAvailable", props.RetainAvailable != nil && *props.RetainAvailable == 1},
		{"UserProperties", len(props.UserProperties) == 1 && props.UserProperties[0].Key == "k" && props.UserProperties[0].Value == "v"},
		{"MaximumPacketSize", props.MaximumPacketSize != nil && *props.MaximumPacketSize == 1024},
		{"WildcardSubAvailable", props.WildcardSubAvailable != nil && *props.WildcardSubAvailable == 1},
		{"SubIDAvailable", props.SubIDAvailable != nil && *props.SubIDAvailable == 1},
		{"SharedSubAvailable", props.SharedSubAvailable != nil && *props.SharedSubAvailable == 1},
	}
	for _, c := range checks {
		if !c.ok {
			t.Errorf("property %s not decoded as expected: %+v", c.name, props)
		}
	}
}

func TestDecodeMockPropertiesErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("malformed length varint", func(t *testing.T) {
		t.Parallel()
		c := newMCursor([]byte{0x80, 0x80, 0x80, 0x80, 0x80})
		if _, err := decodeMockProperties(c); err == nil {
			t.Fatal("expected an error for a length varint exceeding 4 bytes")
		}
	})

	t.Run("length overruns remaining bytes", func(t *testing.T) {
		t.Parallel()
		c := newMCursor([]byte{0x0A, 0x01, 0x02}) // declares 10 bytes, only 2 present
		if _, err := decodeMockProperties(c); err == nil {
			t.Fatal("expected an error for a property length overrunning the packet")
		}
	})

	t.Run("unknown property id", func(t *testing.T) {
		t.Parallel()
		var body bytes.Buffer
		body.WriteByte(0xFE) // not a recognised property id
		var outer bytes.Buffer
		outer.Write(encodeMockVarint(body.Len()))
		outer.Write(body.Bytes())
		c := newMCursor(outer.Bytes())
		if _, err := decodeMockProperties(c); err == nil {
			t.Fatal("expected an error for an unknown property id")
		}
	})
}

// TestDecodeMockConnectMalformedVariants truncates a hand-built CONNECT
// body at every field boundary decodeMockConnect parses, proving each
// stage surfaces an error rather than panicking or silently succeeding.
func TestDecodeMockConnectMalformedVariants(t *testing.T) {
	t.Parallel()

	build := func(parts ...[]byte) []byte {
		var buf bytes.Buffer
		for _, p := range parts {
			buf.Write(p)
		}
		return buf.Bytes()
	}
	str := func(s string) []byte {
		var b bytes.Buffer
		writeMockString(&b, s)
		return b.Bytes()
	}
	u16 := func(v uint16) []byte {
		var b bytes.Buffer
		writeMockU16(&b, v)
		return b.Bytes()
	}
	emptyProps := []byte{0x00}
	badVarint := []byte{0x80, 0x80, 0x80, 0x80, 0x80}

	cases := []struct {
		name string
		body []byte
	}{
		{"empty", build()},
		{"no level", build(str("MQTT"))},
		{"bad level", build(str("MQTT"), []byte{99})},
		{"no flags", build(str("MQTT"), []byte{5})},
		{"no keepalive", build(str("MQTT"), []byte{5}, []byte{0x00})},
		{"bad properties varint", build(str("MQTT"), []byte{5}, []byte{0x00}, u16(30), badVarint)},
		{"no client id", build(str("MQTT"), []byte{5}, []byte{0x00}, u16(30), emptyProps)},
		{"will: bad will props", build(str("MQTT"), []byte{5}, []byte{0x04}, u16(30), emptyProps, str("cid"), badVarint)},
		{"will: no topic", build(str("MQTT"), []byte{5}, []byte{0x04}, u16(30), emptyProps, str("cid"), emptyProps)},
		{"will: no payload", build(str("MQTT"), []byte{5}, []byte{0x04}, u16(30), emptyProps, str("cid"), emptyProps, str("will/topic"))},
		{"user: no username", build(str("MQTT"), []byte{5}, []byte{0x80}, u16(30), emptyProps, str("cid"))},
		{"pass: no password", build(str("MQTT"), []byte{5}, []byte{0xC0}, u16(30), emptyProps, str("cid"), str("user"))},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := decodeMockConnect(tt.body); err == nil {
				t.Fatalf("expected an error for case %q", tt.name)
			}
		})
	}
}

// TestDecodeMockSubscribeUnsubscribeMalformedVariants exercises the
// truncation and empty-filter-list error paths of decodeMockSubscribe and
// decodeMockUnsubscribe.
func TestDecodeMockSubscribeUnsubscribeMalformedVariants(t *testing.T) {
	t.Parallel()

	u16 := func(v uint16) []byte {
		var b bytes.Buffer
		writeMockU16(&b, v)
		return b.Bytes()
	}
	str := func(s string) []byte {
		var b bytes.Buffer
		writeMockString(&b, s)
		return b.Bytes()
	}
	join := func(parts ...[]byte) []byte {
		var buf bytes.Buffer
		for _, p := range parts {
			buf.Write(p)
		}
		return buf.Bytes()
	}

	t.Run("subscribe: no packet id", func(t *testing.T) {
		t.Parallel()
		if _, err := decodeMockSubscribe(protocol.V50, nil); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("subscribe: malformed properties", func(t *testing.T) {
		t.Parallel()
		body := join(u16(1), []byte{0x80, 0x80, 0x80, 0x80, 0x80})
		if _, err := decodeMockSubscribe(protocol.V50, body); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("subscribe: truncated filter", func(t *testing.T) {
		t.Parallel()
		body := join(u16(1), []byte{0x00}, []byte{0x00, 0x05, 'a'}) // claims a 5-byte filter, gives 1
		if _, err := decodeMockSubscribe(protocol.V50, body); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("subscribe: missing options byte", func(t *testing.T) {
		t.Parallel()
		body := join(u16(1), []byte{0x00}, str("a/b")) // filter present, no trailing options byte
		if _, err := decodeMockSubscribe(protocol.V50, body); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("subscribe: no filters", func(t *testing.T) {
		t.Parallel()
		body := join(u16(1), []byte{0x00})
		if _, err := decodeMockSubscribe(protocol.V50, body); err == nil {
			t.Fatal("expected an error for a SUBSCRIBE with no filters")
		}
	})

	t.Run("unsubscribe: no packet id", func(t *testing.T) {
		t.Parallel()
		if _, err := decodeMockUnsubscribe(protocol.V50, nil); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("unsubscribe: malformed properties", func(t *testing.T) {
		t.Parallel()
		body := join(u16(1), []byte{0x80, 0x80, 0x80, 0x80, 0x80})
		if _, err := decodeMockUnsubscribe(protocol.V50, body); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("unsubscribe: truncated filter", func(t *testing.T) {
		t.Parallel()
		body := join(u16(1), []byte{0x00}, []byte{0x00, 0x05, 'a'})
		if _, err := decodeMockUnsubscribe(protocol.V50, body); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("unsubscribe: no filters", func(t *testing.T) {
		t.Parallel()
		body := join(u16(1), []byte{0x00})
		if _, err := decodeMockUnsubscribe(protocol.V50, body); err == nil {
			t.Fatal("expected an error for an UNSUBSCRIBE with no filters")
		}
	})
}

// TestMockLinkAllocIDWrapsSkipsZero proves allocID's 16-bit counter skips
// the reserved identifier 0 on wraparound.
func TestMockLinkAllocIDWrapsSkipsZero(t *testing.T) {
	t.Parallel()

	l := &mockLink{}
	l.nextID = 0xFFFE
	if id := l.allocID(); id != 0xFFFF {
		t.Fatalf("first allocID = %d, want 0xFFFF", id)
	}
	if id := l.allocID(); id != 1 {
		t.Fatalf("allocID after wraparound = %d, want 1 (0 skipped)", id)
	}
}

// TestInjectMethodsBeforeConnectReturnNotConnected proves every Inject*
// helper reports errMockNotConnected (or, for InjectTCPReset, simply does
// nothing) when no client has connected yet.
func TestInjectMethodsBeforeConnectReturnNotConnected(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	if err := b.InjectPublish("t", nil, 0, false, nil); !errors.Is(err, errMockNotConnected) {
		t.Fatalf("InjectPublish: err = %v, want errMockNotConnected", err)
	}
	if err := b.InjectDisconnect(protocol.NormalDisconnection); !errors.Is(err, errMockNotConnected) {
		t.Fatalf("InjectDisconnect: err = %v, want errMockNotConnected", err)
	}
	if err := b.InjectAuth(); !errors.Is(err, errMockNotConnected) {
		t.Fatalf("InjectAuth: err = %v, want errMockNotConnected", err)
	}
	if err := b.InjectRawFrame([]byte{0x00}); !errors.Is(err, errMockNotConnected) {
		t.Fatalf("InjectRawFrame: err = %v, want errMockNotConnected", err)
	}
	b.InjectTCPReset() // must not panic with no active connection
}

// TestConnackTopicAliasMaximumAndConnectProperties proves the mock's
// scriptable CONNACK TopicAliasMaximum property round-trips into
// ConnectResult, and that ConnectProperties() reports the client's own
// CONNECT property block.
func TestConnackTopicAliasMaximumAndConnectProperties(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	topicAliasMax := uint16(7)
	b.SetConnackProperties(&protocol.Properties{TopicAliasMaximum: &topicAliasMax})

	cfg := newIntegrationConfig(b.URL(), "connack-tam")
	cfg.SessionExpirySeconds = 120
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	res, ok := c.ConnectResult()
	if !ok {
		t.Fatal("ConnectResult ok = false")
	}
	if res.TopicAliasMaximum != topicAliasMax {
		t.Fatalf("TopicAliasMaximum = %d, want %d", res.TopicAliasMaximum, topicAliasMax)
	}

	props := b.ConnectProperties()
	if props == nil || props.SessionExpiryInterval == nil || *props.SessionExpiryInterval != 120 {
		t.Fatalf("ConnectProperties = %+v, want SessionExpiryInterval 120", props)
	}
}
