// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func bptr(b byte) *byte       { return &b }
func u16ptr(v uint16) *uint16 { return &v }
func u32ptr(v uint32) *uint32 { return &v }

// allTargets is every context in which a property block can appear: each
// property-carrying control packet plus the CONNECT will block. The
// spec-driven tests iterate this to decide, per property, which contexts
// must round-trip and which must reject.
var allTargets = []struct {
	name   string
	target propTarget
}{
	{"CONNECT", tgConnect},
	{"CONNACK", tgConnack},
	{"PUBLISH", tgPublish},
	{"WILL", willBit},
	{"PUBACK", tgPuback},
	{"PUBREC", tgPubrec},
	{"PUBREL", tgPubrel},
	{"PUBCOMP", tgPubcomp},
	{"SUBSCRIBE", tgSubscribe},
	{"SUBACK", tgSuback},
	{"UNSUBSCRIBE", tgUnsubscribe},
	{"UNSUBACK", tgUnsuback},
	{"DISCONNECT", tgDisconnect},
	{"AUTH", tgAuth},
}

// propFixtures maps every property identifier in propertySpec to a
// Properties value with exactly that one field populated with a
// representative, non-zero value. The spec-driven round-trip test uses it
// as both the encode input and the expected decode output.
var propFixtures = map[byte]*Properties{
	0x01: {PayloadFormat: bptr(1)},
	0x02: {MessageExpiryInterval: u32ptr(3600)},
	0x03: {ContentType: "application/json"},
	0x08: {ResponseTopic: "resp/topic"},
	0x09: {CorrelationData: []byte{0x01, 0x02, 0x03, 0x04}},
	0x0B: {SubscriptionIdentifiers: []uint32{5}},
	0x11: {SessionExpiryInterval: u32ptr(120)},
	0x12: {AssignedClientID: "assigned-id"},
	0x13: {ServerKeepAlive: u16ptr(30)},
	0x15: {AuthMethod: "SCRAM-SHA-1"},
	0x16: {AuthData: []byte{0xAA, 0xBB}},
	0x17: {RequestProblemInfo: bptr(1)},
	0x18: {WillDelayInterval: u32ptr(10)},
	0x19: {RequestResponseInfo: bptr(1)},
	0x1A: {ResponseInfo: "response-info"},
	0x1C: {ServerReference: "other-server:1883"},
	0x1F: {ReasonString: "because"},
	0x21: {ReceiveMaximum: u16ptr(100)},
	0x22: {TopicAliasMaximum: u16ptr(10)},
	0x23: {TopicAlias: u16ptr(7)},
	0x24: {MaximumQoS: bptr(1)},
	0x25: {RetainAvailable: bptr(1)},
	0x26: {UserProperties: []UserProperty{{Key: "k", Value: "v"}}},
	0x27: {MaximumPacketSize: u32ptr(1048576)},
	0x28: {WildcardSubAvailable: bptr(1)},
	0x29: {SubIDAvailable: bptr(1)},
	0x2A: {SharedSubAvailable: bptr(1)},
}

// propBlock frames content as an MQTT property block: a variable-byte
// length prefix followed by content verbatim.
func propBlock(content []byte) []byte {
	var buf bytes.Buffer
	appendVarint(&buf, uint32(len(content)))
	buf.Write(content)
	return buf.Bytes()
}

// firstAllowed returns the first target in allTargets that allowed admits,
// or 0 if none does.
func firstAllowed(allowed propTarget) propTarget {
	for _, tc := range allTargets {
		if allowed&tc.target != 0 {
			return tc.target
		}
	}
	return 0
}

// encodeProps encodes p for target, failing the test on error.
func encodeProps(t *testing.T, p *Properties, target propTarget) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := p.encode(&buf, target); err != nil {
		t.Fatalf("encode(target=%#x): %v", target, err)
	}
	return buf.Bytes()
}

// TestPropertiesRoundTripFromSpec is generated directly from propertySpec:
// for every property identifier and every possible target it asserts that
// an allowed target round-trips the fixture value byte-for-identically and
// that a disallowed target is rejected on both the encode side
// (ErrProtocolViolation) and the decode side (ErrMalformedPacket).
func TestPropertiesRoundTripFromSpec(t *testing.T) {
	t.Parallel()
	for id, allowed := range propertySpec {
		fix := propFixtures[id]
		if fix == nil {
			t.Fatalf("no fixture for property 0x%02X", id)
		}
		anyAllowed := firstAllowed(allowed)
		if anyAllowed == 0 {
			t.Fatalf("property 0x%02X is allowed on no known target", id)
		}
		canonical := encodeProps(t, fix, anyAllowed)

		for _, tc := range allTargets {
			legal := allowed&tc.target != 0
			if legal {
				var buf bytes.Buffer
				if err := fix.encode(&buf, tc.target); err != nil {
					t.Errorf("0x%02X/%s: encode: %v", id, tc.name, err)
					continue
				}
				got, err := decodeProperties(newCursor(buf.Bytes()), tc.target)
				if err != nil {
					t.Errorf("0x%02X/%s: decode: %v", id, tc.name, err)
					continue
				}
				if !reflect.DeepEqual(got, fix) {
					t.Errorf("0x%02X/%s: round-trip mismatch: got=%+v want=%+v", id, tc.name, got, fix)
				}
				continue
			}
			// Disallowed target: encode must refuse to write the property.
			var buf bytes.Buffer
			if err := fix.encode(&buf, tc.target); !errors.Is(err, ErrProtocolViolation) {
				t.Errorf("0x%02X/%s: encode disallowed: got %v, want ErrProtocolViolation", id, tc.name, err)
			}
			// Disallowed target: decoding otherwise-valid bytes must refuse.
			if _, err := decodeProperties(newCursor(canonical), tc.target); !errors.Is(err, ErrMalformedPacket) {
				t.Errorf("0x%02X/%s: decode disallowed: got %v, want ErrMalformedPacket", id, tc.name, err)
			}
		}
	}
}

func TestPropertiesEmptyEncode(t *testing.T) {
	t.Parallel()
	// A nil receiver encodes to a single zero-length byte.
	var nilProps *Properties
	var buf bytes.Buffer
	if err := nilProps.encode(&buf, tgConnect); err != nil {
		t.Fatalf("nil encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0x00}) {
		t.Fatalf("nil encode = %x, want 00", buf.Bytes())
	}
	// A non-nil but empty Properties encodes identically.
	buf.Reset()
	if err := (&Properties{}).encode(&buf, tgConnect); err != nil {
		t.Fatalf("empty encode: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0x00}) {
		t.Fatalf("empty encode = %x, want 00", buf.Bytes())
	}
	// A zero-length property block decodes to a nil Properties.
	got, err := decodeProperties(newCursor([]byte{0x00}), tgConnect)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if got != nil {
		t.Fatalf("decode empty = %+v, want nil", got)
	}
}

func TestPropertiesDuplicateRejected(t *testing.T) {
	t.Parallel()
	// Maximum QoS (0x24) appearing twice in a CONNACK block.
	block := propBlock([]byte{0x24, 0x01, 0x24, 0x00})
	if _, err := decodeProperties(newCursor(block), tgConnack); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("duplicate 0x24: got %v, want ErrMalformedPacket", err)
	}
	// Session Expiry Interval (0x11) appearing twice in a CONNECT block.
	block = propBlock([]byte{0x11, 0, 0, 0, 1, 0x11, 0, 0, 0, 2})
	if _, err := decodeProperties(newCursor(block), tgConnect); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("duplicate 0x11: got %v, want ErrMalformedPacket", err)
	}
}

func TestPropertiesRepeatableAllowed(t *testing.T) {
	t.Parallel()
	// Two User Properties (0x26) survive in registration order.
	content := []byte{
		0x26, 0x00, 0x01, 'a', 0x00, 0x01, '1',
		0x26, 0x00, 0x01, 'b', 0x00, 0x01, '2',
	}
	got, err := decodeProperties(newCursor(propBlock(content)), tgPublish)
	if err != nil {
		t.Fatalf("decode user properties: %v", err)
	}
	want := []UserProperty{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}
	if !reflect.DeepEqual(got.UserProperties, want) {
		t.Fatalf("user properties = %+v, want %+v", got.UserProperties, want)
	}
	// Two Subscription Identifiers (0x0B) survive in a PUBLISH.
	got, err = decodeProperties(newCursor(propBlock([]byte{0x0B, 0x05, 0x0B, 0x0A})), tgPublish)
	if err != nil {
		t.Fatalf("decode subscription identifiers: %v", err)
	}
	if !reflect.DeepEqual(got.SubscriptionIdentifiers, []uint32{5, 10}) {
		t.Fatalf("subscription identifiers = %v, want [5 10]", got.SubscriptionIdentifiers)
	}
}

func TestPropertiesTruncatedValue(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		content []byte
		target  propTarget
	}{
		{"payload format byte missing", []byte{0x01}, tgPublish},
		{"message expiry uint32 truncated", []byte{0x02, 0x00, 0x00, 0x00}, tgPublish},
		{"content type string length prefix truncated", []byte{0x03, 0x00}, tgPublish},
		{"content type string body overruns", []byte{0x03, 0x00, 0x05, 'a', 'b'}, tgPublish},
		{"response topic string length prefix truncated", []byte{0x08, 0x00}, tgPublish},
		{"correlation data binary body overruns", []byte{0x09, 0x00, 0x04, 0x01}, tgPublish},
		{"subscription identifier varint truncated", []byte{0x0B, 0x80}, tgPublish},
		{"session expiry uint32 truncated", []byte{0x11, 0x00, 0x00, 0x00}, tgConnect},
		{"assigned client id string length prefix truncated", []byte{0x12, 0x00}, tgConnack},
		{"server keep alive uint16 truncated", []byte{0x13, 0x00}, tgConnack},
		{"auth method string length prefix truncated", []byte{0x15, 0x00}, tgConnect},
		{"auth data binary length prefix truncated", []byte{0x16, 0x00}, tgConnect},
		{"request problem info byte missing", []byte{0x17}, tgConnect},
		{"will delay interval uint32 truncated", []byte{0x18, 0x00, 0x00}, willBit},
		{"request response info byte missing", []byte{0x19}, tgConnect},
		{"response info string length prefix truncated", []byte{0x1A, 0x00}, tgConnack},
		{"server reference string length prefix truncated", []byte{0x1C, 0x00}, tgConnack},
		{"reason string length prefix truncated", []byte{0x1F, 0x00}, tgConnack},
		{"receive maximum uint16 truncated", []byte{0x21, 0x00}, tgConnect},
		{"topic alias maximum uint16 truncated", []byte{0x22, 0x00}, tgConnect},
		{"topic alias uint16 truncated", []byte{0x23, 0x00}, tgPublish},
		{"maximum qos byte missing", []byte{0x24}, tgConnack},
		{"retain available byte missing", []byte{0x25}, tgConnack},
		{"user property key missing", []byte{0x26}, tgPublish},
		{"user property value missing", []byte{0x26, 0x00, 0x01, 'a'}, tgPublish},
		{"maximum packet size uint32 truncated", []byte{0x27, 0x00, 0x00}, tgConnect},
		{"wildcard sub available byte missing", []byte{0x28}, tgConnack},
		{"sub id available byte missing", []byte{0x29}, tgConnack},
		{"shared sub available byte missing", []byte{0x2A}, tgConnack},
	}
	for _, tc := range cases {
		block := propBlock(tc.content)
		if _, err := decodeProperties(newCursor(block), tc.target); !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("%s: got %v, want ErrMalformedPacket", tc.name, err)
		}
	}
}

func TestPropertiesLengthOverrun(t *testing.T) {
	t.Parallel()
	// The length prefix advertises 5 bytes but only 2 follow.
	if _, err := decodeProperties(newCursor([]byte{0x05, 0x24, 0x01}), tgConnack); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("overrun: got %v, want ErrMalformedPacket", err)
	}
}

func TestPropertiesLengthVarintMalformed(t *testing.T) {
	t.Parallel()
	// The property-length variable byte integer itself is malformed.
	if _, err := decodeProperties(newCursor([]byte{0x80, 0x80, 0x80, 0x80}), tgConnect); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("bad length varint: got %v, want ErrMalformedPacket", err)
	}
}

func TestPropertiesUnknownID(t *testing.T) {
	t.Parallel()
	// 0x07 is not an assigned property identifier.
	block := propBlock([]byte{0x07, 0x00})
	if _, err := decodeProperties(newCursor(block), tgConnack); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("unknown id: got %v, want ErrMalformedPacket", err)
	}
}

func TestPropertiesSubIDValidation(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// Encode rejects a subscription identifier of zero.
	if err := (&Properties{SubscriptionIdentifiers: []uint32{0}}).encode(&buf, tgSubscribe); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("encode subid 0: got %v, want ErrProtocolViolation", err)
	}
	// Encode rejects a subscription identifier beyond the varint range.
	buf.Reset()
	if err := (&Properties{SubscriptionIdentifiers: []uint32{maxVarint + 1}}).encode(&buf, tgSubscribe); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("encode subid overflow: got %v, want ErrProtocolViolation", err)
	}
	// Decode rejects a subscription identifier of zero.
	block := propBlock([]byte{0x0B, 0x00})
	if _, err := decodeProperties(newCursor(block), tgSubscribe); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("decode subid 0: got %v, want ErrMalformedPacket", err)
	}
}

func TestPropertiesEncodeStringTooLong(t *testing.T) {
	t.Parallel()
	p := &Properties{ContentType: strings.Repeat("x", maxStringLen+1)}
	var buf bytes.Buffer
	if err := p.encode(&buf, tgPublish); !errors.Is(err, ErrStringTooLong) {
		t.Fatalf("got %v, want ErrStringTooLong", err)
	}
}

// TestPropertiesUserPropertyDisallowedTarget drives checkEncodeAllowed's
// failure path for User Property (0x26) itself: every real packet target
// admits it (see userPropTargets), so only an artificial zero target can
// exercise the rejection.
func TestPropertiesUserPropertyDisallowedTarget(t *testing.T) {
	t.Parallel()
	p := &Properties{UserProperties: []UserProperty{{Key: "k", Value: "v"}}}
	var buf bytes.Buffer
	if err := p.encode(&buf, propTarget(0)); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("got %v, want ErrProtocolViolation", err)
	}
}

// TestPropertiesUserPropertyFieldsTooLong exercises the length guard on
// both halves of a User Property pair independently.
func TestPropertiesUserPropertyFieldsTooLong(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("x", maxStringLen+1)
	cases := map[string]*Properties{
		"key too long":   {UserProperties: []UserProperty{{Key: big, Value: "v"}}},
		"value too long": {UserProperties: []UserProperty{{Key: "k", Value: big}}},
	}
	for name, p := range cases {
		var buf bytes.Buffer
		if err := p.encode(&buf, tgPublish); !errors.Is(err, ErrStringTooLong) {
			t.Fatalf("%s: got %v, want ErrStringTooLong", name, err)
		}
	}
}
