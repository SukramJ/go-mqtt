// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
)

// seedBytes runs enc against a fresh buffer and returns the encoded bytes.
// It is seed-corpus setup, not the fuzz target itself, so a failure here
// fails the test binary at startup rather than being reported as a crasher.
func seedBytes(tb testing.TB, enc func(w io.Writer) error) []byte {
	tb.Helper()
	var buf bytes.Buffer
	if err := enc(&buf); err != nil {
		tb.Fatalf("seed encode: %v", err)
	}
	return buf.Bytes()
}

// rawFrame hand-assembles a full frame (fixed header + remaining length +
// body) for packet shapes this package only decodes (CONNACK, SUBACK,
// UNSUBACK, AUTH) and therefore has no Encode method to drive a seed with.
func rawFrame(tb testing.TB, header byte, body []byte) []byte {
	tb.Helper()
	var buf bytes.Buffer
	if err := writePacket(&buf, header, body); err != nil {
		tb.Fatalf("seed writePacket: %v", err)
	}
	return buf.Bytes()
}

// varintBytes returns the MQTT variable-byte-integer encoding of v, for
// hand-assembling fixed-header remaining-length boundary seeds.
func varintBytes(v uint32) []byte {
	var buf bytes.Buffer
	appendVarint(&buf, v)
	return buf.Bytes()
}

// connackV5Body hand-assembles a v5 CONNACK variable header (ack flags,
// reason code, property block) since ConnackPacket has no Encode method.
func connackV5Body(tb testing.TB) []byte {
	tb.Helper()
	var buf bytes.Buffer
	buf.WriteByte(0x01) // session present
	buf.WriteByte(byte(BadUserNameOrPassword))
	if err := mergedProps(tgConnack).encode(&buf, tgConnack); err != nil {
		tb.Fatalf("connackV5Body: %v", err)
	}
	return buf.Bytes()
}

// subackV5Body hand-assembles a v5 SUBACK variable header since SubackPacket
// has no Encode method.
func subackV5Body(tb testing.TB) []byte {
	tb.Helper()
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x09})
	if err := mergedProps(tgSuback).encode(&buf, tgSuback); err != nil {
		tb.Fatalf("subackV5Body: %v", err)
	}
	buf.WriteByte(byte(GrantedQoS1))
	return buf.Bytes()
}

// unsubackV5Body hand-assembles a v5 UNSUBACK variable header since
// UnsubackPacket has no Encode method.
func unsubackV5Body(tb testing.TB) []byte {
	tb.Helper()
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x0A})
	if err := mergedProps(tgUnsuback).encode(&buf, tgUnsuback); err != nil {
		tb.Fatalf("unsubackV5Body: %v", err)
	}
	buf.WriteByte(byte(NoSubscriptionExisted))
	return buf.Bytes()
}

// authV5Body hand-assembles an AUTH body since AuthPacket has no Encode
// method (this client never sends AUTH).
func authV5Body(tb testing.TB) []byte {
	tb.Helper()
	var buf bytes.Buffer
	buf.WriteByte(byte(ContinueAuthentication))
	if err := mergedProps(tgAuth).encode(&buf, tgAuth); err != nil {
		tb.Fatalf("authV5Body: %v", err)
	}
	return buf.Bytes()
}

// FuzzReadFrame drives ReadFrame with arbitrary bytes and, on a successful
// parse, dispatches the body through every decoder whose packet type it
// matches, in both protocol versions. Invariant: no panic, however
// malformed the fixed header, remaining-length varint or body.
func FuzzReadFrame(f *testing.F) {
	// CONNECT (client-encode only).
	f.Add(seedBytes(f, (&ConnectPacket{Version: V311, ClientID: "c1", KeepAlive: 30}).Encode))
	f.Add(seedBytes(f, (&ConnectPacket{
		Version: V50, ClientID: "c2", KeepAlive: 60, CleanStart: true,
		Properties: &Properties{SessionExpiryInterval: u32ptr(120)},
		Will: &Will{
			Topic: "lwt", Payload: []byte("bye"), QoS: 1,
			Properties: &Properties{WillDelayInterval: u32ptr(5)},
		},
	}).Encode))

	// CONNACK (decode-only; hand-built).
	f.Add(rawFrame(f, byte(Connack)<<4, []byte{0x00, 0x00}))
	f.Add(rawFrame(f, byte(Connack)<<4, connackV5Body(f)))

	// PUBLISH: QoS 0/1/2, retain/dup, and a v5 topic-alias publish (empty
	// topic + alias, the real-world shape for aliased traffic).
	f.Add(seedBytes(f, (&PublishPacket{Version: V311, Topic: "a/b", Payload: []byte("hi"), QoS: 0}).Encode))
	f.Add(seedBytes(f, (&PublishPacket{
		Version: V311, Topic: "a/b", Payload: []byte("hi"), QoS: 1, PacketID: 7, Dup: true, Retain: true,
	}).Encode))
	f.Add(seedBytes(f, (&PublishPacket{
		Version: V50, Topic: "", Payload: []byte("aliased"), QoS: 0,
		Properties: &Properties{TopicAlias: u16ptr(3)},
	}).Encode))
	f.Add(seedBytes(f, (&PublishPacket{
		Version: V50, Topic: "a/b", Payload: []byte("hi"), QoS: 2, PacketID: 9,
		Properties: mergedProps(tgPublish),
	}).Encode))

	// Acks: PUBACK/PUBREC/PUBREL/PUBCOMP, v3 plus every v5 short/full form.
	for _, typ := range []PacketType{Puback, Pubrec, Pubrel, Pubcomp} {
		target, _ := ackTarget(typ)
		f.Add(seedBytes(f, (&AckPacket{Version: V311, Type: typ, PacketID: 5}).EncodeAck))
		f.Add(seedBytes(f, (&AckPacket{Version: V50, Type: typ, PacketID: 5}).EncodeAck))
		f.Add(seedBytes(f, (&AckPacket{Version: V50, Type: typ, PacketID: 5, ReasonCode: QuotaExceeded}).EncodeAck))
		f.Add(seedBytes(f, (&AckPacket{
			Version: V50, Type: typ, PacketID: 5, ReasonCode: QuotaExceeded, Properties: mergedProps(target),
		}).EncodeAck))
	}

	// SUBSCRIBE (client-encode only) / SUBACK (decode-only; hand-built).
	f.Add(seedBytes(f, (&SubscribePacket{
		Version: V311, PacketID: 3,
		Subscriptions: []Subscription{{Filter: "a/+", Options: SubscribeOptions{QoS: 1}}},
	}).Encode))
	f.Add(seedBytes(f, (&SubscribePacket{
		Version: V50, PacketID: 4,
		Subscriptions: []Subscription{{Filter: "a/#", Options: SubscribeOptions{QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 1}}},
		Properties:    mergedProps(tgSubscribe),
	}).Encode))
	f.Add(rawFrame(f, byte(Suback)<<4, []byte{0x00, 0x03, 0x00}))
	f.Add(rawFrame(f, byte(Suback)<<4, subackV5Body(f)))

	// UNSUBSCRIBE (client-encode only) / UNSUBACK (decode-only; hand-built).
	f.Add(seedBytes(f, (&UnsubscribePacket{Version: V311, PacketID: 6, Filters: []string{"a/b"}}).Encode))
	f.Add(seedBytes(f, (&UnsubscribePacket{
		Version: V50, PacketID: 7, Filters: []string{"a/b", "c/d"}, Properties: mergedProps(tgUnsubscribe),
	}).Encode))
	f.Add(rawFrame(f, byte(Unsuback)<<4, []byte{0x00, 0x08}))
	f.Add(rawFrame(f, byte(Unsuback)<<4, unsubackV5Body(f)))

	// PINGREQ / PINGRESP.
	f.Add(seedBytes(f, EncodePingReq))
	f.Add(seedBytes(f, EncodePingResp))

	// DISCONNECT.
	f.Add(seedBytes(f, (&DisconnectPacket{Version: V311}).Encode))
	f.Add(seedBytes(f, (&DisconnectPacket{Version: V50}).Encode))
	f.Add(seedBytes(f, (&DisconnectPacket{Version: V50, ReasonCode: SessionTakenOver, Properties: mergedProps(tgDisconnect)}).Encode))

	// AUTH (decode-only; hand-built).
	f.Add(rawFrame(f, byte(Auth)<<4, nil))
	f.Add(rawFrame(f, byte(Auth)<<4, authV5Body(f)))

	// Boundary varints on the fixed-header remaining length.
	f.Add(append([]byte{0x00}, varintBytes(127)...))                               // largest 1-byte varint, empty body
	f.Add(append(append([]byte{0x00}, varintBytes(128)...), make([]byte, 3)...))   // smallest 2-byte varint, short/truncated body
	f.Add(append(append([]byte{0x00}, varintBytes(16384)...), make([]byte, 3)...)) // smallest 3-byte varint, short/truncated body
	f.Add(append([]byte{0x00}, varintBytes(2_000_000)...))                         // exceeds the 1 MiB cap below -> ErrFrameTooLarge
	f.Add([]byte{0x00, 0xFF, 0xFF, 0xFF, 0xFF})                                    // malformed varint: 4 continuation bytes, none terminating

	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := ReadFrame(bytes.NewReader(data), 1<<20)
		if err != nil {
			return
		}
		_ = frame.ValidateFlags()

		switch frame.PacketType() {
		case Connack:
			_, _ = DecodeConnack(V311, frame.Body)
			_, _ = DecodeConnack(V50, frame.Body)
		case Publish:
			_, _ = DecodePublish(V311, frame.Header, frame.Body)
			_, _ = DecodePublish(V50, frame.Header, frame.Body)
		case Puback, Pubrec, Pubrel, Pubcomp:
			_, _ = DecodeAck(V311, frame.PacketType(), frame.Body)
			_, _ = DecodeAck(V50, frame.PacketType(), frame.Body)
		case Suback:
			_, _ = DecodeSuback(V311, frame.Body)
			_, _ = DecodeSuback(V50, frame.Body)
		case Unsuback:
			_, _ = DecodeUnsuback(V311, frame.Body)
			_, _ = DecodeUnsuback(V50, frame.Body)
		case Disconnect:
			_, _ = DecodeDisconnect(V311, frame.Body)
			_, _ = DecodeDisconnect(V50, frame.Body)
		case Auth:
			_, _ = DecodeAuth(frame.Body)
		default:
			// Connect/Subscribe/Unsubscribe are client-encode-only in this
			// package (no Decode* exists); Pingreq/Pingresp carry no body;
			// any other value is a reserved/unknown packet type. Nothing to
			// dispatch, but ReadFrame + ValidateFlags above must still not
			// panic on it.
		}
	})
}

// FuzzDecodeProperties drives decodeProperties with arbitrary bytes against
// every property-block context this package defines. Invariant: no panic,
// however malformed the length prefix or property content.
func FuzzDecodeProperties(f *testing.F) {
	for _, fix := range propFixtures {
		for _, tc := range allTargets {
			var buf bytes.Buffer
			if err := fix.encode(&buf, tc.target); err != nil {
				continue // property illegal for this target: nothing to seed
			}
			f.Add(buf.Bytes())
		}
	}
	f.Add([]byte{0x00})                         // zero-length property block
	f.Add([]byte{0x01, 0xFE})                   // length 1, unknown property id 0xFE
	f.Add([]byte{0x05, 0x01})                   // length 5 overruns a 1-byte remainder
	f.Add([]byte{0x02, 0x01, 0x01})             // length 2: PayloadFormat id + value, valid on some targets
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}) // malformed length varint (too long)

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, tc := range allTargets {
			_, _ = decodeProperties(newCursor(data), tc.target)
		}
	})
}

// FuzzPublishRoundTrip builds a valid PUBLISH from fuzzed topic/payload/
// flags, skipping combinations the codec itself rejects (e.g. QoS 3), and
// asserts encode -> ReadFrame -> decode -> re-encode reproduces the exact
// original bytes.
func FuzzPublishRoundTrip(f *testing.F) {
	f.Add("a/b/c", []byte("hello"), byte(0x00))                      // v3 QoS0
	f.Add("a/b", []byte("hello"), byte(0x01))                        // v3 QoS1
	f.Add("a/b", []byte{}, byte(0x02))                               // v3 QoS2, empty payload
	f.Add("", []byte("aliased"), byte(0x10|0x20))                    // v5 QoS0, empty topic + alias
	f.Add("sensors/temp", []byte("23.5"), byte(0x10|0x01|0x04|0x08)) // v5 QoS1, retain+dup
	f.Add("$SYS/x", []byte{}, byte(0x03))                            // QoS 3: invalid, must be skipped

	f.Fuzz(func(t *testing.T, topic string, payload []byte, flags byte) {
		if len(topic) > maxStringLen || len(payload) > 1<<20 {
			t.Skip()
		}

		version := V311
		if flags&0x10 != 0 {
			version = V50
		}
		// The decoder rejects spec-malformed topics (wildcards; an empty
		// topic without a v5 topic alias) that the encoder — which trusts
		// its caller; the client pre-validates via ValidateTopicName —
		// happily writes. Those combinations cannot round-trip by design.
		if strings.ContainsAny(topic, "+#") {
			t.Skip()
		}
		if topic == "" && (version != V50 || flags&0x20 == 0) {
			t.Skip()
		}
		pkt := &PublishPacket{
			Version: version,
			Topic:   topic,
			Payload: payload,
			QoS:     flags & 0x03,
			Retain:  flags&0x04 != 0,
			Dup:     flags&0x08 != 0,
		}
		if pkt.QoS > 0 {
			pkt.PacketID = uint16(flags) + 1 // arithmetic on a byte: never wraps to 0
		}
		if version == V50 && flags&0x20 != 0 {
			pkt.Properties = &Properties{TopicAlias: u16ptr(uint16(flags) + 1)}
		}

		var buf1 bytes.Buffer
		if err := pkt.Encode(&buf1); err != nil {
			return // invalid combo (QoS 3): nothing to round-trip
		}
		// ReadFrame consumes buf1 as an io.Reader, draining it; keep a copy
		// of the original encoded bytes to compare against below.
		original := append([]byte(nil), buf1.Bytes()...)

		frame, err := ReadFrame(&buf1, maxVarint)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		decoded, err := DecodePublish(version, frame.Header, frame.Body)
		if err != nil {
			t.Fatalf("DecodePublish: %v", err)
		}
		var buf2 bytes.Buffer
		if err := decoded.Encode(&buf2); err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !bytes.Equal(original, buf2.Bytes()) {
			t.Fatalf("round trip mismatch:\n first  = %x\n second = %x", original, buf2.Bytes())
		}
	})
}

// FuzzPropertiesRoundTrip decodes arbitrary bytes as a property block against
// every context, and for every context where that succeeds, checks that the
// codec has reached a stable fixed point one generation later: decoding the
// re-encoded bytes again must reproduce the same *Properties value and the
// same wire bytes.
//
// The first decode (of arbitrary, possibly adversarial bytes) is
// deliberately NOT compared directly against the second: Properties treats a
// string/[]byte property's zero value as "absent" (documented on the
// Properties type), so a wire block that explicitly carries a
// present-but-empty string or binary property (e.g. Content Type of length
// 0) decodes to the same Go zero value as "property absent" and is,
// necessarily and by design, not distinguishable after a re-encode. That
// collapse happens once, on the very first encode; every generation after
// it is lossless, which is exactly what this test asserts.
func FuzzPropertiesRoundTrip(f *testing.F) {
	for _, fix := range propFixtures {
		for _, tc := range allTargets {
			var buf bytes.Buffer
			if err := fix.encode(&buf, tc.target); err != nil {
				continue
			}
			f.Add(buf.Bytes())
		}
	}
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, tc := range allTargets {
			p1, err := decodeProperties(newCursor(data), tc.target)
			if err != nil {
				continue
			}
			var buf1 bytes.Buffer
			if err := p1.encode(&buf1, tc.target); err != nil {
				t.Fatalf("encode target=%s: %v", tc.name, err)
			}
			p2, err := decodeProperties(newCursor(buf1.Bytes()), tc.target)
			if err != nil {
				t.Fatalf("decode target=%s: %v", tc.name, err)
			}
			var buf2 bytes.Buffer
			if err := p2.encode(&buf2, tc.target); err != nil {
				t.Fatalf("re-encode target=%s: %v", tc.name, err)
			}
			p3, err := decodeProperties(newCursor(buf2.Bytes()), tc.target)
			if err != nil {
				t.Fatalf("re-decode target=%s: %v", tc.name, err)
			}
			if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
				t.Fatalf("wire fixed point mismatch target=%s:\n gen1 = %x\n gen2 = %x", tc.name, buf1.Bytes(), buf2.Bytes())
			}
			if !reflect.DeepEqual(p2, p3) {
				t.Fatalf("semantic fixed point mismatch target=%s:\n gen1 = %+v\n gen2 = %+v", tc.name, p2, p3)
			}
		}
	})
}

// FuzzTopicMatch drives MatchTopic with arbitrary filter/topic pairs.
// Invariants: it never panics; a spec-valid, wildcard-free topic always
// matches itself; and a topic whose first level begins with '$' never
// matches a filter whose first byte is a wildcard character, mirroring
// MatchTopic's own MQTT-4.7.2-1 guard so a regression there is caught here.
func FuzzTopicMatch(f *testing.F) {
	f.Add("a/b", "a/b")
	f.Add("a/+", "a/b")
	f.Add("a/#", "a/b/c")
	f.Add("#", "a/b")
	f.Add("+", "$SYS/broker")
	f.Add("#", "$SYS/broker")
	f.Add("$SYS/#", "$SYS/broker/load")
	f.Add("sport/+/player1", "sport/tennis/player1")
	f.Add("", "")
	f.Add("/", "/")

	f.Fuzz(func(t *testing.T, filter, topic string) {
		result := MatchTopic(filter, topic)

		if err := ValidateTopicName(topic); err == nil {
			if !MatchTopic(topic, topic) {
				t.Fatalf("MatchTopic(%q, %q) = false, want true (reflexive, wildcard-free)", topic, topic)
			}
		}

		if topic != "" && topic[0] == '$' && filter != "" && (filter[0] == '#' || filter[0] == '+') {
			if result {
				t.Fatalf("MatchTopic(%q, %q) = true, want false ($ first level vs wildcard-first filter)", filter, topic)
			}
		}
	})
}
