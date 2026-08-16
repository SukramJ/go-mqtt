// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestMatchTopicSharedSubscription covers MQTT 5.0 §4.8.2: a shared
// subscription's filter is "$share/{ShareName}/{TopicFilter}" and the broker
// delivers PUBLISHes carrying the REAL topic, so matching has to happen
// against {TopicFilter} with the "$share/{ShareName}/" prefix stripped.
// Before this was fixed, MatchTopic compared "$share" against the topic's
// first level and every message for a shared subscription was silently
// dropped by the root package's dispatch loop.
func TestMatchTopicSharedSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		topic  string
		want   bool
	}{
		// The real topic matches; the "$share/{ShareName}/" prefix is not
		// part of what the broker publishes.
		{"literal", "$share/grp/sensors/temp", "sensors/temp", true},
		{"literal mismatch", "$share/grp/sensors/temp", "sensors/hum", false},
		// The prefixed form must NOT match, the wrapped filter must.
		{"prefixed topic does not match", "$share/grp/sensors/temp", "$share/grp/sensors/temp", false},

		// The ShareName never participates in matching.
		{"sharename ignored a", "$share/a/x", "x", true},
		{"sharename ignored b", "$share/b/x", "x", true},
		{"sharename not a level", "$share/grp/x", "grp/x", false},

		// Wildcards inside the effective filter behave exactly as they do
		// standalone.
		{"plus", "$share/grp/sensors/+", "sensors/temp", true},
		{"plus wrong depth", "$share/grp/sensors/+", "sensors/temp/raw", false},
		{"hash", "$share/grp/sensors/#", "sensors/temp/raw", true},
		{"hash parent level", "$share/grp/sensors/#", "sensors", true},
		{"hash only", "$share/grp/#", "anything/goes", true},
		{"plus only", "$share/grp/+", "x", true},
		{"empty level", "$share/grp/sport/+", "sport/", true},

		// MQTT-4.7.2-1 applies to the EFFECTIVE filter: a wildcard-first
		// wrapped filter still may not pick up "$..." topics.
		{"hash vs $SYS", "$share/g/#", "$SYS/x", false},
		{"plus vs $SYS", "$share/g/+/x", "$SYS/x", false},
		{"literal $SYS wrapped", "$share/g/$SYS/#", "$SYS/broker/load", true},
		{"literal $SYS wrapped exact", "$share/g/$SYS/x", "$SYS/x", true},

		// Shapes that cannot be a well-formed shared subscription stay
		// ordinary literal filters (unchanged pre-existing behavior).
		{"bare $share literal", "$share", "$share", true},
		{"bare $share vs topic", "$share", "x", false},
		{"no third level", "$share/name", "$share/name", true},
		{"no third level vs stripped", "$share/name", "name", false},
		{"trailing separator only", "$share/name/", "$share/name/", true},
		{"empty sharename", "$share//f", "$share//f", true},
		{"empty sharename not stripped", "$share//f", "f", false},
		{"plus in sharename", "$share/+/f", "$share/g/f", true},
		{"plus in sharename not stripped", "$share/+/f", "f", false},
		{"hash in sharename not stripped", "$share/#/f", "f", false},
		{"prefix not at start", "a/$share/g/f", "a/$share/g/f", true},
		{"case sensitive prefix", "$SHARE/g/f", "f", false},
		{"case sensitive prefix literal", "$SHARE/g/f", "$SHARE/g/f", true},

		// A shared subscription whose ShareName is itself "$"-ish or
		// contains a '$' is fine — only '+' and '#' are forbidden there.
		{"dollar sharename", "$share/$g/x", "x", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := MatchTopic(tt.filter, tt.topic); got != tt.want {
				t.Errorf("MatchTopic(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
			}
		})
	}
}

// TestMatchTopicSharedEquivalence pins the core invariant: wrapping any
// filter in a well-formed "$share/{ShareName}/" prefix must not change what
// it matches.
func TestMatchTopicSharedEquivalence(t *testing.T) {
	t.Parallel()

	filters := []string{"a/b", "a/+", "a/#", "#", "+", "$SYS/#", "/", "a/+/c", "sport/"}
	topics := []string{"a/b", "a/b/c", "a", "$SYS/broker", "/", "", "sport/", "x"}

	for _, f := range filters {
		for _, topic := range topics {
			want := MatchTopic(f, topic)
			shared := "$share/grp/" + f
			if got := MatchTopic(shared, topic); got != want {
				t.Errorf("MatchTopic(%q, %q) = %v, want %v (same as MatchTopic(%q, %q))",
					shared, topic, got, want, f, topic)
			}
		}
	}
}

// TestValidateTopicFilterShared covers the §4.8.2 structural rules the
// validator now enforces, and pins the borderline shapes documented on
// ValidateTopicFilter.
func TestValidateTopicFilterShared(t *testing.T) {
	t.Parallel()

	good := []struct {
		name   string
		filter string
	}{
		{"simple", "$share/grp/sensors/temp"},
		{"single level", "$share/grp/x"},
		{"plus", "$share/grp/sensors/+"},
		{"hash", "$share/grp/sensors/#"},
		{"hash only", "$share/grp/#"},
		{"plus only", "$share/grp/+"},
		{"one char sharename", "$share/g/x"},
		{"sharename with dollar", "$share/$g/x"},
		{"wrapped filter with empty levels", "$share/g//a"},
		{"wrapped $SYS", "$share/g/$SYS/#"},
		// "$share" alone does not start with "$share/", so it is not a
		// shared subscription at all — an ordinary one-level filter.
		{"bare $share is an ordinary filter", "$share"},
		// Not the prefix (case sensitive), so ordinary rules apply.
		{"uppercase is an ordinary filter", "$SHARE/g/f"},
		{"prefix not at start", "a/$share/g/f"},
	}
	for _, tt := range good {
		t.Run("good/"+tt.name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateTopicFilter(tt.filter); err != nil {
				t.Errorf("ValidateTopicFilter(%q) = %v, want nil", tt.filter, err)
			}
		})
	}

	bad := []struct {
		name   string
		filter string
	}{
		{"prefix only", "$share/"},
		{"no separator after sharename", "$share/name"},
		{"empty wrapped filter", "$share/name/"},
		{"empty sharename", "$share//f"},
		{"empty sharename and filter", "$share//"},
		{"plus sharename", "$share/+/f"},
		{"hash sharename", "$share/#/f"},
		{"plus inside sharename", "$share/na+me/f"},
		{"hash inside sharename", "$share/na#me/f"},
		{"wrapped hash not last", "$share/g/a/#/b"},
		{"wrapped hash not whole level", "$share/g/a#"},
		{"wrapped plus not whole level", "$share/g/a+/b"},
	}
	for _, tt := range bad {
		t.Run("bad/"+tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateTopicFilter(tt.filter)
			if err == nil {
				t.Fatalf("ValidateTopicFilter(%q) = nil, want error", tt.filter)
			}
			if !errors.Is(err, ErrProtocolViolation) {
				t.Errorf("ValidateTopicFilter(%q) error = %v, want wrapping ErrProtocolViolation", tt.filter, err)
			}
		})
	}
}

// TestValidateTopicFilterSharedNonSharedUnchanged guards the "keep
// non-$share behavior byte-for-byte identical" requirement: the shared
// branch must be reachable only through the exact "$share/" prefix.
func TestValidateTopicFilterSharedNonSharedUnchanged(t *testing.T) {
	t.Parallel()

	good := []string{"#", "+", "a/+/#", "a/b", "$SYS/#", "/", "a//b", "$share"}
	for _, s := range good {
		if err := ValidateTopicFilter(s); err != nil {
			t.Errorf("ValidateTopicFilter(%q) = %v, want nil", s, err)
		}
	}

	bad := []string{"", "a+", "a/#/b", "#b", "a#", "a/++/b"}
	for _, s := range bad {
		if err := ValidateTopicFilter(s); !errors.Is(err, ErrProtocolViolation) {
			t.Errorf("ValidateTopicFilter(%q) error = %v, want wrapping ErrProtocolViolation", s, err)
		}
	}
}

// TestSplitShared exercises the decomposition helper directly so the
// structural contract MatchTopic and ValidateTopicFilter share is pinned in
// one place.
func TestSplitShared(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in        string
		wantShare string
		wantRest  string
		wantOK    bool
	}{
		{"$share/g/a/b", "g", "a/b", true},
		{"$share/g/#", "g", "#", true},
		{"$share/g//a", "g", "/a", true},
		{"$share/$g/a", "$g", "a", true},
		{"$share", "", "", false},
		{"$share/", "", "", false},
		{"$share/g", "", "", false},
		{"$share/g/", "", "", false},
		{"$share//a", "", "", false},
		{"$share/+/a", "", "", false},
		{"$share/#/a", "", "", false},
		{"$share/g+h/a", "", "", false},
		{"a/$share/g/a", "", "", false},
		{"", "", "", false},
		{"#", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			share, rest, ok := splitShared(tt.in)
			if share != tt.wantShare || rest != tt.wantRest || ok != tt.wantOK {
				t.Errorf("splitShared(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tt.in, share, rest, ok, tt.wantShare, tt.wantRest, tt.wantOK)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Round 4 spec-conformance audit (F-22..F-34)
//
// Every decode-side rejection below is pinned twice: the audit's probe byte
// vector must be refused, and the nearest still-legal vector must keep
// decoding, so a future "fix" cannot satisfy the test by rejecting more.
// ---------------------------------------------------------------------------

// encodeErr runs enc and returns its error, discarding the bytes.
func encodeErr(enc func(w io.Writer) error) error { return enc(io.Discard) }

// ---------------------------------------------------------------------------
// F-22: Topic Alias 0 is a Protocol Error (§3.3.2.3.4)
// ---------------------------------------------------------------------------

func TestTopicAliasZeroRejected(t *testing.T) {
	t.Parallel()

	t.Run("decode property block", func(t *testing.T) {
		t.Parallel()

		if _, err := decodeProperties(newCursor(propBlock([]byte{0x23, 0x00, 0x00})), tgPublish); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("alias 0: got %v, want ErrMalformedPacket", err)
		}
		// The nearest legal value still decodes.
		got, err := decodeProperties(newCursor(propBlock([]byte{0x23, 0x00, 0x01})), tgPublish)
		if err != nil {
			t.Fatalf("alias 1: %v", err)
		}
		if got.TopicAlias == nil || *got.TopicAlias != 1 {
			t.Fatalf("alias 1 decoded as %+v", got.TopicAlias)
		}
	})

	t.Run("decode PUBLISH", func(t *testing.T) {
		t.Parallel()

		// Topic "t" plus a Topic Alias of 0.
		if _, err := DecodePublish(V50, 0x30, []byte{0x00, 0x01, 't', 0x03, 0x23, 0x00, 0x00}); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("topic + alias 0: got %v, want ErrMalformedPacket", err)
		}
		// The empty-topic hole: alias 0 no longer satisfies the "empty topic
		// needs an alias" guard.
		if _, err := DecodePublish(V50, 0x30, []byte{0x00, 0x00, 0x03, 0x23, 0x00, 0x00}); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("empty topic + alias 0: got %v, want ErrMalformedPacket", err)
		}
		// Nearest legal vector: the same packet with alias 1.
		if _, err := DecodePublish(V50, 0x30, []byte{0x00, 0x00, 0x03, 0x23, 0x00, 0x01}); err != nil {
			t.Fatalf("empty topic + alias 1: %v", err)
		}
	})

	t.Run("encode", func(t *testing.T) {
		t.Parallel()

		zero, one := uint16(0), uint16(1)
		bad := &PublishPacket{Version: V50, Topic: "t", Properties: &Properties{TopicAlias: &zero}}
		if err := encodeErr(bad.Encode); !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("encode alias 0: got %v, want ErrProtocolViolation", err)
		}
		ok := &PublishPacket{Version: V50, Topic: "t", Properties: &Properties{TopicAlias: &one}}
		if err := encodeErr(ok.Encode); err != nil {
			t.Fatalf("encode alias 1: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// F-23: DecodePublish rejects an unsupported protocol version
// ---------------------------------------------------------------------------

func TestDecodePublishUnsupportedVersion(t *testing.T) {
	t.Parallel()

	// Topic "t" then one byte that is a v5 property length (0x00, no
	// properties) and a v3 payload alike — decodable under both real
	// versions, so only the version guard can distinguish the outcomes.
	body := []byte{0x00, 0x01, 't', 0x00}

	for _, v := range []Version{Version(0), Version(3), Version(6), Version(255)} {
		if _, err := DecodePublish(v, 0x30, body); !errors.Is(err, ErrProtocolViolation) {
			t.Errorf("DecodePublish(version %d): got %v, want ErrProtocolViolation", byte(v), err)
		}
	}
	for _, v := range []Version{V311, V50} {
		if _, err := DecodePublish(v, 0x30, body); err != nil {
			t.Errorf("DecodePublish(%s): %v", v, err)
		}
	}
}

// ---------------------------------------------------------------------------
// F-24: every encoder rejects an unsupported protocol version
// ---------------------------------------------------------------------------

func TestEncodeUnsupportedVersionRejected(t *testing.T) {
	t.Parallel()

	build := map[string]func(v Version) func(w io.Writer) error{
		"PUBLISH": func(v Version) func(io.Writer) error {
			return (&PublishPacket{Version: v, Topic: "t", Payload: []byte("x")}).Encode
		},
		"PUBACK": func(v Version) func(io.Writer) error {
			return (&AckPacket{Version: v, Type: Puback, PacketID: 1}).EncodeAck
		},
		"PUBREL": func(v Version) func(io.Writer) error {
			return (&AckPacket{Version: v, Type: Pubrel, PacketID: 1}).EncodeAck
		},
		"SUBSCRIBE": func(v Version) func(io.Writer) error {
			return (&SubscribePacket{
				Version: v, PacketID: 1,
				Subscriptions: []Subscription{{Filter: "a", Options: SubscribeOptions{QoS: 1}}},
			}).Encode
		},
		"UNSUBSCRIBE": func(v Version) func(io.Writer) error {
			return (&UnsubscribePacket{Version: v, PacketID: 1, Filters: []string{"a"}}).Encode
		},
		"CONNECT": func(v Version) func(io.Writer) error {
			return (&ConnectPacket{Version: v, ClientID: "c"}).Encode
		},
		"DISCONNECT": func(v Version) func(io.Writer) error {
			return (&DisconnectPacket{Version: v}).Encode
		},
	}

	for name, mk := range build {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for _, v := range []Version{Version(0), Version(3), Version(6), Version(255)} {
				if err := encodeErr(mk(v)); !errors.Is(err, ErrProtocolViolation) {
					t.Errorf("version %d: got %v, want ErrProtocolViolation", byte(v), err)
				}
			}
			for _, v := range []Version{V311, V50} {
				if err := encodeErr(mk(v)); err != nil {
					t.Errorf("%s: %v", v, err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// F-25: packet identifier 0 is illegal wherever one is carried
// ([MQTT-2.2.1-3])
// ---------------------------------------------------------------------------

func TestPacketIdentifierZeroRejectedOnEncode(t *testing.T) {
	t.Parallel()

	for _, v := range []Version{V311, V50} {
		for _, typ := range []PacketType{Puback, Pubrec, Pubrel, Pubcomp} {
			if err := encodeErr((&AckPacket{Version: v, Type: typ, PacketID: 0}).EncodeAck); !errors.Is(err, ErrProtocolViolation) {
				t.Errorf("%s %s id 0: got %v, want ErrProtocolViolation", v, typ, err)
			}
			if err := encodeErr((&AckPacket{Version: v, Type: typ, PacketID: 1}).EncodeAck); err != nil {
				t.Errorf("%s %s id 1: %v", v, typ, err)
			}
		}

		sub := func(id uint16) func(io.Writer) error {
			return (&SubscribePacket{
				Version: v, PacketID: id,
				Subscriptions: []Subscription{{Filter: "a", Options: SubscribeOptions{QoS: 1}}},
			}).Encode
		}
		if err := encodeErr(sub(0)); !errors.Is(err, ErrProtocolViolation) {
			t.Errorf("%s SUBSCRIBE id 0: got %v, want ErrProtocolViolation", v, err)
		}
		if err := encodeErr(sub(1)); err != nil {
			t.Errorf("%s SUBSCRIBE id 1: %v", v, err)
		}

		unsub := func(id uint16) func(io.Writer) error {
			return (&UnsubscribePacket{Version: v, PacketID: id, Filters: []string{"a"}}).Encode
		}
		if err := encodeErr(unsub(0)); !errors.Is(err, ErrProtocolViolation) {
			t.Errorf("%s UNSUBSCRIBE id 0: got %v, want ErrProtocolViolation", v, err)
		}
		if err := encodeErr(unsub(1)); err != nil {
			t.Errorf("%s UNSUBSCRIBE id 1: %v", v, err)
		}
	}
}

func TestPacketIdentifierZeroRejectedOnDecode(t *testing.T) {
	t.Parallel()

	for _, v := range []Version{V311, V50} {
		for _, typ := range []PacketType{Puback, Pubrec, Pubrel, Pubcomp} {
			if _, err := DecodeAck(v, typ, []byte{0x00, 0x00}); !errors.Is(err, ErrMalformedPacket) {
				t.Errorf("%s %s id 0: got %v, want ErrMalformedPacket", v, typ, err)
			}
			if _, err := DecodeAck(v, typ, []byte{0x00, 0x01}); err != nil {
				t.Errorf("%s %s id 1: %v", v, typ, err)
			}
		}
	}

	subackCases := []struct {
		v       Version
		bad, ok []byte
		name    string
	}{
		{V311, []byte{0x00, 0x00, 0x01}, []byte{0x00, 0x01, 0x01}, "v3 SUBACK"},
		{V50, []byte{0x00, 0x00, 0x00, 0x01}, []byte{0x00, 0x01, 0x00, 0x01}, "v5 SUBACK"},
	}
	for _, tc := range subackCases {
		if _, err := DecodeSuback(tc.v, tc.bad); !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("%s id 0: got %v, want ErrMalformedPacket", tc.name, err)
		}
		if _, err := DecodeSuback(tc.v, tc.ok); err != nil {
			t.Errorf("%s id 1: %v", tc.name, err)
		}
	}

	unsubackCases := []struct {
		v       Version
		bad, ok []byte
		name    string
	}{
		{V311, []byte{0x00, 0x00}, []byte{0x00, 0x01}, "v3 UNSUBACK"},
		{V50, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x00, 0x01, 0x00, 0x00}, "v5 UNSUBACK"},
	}
	for _, tc := range unsubackCases {
		if _, err := DecodeUnsuback(tc.v, tc.bad); !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("%s id 0: got %v, want ErrMalformedPacket", tc.name, err)
		}
		if _, err := DecodeUnsuback(tc.v, tc.ok); err != nil {
			t.Errorf("%s id 1: %v", tc.name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// F-26: DUP must be 0 at QoS 0 ([MQTT-3.3.1-2])
// ---------------------------------------------------------------------------

func TestPublishDupAtQoSZeroRejected(t *testing.T) {
	t.Parallel()

	t.Run("ValidateFlags", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name   string
			header byte
			ok     bool
		}{
			{"dup at qos0", byte(Publish)<<4 | 0x08, false},        // 0x38
			{"dup+retain at qos0", byte(Publish)<<4 | 0x09, false}, // 0x39
			{"qos0 no dup", byte(Publish) << 4, true},              // 0x30
			{"retain at qos0", byte(Publish)<<4 | 0x01, true},      // 0x31
			{"dup at qos1", byte(Publish)<<4 | 0x0A, true},         // 0x3A
			{"dup at qos2", byte(Publish)<<4 | 0x0C, true},         // 0x3C
		}
		for _, tc := range cases {
			err := (Frame{Header: tc.header}).ValidateFlags()
			switch {
			case tc.ok && err != nil:
				t.Errorf("%s (%#02x): unexpected error %v", tc.name, tc.header, err)
			case !tc.ok && !errors.Is(err, ErrMalformedPacket):
				t.Errorf("%s (%#02x): got %v, want ErrMalformedPacket", tc.name, tc.header, err)
			}
		}
	})

	t.Run("DecodePublish", func(t *testing.T) {
		t.Parallel()

		for _, v := range []Version{V311, V50} {
			body := []byte{0x00, 0x01, 't'}
			if v == V50 {
				body = append(body, 0x00) // empty property block
			}
			if _, err := DecodePublish(v, 0x38, body); !errors.Is(err, ErrMalformedPacket) {
				t.Errorf("%s dup at qos0: got %v, want ErrMalformedPacket", v, err)
			}
			// Nearest legal vectors: same frame without DUP, and DUP at QoS 1.
			if _, err := DecodePublish(v, 0x30, body); err != nil {
				t.Errorf("%s qos0 without dup: %v", v, err)
			}
			qos1 := []byte{0x00, 0x01, 't', 0x00, 0x07}
			if v == V50 {
				qos1 = append(qos1, 0x00)
			}
			if _, err := DecodePublish(v, 0x3A, qos1); err != nil {
				t.Errorf("%s dup at qos1: %v", v, err)
			}
		}
	})

	t.Run("Encode", func(t *testing.T) {
		t.Parallel()

		for _, v := range []Version{V311, V50} {
			bad := &PublishPacket{Version: v, Topic: "t", QoS: 0, Dup: true}
			if err := encodeErr(bad.Encode); !errors.Is(err, ErrProtocolViolation) {
				t.Errorf("%s encode dup at qos0: got %v, want ErrProtocolViolation", v, err)
			}
			ok := &PublishPacket{Version: v, Topic: "t", QoS: 1, PacketID: 7, Dup: true}
			if err := encodeErr(ok.Encode); err != nil {
				t.Errorf("%s encode dup at qos1: %v", v, err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// F-27: non-minimal variable byte integers ([MQTT-1.5.5-1])
// ---------------------------------------------------------------------------

func TestVarintNonMinimalRejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		in    []byte
		want  uint32
		valid bool
	}{
		{"one byte 0", []byte{0x00}, 0, true},
		{"one byte 1", []byte{0x01}, 1, true},
		{"one byte 127", []byte{0x7F}, 127, true},
		{"two bytes 128", []byte{0x80, 0x01}, 128, true},
		{"three bytes 16384", []byte{0x80, 0x80, 0x01}, 16384, true},
		{"four bytes max", []byte{0xFF, 0xFF, 0xFF, 0x7F}, maxVarint, true},
		{"two bytes for 0", []byte{0x80, 0x00}, 0, false},
		{"two bytes for 1", []byte{0x81, 0x00}, 0, false},
		{"two bytes for 127", []byte{0xFF, 0x00}, 0, false},
		{"three bytes for 128", []byte{0x80, 0x81, 0x00}, 0, false},
		{"four bytes for 1", []byte{0x81, 0x80, 0x80, 0x00}, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotStream, errStream := readVarintFrom(bytes.NewReader(tc.in))
			gotCursor, errCursor := newCursor(tc.in).readVarint()
			if tc.valid {
				if errStream != nil || gotStream != tc.want {
					t.Errorf("readVarintFrom(%x) = %d, %v; want %d, nil", tc.in, gotStream, errStream, tc.want)
				}
				if errCursor != nil || gotCursor != tc.want {
					t.Errorf("cursor.readVarint(%x) = %d, %v; want %d, nil", tc.in, gotCursor, errCursor, tc.want)
				}
				return
			}
			if !errors.Is(errStream, ErrMalformedPacket) {
				t.Errorf("readVarintFrom(%x) = %d, %v; want ErrMalformedPacket", tc.in, gotStream, errStream)
			}
			if !errors.Is(errCursor, ErrMalformedPacket) {
				t.Errorf("cursor.readVarint(%x) = %d, %v; want ErrMalformedPacket", tc.in, gotCursor, errCursor)
			}
		})
	}
}

// TestVarintNonMinimalRejectedInContext walks the three places a varint is
// read from untrusted bytes: the fixed-header remaining length, the property
// block length and a Subscription Identifier value.
func TestVarintNonMinimalRejectedInContext(t *testing.T) {
	t.Parallel()

	// Fixed-header remaining length "0" spelled in two bytes.
	if _, err := ReadFrame(bytes.NewReader([]byte{byte(Pingreq) << 4, 0x80, 0x00}), 1<<20); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("ReadFrame non-minimal length: got %v, want ErrMalformedPacket", err)
	}
	if _, err := ReadFrame(bytes.NewReader([]byte{byte(Pingreq) << 4, 0x00}), 1<<20); err != nil {
		t.Errorf("ReadFrame minimal length: %v", err)
	}

	// Property block length "2" spelled in two bytes.
	if _, err := decodeProperties(newCursor([]byte{0x82, 0x00, 0x01, 0x01}), tgPublish); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("property length non-minimal: got %v, want ErrMalformedPacket", err)
	}
	if _, err := decodeProperties(newCursor([]byte{0x02, 0x01, 0x01}), tgPublish); err != nil {
		t.Errorf("property length minimal: %v", err)
	}

	// Subscription Identifier "1" spelled in two bytes.
	if _, err := decodeProperties(newCursor(propBlock([]byte{0x0B, 0x81, 0x00})), tgPublish); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("subscription identifier non-minimal: got %v, want ErrMalformedPacket", err)
	}
	got, err := decodeProperties(newCursor(propBlock([]byte{0x0B, 0x01})), tgPublish)
	if err != nil {
		t.Fatalf("subscription identifier minimal: %v", err)
	}
	if len(got.SubscriptionIdentifiers) != 1 || got.SubscriptionIdentifiers[0] != 1 {
		t.Fatalf("subscription identifiers = %v, want [1]", got.SubscriptionIdentifiers)
	}
}

// ---------------------------------------------------------------------------
// F-28: at most one Subscription Identifier per SUBSCRIBE (§3.8.2.1.2)
// ---------------------------------------------------------------------------

func TestSubscriptionIdentifierMultiplicity(t *testing.T) {
	t.Parallel()

	t.Run("decode", func(t *testing.T) {
		t.Parallel()

		two := propBlock([]byte{0x0B, 0x01, 0x0B, 0x02})
		if _, err := decodeProperties(newCursor(two), tgSubscribe); !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("two ids on SUBSCRIBE: got %v, want ErrMalformedPacket", err)
		}
		// A PUBLISH legitimately carries one per matching subscription.
		got, err := decodeProperties(newCursor(two), tgPublish)
		if err != nil {
			t.Fatalf("two ids on PUBLISH: %v", err)
		}
		if len(got.SubscriptionIdentifiers) != 2 {
			t.Fatalf("PUBLISH ids = %v, want two", got.SubscriptionIdentifiers)
		}
		// One id on a SUBSCRIBE stays legal.
		got, err = decodeProperties(newCursor(propBlock([]byte{0x0B, 0x01})), tgSubscribe)
		if err != nil {
			t.Fatalf("one id on SUBSCRIBE: %v", err)
		}
		if len(got.SubscriptionIdentifiers) != 1 {
			t.Fatalf("SUBSCRIBE ids = %v, want one", got.SubscriptionIdentifiers)
		}
	})

	t.Run("encode", func(t *testing.T) {
		t.Parallel()

		two := &Properties{SubscriptionIdentifiers: []uint32{1, 2}}
		if err := two.encode(&bytes.Buffer{}, tgSubscribe); !errors.Is(err, ErrProtocolViolation) {
			t.Errorf("two ids on SUBSCRIBE: got %v, want ErrProtocolViolation", err)
		}
		if err := two.encode(&bytes.Buffer{}, tgPublish); err != nil {
			t.Errorf("two ids on PUBLISH: %v", err)
		}
		one := &Properties{SubscriptionIdentifiers: []uint32{1}}
		if err := one.encode(&bytes.Buffer{}, tgSubscribe); err != nil {
			t.Errorf("one id on SUBSCRIBE: %v", err)
		}

		// ... and through the packet encoder itself.
		pkt := &SubscribePacket{
			Version: V50, PacketID: 1,
			Subscriptions: []Subscription{{Filter: "a", Options: SubscribeOptions{QoS: 1}}},
			Properties:    two,
		}
		if err := encodeErr(pkt.Encode); !errors.Is(err, ErrProtocolViolation) {
			t.Errorf("SubscribePacket.Encode with two ids: got %v, want ErrProtocolViolation", err)
		}
		pkt.Properties = one
		if err := encodeErr(pkt.Encode); err != nil {
			t.Errorf("SubscribePacket.Encode with one id: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// F-29: property values the spec declares a Protocol Error
// ---------------------------------------------------------------------------

func TestPropertyValueRangesDecode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		content []byte
		target  propTarget
		valid   bool
	}{
		// Payload Format Indicator (§3.3.2.3.2): 0 or 1.
		{"payload format 0", []byte{0x01, 0x00}, tgPublish, true},
		{"payload format 1", []byte{0x01, 0x01}, tgPublish, true},
		{"payload format 2", []byte{0x01, 0x02}, tgPublish, false},
		{"payload format 255", []byte{0x01, 0xFF}, tgPublish, false},
		// Request Problem Information (§3.1.2.11.7) / Request Response
		// Information (§3.1.2.11.6): 0 or 1.
		{"request problem 1", []byte{0x17, 0x01}, tgConnect, true},
		{"request problem 2", []byte{0x17, 0x02}, tgConnect, false},
		{"request response 0", []byte{0x19, 0x00}, tgConnect, true},
		{"request response 2", []byte{0x19, 0x02}, tgConnect, false},
		// Receive Maximum (§3.1.2.11.3 / §3.2.2.3.3): never 0.
		{"receive maximum 1", []byte{0x21, 0x00, 0x01}, tgConnack, true},
		{"receive maximum 0", []byte{0x21, 0x00, 0x00}, tgConnack, false},
		// Maximum Packet Size (§3.1.2.11.4 / §3.2.2.3.6): never 0.
		{"maximum packet size 1", []byte{0x27, 0x00, 0x00, 0x00, 0x01}, tgConnack, true},
		{"maximum packet size 0", []byte{0x27, 0x00, 0x00, 0x00, 0x00}, tgConnack, false},
		// Topic Alias (§3.3.2.3.4): never 0 — see also F-22 above.
		{"topic alias 1", []byte{0x23, 0x00, 0x01}, tgPublish, true},
		{"topic alias 0", []byte{0x23, 0x00, 0x00}, tgPublish, false},
		// Maximum QoS (§3.2.2.3.4): 0 or 1 only.
		{"maximum qos 0", []byte{0x24, 0x00}, tgConnack, true},
		{"maximum qos 1", []byte{0x24, 0x01}, tgConnack, true},
		{"maximum qos 2", []byte{0x24, 0x02}, tgConnack, false},
		// Availability flags (§3.2.2.3.5 and following): 0 or 1.
		{"retain available 1", []byte{0x25, 0x01}, tgConnack, true},
		{"retain available 2", []byte{0x25, 0x02}, tgConnack, false},
		{"wildcard sub available 0", []byte{0x28, 0x00}, tgConnack, true},
		{"wildcard sub available 3", []byte{0x28, 0x03}, tgConnack, false},
		{"sub id available 1", []byte{0x29, 0x01}, tgConnack, true},
		{"sub id available 9", []byte{0x29, 0x09}, tgConnack, false},
		{"shared sub available 0", []byte{0x2A, 0x00}, tgConnack, true},
		{"shared sub available 255", []byte{0x2A, 0xFF}, tgConnack, false},
		// Unconstrained numerics keep accepting 0 and large values.
		{"topic alias maximum 0", []byte{0x22, 0x00, 0x00}, tgConnack, true},
		{"session expiry 0", []byte{0x11, 0x00, 0x00, 0x00, 0x00}, tgConnect, true},
		{"message expiry 0", []byte{0x02, 0x00, 0x00, 0x00, 0x00}, tgPublish, true},
		{"server keep alive 0", []byte{0x13, 0x00, 0x00}, tgConnack, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeProperties(newCursor(propBlock(tc.content)), tc.target)
			if tc.valid && err != nil {
				t.Errorf("decode %x: unexpected error %v", tc.content, err)
			}
			if !tc.valid && !errors.Is(err, ErrMalformedPacket) {
				t.Errorf("decode %x: got %v, want ErrMalformedPacket", tc.content, err)
			}
		})
	}
}

func TestPropertyValueRangesEncode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		props  *Properties
		target propTarget
		valid  bool
	}{
		{"payload format 1", &Properties{PayloadFormat: bptr(1)}, tgPublish, true},
		{"payload format 2", &Properties{PayloadFormat: bptr(2)}, tgPublish, false},
		{"request problem 1", &Properties{RequestProblemInfo: bptr(1)}, tgConnect, true},
		{"request problem 2", &Properties{RequestProblemInfo: bptr(2)}, tgConnect, false},
		{"request response 0", &Properties{RequestResponseInfo: bptr(0)}, tgConnect, true},
		{"request response 7", &Properties{RequestResponseInfo: bptr(7)}, tgConnect, false},
		{"receive maximum 1", &Properties{ReceiveMaximum: u16ptr(1)}, tgConnect, true},
		{"receive maximum 0", &Properties{ReceiveMaximum: u16ptr(0)}, tgConnect, false},
		{"maximum packet size 1", &Properties{MaximumPacketSize: u32ptr(1)}, tgConnect, true},
		{"maximum packet size 0", &Properties{MaximumPacketSize: u32ptr(0)}, tgConnect, false},
		{"topic alias 1", &Properties{TopicAlias: u16ptr(1)}, tgPublish, true},
		{"topic alias 0", &Properties{TopicAlias: u16ptr(0)}, tgPublish, false},
		{"maximum qos 1", &Properties{MaximumQoS: bptr(1)}, tgConnack, true},
		{"maximum qos 2", &Properties{MaximumQoS: bptr(2)}, tgConnack, false},
		{"retain available 1", &Properties{RetainAvailable: bptr(1)}, tgConnack, true},
		{"retain available 2", &Properties{RetainAvailable: bptr(2)}, tgConnack, false},
		{"wildcard sub available 0", &Properties{WildcardSubAvailable: bptr(0)}, tgConnack, true},
		{"wildcard sub available 2", &Properties{WildcardSubAvailable: bptr(2)}, tgConnack, false},
		{"sub id available 1", &Properties{SubIDAvailable: bptr(1)}, tgConnack, true},
		{"sub id available 2", &Properties{SubIDAvailable: bptr(2)}, tgConnack, false},
		{"shared sub available 0", &Properties{SharedSubAvailable: bptr(0)}, tgConnack, true},
		{"shared sub available 2", &Properties{SharedSubAvailable: bptr(2)}, tgConnack, false},
		// Unconstrained numerics may still be zero.
		{"topic alias maximum 0", &Properties{TopicAliasMaximum: u16ptr(0)}, tgConnect, true},
		{"session expiry 0", &Properties{SessionExpiryInterval: u32ptr(0)}, tgConnect, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.props.encode(&bytes.Buffer{}, tc.target)
			if tc.valid && err != nil {
				t.Errorf("encode: unexpected error %v", err)
			}
			if !tc.valid && !errors.Is(err, ErrProtocolViolation) {
				t.Errorf("encode: got %v, want ErrProtocolViolation", err)
			}
		})
	}
}

// TestPropertyValueRangesSymmetric proves encode and decode share one table:
// every value one direction refuses, the other refuses too.
func TestPropertyValueRangesSymmetric(t *testing.T) {
	t.Parallel()

	for id := range 0x100 {
		for _, v := range []uint32{0, 1, 2, 255, 65535, 1 << 20} {
			why := propertyValueViolation(byte(id), v)
			encErr := checkEncodeValue(byte(id), v)
			if (why != "") != (encErr != nil) {
				t.Fatalf("property 0x%02X value %d: table says %q but checkEncodeValue returned %v", id, v, why, encErr)
			}
			if encErr != nil && !errors.Is(encErr, ErrProtocolViolation) {
				t.Fatalf("property 0x%02X value %d: encode error %v does not wrap ErrProtocolViolation", id, v, encErr)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// F-30: CONNACK Session Present = 1 with a non-zero reason code
// ([MQTT-3.2.2-4] / v3 §3.2.2.2)
// ---------------------------------------------------------------------------

func TestDecodeConnackSessionPresentWithFailure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		v     Version
		body  []byte
		valid bool
	}{
		{"v3 present + accepted", V311, []byte{0x01, 0x00}, true},
		{"v3 absent + refused", V311, []byte{0x00, 0x05}, true},
		{"v3 present + refused", V311, []byte{0x01, 0x05}, false},
		{"v3 present + unknown refusal", V311, []byte{0x01, 0x7F}, false},
		{"v5 present + success", V50, []byte{0x01, 0x00, 0x00}, true},
		{"v5 absent + refused", V50, []byte{0x00, 0x86, 0x00}, true},
		{"v5 present + refused", V50, []byte{0x01, 0x86, 0x00}, false},
		{"v5 present + non-error non-zero", V50, []byte{0x01, 0x10, 0x00}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeConnack(tc.v, tc.body)
			if tc.valid && err != nil {
				t.Errorf("DecodeConnack(%s, %x): unexpected error %v", tc.v, tc.body, err)
			}
			if !tc.valid && !errors.Is(err, ErrMalformedPacket) {
				t.Errorf("DecodeConnack(%s, %x): got %v, want ErrMalformedPacket", tc.v, tc.body, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// F-31: SUBACK / UNSUBACK reason-code ranges (§3.9.3, §3.11.3)
// ---------------------------------------------------------------------------

func TestDecodeSubackReasonCodeRange(t *testing.T) {
	t.Parallel()

	// Every byte value, checked against the version's defined set: this is
	// exhaustive rather than sampled so a code added to one table without the
	// other cannot slip through.
	for code := range 0x100 {
		rc := byte(code)

		v3Body := []byte{0x00, 0x01, rc}
		_, err := DecodeSuback(V311, v3Body)
		if want := subackReasonV311[rc]; want && err != nil {
			t.Errorf("v3 SUBACK 0x%02X: unexpected error %v", rc, err)
		} else if !want && !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("v3 SUBACK 0x%02X: got %v, want ErrMalformedPacket", rc, err)
		}

		v5Body := []byte{0x00, 0x01, 0x00, rc}
		_, err = DecodeSuback(V50, v5Body)
		if want := subackReasonV50[rc]; want && err != nil {
			t.Errorf("v5 SUBACK 0x%02X: unexpected error %v", rc, err)
		} else if !want && !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("v5 SUBACK 0x%02X: got %v, want ErrMalformedPacket", rc, err)
		}

		unsubBody := []byte{0x00, 0x01, 0x00, rc}
		_, err = DecodeUnsuback(V50, unsubBody)
		if want := unsubackReasonV50[rc]; want && err != nil {
			t.Errorf("v5 UNSUBACK 0x%02X: unexpected error %v", rc, err)
		} else if !want && !errors.Is(err, ErrMalformedPacket) {
			t.Errorf("v5 UNSUBACK 0x%02X: got %v, want ErrMalformedPacket", rc, err)
		}
	}
}

// TestDecodeSubackReasonCodeProbes spells out the audit's concrete probes
// (and the neighbours that must stay legal) so the intent survives a future
// refactor of the exhaustive loop above.
func TestDecodeSubackReasonCodeProbes(t *testing.T) {
	t.Parallel()

	// The reported symptom: 0x03 used to surface as a nonsense "granted
	// QoS 3" with a nil error.
	if _, err := DecodeSuback(V311, []byte{0x00, 0x01, 0x03}); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("v3 SUBACK granted QoS 3: got %v, want ErrMalformedPacket", err)
	}
	if _, err := DecodeSuback(V50, []byte{0x00, 0x01, 0x00, 0x03}); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("v5 SUBACK granted QoS 3: got %v, want ErrMalformedPacket", err)
	}
	// A v5-only failure code is malformed on a v3.1.1 link.
	if _, err := DecodeSuback(V311, []byte{0x00, 0x01, 0x87}); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("v3 SUBACK 0x87: got %v, want ErrMalformedPacket", err)
	}
	if _, err := DecodeSuback(V50, []byte{0x00, 0x01, 0x00, 0x87}); err != nil {
		t.Errorf("v5 SUBACK 0x87: %v", err)
	}
	// UNSUBACK does not define the grant codes, SUBACK does not define
	// "no subscription existed".
	if _, err := DecodeUnsuback(V50, []byte{0x00, 0x01, 0x00, 0x01}); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("v5 UNSUBACK 0x01: got %v, want ErrMalformedPacket", err)
	}
	if _, err := DecodeSuback(V50, []byte{0x00, 0x01, 0x00, 0x11}); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("v5 SUBACK 0x11: got %v, want ErrMalformedPacket", err)
	}
	if _, err := DecodeUnsuback(V50, []byte{0x00, 0x01, 0x00, 0x11}); err != nil {
		t.Errorf("v5 UNSUBACK 0x11: %v", err)
	}
	// A multi-filter SUBACK rejects on the offending byte, not the first.
	if _, err := DecodeSuback(V50, []byte{0x00, 0x01, 0x00, 0x00, 0x02, 0x7F}); !errors.Is(err, ErrMalformedPacket) {
		t.Errorf("v5 SUBACK trailing 0x7F: got %v, want ErrMalformedPacket", err)
	}
	if _, err := DecodeSuback(V50, []byte{0x00, 0x01, 0x00, 0x00, 0x02, 0x80}); err != nil {
		t.Errorf("v5 SUBACK 0x00,0x02,0x80: %v", err)
	}
}

// ---------------------------------------------------------------------------
// F-34: ReadFrame caps the TOTAL packet size (§2.1.4), not just the
// remaining length
// ---------------------------------------------------------------------------

func TestReadFrameCapsTotalPacketSize(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		bodyLen  int
		overhead int // fixed header byte + remaining-length varint
	}{
		{"one-byte length", 5, 2},
		{"largest one-byte length", 127, 2},
		{"smallest two-byte length", 128, 3},
		{"three-byte length", 16384, 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			body := bytes.Repeat([]byte{'x'}, tc.bodyLen)
			total := uint32(tc.overhead + tc.bodyLen)

			var buf bytes.Buffer
			if err := writePacket(&buf, byte(Publish)<<4, body); err != nil {
				t.Fatalf("writePacket: %v", err)
			}
			if uint32(buf.Len()) != total {
				t.Fatalf("encoded %d bytes, want %d", buf.Len(), total)
			}

			// Exactly at the cap: accepted.
			raw := append([]byte(nil), buf.Bytes()...)
			f, err := ReadFrame(bytes.NewReader(raw), total)
			if err != nil {
				t.Fatalf("at cap %d: %v", total, err)
			}
			if len(f.Body) != tc.bodyLen {
				t.Fatalf("body = %d bytes, want %d", len(f.Body), tc.bodyLen)
			}
			// One byte under: refused, even though the remaining length alone
			// would still fit (that is the off-by-overhead this fixes).
			if _, err := ReadFrame(bytes.NewReader(raw), total-1); !errors.Is(err, ErrFrameTooLarge) {
				t.Fatalf("one under cap: got %v, want ErrFrameTooLarge", err)
			}
			// A cap smaller than the fixed header itself must not underflow.
			for _, tiny := range []uint32{0, 1, 2} {
				if _, err := ReadFrame(bytes.NewReader(raw), tiny); !errors.Is(err, ErrFrameTooLarge) {
					t.Fatalf("cap %d: got %v, want ErrFrameTooLarge", tiny, err)
				}
			}
		})
	}
}

// TestReadFrameTinyCapEmptyPacket pins the smallest legal packet against the
// smallest cap that can admit it: PINGREQ is two bytes on the wire.
func TestReadFrameTinyCapEmptyPacket(t *testing.T) {
	t.Parallel()

	raw := []byte{byte(Pingreq) << 4, 0x00}
	if _, err := ReadFrame(bytes.NewReader(raw), 2); err != nil {
		t.Fatalf("PINGREQ at cap 2: %v", err)
	}
	if _, err := ReadFrame(bytes.NewReader(raw), 1); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("PINGREQ at cap 1: got %v, want ErrFrameTooLarge", err)
	}
}
