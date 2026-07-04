// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

// Regression tests for the round-1 adversarial-review findings in the codec:
// inbound MQTT UTF-8 strings (topic names and string properties) must be
// rejected as Malformed Packets when they are not well-formed UTF-8 or carry
// U+0000 (§1.5.4), and DecodeSuback must reject an unknown protocol version
// like its four sibling decoders instead of returning a bogus success.

import (
	"errors"
	"testing"
)

// TestDecodePublishRejectsMalformedTopicUTF8 proves an inbound PUBLISH whose
// topic name is not well-formed UTF-8, or contains U+0000, is a Malformed
// Packet rather than a corrupt topic handed to the application.
func TestDecodePublishRejectsMalformedTopicUTF8(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body []byte
	}{
		// Topic length 2, bytes 0xFF 0xFE: not valid UTF-8.
		{"invalid-utf8", []byte{0x00, 0x02, 0xFF, 0xFE}},
		// Topic length 2, bytes 'a' 0x00: embedded U+0000.
		{"embedded-nul", []byte{0x00, 0x02, 0x61, 0x00}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodePublish(V50, byte(Publish)<<4, tt.body); !errors.Is(err, ErrMalformedPacket) {
				t.Fatalf("DecodePublish err = %v, want ErrMalformedPacket", err)
			}
		})
	}
}

// TestDecodePublishRejectsMalformedStringProperty proves the well-formedness
// check applies to string properties too, not just the topic: a v5 PUBLISH
// whose Content Type property carries U+0000 is a Malformed Packet.
func TestDecodePublishRejectsMalformedStringProperty(t *testing.T) {
	t.Parallel()

	// Topic "t"; property block of 5 bytes: 0x03 (Content Type) + string
	// length 2 "a\x00" (embedded U+0000).
	body := []byte{
		0x00, 0x01, 0x74, // topic "t"
		0x05,                         // property length
		0x03, 0x00, 0x02, 0x61, 0x00, // Content Type = "a\x00"
	}
	if _, err := DecodePublish(V50, byte(Publish)<<4, body); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("DecodePublish err = %v, want ErrMalformedPacket", err)
	}
}

// TestDecodeSubackRejectsUnknownVersion proves DecodeSuback rejects an
// unsupported protocol version with ErrProtocolViolation (matching its four
// sibling decoders) rather than misparsing a v5-shaped body as v3 return
// codes and returning a bogus success.
func TestDecodeSubackRejectsUnknownVersion(t *testing.T) {
	t.Parallel()

	body := []byte{0x00, 0x01, 0x00} // packet id 1, one reason-code byte
	if _, err := DecodeSuback(Version(9), body); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("DecodeSuback(v9) err = %v, want ErrProtocolViolation", err)
	}
	// The two supported versions still decode.
	if _, err := DecodeSuback(V311, []byte{0x00, 0x01, 0x00}); err != nil {
		t.Fatalf("DecodeSuback(v3): %v", err)
	}
	if _, err := DecodeSuback(V50, []byte{0x00, 0x01, 0x00, 0x00}); err != nil {
		t.Fatalf("DecodeSuback(v5): %v", err)
	}
}
