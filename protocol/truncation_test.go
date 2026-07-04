// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

// mergeProperties copies every non-zero field of src into dst. It is used to
// assemble a single Properties value carrying every property legal for a
// given context out of the single-field fixtures in propFixtures.
func mergeProperties(dst, src *Properties) {
	dv := reflect.ValueOf(dst).Elem()
	sv := reflect.ValueOf(src).Elem()
	for i := range sv.NumField() {
		sf := sv.Field(i)
		if sf.IsZero() {
			continue
		}
		dv.Field(i).Set(sf)
	}
}

// mergedProps returns a Properties value with every property propertySpec
// admits for target populated, so a decoder body built from it dispatches
// decodeOne for every legal property identifier at least once.
func mergedProps(target propTarget) *Properties {
	merged := &Properties{}
	for id, allowed := range propertySpec {
		if allowed&target == 0 {
			continue
		}
		if fix, ok := propFixtures[id]; ok {
			mergeProperties(merged, fix)
		}
	}
	return merged
}

// bodyOf encodes a full frame via enc and reads it back through ReadFrame,
// returning the fixed-header byte and the body — the two halves each
// Decode* function actually consumes.
func bodyOf(t *testing.T, enc func(w io.Writer) error) (header byte, body []byte) {
	t.Helper()
	var buf bytes.Buffer
	if err := enc(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	f, err := ReadFrame(&buf, 1<<20)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return f.Header, f.Body
}

// noShortForm rejects every strict prefix of a full body — used by
// decoders whose wire layout has no implicit-length trailing field and no
// spec short form.
func noShortForm(int) bool { return false }

// assertTruncation is the prefix-loop helper: it checks that decode(full)
// succeeds, then walks every strict prefix of full. Some MQTT 5 packets
// deliberately support truncated wire forms (2/3-byte acks, empty-body
// DISCONNECT/AUTH) and PUBLISH/SUBACK/UNSUBACK end in an implicit-length
// field (payload, reason-code list) that a truncated cut simply shortens
// rather than corrupts; validShort reports whether prefix length i is one
// of those legitimate cases. Every other prefix must error. No prefix may
// ever panic, per the "decode paths never panic" hard constraint.
func assertTruncation(t *testing.T, name string, full []byte, validShort func(i int) bool, decode func([]byte) error) {
	t.Helper()
	if err := decode(full); err != nil {
		t.Fatalf("%s: full body: unexpected error: %v", name, err)
	}
	for i := range full {
		func(i int) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: prefix len %d panicked: %v", name, i, r)
				}
			}()
			err := decode(full[:i])
			switch {
			case validShort(i) && err != nil:
				t.Errorf("%s: prefix len %d: expected a valid short form, got error: %v", name, i, err)
			case !validShort(i) && err == nil:
				t.Errorf("%s: prefix len %d: expected error, got nil", name, i)
			}
		}(i)
	}
}

// TestDecodeTruncationExhaustive builds a valid, property-rich body for
// every packet type this package decodes, in both protocol versions where
// applicable, and asserts truncation at every byte boundary either errors
// or lands on a documented spec short form — never panicking.
func TestDecodeTruncationExhaustive(t *testing.T) {
	t.Parallel()

	t.Run("Connack", func(t *testing.T) {
		t.Parallel()
		// No short form in either version: the ack-flags byte, reason code
		// and (v5) property-length prefix are all mandatory.
		assertTruncation(t, "v3", []byte{0x01, 0x00}, noShortForm, func(b []byte) error {
			_, err := DecodeConnack(V311, b)
			return err
		})

		var v5 bytes.Buffer
		v5.WriteByte(0x01) // session present
		v5.WriteByte(byte(BadUserNameOrPassword))
		if err := mergedProps(tgConnack).encode(&v5, tgConnack); err != nil {
			t.Fatalf("encode props: %v", err)
		}
		assertTruncation(t, "v5", v5.Bytes(), noShortForm, func(b []byte) error {
			_, err := DecodeConnack(V50, b)
			return err
		})
	})

	t.Run("Publish", func(t *testing.T) {
		t.Parallel()
		payload := []byte("hi")

		h3, body3 := bodyOf(t, (&PublishPacket{Version: V311, Topic: "a/b", Payload: payload, QoS: 1, PacketID: 9}).Encode)
		payloadStart3 := len(body3) - len(payload)
		assertTruncation(t, "v3", body3, func(i int) bool { return i >= payloadStart3 }, func(b []byte) error {
			_, err := DecodePublish(V311, h3, b)
			return err
		})

		h5, body5 := bodyOf(t, (&PublishPacket{
			Version: V50, Topic: "a/b", Payload: payload, QoS: 2, PacketID: 9,
			Properties: mergedProps(tgPublish),
		}).Encode)
		// The PUBLISH payload has no length prefix of its own (it is
		// whatever remains after the variable header): cutting anywhere
		// inside it yields a shorter, still-valid payload, not an error.
		payloadStart5 := len(body5) - len(payload)
		assertTruncation(t, "v5", body5, func(i int) bool { return i >= payloadStart5 }, func(b []byte) error {
			_, err := DecodePublish(V50, h5, b)
			return err
		})
	})

	for _, typ := range []PacketType{Puback, Pubrec, Pubrel, Pubcomp} {
		t.Run(typ.String(), func(t *testing.T) {
			t.Parallel()
			target, ok := ackTarget(typ)
			if !ok {
				t.Fatalf("no ack target for %s", typ)
			}

			_, body3 := bodyOf(t, (&AckPacket{Version: V311, Type: typ, PacketID: 9}).EncodeAck)
			assertTruncation(t, "v3", body3, noShortForm, func(b []byte) error {
				_, err := DecodeAck(V311, typ, b)
				return err
			})

			_, body5 := bodyOf(t, (&AckPacket{
				Version: V50, Type: typ, PacketID: 9, ReasonCode: QuotaExceeded,
				Properties: mergedProps(target),
			}).EncodeAck)
			// MQTT 5 acks support the 2-byte (no reason, no props) and
			// 3-byte (reason, no props) short forms; both are legitimate.
			assertTruncation(t, "v5", body5, func(i int) bool { return i == 2 || i == 3 }, func(b []byte) error {
				_, err := DecodeAck(V50, typ, b)
				return err
			})
		})
	}

	t.Run("Suback", func(t *testing.T) {
		t.Parallel()
		// A single reason code, so dropping it (rather than merely
		// shortening a multi-code list) always trips the explicit "no
		// reason codes" check — no ambiguous, non-erroring truncation.
		assertTruncation(t, "v3", []byte{0x00, 0x01, 0x00}, noShortForm, func(b []byte) error {
			_, err := DecodeSuback(V311, b)
			return err
		})

		var v5 bytes.Buffer
		v5.Write([]byte{0x00, 0x01})
		if err := mergedProps(tgSuback).encode(&v5, tgSuback); err != nil {
			t.Fatalf("encode props: %v", err)
		}
		v5.WriteByte(byte(GrantedQoS1))
		assertTruncation(t, "v5", v5.Bytes(), noShortForm, func(b []byte) error {
			_, err := DecodeSuback(V50, b)
			return err
		})
	})

	t.Run("Unsuback", func(t *testing.T) {
		t.Parallel()
		assertTruncation(t, "v3", []byte{0x00, 0x01}, noShortForm, func(b []byte) error {
			_, err := DecodeUnsuback(V311, b)
			return err
		})

		var v5 bytes.Buffer
		v5.Write([]byte{0x00, 0x01})
		if err := mergedProps(tgUnsuback).encode(&v5, tgUnsuback); err != nil {
			t.Fatalf("encode props: %v", err)
		}
		v5.WriteByte(byte(NoSubscriptionExisted))
		assertTruncation(t, "v5", v5.Bytes(), noShortForm, func(b []byte) error {
			_, err := DecodeUnsuback(V50, b)
			return err
		})
	})

	t.Run("Disconnect", func(t *testing.T) {
		t.Parallel()
		var v5 bytes.Buffer
		v5.WriteByte(byte(SessionTakenOver))
		if err := mergedProps(tgDisconnect).encode(&v5, tgDisconnect); err != nil {
			t.Fatalf("encode props: %v", err)
		}
		// The empty body (defaults to Normal Disconnection) and the
		// reason-only body (no properties) are both legitimate v5 short
		// forms.
		assertTruncation(t, "v5", v5.Bytes(), func(i int) bool { return i == 0 || i == 1 }, func(b []byte) error {
			_, err := DecodeDisconnect(V50, b)
			return err
		})
	})

	t.Run("Auth", func(t *testing.T) {
		t.Parallel()
		var body bytes.Buffer
		body.WriteByte(byte(ContinueAuthentication))
		if err := mergedProps(tgAuth).encode(&body, tgAuth); err != nil {
			t.Fatalf("encode props: %v", err)
		}
		// Same two short forms as DISCONNECT: empty (Success, implicit)
		// and reason-only (no properties).
		assertTruncation(t, "full", body.Bytes(), func(i int) bool { return i == 0 || i == 1 }, func(b []byte) error {
			_, err := DecodeAuth(b)
			return err
		})
	})
}
