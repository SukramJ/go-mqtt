// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the codec. All decode-side failures wrap
// [ErrMalformedPacket] (or [ErrFrameTooLarge]); all encode-side input
// violations wrap [ErrProtocolViolation] or [ErrStringTooLong]. Callers
// match them with [errors.Is].
var (
	// ErrMalformedPacket indicates a packet whose bytes could not be
	// decoded per the MQTT spec (truncated field, illegal length,
	// disallowed/duplicate property, ...). Every decoder returns a value
	// wrapping this sentinel; decoders never panic on arbitrary input.
	ErrMalformedPacket = errors.New("mqtt: malformed packet")

	// ErrProtocolViolation indicates a semantically illegal packet — one
	// that is structurally decodable but forbidden by the spec (e.g. a
	// v3.1.1 CONNECT carrying a password without a username).
	ErrProtocolViolation = errors.New("mqtt: protocol violation")

	// ErrStringTooLong indicates an attempt to encode a UTF-8 string or
	// binary field longer than 65535 bytes, which the two-byte length
	// prefix cannot represent. Guards against silent uint16 truncation.
	ErrStringTooLong = errors.New("mqtt: string exceeds 65535 bytes")

	// ErrFrameTooLarge indicates a fixed header advertising a remaining
	// length beyond the caller-supplied limit (see [ReadFrame]), or an
	// outbound packet whose body exceeds the variable-byte-integer range.
	// The size is checked before any body buffer is allocated.
	ErrFrameTooLarge = errors.New("mqtt: remaining length exceeds limit")
)

// wrapMalformed annotates a decode failure with context while keeping
// [ErrMalformedPacket] in the error chain for [errors.Is].
func wrapMalformed(reason string) error {
	return fmt.Errorf("%w: %s", ErrMalformedPacket, reason)
}
