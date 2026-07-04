// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import "fmt"

// Version is the MQTT protocol level carried in the CONNECT variable
// header (byte value, not a display string).
type Version byte

// Supported MQTT protocol levels.
const (
	// V311 is MQTT 3.1.1, protocol level 4.
	V311 Version = 4
	// V50 is MQTT 5.0, protocol level 5.
	V50 Version = 5
)

// String returns a human-readable name for the protocol version.
func (v Version) String() string {
	switch v {
	case V311:
		return "MQTT 3.1.1"
	case V50:
		return "MQTT 5.0"
	default:
		return fmt.Sprintf("MQTT(level %d)", byte(v))
	}
}

// Valid reports whether v is a protocol level this package implements.
func (v Version) Valid() bool {
	return v == V311 || v == V50
}
