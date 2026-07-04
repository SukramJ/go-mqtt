// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the client. Callers match them with
// [errors.Is]; a broker-supplied failure reason surfaces as a
// *[ReasonError] matched with [errors.As].
var (
	// ErrNotConnected is returned by Publish/Subscribe/Unsubscribe when no
	// session is currently established.
	ErrNotConnected = errors.New("mqtt: not connected")

	// ErrAlreadyConnected is returned (wrapped) by Connect when a session
	// is already established; [Lifecycle] treats it as an idempotent
	// no-op rather than a reconnect trigger.
	ErrAlreadyConnected = errors.New("mqtt: already connected")

	// ErrConnectionLost is returned to every in-flight Publish/Subscribe
	// waiter when the underlying connection drops before its
	// acknowledgement arrives.
	ErrConnectionLost = errors.New("mqtt: connection lost")

	// ErrPacketTooLarge is returned when an outbound packet would exceed
	// the broker-advertised Maximum Packet Size.
	ErrPacketTooLarge = errors.New("mqtt: packet exceeds maximum packet size")

	// ErrPacketIDExhausted is returned when no MQTT packet identifier is
	// free, i.e. all 65535 non-zero identifiers are in use by in-flight
	// QoS 1/2 publishes and pending SUBSCRIBE/UNSUBSCRIBE requests.
	ErrPacketIDExhausted = errors.New("mqtt: packet identifiers exhausted")
)

// ReasonError reports a broker-supplied MQTT 5.0 failure reason code
// (§2.4) on a packet the client sent. It is returned by Publish for a
// PUBACK/PUBREC carrying a code >= 0x80, and by Subscribe for a SUBACK
// grant that is a failure code. Match it with [errors.As]:
//
//	var re *mqtt.ReasonError
//	if errors.As(err, &re) && re.Code == mqtt.NotAuthorized { ... }
type ReasonError struct {
	// Packet names the control packet the broker rejected, e.g.
	// "PUBLISH", "SUBSCRIBE".
	Packet string
	// Code is the reason code the broker returned (>= 0x80).
	Code ReasonCode
	// Reason is the optional human-readable Reason String property
	// (0x1F) the broker attached, empty when absent.
	Reason string
}

// Error implements the error interface.
func (e *ReasonError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("mqtt: %s rejected: %s (0x%02X): %s", e.Packet, e.Code, byte(e.Code), e.Reason)
	}
	return fmt.Sprintf("mqtt: %s rejected: %s (0x%02X)", e.Packet, e.Code, byte(e.Code))
}
