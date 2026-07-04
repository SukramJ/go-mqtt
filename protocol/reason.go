// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import "fmt"

// ReasonCode is a single-byte MQTT 5.0 reason code carried in CONNACK,
// PUBACK/PUBREC/PUBREL/PUBCOMP, SUBACK/UNSUBACK, DISCONNECT and AUTH.
// Values >= 0x80 denote failure (see [ReasonCode.IsError]). In MQTT
// 3.1.1 these packets carry narrower return/QoS bytes; this package maps
// the 3.1.1 CONNACK return codes onto their v5 equivalents.
type ReasonCode byte

// MQTT 5.0 reason codes (spec §2.4). Some byte values carry more than
// one spec name depending on the packet that uses them (0x00 is Success,
// Normal disconnection and Granted QoS 0); the aliases below share the
// value and [ReasonCode.String] returns the canonical name.
const (
	Success                             ReasonCode = 0x00
	NormalDisconnection                 ReasonCode = 0x00
	GrantedQoS0                         ReasonCode = 0x00
	GrantedQoS1                         ReasonCode = 0x01
	GrantedQoS2                         ReasonCode = 0x02
	DisconnectWithWillMessage           ReasonCode = 0x04
	NoMatchingSubscribers               ReasonCode = 0x10
	NoSubscriptionExisted               ReasonCode = 0x11
	ContinueAuthentication              ReasonCode = 0x18
	ReAuthenticate                      ReasonCode = 0x19
	UnspecifiedError                    ReasonCode = 0x80
	MalformedPacketReason               ReasonCode = 0x81
	ProtocolErrorReason                 ReasonCode = 0x82
	ImplementationSpecificError         ReasonCode = 0x83
	UnsupportedProtocolVersion          ReasonCode = 0x84
	ClientIdentifierNotValid            ReasonCode = 0x85
	BadUserNameOrPassword               ReasonCode = 0x86
	NotAuthorized                       ReasonCode = 0x87
	ServerUnavailable                   ReasonCode = 0x88
	ServerBusy                          ReasonCode = 0x89
	Banned                              ReasonCode = 0x8A
	ServerShuttingDown                  ReasonCode = 0x8B
	BadAuthenticationMethod             ReasonCode = 0x8C
	KeepAliveTimeout                    ReasonCode = 0x8D
	SessionTakenOver                    ReasonCode = 0x8E
	TopicFilterInvalid                  ReasonCode = 0x8F
	TopicNameInvalid                    ReasonCode = 0x90
	PacketIdentifierInUse               ReasonCode = 0x91
	PacketIdentifierNotFound            ReasonCode = 0x92
	ReceiveMaximumExceeded              ReasonCode = 0x93
	TopicAliasInvalid                   ReasonCode = 0x94
	PacketTooLargeReason                ReasonCode = 0x95
	MessageRateTooHigh                  ReasonCode = 0x96
	QuotaExceeded                       ReasonCode = 0x97
	AdministrativeAction                ReasonCode = 0x98
	PayloadFormatInvalid                ReasonCode = 0x99
	RetainNotSupported                  ReasonCode = 0x9A
	QoSNotSupported                     ReasonCode = 0x9B
	UseAnotherServer                    ReasonCode = 0x9C
	ServerMoved                         ReasonCode = 0x9D
	SharedSubscriptionsNotSupported     ReasonCode = 0x9E
	ConnectionRateExceeded              ReasonCode = 0x9F
	MaximumConnectTime                  ReasonCode = 0xA0
	SubscriptionIdentifiersNotSupported ReasonCode = 0xA1
	WildcardSubscriptionsNotSupported   ReasonCode = 0xA2
)

// reasonNames maps each reason code value to its canonical spec name.
// Codes that alias the same byte (0x00) appear once.
var reasonNames = map[ReasonCode]string{
	0x00: "Success",
	0x01: "Granted QoS 1",
	0x02: "Granted QoS 2",
	0x04: "Disconnect with Will Message",
	0x10: "No matching subscribers",
	0x11: "No subscription existed",
	0x18: "Continue authentication",
	0x19: "Re-authenticate",
	0x80: "Unspecified error",
	0x81: "Malformed Packet",
	0x82: "Protocol Error",
	0x83: "Implementation specific error",
	0x84: "Unsupported Protocol Version",
	0x85: "Client Identifier not valid",
	0x86: "Bad User Name or Password",
	0x87: "Not authorized",
	0x88: "Server unavailable",
	0x89: "Server busy",
	0x8A: "Banned",
	0x8B: "Server shutting down",
	0x8C: "Bad authentication method",
	0x8D: "Keep Alive timeout",
	0x8E: "Session taken over",
	0x8F: "Topic Filter invalid",
	0x90: "Topic Name invalid",
	0x91: "Packet Identifier in use",
	0x92: "Packet Identifier not found",
	0x93: "Receive Maximum exceeded",
	0x94: "Topic Alias invalid",
	0x95: "Packet too large",
	0x96: "Message rate too high",
	0x97: "Quota exceeded",
	0x98: "Administrative action",
	0x99: "Payload format invalid",
	0x9A: "Retain not supported",
	0x9B: "QoS not supported",
	0x9C: "Use another server",
	0x9D: "Server moved",
	0x9E: "Shared Subscriptions not supported",
	0x9F: "Connection rate exceeded",
	0xA0: "Maximum connect time",
	0xA1: "Subscription Identifiers not supported",
	0xA2: "Wildcard Subscriptions not supported",
}

// IsError reports whether the reason code denotes a failure. MQTT 5.0
// reserves 0x80 and above for error reason codes (spec §2.4).
func (c ReasonCode) IsError() bool { return c >= 0x80 }

// String returns the reason code's canonical spec name, or a hex form
// for values this package does not name.
func (c ReasonCode) String() string {
	if name, ok := reasonNames[c]; ok {
		return name
	}
	return fmt.Sprintf("ReasonCode(0x%02X)", byte(c))
}

// v3ConnackReason maps an MQTT 3.1.1 CONNACK return code (§3.2.2.3) onto
// the equivalent MQTT 5.0 reason code so both versions surface the same
// descriptive text.
var v3ConnackReason = map[byte]ReasonCode{
	1: UnsupportedProtocolVersion,
	2: ClientIdentifierNotValid,
	3: ServerUnavailable,
	4: BadUserNameOrPassword,
	5: NotAuthorized,
}

// V3ConnackError translates an MQTT 3.1.1 CONNACK return code into an
// error. Return code 0 (connection accepted) yields nil; codes 1–5 map
// to their v5-equivalent descriptive message; any other non-zero code
// yields a generic refusal error.
func V3ConnackError(code byte) error {
	if code == 0 {
		return nil
	}
	if rc, ok := v3ConnackReason[code]; ok {
		return fmt.Errorf("mqtt: connection refused: %s", rc)
	}
	return fmt.Errorf("mqtt: connection refused with unknown return code 0x%02X", code)
}
