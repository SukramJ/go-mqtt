// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

// Package protocol implements the MQTT wire codec — packet encode/decode
// for both MQTT 3.1.1 (protocol level 4, [V311]) and MQTT 5.0 (protocol
// level 5, [V50]) — that the client library builds on. One codebase covers
// both dialects: every packet type takes a [Version] argument (v5 packets
// are strict wire supersets of v3.1.1, adding a property block and/or
// reason codes). It is deliberately pure-Go with zero third-party
// dependencies, and every decoder is bounds-checked and never panics on
// arbitrary input (see [ReadFrame] and the cursor decoder in wire.go).
//
// Implemented:
//   - CONNECT (encode only — a client never decodes one) with optional
//     Last Will and Testament + username/password; [V50] adds the CONNECT
//     property block (session expiry, receive maximum, maximum packet
//     size, topic alias maximum, request problem/response information,
//     user properties, ...) and a will property block (delay interval,
//     message expiry, content type, response topic, correlation data,
//     payload format, user properties). CONNACK decode for both versions,
//     including the [V50] property block and the [V311] return-code to
//     [V50] reason-code mapping.
//   - PUBLISH at QoS 0, 1 and 2 (encode and decode), with the full
//     acknowledgement handshake: PUBACK (QoS 1) and PUBREC / PUBREL /
//     PUBCOMP (QoS 2 exactly-once) — one [AckPacket] shape for all four.
//     [V50] adds the PUBLISH property block (payload format, message
//     expiry, content type, response topic, correlation data, topic alias,
//     subscription identifiers, user properties) and a reason code +
//     property block on every ack in the QoS 2 handshake, including the
//     spec's short forms (empty body = success, 2-byte body = reason only).
//   - SUBSCRIBE (encode only) and UNSUBSCRIBE (encode only), each carrying
//     one or more topic filters per frame — [V50] adds the subscription
//     options (No Local, Retain As Published, Retain Handling) beyond the
//     QoS bits [V311] carries, plus a property block (subscription
//     identifier, user properties). SUBACK / UNSUBACK decode for both
//     versions, one reason code per filter (a [V311] UNSUBACK carries none
//     — an empty body is the whole packet).
//   - PINGREQ / PINGRESP heartbeat, identical on both versions.
//   - DISCONNECT (encode and decode, both versions, including the [V50]
//     reason code + property block and its short forms) and AUTH (decode
//     only — see Deliberately out of scope below).
//   - The MQTT 5.0 property model ([Properties]) with a single allow-table
//     ("propertySpec") used to validate both encode and decode against
//     which packet type(s) may carry which property, rejecting unknown,
//     disallowed, duplicated (other than User Property, and Subscription
//     Identifier on a PUBLISH, which the spec allows to repeat), truncated
//     or out-of-range properties — the value ranges the spec declares a
//     Protocol Error (Receive Maximum 0, Maximum Packet Size 0, Topic Alias
//     0, Maximum QoS and the boolean/availability flags outside {0,1}) are
//     enforced identically in both directions from one table
//     ("propertyValueViolation").
//   - Reason codes ([ReasonCode]) for all MQTT 5.0 control packets.
//   - Topic name/filter validation and wildcard matching ([MatchTopic],
//     [ValidateTopicName], [ValidateTopicFilter]), including the §4.7.1.2
//     parent-level match (`a/#` matches `a`) and the rule that a wildcard
//     filter never matches a topic starting with `$`. Shared-subscription
//     filters (`$share/{ShareName}/{filter}`, §4.8.2) are validated
//     structurally and matched against the real topic a PUBLISH carries,
//     with the `$share/{ShareName}/` prefix stripped before matching.
//
// Limits and known gaps:
//
//   - Memory amplification from repeatable properties is bounded only by
//     the frame-size cap, not by a count limit — the spec sets none. User
//     Property (0x26) and, on a PUBLISH, Subscription Identifier (0x0B) may
//     repeat arbitrarily often, and each occurrence allocates: a User
//     Property costs 5 wire bytes (id + two empty-string length prefixes)
//     but roughly 48 bytes of [UserProperty] slice element plus two string
//     headers, and a Subscription Identifier costs 2 wire bytes for an 8-
//     byte slot. A hostile 1 MiB frame — the [ReadFrame] cap the client
//     defaults to — therefore decodes to on the order of 6-7 MB of live
//     heap (~210k user properties, or ~520k subscription identifiers, plus
//     slice-growth overshoot) before the packet is handed to the caller.
//     That is the deliberate bound: lower the packet-size cap the client
//     advertises (TCPConfig.MaximumPacketSize) to lower it further.
//   - The property allow-table is direction-blind: it encodes which packet
//     type may carry a property, not which peer may send it, so a
//     client-encoded PUBLISH carrying a Subscription Identifier
//     ([MQTT-3.3.4-6], server-to-client only) is accepted by this encoder.
//     See the propertySpec comment in properties.go for why that dimension
//     is deliberately not modelled.
//
// Deliberately out of scope (see the module's CLAUDE.md for the full
// rationale): sending AUTH / participating in enhanced re-authentication
// (AUTH is decoded only, so the adapter can reject it); outbound topic
// aliasing (inbound alias resolution is a root-package, not protocol,
// concern); client-side API sugar for shared subscriptions (the `$share/`
// filter syntax is understood by [MatchTopic] / [ValidateTopicFilter] and
// passes through the codec untouched, but there is no helper that builds
// or manages one); any transport beyond what the root package builds on
// this codec (no WebSocket framing here).
package protocol
