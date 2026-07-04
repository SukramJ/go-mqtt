// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

// Package protocol implements the MQTT wire codec — packet encode/decode
// for both MQTT 3.1.1 (protocol level 4, [V311]) and MQTT 5.0 (protocol
// level 5, [V50]) — that the client library builds on. It is deliberately
// pure-Go with zero third-party dependencies.
//
// Feature coverage:
//   - Both dialects: MQTT 3.1.1 (level 0x04) and MQTT 5.0 (level 0x05),
//     selected per packet by the [Version] argument.
//   - CONNECT / CONNACK with optional will (LWT) + username/password, and
//     the MQTT 5.0 property block (session expiry, receive maximum,
//     maximum packet size, topic alias maximum, assigned client id, ...).
//   - PUBLISH at QoS 0, 1 and 2, with the full acknowledgement handshake:
//     PUBACK (QoS 1) and PUBREC / PUBREL / PUBCOMP (QoS 2 exactly-once).
//   - SUBSCRIBE / SUBACK and UNSUBSCRIBE / UNSUBACK, one topic filter per
//     frame, including the MQTT 5.0 subscription options and reason codes.
//   - PINGREQ / PINGRESP heartbeat.
//   - DISCONNECT and AUTH, including MQTT 5.0 reason codes and properties.
//   - The MQTT 5.0 property model ([Properties]), reason codes ([ReasonCode]),
//     and topic name/filter validation and wildcard matching ([MatchTopic],
//     [ValidateTopicName], [ValidateTopicFilter]).
//
// [ReadFrame] enforces a caller-supplied maximum remaining length so a
// malformed or hostile length field cannot stall or exhaust the reader.
package protocol
