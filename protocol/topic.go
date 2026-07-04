// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// maxTopicLen is the largest byte length a topic name or topic filter
// may occupy (the two-byte length prefix that carries it on the wire
// cannot represent more).
const maxTopicLen = 0xFFFF

// MatchTopic reports whether topic matches filter per MQTT 5.0 §4.7 /
// MQTT 3.1.1 §4.7 (the wildcard semantics are identical between the two
// protocol versions). filter and topic are split into '/'-separated
// levels (an empty level, e.g. from a leading, trailing or doubled '/',
// is a legal, distinct level).
//
//   - '+' matches exactly one level, including an empty one.
//   - '#' must be the last filter level; it matches that level's parent
//     (MatchTopic("a/#", "a") is true — the level "#" replaces need not
//     be present at all) plus any number of further levels.
//   - A filter whose first level is the literal wildcard "#" or "+"
//     never matches a topic whose first level begins with '$' — this is
//     the MQTT-4.7.2-1 requirement that keeps ordinary wildcard
//     subscriptions from picking up server-internal "$SYS/..." topics.
//     A filter whose first level is a literal string (e.g. "$SYS/#")
//     is unaffected and matches normally.
//
// MatchTopic does not validate filter or topic; callers that accept
// filters/topics from untrusted input should run them through
// [ValidateTopicFilter] / [ValidateTopicName] first.
func MatchTopic(filter, topic string) bool {
	if topic != "" && topic[0] == '$' && filter != "" && (filter[0] == '#' || filter[0] == '+') {
		return false
	}
	return matchLevels(strings.Split(filter, "/"), strings.Split(topic, "/"))
}

// matchLevels compares filter levels against topic levels one at a
// time. '#' (only meaningful as the final filter level) matches the
// rest of the topic, including zero further levels; '+' matches
// exactly one (possibly empty) topic level; any other filter level
// must match the corresponding topic level byte-for-byte.
func matchLevels(filterLevels, topicLevels []string) bool {
	for i, fl := range filterLevels {
		if fl == "#" {
			return true
		}
		if i >= len(topicLevels) {
			return false
		}
		if fl == "+" {
			continue
		}
		if fl != topicLevels[i] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}

// ValidateTopicName checks s against the constraints MQTT places on a
// topic name (the topic a PUBLISH carries), per MQTT 5.0 §4.7 / MQTT
// 3.1.1 §4.7: non-empty, no wildcard characters ('+' or '#'), no U+0000,
// valid UTF-8, and no more than 65535 bytes (the wire length prefix's
// range).
func ValidateTopicName(s string) error {
	if s == "" {
		return fmt.Errorf("%w: empty topic name", ErrProtocolViolation)
	}
	if len(s) > maxTopicLen {
		return fmt.Errorf("%w: topic name %d bytes exceeds %d", ErrProtocolViolation, len(s), maxTopicLen)
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("%w: topic name is not valid UTF-8", ErrProtocolViolation)
	}
	if strings.ContainsRune(s, 0) {
		return fmt.Errorf("%w: topic name contains U+0000", ErrProtocolViolation)
	}
	if strings.ContainsAny(s, "+#") {
		return fmt.Errorf("%w: topic name contains a wildcard character", ErrProtocolViolation)
	}
	return nil
}

// ValidateTopicFilter checks s against the constraints MQTT places on a
// topic filter (SUBSCRIBE/UNSUBSCRIBE), per MQTT 5.0 §4.7 / MQTT 3.1.1
// §4.7: non-empty, no U+0000, no more than 65535 bytes, '#' only as the
// last character and only occupying a whole level, and '+' only
// occupying a whole level (anywhere in the filter).
func ValidateTopicFilter(s string) error {
	if s == "" {
		return fmt.Errorf("%w: empty topic filter", ErrProtocolViolation)
	}
	if len(s) > maxTopicLen {
		return fmt.Errorf("%w: topic filter %d bytes exceeds %d", ErrProtocolViolation, len(s), maxTopicLen)
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("%w: topic filter is not valid UTF-8", ErrProtocolViolation)
	}
	if strings.ContainsRune(s, 0) {
		return fmt.Errorf("%w: topic filter contains U+0000", ErrProtocolViolation)
	}
	levels := strings.Split(s, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return fmt.Errorf("%w: '#' must occupy a whole level and be the last level", ErrProtocolViolation)
		}
		if strings.Contains(level, "+") && level != "+" {
			return fmt.Errorf("%w: '+' must occupy a whole level", ErrProtocolViolation)
		}
	}
	return nil
}
