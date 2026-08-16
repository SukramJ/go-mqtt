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

// sharedPrefix is the fixed first level (including its level separator) of
// an MQTT 5.0 §4.8.2 shared-subscription filter,
// "$share/{ShareName}/{TopicFilter}".
const sharedPrefix = "$share/"

// splitShared decomposes a shared-subscription filter into its ShareName and
// the wrapped {TopicFilter} it shares, per MQTT 5.0 §4.8.2. ok reports
// whether s is structurally a shared subscription at all, i.e. whether it
//
//   - starts with "$share/" [MQTT-4.8.2-1],
//   - carries a ShareName of at least one character that contains neither
//     '+' nor '#' and is followed by a '/' [MQTT-4.8.2-1, MQTT-4.8.2-2]
//     (a '/' inside the ShareName is by construction the separator that
//     terminates it, so "no '/' in the ShareName" needs no extra check),
//   - and has a non-empty {TopicFilter} after that '/' [MQTT-4.8.2-2].
//
// Anything else — "$share" with no separator, "$share/name" with no third
// level, "$share//f" with an empty ShareName — is not a well-formed shared
// subscription and is reported as ok == false, leaving callers to treat it
// as the ordinary, literal filter it is on the wire.
//
// splitShared performs no other validation: the wrapped filter's own
// wildcard placement is [ValidateTopicFilter]'s business, exactly as for a
// non-shared filter.
func splitShared(s string) (shareName, filter string, ok bool) {
	rest, found := strings.CutPrefix(s, sharedPrefix)
	if !found {
		return "", "", false
	}
	shareName, filter, found = strings.Cut(rest, "/")
	if !found || shareName == "" || filter == "" || strings.ContainsAny(shareName, "+#") {
		return "", "", false
	}
	return shareName, filter, true
}

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
// A shared-subscription filter ("$share/{ShareName}/{TopicFilter}", MQTT
// 5.0 §4.8.2) matches against the topic a PUBLISH actually carries, which
// is the real topic and never contains the "$share/{ShareName}/" prefix:
// the prefix is stripped and only {TopicFilter} takes part in matching, so
// MatchTopic("$share/grp/sensors/+", "sensors/temp") is true. The ShareName
// itself never participates, and the '$'-topic protection above applies to
// the effective filter after stripping — MatchTopic("$share/grp/#",
// "$SYS/x") is false, exactly as for the bare filter "#". A string that
// cannot be a well-formed shared subscription (no third level, an empty
// ShareName, or a wildcard character inside the ShareName — see
// splitShared) is matched verbatim as the ordinary filter it is, so the
// literal filter "$share" still matches the literal topic "$share".
//
// MatchTopic does not validate filter or topic; callers that accept
// filters/topics from untrusted input should run them through
// [ValidateTopicFilter] / [ValidateTopicName] first.
func MatchTopic(filter, topic string) bool {
	if _, wrapped, ok := splitShared(filter); ok {
		filter = wrapped
	}
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
//
// A filter starting with "$share/" is additionally checked against the
// shared-subscription structure of MQTT 5.0 §4.8.2,
// "$share/{ShareName}/{TopicFilter}", and the wildcard rules above are
// then applied to {TopicFilter} rather than to the whole string (the
// "$share" and ShareName levels are structural, not matchable). Each
// borderline shape is decided strictly per spec:
//
//   - "$share" — does not start with "$share/", so it is not a shared
//     subscription at all [MQTT-4.8.2-1]. Accepted as the ordinary,
//     literal one-level filter it is on the wire.
//   - "$share/" — starts with the prefix but has no ShareName and no
//     separator after it. Rejected [MQTT-4.8.2-1].
//   - "$share//f" — empty ShareName, which must be at least one
//     character. Rejected [MQTT-4.8.2-1].
//   - "$share/name" — the ShareName must be followed by a '/'. Rejected
//     [MQTT-4.8.2-2].
//   - "$share/name/" — that '/' must be followed by a Topic Filter, and
//     an empty topic filter is invalid per §4.7. Rejected
//     [MQTT-4.8.2-2].
//   - "$share/na+me/f", "$share/na#me/f" — a ShareName must contain
//     neither '+' nor '#'. Rejected [MQTT-4.8.2-2]. (A '/' cannot occur
//     inside a ShareName by construction: it terminates it.)
//   - "$share/name/a/#", "$share/name/+/b" — well-formed; the wrapped
//     filter is validated exactly like a standalone one, so
//     "$share/name/a/#/b" is rejected for the same reason "a/#/b" is.
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
	if rest, found := strings.CutPrefix(s, sharedPrefix); found {
		shareName, wrapped, split := strings.Cut(rest, "/")
		switch {
		case !split:
			return fmt.Errorf("%w: shared subscription filter must be $share/{ShareName}/{filter}", ErrProtocolViolation)
		case shareName == "":
			return fmt.Errorf("%w: shared subscription ShareName must be at least one character", ErrProtocolViolation)
		case strings.ContainsAny(shareName, "+#"):
			return fmt.Errorf("%w: shared subscription ShareName must not contain '+' or '#'", ErrProtocolViolation)
		case wrapped == "":
			return fmt.Errorf("%w: shared subscription carries an empty topic filter", ErrProtocolViolation)
		}
		return validateFilterLevels(wrapped)
	}
	return validateFilterLevels(s)
}

// validateFilterLevels enforces MQTT's wildcard placement rules on a
// (non-shared) topic filter: '#' only as the last level and occupying that
// level whole, '+' only occupying a level whole.
func validateFilterLevels(s string) error {
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
