// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"errors"
	"testing"
)

// TestMatchTopicSharedSubscription covers MQTT 5.0 §4.8.2: a shared
// subscription's filter is "$share/{ShareName}/{TopicFilter}" and the broker
// delivers PUBLISHes carrying the REAL topic, so matching has to happen
// against {TopicFilter} with the "$share/{ShareName}/" prefix stripped.
// Before this was fixed, MatchTopic compared "$share" against the topic's
// first level and every message for a shared subscription was silently
// dropped by the root package's dispatch loop.
func TestMatchTopicSharedSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		topic  string
		want   bool
	}{
		// The real topic matches; the "$share/{ShareName}/" prefix is not
		// part of what the broker publishes.
		{"literal", "$share/grp/sensors/temp", "sensors/temp", true},
		{"literal mismatch", "$share/grp/sensors/temp", "sensors/hum", false},
		// The prefixed form must NOT match, the wrapped filter must.
		{"prefixed topic does not match", "$share/grp/sensors/temp", "$share/grp/sensors/temp", false},

		// The ShareName never participates in matching.
		{"sharename ignored a", "$share/a/x", "x", true},
		{"sharename ignored b", "$share/b/x", "x", true},
		{"sharename not a level", "$share/grp/x", "grp/x", false},

		// Wildcards inside the effective filter behave exactly as they do
		// standalone.
		{"plus", "$share/grp/sensors/+", "sensors/temp", true},
		{"plus wrong depth", "$share/grp/sensors/+", "sensors/temp/raw", false},
		{"hash", "$share/grp/sensors/#", "sensors/temp/raw", true},
		{"hash parent level", "$share/grp/sensors/#", "sensors", true},
		{"hash only", "$share/grp/#", "anything/goes", true},
		{"plus only", "$share/grp/+", "x", true},
		{"empty level", "$share/grp/sport/+", "sport/", true},

		// MQTT-4.7.2-1 applies to the EFFECTIVE filter: a wildcard-first
		// wrapped filter still may not pick up "$..." topics.
		{"hash vs $SYS", "$share/g/#", "$SYS/x", false},
		{"plus vs $SYS", "$share/g/+/x", "$SYS/x", false},
		{"literal $SYS wrapped", "$share/g/$SYS/#", "$SYS/broker/load", true},
		{"literal $SYS wrapped exact", "$share/g/$SYS/x", "$SYS/x", true},

		// Shapes that cannot be a well-formed shared subscription stay
		// ordinary literal filters (unchanged pre-existing behavior).
		{"bare $share literal", "$share", "$share", true},
		{"bare $share vs topic", "$share", "x", false},
		{"no third level", "$share/name", "$share/name", true},
		{"no third level vs stripped", "$share/name", "name", false},
		{"trailing separator only", "$share/name/", "$share/name/", true},
		{"empty sharename", "$share//f", "$share//f", true},
		{"empty sharename not stripped", "$share//f", "f", false},
		{"plus in sharename", "$share/+/f", "$share/g/f", true},
		{"plus in sharename not stripped", "$share/+/f", "f", false},
		{"hash in sharename not stripped", "$share/#/f", "f", false},
		{"prefix not at start", "a/$share/g/f", "a/$share/g/f", true},
		{"case sensitive prefix", "$SHARE/g/f", "f", false},
		{"case sensitive prefix literal", "$SHARE/g/f", "$SHARE/g/f", true},

		// A shared subscription whose ShareName is itself "$"-ish or
		// contains a '$' is fine — only '+' and '#' are forbidden there.
		{"dollar sharename", "$share/$g/x", "x", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := MatchTopic(tt.filter, tt.topic); got != tt.want {
				t.Errorf("MatchTopic(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
			}
		})
	}
}

// TestMatchTopicSharedEquivalence pins the core invariant: wrapping any
// filter in a well-formed "$share/{ShareName}/" prefix must not change what
// it matches.
func TestMatchTopicSharedEquivalence(t *testing.T) {
	t.Parallel()

	filters := []string{"a/b", "a/+", "a/#", "#", "+", "$SYS/#", "/", "a/+/c", "sport/"}
	topics := []string{"a/b", "a/b/c", "a", "$SYS/broker", "/", "", "sport/", "x"}

	for _, f := range filters {
		for _, topic := range topics {
			want := MatchTopic(f, topic)
			shared := "$share/grp/" + f
			if got := MatchTopic(shared, topic); got != want {
				t.Errorf("MatchTopic(%q, %q) = %v, want %v (same as MatchTopic(%q, %q))",
					shared, topic, got, want, f, topic)
			}
		}
	}
}

// TestValidateTopicFilterShared covers the §4.8.2 structural rules the
// validator now enforces, and pins the borderline shapes documented on
// ValidateTopicFilter.
func TestValidateTopicFilterShared(t *testing.T) {
	t.Parallel()

	good := []struct {
		name   string
		filter string
	}{
		{"simple", "$share/grp/sensors/temp"},
		{"single level", "$share/grp/x"},
		{"plus", "$share/grp/sensors/+"},
		{"hash", "$share/grp/sensors/#"},
		{"hash only", "$share/grp/#"},
		{"plus only", "$share/grp/+"},
		{"one char sharename", "$share/g/x"},
		{"sharename with dollar", "$share/$g/x"},
		{"wrapped filter with empty levels", "$share/g//a"},
		{"wrapped $SYS", "$share/g/$SYS/#"},
		// "$share" alone does not start with "$share/", so it is not a
		// shared subscription at all — an ordinary one-level filter.
		{"bare $share is an ordinary filter", "$share"},
		// Not the prefix (case sensitive), so ordinary rules apply.
		{"uppercase is an ordinary filter", "$SHARE/g/f"},
		{"prefix not at start", "a/$share/g/f"},
	}
	for _, tt := range good {
		t.Run("good/"+tt.name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateTopicFilter(tt.filter); err != nil {
				t.Errorf("ValidateTopicFilter(%q) = %v, want nil", tt.filter, err)
			}
		})
	}

	bad := []struct {
		name   string
		filter string
	}{
		{"prefix only", "$share/"},
		{"no separator after sharename", "$share/name"},
		{"empty wrapped filter", "$share/name/"},
		{"empty sharename", "$share//f"},
		{"empty sharename and filter", "$share//"},
		{"plus sharename", "$share/+/f"},
		{"hash sharename", "$share/#/f"},
		{"plus inside sharename", "$share/na+me/f"},
		{"hash inside sharename", "$share/na#me/f"},
		{"wrapped hash not last", "$share/g/a/#/b"},
		{"wrapped hash not whole level", "$share/g/a#"},
		{"wrapped plus not whole level", "$share/g/a+/b"},
	}
	for _, tt := range bad {
		t.Run("bad/"+tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateTopicFilter(tt.filter)
			if err == nil {
				t.Fatalf("ValidateTopicFilter(%q) = nil, want error", tt.filter)
			}
			if !errors.Is(err, ErrProtocolViolation) {
				t.Errorf("ValidateTopicFilter(%q) error = %v, want wrapping ErrProtocolViolation", tt.filter, err)
			}
		})
	}
}

// TestValidateTopicFilterSharedNonSharedUnchanged guards the "keep
// non-$share behavior byte-for-byte identical" requirement: the shared
// branch must be reachable only through the exact "$share/" prefix.
func TestValidateTopicFilterSharedNonSharedUnchanged(t *testing.T) {
	t.Parallel()

	good := []string{"#", "+", "a/+/#", "a/b", "$SYS/#", "/", "a//b", "$share"}
	for _, s := range good {
		if err := ValidateTopicFilter(s); err != nil {
			t.Errorf("ValidateTopicFilter(%q) = %v, want nil", s, err)
		}
	}

	bad := []string{"", "a+", "a/#/b", "#b", "a#", "a/++/b"}
	for _, s := range bad {
		if err := ValidateTopicFilter(s); !errors.Is(err, ErrProtocolViolation) {
			t.Errorf("ValidateTopicFilter(%q) error = %v, want wrapping ErrProtocolViolation", s, err)
		}
	}
}

// TestSplitShared exercises the decomposition helper directly so the
// structural contract MatchTopic and ValidateTopicFilter share is pinned in
// one place.
func TestSplitShared(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in        string
		wantShare string
		wantRest  string
		wantOK    bool
	}{
		{"$share/g/a/b", "g", "a/b", true},
		{"$share/g/#", "g", "#", true},
		{"$share/g//a", "g", "/a", true},
		{"$share/$g/a", "$g", "a", true},
		{"$share", "", "", false},
		{"$share/", "", "", false},
		{"$share/g", "", "", false},
		{"$share/g/", "", "", false},
		{"$share//a", "", "", false},
		{"$share/+/a", "", "", false},
		{"$share/#/a", "", "", false},
		{"$share/g+h/a", "", "", false},
		{"a/$share/g/a", "", "", false},
		{"", "", "", false},
		{"#", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			share, rest, ok := splitShared(tt.in)
			if share != tt.wantShare || rest != tt.wantRest || ok != tt.wantOK {
				t.Errorf("splitShared(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tt.in, share, rest, ok, tt.wantShare, tt.wantRest, tt.wantOK)
			}
		})
	}
}
