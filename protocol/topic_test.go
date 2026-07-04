// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"errors"
	"strings"
	"testing"
)

func TestMatchTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter string
		topic  string
		want   bool
	}{
		{"a/b", "a/b", true},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"a/#", "a/b/c", true},
		{"#", "anything/goes", true},
		{"a/+/c", "a/b/c", true},
		{"a/#", "a", true},
		{"sport/+", "sport/", true},
		{"+/+", "/finance", true},
		{"/+", "/finance", true},
		{"#", "$SYS/x", false},
		{"+/x", "$y/x", false},
		{"$SYS/#", "$SYS/broker", true},
		{"sport/#", "sport", true},
		{"+", "x", true},
		{"+", "x/y", false},
		{"a/b", "a", false},   // filter has more non-# levels than the topic
		{"a/b", "a/c", false}, // literal level mismatch
	}

	for _, tt := range tests {
		t.Run(tt.filter+"|"+tt.topic, func(t *testing.T) {
			t.Parallel()

			if got := MatchTopic(tt.filter, tt.topic); got != tt.want {
				t.Errorf("MatchTopic(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
			}
		})
	}
}

func TestValidateTopicFilter(t *testing.T) {
	t.Parallel()

	bad := []string{"a+", "a/#/b", "#b", "a#", "", "a/++/b"}
	for _, s := range bad {
		t.Run("bad/"+s, func(t *testing.T) {
			t.Parallel()

			if err := ValidateTopicFilter(s); err == nil {
				t.Errorf("ValidateTopicFilter(%q) = nil, want error", s)
			} else if !errors.Is(err, ErrProtocolViolation) {
				t.Errorf("ValidateTopicFilter(%q) error = %v, want wrapping ErrProtocolViolation", s, err)
			}
		})
	}

	good := []string{"#", "+", "a/+/#", "a/b"}
	for _, s := range good {
		t.Run("good/"+s, func(t *testing.T) {
			t.Parallel()

			if err := ValidateTopicFilter(s); err != nil {
				t.Errorf("ValidateTopicFilter(%q) = %v, want nil", s, err)
			}
		})
	}
}

func TestValidateTopicFilterLength(t *testing.T) {
	t.Parallel()

	if err := ValidateTopicFilter(strings.Repeat("a", maxTopicLen+1)); !errors.Is(err, ErrProtocolViolation) {
		t.Errorf("ValidateTopicFilter(too long) error = %v, want wrapping ErrProtocolViolation", err)
	}

	if err := ValidateTopicFilter("a\x00b"); !errors.Is(err, ErrProtocolViolation) {
		t.Errorf("ValidateTopicFilter(NUL) error = %v, want wrapping ErrProtocolViolation", err)
	}

	if err := ValidateTopicFilter(strings.Repeat("a", maxTopicLen)); err != nil {
		t.Errorf("ValidateTopicFilter(max length) = %v, want nil", err)
	}

	if err := ValidateTopicFilter("not-\xffutf8"); !errors.Is(err, ErrProtocolViolation) {
		t.Errorf("ValidateTopicFilter(invalid utf8) error = %v, want wrapping ErrProtocolViolation", err)
	}
}

func TestValidateTopicName(t *testing.T) {
	t.Parallel()

	bad := []string{"", "a/+", "a/#", "sensor/#/x", "a\x00b"}
	for _, s := range bad {
		t.Run("bad/"+s, func(t *testing.T) {
			t.Parallel()

			if err := ValidateTopicName(s); !errors.Is(err, ErrProtocolViolation) {
				t.Errorf("ValidateTopicName(%q) error = %v, want wrapping ErrProtocolViolation", s, err)
			}
		})
	}

	good := []string{"a", "a/b/c", "sport/tennis/player1", "$SYS/broker/uptime"}
	for _, s := range good {
		t.Run("good/"+s, func(t *testing.T) {
			t.Parallel()

			if err := ValidateTopicName(s); err != nil {
				t.Errorf("ValidateTopicName(%q) = %v, want nil", s, err)
			}
		})
	}

	if err := ValidateTopicName(strings.Repeat("a", maxTopicLen+1)); !errors.Is(err, ErrProtocolViolation) {
		t.Errorf("ValidateTopicName(too long) error = %v, want wrapping ErrProtocolViolation", err)
	}

	if err := ValidateTopicName("not-\xffutf8"); !errors.Is(err, ErrProtocolViolation) {
		t.Error("ValidateTopicName(invalid utf8) expected ErrProtocolViolation")
	}
}
