// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package protocol

import (
	"strings"
	"testing"
)

func TestReasonCodeIsError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code ReasonCode
		err  bool
	}{
		{Success, false},
		{GrantedQoS0, false},
		{GrantedQoS1, false},
		{GrantedQoS2, false},
		{NormalDisconnection, false},
		{NoMatchingSubscribers, false},
		{ReasonCode(0x7F), false},
		{UnspecifiedError, true},
		{BadUserNameOrPassword, true},
		{NotAuthorized, true},
		{WildcardSubscriptionsNotSupported, true},
	}
	for _, tc := range cases {
		if got := tc.code.IsError(); got != tc.err {
			t.Errorf("ReasonCode(0x%02X).IsError() = %v, want %v", byte(tc.code), got, tc.err)
		}
	}
}

func TestReasonCodeString(t *testing.T) {
	t.Parallel()
	cases := map[ReasonCode]string{
		Success:                             "Success",
		GrantedQoS1:                         "Granted QoS 1",
		GrantedQoS2:                         "Granted QoS 2",
		UnsupportedProtocolVersion:          "Unsupported Protocol Version",
		BadUserNameOrPassword:               "Bad User Name or Password",
		TopicAliasInvalid:                   "Topic Alias invalid",
		QuotaExceeded:                       "Quota exceeded",
		WildcardSubscriptionsNotSupported:   "Wildcard Subscriptions not supported",
		SubscriptionIdentifiersNotSupported: "Subscription Identifiers not supported",
	}
	for code, want := range cases {
		if got := code.String(); got != want {
			t.Errorf("ReasonCode(0x%02X).String() = %q, want %q", byte(code), got, want)
		}
	}
	// Aliases share the canonical name of their value.
	if GrantedQoS0.String() != "Success" || NormalDisconnection.String() != "Success" {
		t.Error("0x00 aliases should stringify to Success")
	}
	// Unknown code falls back to a hex representation.
	if got := ReasonCode(0x7F).String(); !strings.Contains(got, "0x7F") {
		t.Errorf("unknown reason String = %q", got)
	}
}

func TestReasonNamesComplete(t *testing.T) {
	t.Parallel()
	// Every named code must round-trip through the name table (except the
	// hex fallback path, which is covered separately).
	for code := range reasonNames {
		if code.String() == "" {
			t.Errorf("reason 0x%02X has empty name", byte(code))
		}
	}
}

func TestV3ConnackError(t *testing.T) {
	t.Parallel()
	if err := V3ConnackError(0); err != nil {
		t.Fatalf("code 0 should be nil, got %v", err)
	}
	cases := map[byte]string{
		1: "Unsupported Protocol Version",
		2: "Client Identifier not valid",
		3: "Server unavailable",
		4: "Bad User Name or Password",
		5: "Not authorized",
	}
	for code, want := range cases {
		err := V3ConnackError(code)
		if err == nil {
			t.Fatalf("code %d should be an error", code)
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("code %d error %q missing %q", code, err.Error(), want)
		}
	}
	// Unknown non-zero code still yields a refusal error.
	err := V3ConnackError(99)
	if err == nil || !strings.Contains(err.Error(), "0x63") {
		t.Fatalf("unknown code error = %v", err)
	}
}
