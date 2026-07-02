// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"crypto/tls"
	"testing"
)

func TestNewClientTLSConfigSetsServerName(t *testing.T) {
	cfg := NewClientTLSConfig("broker.example.com", false)
	if cfg.ServerName != "broker.example.com" {
		t.Fatalf("ServerName = %q, want %q", cfg.ServerName, "broker.example.com")
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify must default to false")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %#x, want TLS 1.2 (%#x)", cfg.MinVersion, tls.VersionTLS12)
	}
}

func TestNewClientTLSConfigInsecureOptIn(t *testing.T) {
	cfg := NewClientTLSConfig("broker.example.com", true)
	if !cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify=true when explicitly requested")
	}
	// ServerName must still be set even in the insecure opt-in case —
	// callers may reuse the config, and there is no reason to drop it.
	if cfg.ServerName != "broker.example.com" {
		t.Fatalf("ServerName = %q, want %q", cfg.ServerName, "broker.example.com")
	}
}
