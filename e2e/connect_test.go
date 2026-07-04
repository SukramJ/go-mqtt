// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package e2e

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	mqtt "github.com/SukramJ/go-mqtt"
	"github.com/SukramJ/go-mqtt/protocol"
)

// TestConnect exercises a plain CONNECT/CONNACK round trip against both
// brokers on both protocol versions, asserting the negotiated
// [mqtt.ConnectResult].
func TestConnect(t *testing.T) {
	t.Parallel()

	for _, b := range brokerTable {
		for _, v := range versionTable {
			t.Run(fmt.Sprintf("%s_%s", b.name, v), func(t *testing.T) {
				t.Parallel()
				brokerAddr := brokerURL(t, b.envVar)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				c := newClient(t, brokerAddr, v)
				if err := c.Connect(ctx); err != nil {
					t.Fatalf("Connect: %v", err)
				}
				if !c.IsConnected() {
					t.Error("IsConnected = false right after a successful Connect")
				}
				res, ok := c.ConnectResult()
				if !ok {
					t.Fatal("ConnectResult ok=false after a successful Connect")
				}
				if res.ReasonCode.IsError() {
					t.Errorf("ConnectResult.ReasonCode = %v, want a success code", res.ReasonCode)
				}
				if res.SessionPresent {
					t.Error("SessionPresent = true for a fresh CleanStart=true session")
				}
			})
		}
	}
}

// TestConnectWithCredentials is the positive counterpart to
// TestConnectBadCredentials: the right username/password against the
// same auth-enforcing listener must succeed, confirming the fixture
// itself (not just its rejection paths) actually works.
func TestConnectWithCredentials(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquittoAuth)

	for _, v := range versionTable {
		t.Run(v.String(), func(t *testing.T) {
			t.Parallel()
			c := newClient(t, brokerAddr, v, func(cfg *mqtt.TCPConfig) {
				cfg.Username = mosquittoAuthUser
				cfg.Password = mosquittoAuthPass
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := c.Connect(ctx); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			if !c.IsConnected() {
				t.Error("IsConnected = false after a successful Connect")
			}
		})
	}
}

// TestConnectBadCredentials exercises the MQTT_E2E_MOSQUITTO_AUTH
// listener's username/password enforcement (allow_anonymous false,
// password_file — see e2e/testdata/mosquitto.conf) with a valid username
// and a wrong password.
func TestConnectBadCredentials(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquittoAuth)

	for _, v := range versionTable {
		t.Run(v.String(), func(t *testing.T) {
			t.Parallel()
			c := newClient(t, brokerAddr, v, func(cfg *mqtt.TCPConfig) {
				cfg.Username = mosquittoAuthUser
				cfg.Password = "definitely-the-wrong-password"
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			assertAuthRejected(t, c.Connect(ctx))
			if c.IsConnected() {
				t.Error("IsConnected = true after a rejected CONNECT")
			}
		})
	}
}

// TestConnectAnonymousRejected connects to the same auth-enforcing
// listener with no credentials at all: allow_anonymous false must refuse
// the CONNECT just as it does a wrong password.
func TestConnectAnonymousRejected(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquittoAuth)

	for _, v := range versionTable {
		t.Run(v.String(), func(t *testing.T) {
			t.Parallel()
			c := newClient(t, brokerAddr, v)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			assertAuthRejected(t, c.Connect(ctx))
		})
	}
}

// assertAuthRejected asserts err is a *mqtt.ReasonError carrying one of
// the two CONNACK codes brokers use to say "you may not connect":
// BadUserNameOrPassword (0x86) or NotAuthorized (0x87). Which of the two
// a given broker (or broker version/config) picks for a bad password vs.
// no credentials at all is implementation-defined — MQTT 3.1.1's own
// CONNACK return code 5 ("not authorized") maps onto 0x87 in this codec
// (see protocol.v3ConnackReason) while mosquitto has, across versions,
// used both wordings for what is functionally the same refusal — so both
// are accepted here rather than asserting a single exact code.
func assertAuthRejected(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Connect succeeded, want it rejected")
	}
	var re *mqtt.ReasonError
	if !errors.As(err, &re) {
		t.Fatalf("Connect error = %v (%T), want *mqtt.ReasonError", err, err)
	}
	if re.Code != protocol.BadUserNameOrPassword && re.Code != protocol.NotAuthorized {
		t.Fatalf("ReasonError.Code = %v, want BadUserNameOrPassword or NotAuthorized", re.Code)
	}
}

// TestConnectTLS dials the MQTT_E2E_MOSQUITTO_TLS listener with the
// generated CA pinned and the correct SAN (localhost, per e2e/gencert),
// then again with a deliberately wrong ServerName to confirm hostname
// verification is actually enforced.
func TestConnectTLS(t *testing.T) {
	t.Parallel()
	brokerAddr := brokerURL(t, envMosquittoTLS)

	t.Run("ok", func(t *testing.T) {
		t.Parallel()
		c := newClient(t, brokerAddr, mqtt.ProtocolV50, func(cfg *mqtt.TCPConfig) {
			cfg.TLSConfig = clientTLSConfig(t, "localhost")
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Connect(ctx); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		if !c.IsConnected() {
			t.Error("IsConnected = false after a successful TLS Connect")
		}
	})

	t.Run("wrong_server_name", func(t *testing.T) {
		t.Parallel()
		c := newClient(t, brokerAddr, mqtt.ProtocolV50, func(cfg *mqtt.TCPConfig) {
			cfg.TLSConfig = clientTLSConfig(t, "not-the-broker.invalid")
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Connect(ctx); err == nil {
			t.Fatal("Connect with a wrong TLS ServerName succeeded, want a certificate verification failure")
		}
		if c.IsConnected() {
			t.Error("IsConnected = true after a failed TLS handshake")
		}
	})
}
