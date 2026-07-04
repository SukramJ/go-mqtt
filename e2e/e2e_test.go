// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

// Package e2e exercises [github.com/SukramJ/go-mqtt] against real MQTT
// brokers (mosquitto and emqx) reachable over the network. Every test is
// env-gated: it t.Skip()s cleanly when the broker environment variable it
// needs is unset, or when a short TCP dial probe against that address
// fails — so `go test ./e2e/...` succeeds (every test skipped) on a
// machine with no brokers running, e.g. `make e2e-up` was never called,
// or a plain `go test ./...` in a sandbox without docker.
//
// See harness_test.go for the in-test TCP proxy used to simulate severed
// connections/reconnects and TLS config loading, and the Makefile e2e-*
// targets for how the docker-backed brokers are provisioned and wired to
// the environment variables this package reads.
package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/SukramJ/go-mqtt"
)

// Environment variables gating each broker endpoint / feature. See the
// Makefile's e2e-up / test-e2e targets for how they are populated in CI
// and local docker runs.
const (
	envMosquitto     = "MQTT_E2E_MOSQUITTO"      // tcp://host:1883, anonymous
	envMosquittoTLS  = "MQTT_E2E_MOSQUITTO_TLS"  // tls://host:8883, anonymous
	envMosquittoAuth = "MQTT_E2E_MOSQUITTO_AUTH" // tcp://host:1884, username/password
	envEMQX          = "MQTT_E2E_EMQX"           // tcp://host:2883, anonymous
	envCertsDir      = "MQTT_E2E_CERTS_DIR"      // dir holding ca.pem/server.pem/server.key
)

// mosquittoAuthUser / mosquittoAuthPass are the credentials `make e2e-up`
// provisions on the MQTT_E2E_MOSQUITTO_AUTH listener via mosquitto_passwd
// (see the Makefile's e2e-up target).
const (
	mosquittoAuthUser = "e2e"
	mosquittoAuthPass = "e2epass" //nolint:gosec // fixture credential for a throwaway local/CI broker, not a secret
)

// dialProbeTimeout bounds the raw TCP reachability probe brokerURL runs
// before handing a test a broker address: long enough to tolerate a
// slow-starting container, short enough that a genuinely absent broker
// fails fast instead of stalling the whole package.
const dialProbeTimeout = 2 * time.Second

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

// brokerDesc names one broker under test alongside the environment
// variable that carries its plain-TCP address.
type brokerDesc struct {
	name   string
	envVar string
}

// brokerTable is the {mosquitto, emqx} pair the cross-broker tests in
// connect_test.go and pubsub_test.go iterate.
var brokerTable = []brokerDesc{
	{"mosquitto", envMosquitto},
	{"emqx", envEMQX},
}

// versionTable is the {MQTT 3.1.1, MQTT 5.0} pair the cross-version tests
// iterate.
var versionTable = []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV50}

// seq is a per-process monotonic counter. Every generated topic prefix
// and client ID mixes it with the OS process ID rather than a wall
// clock: two test binaries started in the same millisecond (routine
// under `go test -parallel`, or across a package's own parallel
// sub-tests) would otherwise risk colliding on the same broker.
var seq atomic.Uint64

// nextSeq returns the next value from the process-wide counter.
func nextSeq() uint64 { return seq.Add(1) }

// dialHost resolves the host:port a bare TCP probe (or proxy target)
// should dial for u, defaulting the port the same way [mqtt.TCPClient]
// does — 1883 plain, 8883 TLS — when the URL omits one.
func dialHost(u *url.URL) string {
	if u.Port() != "" {
		return u.Host
	}
	port := "1883"
	switch u.Scheme {
	case "tls", "ssl", "mqtts":
		port = "8883"
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// brokerURL returns the broker endpoint configured for envVar, or skips
// the test (cleanly, so it counts as skipped rather than failed) when the
// variable is unset or a short TCP dial against the address fails. The
// latter catches a stale/never-started broker without hanging the whole
// suite on a connect timeout.
func brokerURL(t *testing.T, envVar string) string {
	t.Helper()
	raw := os.Getenv(envVar)
	if raw == "" {
		t.Skipf("%s not set, skipping", envVar)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Skipf("%s=%q is not a valid URL: %v", envVar, raw, err)
	}
	host := dialHost(u)
	conn, err := net.DialTimeout("tcp", host, dialProbeTimeout)
	if err != nil {
		t.Skipf("%s: broker not reachable at %s: %v", envVar, host, err)
	}
	_ = conn.Close()
	return raw
}

// brokerHostPort parses raw (as returned by brokerURL) down to a bare
// host:port, the form net.Dial and the in-test proxy's target want.
func brokerHostPort(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse broker url %q: %v", raw, err)
	}
	return dialHost(u)
}

// uniqueTopicPrefix returns a topic prefix unique to this process and
// call, safe to use as the root of every topic a test publishes/
// subscribes under so parallel tests — and parallel runs against a
// shared, persistent broker — never observe each other's traffic or
// leftover retained messages.
func uniqueTopicPrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("gomqtt-e2e/%d/%d", os.Getpid(), nextSeq())
}

// uniqueClientID returns an MQTT client identifier unique to this process
// and call, tagged with a short protocol-version marker for readability
// in broker logs.
func uniqueClientID(version mqtt.ProtocolVersion) string {
	tag := "v5"
	if version == mqtt.ProtocolV311 {
		tag = "v3"
	}
	return fmt.Sprintf("gomqtt-e2e-%s-%d-%d", tag, os.Getpid(), nextSeq())
}

// testLogger returns a logger tagged with the test name. It writes
// through slog.Default's handler (stderr), deliberately never via t.Log:
// the TCPClient's read/keep-alive goroutines can still be unwinding a
// beat after a test function returns (Disconnect only waits on them
// bounded by its ctx), and logging via t.Log after that point panics
// ("Log in goroutine after Test has completed").
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.Default().With("test", t.Name())
}

// newTCPConfig builds a baseline [mqtt.TCPConfig] for broker at the given
// protocol version: a fresh unique ClientID, a clean session, and
// generous-but-bounded timeouts so a slow CI runner doesn't flake but a
// genuinely wedged connect/ack still fails the test instead of hanging
// it. Each mutate function, applied in order, lets a test override
// exactly the fields its scenario cares about (CleanStart,
// SessionExpirySeconds, Will, TLSConfig, ...).
func newTCPConfig(t *testing.T, broker string, version mqtt.ProtocolVersion, mutate ...func(*mqtt.TCPConfig)) mqtt.TCPConfig {
	t.Helper()
	cfg := mqtt.TCPConfig{
		BrokerURL:       broker,
		ClientID:        uniqueClientID(version),
		ProtocolVersion: version,
		CleanStart:      true,
		DialTimeout:     8 * time.Second,
		AckTimeout:      8 * time.Second,
		Logger:          testLogger(t),
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg
}

// newClient constructs a [mqtt.TCPClient] from newTCPConfig(...) and
// registers a best-effort Disconnect on test cleanup so a failed
// assertion (or an early return) never leaks a live socket into a later
// test.
func newClient(t *testing.T, broker string, version mqtt.ProtocolVersion, mutate ...func(*mqtt.TCPConfig)) *mqtt.TCPClient {
	t.Helper()
	c := mqtt.NewTCPClient(newTCPConfig(t, broker, version, mutate...))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Disconnect(ctx)
	})
	return c
}

// connectClient builds and connects a client in one step, failing the
// test immediately on a connect error.
func connectClient(t *testing.T, broker string, version mqtt.ProtocolVersion, mutate ...func(*mqtt.TCPConfig)) *mqtt.TCPClient {
	t.Helper()
	c := newClient(t, broker, version, mutate...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect(%s, %s): %v", broker, version, err)
	}
	return c
}

// pollUntil polls fn every 20ms until it returns true or timeout
// elapses, returning fn's final value. Used instead of a fixed sleep so
// a test proceeds as soon as its condition is met and only pays the full
// timeout when something is actually stuck.
func pollUntil(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return true
		}
		if time.Now().After(deadline) {
			return fn()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// msgCollector buffers messages delivered to a [mqtt.MessageHandler] so a
// test can assert on them from outside the synchronous read-loop
// dispatch call. Handler defensively copies every slice off the
// [mqtt.Message] before buffering it — the handler contract only
// guarantees the *Message is valid for the duration of the call.
type msgCollector struct {
	ch chan *mqtt.Message
}

// newMsgCollector returns a collector buffering up to capacity messages
// before Handler starts silently dropping — sized generously so only a
// test that itself publishes an unexpectedly large burst without
// draining would ever hit that path.
func newMsgCollector(capacity int) *msgCollector {
	return &msgCollector{ch: make(chan *mqtt.Message, capacity)}
}

// Handler is the [mqtt.MessageHandler] to pass to Subscribe.
func (m *msgCollector) Handler(msg *mqtt.Message) {
	cp := *msg
	cp.Payload = append([]byte(nil), msg.Payload...)
	cp.CorrelationData = append([]byte(nil), msg.CorrelationData...)
	cp.UserProperties = append([]mqtt.UserProperty(nil), msg.UserProperties...)
	cp.SubscriptionIdentifiers = append([]uint32(nil), msg.SubscriptionIdentifiers...)
	select {
	case m.ch <- &cp:
	default:
	}
}

// Next waits up to timeout for the next buffered message, failing the
// test if none arrives in time.
func (m *msgCollector) Next(t *testing.T, timeout time.Duration) *mqtt.Message {
	t.Helper()
	select {
	case msg := <-m.ch:
		return msg
	case <-time.After(timeout):
		t.Fatal("msgCollector: timed out waiting for a message")
		return nil
	}
}

// NextOrNone waits up to timeout for a message and reports whether one
// arrived, without failing the test — used for negative assertions (e.g.
// "this expired / suppressed message must NOT be delivered").
func (m *msgCollector) NextOrNone(timeout time.Duration) (*mqtt.Message, bool) {
	select {
	case msg := <-m.ch:
		return msg, true
	case <-time.After(timeout):
		return nil, false
	}
}
