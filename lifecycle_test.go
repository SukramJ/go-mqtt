// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Mock-broker-based Lifecycle tests: a real TCPClient wrapped in a
// Lifecycle, driven against mockBroker. lifecycle_unit_test.go covers the
// backoff/notifier state machine in isolation against stub Connectors;
// this file proves the same Lifecycle wiring holds end to end against the
// adapter's actual ConnectionNotifier/Connector behavior — event-driven
// reconnect after a dropped socket, the PINGRESP watchdog, CONNACK
// rejection, Stop/Start state, and resubscribe replay — all through
// Lifecycle.Start/Stop rather than calling TCPClient.Connect by hand
// (adapter_integration_test.go's mustConnect). Shared helpers
// (newIntegrationConfig, lcPoll) live in adapter_integration_test.go and
// lifecycle_unit_test.go, already package-visible.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// TestLifecycleReconnectsPromptlyAfterTCPReset proves a Lifecycle wrapping a
// live TCPClient reconnects via the ConnectionLost notifier, not the timer
// fallback: InitialBackoff is set far larger than any plausible localhost
// reconnect, so a fast reconnect can only be explained by the event-driven
// path.
func TestLifecycleReconnectsPromptlyAfterTCPReset(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "lc-reset"))
	lc := NewLifecycle(LifecycleConfig{InitialBackoff: 3 * time.Second, MaxBackoff: 3 * time.Second}, c)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := lc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lc.Stop(context.Background()) })
	if got := b.ConnCount(); got != 1 {
		t.Fatalf("ConnCount after Start = %d, want 1", got)
	}

	start := time.Now()
	b.InjectTCPReset()
	if !lcPoll(2*time.Second, func() bool { return b.ConnCount() >= 2 }) {
		t.Fatal("lifecycle never reconnected after InjectTCPReset")
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("reconnect took %v — did not clearly beat the 3s backoff timer "+
			"(event-driven notifier path may not have fired)", elapsed)
	}
}

// TestLifecyclePingWatchdogReconnectsAndRecovers proves persistent PINGRESP
// silence trips the watchdog (torn down as a lost connection, so the
// Lifecycle reconnects), and that once pings are answered again the client
// settles into a stable connected state rather than looping forever —
// DropPings is sticky across reconnects, so a fresh link would otherwise
// keep re-tripping the same watchdog.
func TestLifecyclePingWatchdogReconnectsAndRecovers(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "lc-watchdog"))
	// Package-test-only override (see adapter_tcp.go's pingInterval field
	// doc) so the watchdog trips well within the test budget instead of
	// waiting out the 30s keep-alive floor.
	c.pingInterval = 30 * time.Millisecond
	lc := NewLifecycle(LifecycleConfig{InitialBackoff: 2 * time.Second, MaxBackoff: 2 * time.Second}, c)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := lc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lc.Stop(context.Background()) })

	b.DropPings(true)
	if !lcPoll(2*time.Second, func() bool { return b.ConnCount() >= 2 }) {
		t.Fatal("watchdog never tripped a reconnect under persistent PINGRESP silence")
	}

	b.DropPings(false)
	if !lcPoll(2*time.Second, func() bool { return c.IsConnected() }) {
		t.Fatal("client never recovered a stable connection once pings resumed")
	}
	settled := b.ConnCount()
	for range 10 {
		time.Sleep(20 * time.Millisecond)
		if b.ConnCount() != settled || !c.IsConnected() {
			t.Fatalf("connection unstable after recovery: ConnCount %d -> %d, IsConnected=%v",
				settled, b.ConnCount(), c.IsConnected())
		}
	}
}

// TestLifecyclePingWatchdogToleratesSingleDroppedPing proves the
// pingTimeoutThreshold=2 tolerance: a single dropped PINGRESP must be
// ridden out without tearing the connection down.
func TestLifecyclePingWatchdogToleratesSingleDroppedPing(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "lc-watchdog-tolerant"))
	c.pingInterval = 30 * time.Millisecond
	lc := NewLifecycle(LifecycleConfig{InitialBackoff: 2 * time.Second, MaxBackoff: 2 * time.Second}, c)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := lc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lc.Stop(context.Background()) })

	b.DropNextPings(1)

	// Give it several ping intervals' worth of headroom (proving a
	// negative requires waiting out a window, not polling for an event),
	// then confirm no reconnect happened.
	time.Sleep(300 * time.Millisecond)
	if got := b.ConnCount(); got != 1 {
		t.Fatalf("ConnCount = %d, want 1 (a single dropped ping must not trigger a reconnect)", got)
	}
	if !c.IsConnected() {
		t.Fatal("IsConnected = false after tolerating a single dropped ping")
	}
}

// TestLifecycleServerKeepAliveOverridesPingCadenceAndWatchdogStillTrips
// proves the CONNACK Server Keep Alive property reschedules the ping
// cadence (spec MUST) rather than the client sticking to its requested
// KeepAlive, and that the watchdog still trips on persistent silence at
// that negotiated cadence.
func TestLifecycleServerKeepAliveOverridesPingCadenceAndWatchdogStillTrips(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	ka := uint16(1) // seconds; smallest meaningful non-zero Server Keep Alive
	b.SetConnackProperties(&protocol.Properties{ServerKeepAlive: &ka})

	c := NewTCPClient(newIntegrationConfig(b.URL(), "lc-ska")) // no pingInterval override
	lc := NewLifecycle(LifecycleConfig{InitialBackoff: 2 * time.Second, MaxBackoff: 2 * time.Second}, c)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := lc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lc.Stop(context.Background()) })

	res, ok := c.ConnectResult()
	if !ok || res.ServerKeepAlive != time.Second {
		t.Fatalf("ConnectResult.ServerKeepAlive = %v, ok=%v, want 1s", res.ServerKeepAlive, ok)
	}

	// Effective cadence is ServerKeepAlive/2 = 500ms. Coarse assert: at
	// least 2 pings land within 1.3s, and it does not run away far past
	// that cadence — wide enough to absorb scheduler jitter without being
	// meaningless.
	if !lcPoll(1300*time.Millisecond, func() bool { return b.PingCount() >= 2 }) {
		t.Fatalf("PingCount = %d after 1.3s, want >= 2 at the 500ms overridden cadence", b.PingCount())
	}
	if got := b.PingCount(); got > 6 {
		t.Fatalf("PingCount = %d, cadence looks far faster than the 500ms override", got)
	}

	b.DropPings(true)
	if !lcPoll(2*time.Second, func() bool { return b.ConnCount() >= 2 }) {
		t.Fatal("watchdog never tripped a reconnect at the ServerKeepAlive-derived cadence")
	}
}

// TestLifecycleConnackRejectionFailsStartWithoutReconnectStorm proves a
// rejected CONNACK — both a direct MQTT 5.0 reason code and an MQTT 3.1.1
// return code mapped onto its v5 equivalent — fails Lifecycle.Start
// synchronously with a *ReasonError, and that the failure path never spins
// up the background reconnect loop (so no storm of reconnect attempts
// follows).
func TestLifecycleConnackRejectionFailsStartWithoutReconnectStorm(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		version    ProtocolVersion
		rejectByte byte
	}{
		{"v5 direct reason code", ProtocolV50, byte(protocol.ClientIdentifierNotValid)},
		{"v3 return code mapped to v5 reason", ProtocolV311, 0x02}, // -> ClientIdentifierNotValid
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newMockBroker(t)
			b.RejectNextConnect(tc.rejectByte)

			cfg := newIntegrationConfig(b.URL(), "lc-reject")
			cfg.ProtocolVersion = tc.version
			c := NewTCPClient(cfg)
			lc := NewLifecycle(LifecycleConfig{InitialBackoff: 50 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}, c)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			err := lc.Start(ctx)
			var re *ReasonError
			if !errors.As(err, &re) {
				t.Fatalf("Start error = %v, want *ReasonError", err)
			}
			if re.Code != protocol.ClientIdentifierNotValid {
				t.Fatalf("reason code = %v, want ClientIdentifierNotValid", re.Code)
			}

			// A failed first connect must not leave the loop running: give
			// it several would-be backoff intervals, then confirm the
			// attempt count never grew past the single rejected CONNECT.
			time.Sleep(200 * time.Millisecond)
			if got := b.ConnCount(); got != 1 {
				t.Fatalf("ConnCount = %d after a failed Start, want 1 (no reconnect storm)", got)
			}
		})
	}
}

// TestLifecycleStopClearsStateAndAllowsFreshRestart proves Stop tears the
// session down and clears the started flag, a concurrent double Start is
// rejected, and Start after Stop dials a genuinely fresh connection (not a
// bounce off ErrAlreadyConnected — the link was actually closed, not just
// the lifecycle's own bookkeeping).
func TestLifecycleStopClearsStateAndAllowsFreshRestart(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "lc-stop-restart"))
	lc := NewLifecycle(DefaultLifecycle(), c)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := lc.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if got := b.ConnCount(); got != 1 {
		t.Fatalf("ConnCount after first Start = %d, want 1", got)
	}

	if err := lc.Start(ctx); err == nil {
		t.Fatal("concurrent double Start returned nil, want an error")
	}

	if err := lc.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if c.IsConnected() {
		t.Fatal("IsConnected = true after Stop")
	}

	if err := lc.Start(ctx); err != nil {
		t.Fatalf("Start after Stop: %v (want a fresh dial, not ErrAlreadyConnected)", err)
	}
	t.Cleanup(func() { _ = lc.Stop(context.Background()) })
	if got := b.ConnCount(); got != 2 {
		t.Fatalf("ConnCount after restart = %d, want 2 (fresh dial)", got)
	}
}

// TestLifecycleResubscribeReplayPreservesQoSAndNoLocalAfterReset proves the
// fire-and-log resubscribe replay lands after an event-driven Lifecycle
// reconnect (no manual reconnect call in this test — unlike
// adapter_integration_test.go's TestResubscribeReplayPreservesOptionsAndOrder,
// which drives TCPClient.Connect by hand), preserving both the requested
// QoS and an MQTT 5.0 subscription option (NoLocal).
func TestLifecycleResubscribeReplayPreservesQoSAndNoLocalAfterReset(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "lc-resub"))
	lc := NewLifecycle(LifecycleConfig{InitialBackoff: 20 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}, c)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := lc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lc.Stop(context.Background()) })

	if _, err := c.Subscribe(ctx, "lc/resub", QoS2, func(*Message) {}, WithNoLocal()); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if before := b.Subscriptions(); len(before) != 1 {
		t.Fatalf("Subscriptions() before reset = %d, want 1", len(before))
	}

	b.SetSessionPresent(false)
	b.InjectTCPReset()

	if !lcPoll(2*time.Second, func() bool { return len(b.Subscriptions()) >= 2 }) {
		t.Fatalf("resubscribe replay never landed after an event-driven reconnect: %d frames",
			len(b.Subscriptions()))
	}
	if !lcPoll(time.Second, func() bool { return c.IsConnected() }) {
		t.Fatal("client never settled back into a connected state")
	}

	replayed := b.Subscriptions()[1]
	if replayed.Filter != "lc/resub" {
		t.Fatalf("replayed filter = %q, want lc/resub", replayed.Filter)
	}
	if replayed.Options.QoS != byte(QoS2) {
		t.Fatalf("replayed QoS = %d, want %d", replayed.Options.QoS, QoS2)
	}
	if !replayed.Options.NoLocal {
		t.Fatal("replayed NoLocal option lost across the reconnect replay")
	}
}
