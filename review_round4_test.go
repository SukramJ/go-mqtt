// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Regression tests for the round-4 (full-codebase audit) high-severity
// findings: the teardown/Connect race that poisoned a healthy new
// session's quota (F-01), the undamped event-driven reconnect storm
// against a flapping broker (F-02), and the circuit breaker counting
// client-side validation errors as broker failures (F-03).

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// TestTeardownPoisonsSharedStateBeforeClearingLink proves teardownLink
// keeps the link pointer published until the shared waiter table and send
// quota are failed. Before the fix the pointer was cleared first, so a
// Connect landing in the window established a healthy new session whose
// quota the stale teardown then marked failed — every subsequent QoS>0
// publish returned ErrConnectionLost forever while IsConnected reported
// true, and no reconnect could repair it.
func TestTeardownPoisonsSharedStateBeforeClearingLink(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "teardown-order"))
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	l := c.link.Load()
	if l == nil {
		t.Fatal("no link after connect")
	}

	// Park the teardown inside failAllWaiters — which the fix moves BEFORE
	// the link-pointer clear — by holding the waiter mutex from the test.
	c.waitersMu.Lock()
	b.InjectTCPReset()

	// Wait until the read loop noticed the reset and entered teardownLink.
	deadline := time.Now().Add(3 * time.Second)
	for !isStopping(l) {
		if time.Now().After(deadline) {
			c.waitersMu.Unlock()
			t.Fatal("teardown never started")
		}
		time.Sleep(time.Millisecond)
	}

	// While the teardown is parked, the link must still read as the old
	// one, so a concurrent Connect refuses with ErrAlreadyConnected
	// instead of establishing a session the parked teardown would poison.
	for range 50 {
		cctx, ccancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		err := c.Connect(cctx)
		ccancel()
		if err == nil {
			c.waitersMu.Unlock()
			t.Fatal("Connect succeeded while a teardown was still poisoning shared state")
		}
		if !errors.Is(err, ErrAlreadyConnected) {
			c.waitersMu.Unlock()
			t.Fatalf("Connect: got %v, want ErrAlreadyConnected", err)
		}
		time.Sleep(time.Millisecond)
	}
	c.waitersMu.Unlock()

	// Once the teardown completes, a reconnect must yield a fully healthy
	// session: QoS 1 publishes acquire quota and complete.
	deadline = time.Now().Add(3 * time.Second)
	for c.link.Load() != nil {
		if time.Now().After(deadline) {
			t.Fatal("teardown never cleared the link")
		}
		time.Sleep(time.Millisecond)
	}
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if err := c.Publish(ctx, "audit/teardown", []byte("ok"), QoS1, false); err != nil {
		t.Fatalf("QoS1 publish on the reconnected session: %v (quota poisoned by stale teardown?)", err)
	}
}

// flappyConnector simulates a broker that accepts every CONNECT and then
// immediately drops the socket: each Connect succeeds and instantly
// signals a connection loss.
type flappyConnector struct {
	connects atomic.Int32
	lost     chan struct{}
}

func newFlappyConnector() *flappyConnector {
	return &flappyConnector{lost: make(chan struct{}, 1)}
}

func (f *flappyConnector) Connect(context.Context) error {
	f.connects.Add(1)
	select {
	case f.lost <- struct{}{}:
	default:
	}
	return nil
}

func (f *flappyConnector) Disconnect(context.Context) error { return nil }

func (f *flappyConnector) ConnectionLost() <-chan struct{} { return f.lost }

// TestLifecycleFlappingBrokerIsDamped proves the event-driven reconnect
// path applies a growing delay when connections die within FlapWindow of
// coming up. Before the fix the lost-event branch reset the backoff and
// reconnected with no delay at all, producing thousands of full
// dial+CONNECT cycles per second against a flapping broker.
func TestLifecycleFlappingBrokerIsDamped(t *testing.T) {
	t.Parallel()

	f := newFlappyConnector()
	lc := NewLifecycle(LifecycleConfig{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     time.Second,
		Jitter:         -1, // disable (<=0 returns d unchanged)
		FlapWindow:     time.Hour,
	}, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := lc.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = lc.Stop(context.Background()) }()

	time.Sleep(300 * time.Millisecond)
	got := f.connects.Load()
	// Damped: first connect plus a handful of delayed retries (50, 100,
	// 200ms flap delays, plus at most a few timer-driven probes). The
	// undamped loop measured six-digit connect counts in this window.
	if got > 15 {
		t.Fatalf("flapping connector was reconnected %d times in 300ms — event-driven reconnect is undamped", got)
	}
	if got < 2 {
		t.Fatalf("reconnect loop appears dead: %d connects", got)
	}
}

// stableConnector succeeds on every Connect and lets the test signal a
// single connection loss.
type stableConnector struct {
	connects atomic.Int32
	lost     chan struct{}
}

func (s *stableConnector) Connect(context.Context) error {
	s.connects.Add(1)
	return nil
}

func (s *stableConnector) Disconnect(context.Context) error { return nil }

func (s *stableConnector) ConnectionLost() <-chan struct{} { return s.lost }

// TestLifecycleStableConnectionReconnectsImmediately locks in the fast
// path the flap damping must not regress: a connection that outlived
// FlapWindow reconnects immediately on a loss event, not after a backoff.
func TestLifecycleStableConnectionReconnectsImmediately(t *testing.T) {
	t.Parallel()

	s := &stableConnector{lost: make(chan struct{}, 1)}
	lc := NewLifecycle(LifecycleConfig{
		InitialBackoff: 10 * time.Second, // timer path must play no part
		MaxBackoff:     30 * time.Second,
		Jitter:         -1,
		FlapWindow:     time.Nanosecond, // any measurable uptime counts as stable
	}, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := lc.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = lc.Stop(context.Background()) }()

	s.lost <- struct{}{}
	deadline := time.Now().Add(time.Second)
	for s.connects.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("no immediate reconnect after a stable connection dropped (connects=%d)", s.connects.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// validationErrPublisher always fails with the caller-side validation
// error TCPClient.Publish raises for an invalid topic.
type validationErrPublisher struct{ err error }

func (p *validationErrPublisher) Publish(context.Context, string, []byte, QoS, bool, ...PublishOption) error {
	return p.err
}

// TestBreakerIgnoresLocalValidationErrors proves client-side validation
// failures never trip the circuit — the documented contract. Before the
// fix any protocol.ErrProtocolViolation fell through countableFailure's
// final catch-all and opened the circuit for every healthy publish.
func TestBreakerIgnoresLocalValidationErrors(t *testing.T) {
	t.Parallel()

	topicErr := fmt.Errorf("mqtt/tcp: %w", protocol.ValidateTopicName("a/+/b"))
	if !errors.Is(topicErr, protocol.ErrProtocolViolation) {
		t.Fatalf("test premise: %v is not a protocol violation", topicErr)
	}

	p := &validationErrPublisher{err: topicErr}
	br := NewBreaker(p, BreakerConfig{FailureThreshold: 2})
	for range 10 {
		if err := br.Publish(context.Background(), "a/+/b", nil, QoS1, false); !errors.Is(err, protocol.ErrProtocolViolation) {
			t.Fatalf("publish: got %v, want the validation error passed through", err)
		}
	}
	if got := br.State(); got != BreakerClosed {
		t.Fatalf("breaker state after local validation errors: %v, want closed", got)
	}

	// Genuine broker-side symptoms must still count.
	p.err = ErrConnectionLost
	for range 2 {
		_ = br.Publish(context.Background(), "ok/topic", nil, QoS1, false)
	}
	if got := br.State(); got != BreakerOpen {
		t.Fatalf("breaker state after broker failures: %v, want open", got)
	}
}

// TestCountableFailureClassification pins the neutral-vs-countable table
// for every error shape TCPClient.Publish can return.
func TestCountableFailureClassification(t *testing.T) {
	t.Parallel()

	bg := context.Background()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"ack timeout", errAckTimeout, true},
		{"connection lost", ErrConnectionLost, true},
		{"not connected", ErrNotConnected, true},
		{"broker reject", &ReasonError{Packet: "PUBLISH", Code: protocol.NotAuthorized}, true},
		{"packet too large", ErrPacketTooLarge, false},
		{"packet ids exhausted", ErrPacketIDExhausted, false},
		{"invalid topic", fmt.Errorf("mqtt/tcp: %w", protocol.ValidateTopicName("a/#/b")), false},
		{"qos above maximum", fmt.Errorf("mqtt/tcp: %w: QoS 2 above broker maximum", protocol.ErrProtocolViolation), false},
		{"malformed", fmt.Errorf("encode: %w", protocol.ErrMalformedPacket), false},
		{"string too long", fmt.Errorf("encode: %w", protocol.ErrStringTooLong), false},
	}
	for _, tc := range cases {
		if got := countableFailure(bg, tc.err); got != tc.want {
			t.Errorf("countableFailure(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}
