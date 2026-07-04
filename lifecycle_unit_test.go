// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Lifecycle unit tests — stub connectors only, no network. The mock-broker
// integration tests live in lifecycle_test.go.
// ---------------------------------------------------------------------------

// lcPoll polls fn up to timeout, sleeping 2 ms between attempts, and
// reports whether fn returned true before the deadline. Named distinctly
// so it does not collide with the mock-broker helpers.
func lcPoll(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}

// stubConnector is a minimal in-memory [Connector] for the timer-only
// lifecycle paths.
type stubConnector struct {
	connects    atomic.Int32
	disconnects atomic.Int32
	err         atomic.Value // error returned by Connect, nil when unset
}

func (s *stubConnector) Connect(_ context.Context) error {
	s.connects.Add(1)
	if e := s.err.Load(); e != nil {
		if err, ok := e.(error); ok {
			return err
		}
	}
	return nil
}

func (s *stubConnector) Disconnect(_ context.Context) error {
	s.disconnects.Add(1)
	return nil
}

func TestLifecycleStartFiresOnConnect(t *testing.T) {
	t.Parallel()

	s := &stubConnector{}
	l := NewLifecycle(DefaultLifecycle(), s)
	var callbacks int
	l.OnConnect(func(context.Context) { callbacks++ })
	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = l.Stop(context.Background()) }()
	if s.connects.Load() != 1 || callbacks != 1 {
		t.Fatalf("connects=%d cb=%d", s.connects.Load(), callbacks)
	}
}

func TestLifecycleFirstConnectErrorBubbles(t *testing.T) {
	t.Parallel()

	s := &stubConnector{}
	s.err.Store(errors.New("boom"))
	l := NewLifecycle(DefaultLifecycle(), s)
	if err := l.Start(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestLifecycleStopCallsDisconnect(t *testing.T) {
	t.Parallel()

	s := &stubConnector{}
	l := NewLifecycle(DefaultLifecycle(), s)
	_ = l.Start(context.Background())
	if err := l.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if s.disconnects.Load() != 1 {
		t.Fatalf("disconnects=%d", s.disconnects.Load())
	}
}

func TestLifecycleJitterBounded(t *testing.T) {
	t.Parallel()

	cfg := DefaultLifecycle()
	cfg.Jitter = 10 * time.Millisecond
	l := NewLifecycle(cfg, &stubConnector{})
	for range 50 {
		got := l.jittered(100 * time.Millisecond)
		if got < 90*time.Millisecond || got > 110*time.Millisecond {
			t.Fatalf("jittered=%v", got)
		}
	}
}

// notifyingStub is a [Connector] that also implements [ConnectionNotifier].
// It models a real adapter: the first Connect establishes the session,
// further Connects while still "connected" return a wrapped
// [ErrAlreadyConnected] (the idle-probe path), and signalLost mimics the
// socket dropping — it clears the connected flag and pushes a value onto
// the buffered, non-blocking ConnectionLost channel. When failNext is set,
// Connect fails outright, exercising the reconnect-failure backoff path.
type notifyingStub struct {
	connects    atomic.Int32
	disconnects atomic.Int32
	connected   atomic.Bool
	failNext    atomic.Bool
	lost        chan struct{}
}

func newNotifyingStub() *notifyingStub {
	return &notifyingStub{lost: make(chan struct{}, 1)}
}

func (n *notifyingStub) Connect(_ context.Context) error {
	n.connects.Add(1)
	if n.failNext.Load() {
		return errors.New("stub: dial failed")
	}
	if !n.connected.CompareAndSwap(false, true) {
		return fmt.Errorf("stub: %w", ErrAlreadyConnected)
	}
	return nil
}

func (n *notifyingStub) Disconnect(_ context.Context) error {
	n.disconnects.Add(1)
	n.connected.Store(false)
	return nil
}

func (n *notifyingStub) ConnectionLost() <-chan struct{} { return n.lost }

// signalLost models the adapter noticing the socket dropped: the session
// is gone (so the next Connect re-dials) and the loss is announced on the
// non-blocking channel.
func (n *notifyingStub) signalLost() {
	n.connected.Store(false)
	select {
	case n.lost <- struct{}{}:
	default:
	}
}

// TestLifecycleNotifierTriggersPromptReconnect proves the loop reconnects
// as soon as the ConnectionLost channel fires, well before the (here huge)
// backoff timer would ever elapse. InitialBackoff is set to 10s so the
// only way a second connect can happen within the test window is the
// event-driven notifier path.
func TestLifecycleNotifierTriggersPromptReconnect(t *testing.T) {
	t.Parallel()

	s := newNotifyingStub()
	cfg := LifecycleConfig{
		InitialBackoff: 10 * time.Second, // timer path would not fire in-window
		MaxBackoff:     10 * time.Second,
		Jitter:         0,
	}
	l := NewLifecycle(cfg, s)

	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = l.Stop(stopCtx)
	})

	if got := s.connects.Load(); got != 1 {
		t.Fatalf("after Start: connects=%d, want 1", got)
	}

	// Fire the loss signal and time how long the reconnect takes.
	start := time.Now()
	s.signalLost()

	if !lcPoll(2*time.Second, func() bool { return s.connects.Load() >= 2 }) {
		t.Fatalf("notifier did not trigger a reconnect; connects=%d", s.connects.Load())
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("reconnect took %v — did not beat the 10s backoff timer", elapsed)
	}
}

// TestLifecycleNotifierResetsBackoff proves the loop resets the backoff to
// InitialBackoff when a loss signal arrives. The connector is first driven
// into its MaxBackoff idle-probe state (first connect OK, second returns
// ErrAlreadyConnected). Then failNext is armed and a loss is signalled: if
// the backoff was reset to InitialBackoff, the following failing reconnects
// grow from InitialBackoff and fire several times inside the window; if it
// were left at MaxBackoff, at most one attempt would fit.
func TestLifecycleNotifierResetsBackoff(t *testing.T) {
	t.Parallel()

	s := newNotifyingStub()
	cfg := LifecycleConfig{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     3 * time.Second,
		Jitter:         0,
	}
	l := NewLifecycle(cfg, s)

	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = l.Stop(stopCtx)
	})

	// Wait for the idle probe (connect #2 → ErrAlreadyConnected) which
	// parks the backoff at MaxBackoff.
	if !lcPoll(2*time.Second, func() bool { return s.connects.Load() >= 2 }) {
		t.Fatalf("idle probe never ran; connects=%d", s.connects.Load())
	}
	baseline := s.connects.Load()

	// Arm failing reconnects, then signal loss. With the backoff reset to
	// InitialBackoff (20ms) the failing attempts grow 20→40→80ms and land
	// several times in 400ms; stuck at MaxBackoff (3s) only one would.
	s.failNext.Store(true)
	s.signalLost()

	if !lcPoll(400*time.Millisecond, func() bool { return s.connects.Load()-baseline >= 3 }) {
		t.Fatalf(
			"backoff not reset after loss: only %d reconnect attempts in 400ms (want >= 3)",
			s.connects.Load()-baseline,
		)
	}
}
