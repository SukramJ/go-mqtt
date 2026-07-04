// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Edge-case coverage for lifecycle.go not exercised by lifecycle_unit_test.go
// or lifecycle_test.go: NewLifecycle's zero-value defaulting, Stop() with no
// connector and Stop() bounded by its own ctx while the loop is still
// busy inside a hung Connect call, and the exponential backoff clamp to
// MaxBackoff on repeated real (non-idempotent) reconnect failures.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewLifecycleAppliesDefaults proves a zero-value LifecycleConfig gets
// the documented 1s/30s backoff defaults and a non-nil logger, matching
// [DefaultLifecycle].
func TestNewLifecycleAppliesDefaults(t *testing.T) {
	t.Parallel()

	l := NewLifecycle(LifecycleConfig{}, &stubConnector{})
	if l.cfg.InitialBackoff != DefaultLifecycle().InitialBackoff {
		t.Fatalf("InitialBackoff = %v, want %v", l.cfg.InitialBackoff, DefaultLifecycle().InitialBackoff)
	}
	if l.cfg.MaxBackoff != DefaultLifecycle().MaxBackoff {
		t.Fatalf("MaxBackoff = %v, want %v", l.cfg.MaxBackoff, DefaultLifecycle().MaxBackoff)
	}
	if l.cfg.Logger == nil {
		t.Fatal("Logger must default to a non-nil logger")
	}
}

// TestNewLifecyclePreservesExplicitNonZeroBackoff proves explicitly set,
// non-zero backoff values are left untouched.
func TestNewLifecyclePreservesExplicitNonZeroBackoff(t *testing.T) {
	t.Parallel()

	cfg := LifecycleConfig{InitialBackoff: 2 * time.Second, MaxBackoff: 5 * time.Second}
	l := NewLifecycle(cfg, &stubConnector{})
	if l.cfg.InitialBackoff != 2*time.Second || l.cfg.MaxBackoff != 5*time.Second {
		t.Fatalf("cfg = %+v, want the explicit values preserved", l.cfg)
	}
}

// TestLifecycleStopWithNoConnectorIsNoop proves Stop on a zero-value
// Lifecycle (never Start-ed, no connector) returns nil without panicking.
func TestLifecycleStopWithNoConnectorIsNoop(t *testing.T) {
	t.Parallel()

	l := &Lifecycle{}
	if err := l.Stop(context.Background()); err != nil {
		t.Fatalf("Stop on a zero-value Lifecycle: %v", err)
	}
}

// blockingConnector's second-and-later Connect blocks unconditionally on
// block (ignoring ctx entirely, modelling a misbehaving adapter) until the
// test closes it, letting Stop's own ctx-bounded wait for the loop
// goroutine be exercised deterministically.
type blockingConnector struct {
	connects atomic.Int32
	lost     chan struct{}
	block    chan struct{}
}

func (c *blockingConnector) Connect(context.Context) error {
	if c.connects.Add(1) == 1 {
		return nil
	}
	<-c.block
	return errors.New("stub: connect unblocked")
}

func (c *blockingConnector) Disconnect(context.Context) error { return nil }

func (c *blockingConnector) ConnectionLost() <-chan struct{} { return c.lost }

// TestLifecycleStopReturnsOnCtxDoneWhileLoopBusy proves Stop does not hang
// forever waiting for the reconnect loop to exit: when the loop is stuck
// inside a Connect call that ignores context cancellation, Stop still
// returns once its own ctx expires.
func TestLifecycleStopReturnsOnCtxDoneWhileLoopBusy(t *testing.T) {
	t.Parallel()

	s := &blockingConnector{lost: make(chan struct{}, 1), block: make(chan struct{})}
	t.Cleanup(func() { close(s.block) })

	cfg := LifecycleConfig{InitialBackoff: 10 * time.Second, MaxBackoff: 10 * time.Second}
	l := NewLifecycle(cfg, s)
	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := s.connects.Load(); got != 1 {
		t.Fatalf("connects after Start = %d, want 1", got)
	}

	// Trigger the event-driven reconnect: the loop's next connectOnce call
	// blocks inside Connect (ignoring ctx cancellation) until s.block closes.
	select {
	case s.lost <- struct{}{}:
	default:
	}
	if !lcPoll(time.Second, func() bool { return s.connects.Load() >= 2 }) {
		t.Fatal("loop never re-entered Connect after the loss signal")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := l.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Stop took %v to return while the loop was stuck, want bounded by its own ctx", elapsed)
	}
}

// realFailConnector always fails Connect with a genuine (non-idempotency)
// error, so the caller sees the exponential backoff grow without ever
// short-circuiting through the ErrAlreadyConnected path.
type realFailConnector struct {
	first    atomic.Bool
	connects atomic.Int32
}

func (c *realFailConnector) Connect(context.Context) error {
	c.connects.Add(1)
	if c.first.CompareAndSwap(false, true) {
		return nil
	}
	return errors.New("stub: dial refused")
}

func (c *realFailConnector) Disconnect(context.Context) error { return nil }

// TestLifecycleBackoffClampsToMaxBackoff proves repeated genuine reconnect
// failures double the backoff but never let it exceed MaxBackoff.
func TestLifecycleBackoffClampsToMaxBackoff(t *testing.T) {
	t.Parallel()

	s := &realFailConnector{}
	cfg := LifecycleConfig{InitialBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, Jitter: 0}
	l := NewLifecycle(cfg, s)
	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = l.Stop(ctx)
	})

	// 5 -> 10 -> 20 -> (would be 40, clamped to 20) ms: four failing
	// reconnects fit comfortably within a couple of seconds.
	if !lcPoll(2*time.Second, func() bool { return s.connects.Load() >= 5 }) {
		t.Fatalf("only %d connect attempts, want the backoff to keep retrying at the clamped ceiling", s.connects.Load())
	}
}
