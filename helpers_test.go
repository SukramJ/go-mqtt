// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// recordingPublisher and recordingSubscriber are the two halves SplitClient
// joins; each records that it — and not the other — was the one called.
type recordingPublisher struct{ calls atomic.Int32 }

func (p *recordingPublisher) Publish(context.Context, string, []byte, QoS, bool, ...PublishOption) error {
	p.calls.Add(1)
	return nil
}

type recordingSubscriber struct {
	subscribes   atomic.Int32
	unsubscribes atomic.Int32
}

func (s *recordingSubscriber) Subscribe(context.Context, string, QoS, MessageHandler, ...SubscribeOption) (SubscribeResult, error) {
	s.subscribes.Add(1)
	return SubscribeResult{}, nil
}

func (s *recordingSubscriber) Unsubscribe(context.Context, string) error {
	s.unsubscribes.Add(1)
	return nil
}

// TestSplitClientRoutesEachHalf pins the whole contract: publish goes to the
// Publisher, subscribe and unsubscribe to the Subscriber, and neither leaks
// into the other. Getting this backwards would send every publish through an
// undecorated client and silently defeat a consumer's circuit breaker.
func TestSplitClientRoutesEachHalf(t *testing.T) {
	t.Parallel()

	pub := &recordingPublisher{}
	sub := &recordingSubscriber{}
	client := SplitClient(pub, sub)

	ctx := context.Background()
	if err := client.Publish(ctx, "t", nil, QoS0, false); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := client.Subscribe(ctx, "t", QoS0, func(*Message) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := client.Unsubscribe(ctx, "t"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	if got := pub.calls.Load(); got != 1 {
		t.Errorf("publisher calls = %d, want 1", got)
	}
	if got := sub.subscribes.Load(); got != 1 {
		t.Errorf("subscribes = %d, want 1", got)
	}
	if got := sub.unsubscribes.Load(); got != 1 {
		t.Errorf("unsubscribes = %d, want 1", got)
	}
}

// TestSplitClientWrapsOnlyThePublisher is the case the helper was extracted
// for: a Breaker on the publish path, the raw client on the subscribe path.
func TestSplitClientWrapsOnlyThePublisher(t *testing.T) {
	t.Parallel()

	pub := &recordingPublisher{}
	sub := &recordingSubscriber{}
	breaker := NewBreaker(pub, BreakerConfig{})
	client := SplitClient(breaker, sub)

	if err := client.Publish(context.Background(), "t", nil, QoS0, false); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := pub.calls.Load(); got != 1 {
		t.Errorf("publisher calls = %d, want 1 — the breaker did not forward", got)
	}
	// The subscribe half must still reach the raw subscriber: a Breaker has no
	// Subscribe at all, so joining it with one is the only way this compiles —
	// which is precisely the shape the helper exists to express.
	if _, err := client.Subscribe(context.Background(), "t", QoS0, func(*Message) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := sub.subscribes.Load(); got != 1 {
		t.Errorf("subscribes = %d, want 1", got)
	}
}

// fakeStarter fails a fixed number of times before succeeding.
type fakeStarter struct {
	failures int
	calls    atomic.Int32
	err      error
	onCall   func(attempt int)
}

func (f *fakeStarter) Start(context.Context) error {
	n := int(f.calls.Add(1))
	if f.onCall != nil {
		f.onCall(n)
	}
	if n <= f.failures {
		if f.err != nil {
			return f.err
		}
		return errors.New("dial refused")
	}
	return nil
}

// TestConnectWithRetrySucceedsAfterFailures is the daemon case: the broker is
// not up at boot, and the process comes up anyway.
func TestConnectWithRetrySucceedsAfterFailures(t *testing.T) {
	t.Parallel()

	starter := &fakeStarter{failures: 2}
	err := ConnectWithRetry(context.Background(), starter, RetryConfig{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("ConnectWithRetry: %v", err)
	}
	if got := starter.calls.Load(); got != 3 {
		t.Errorf("Start calls = %d, want 3", got)
	}
}

// TestConnectWithRetryReturnsImmediatelyOnSuccess guards against the helper
// sleeping before the first attempt — a broker that is already up must not
// cost a backoff.
func TestConnectWithRetryReturnsImmediatelyOnSuccess(t *testing.T) {
	t.Parallel()

	starter := &fakeStarter{}
	start := time.Now()
	if err := ConnectWithRetry(context.Background(), starter, RetryConfig{
		InitialBackoff: time.Hour,
	}); err != nil {
		t.Fatalf("ConnectWithRetry: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v — it slept before the first attempt", elapsed)
	}
	if got := starter.calls.Load(); got != 1 {
		t.Errorf("Start calls = %d, want 1", got)
	}
}

// TestConnectWithRetryHonoursMaxAttempts pins the bounded form, and that the
// last connect error survives errors.Is through the wrapping.
func TestConnectWithRetryHonoursMaxAttempts(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("broker refused")
	starter := &fakeStarter{failures: 1000, err: sentinel}
	err := ConnectWithRetry(context.Background(), starter, RetryConfig{
		InitialBackoff: time.Millisecond,
		MaxAttempts:    3,
	})
	if err == nil {
		t.Fatal("ConnectWithRetry succeeded, want failure")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it to wrap the last Start error", err)
	}
	if got := starter.calls.Load(); got != 3 {
		t.Errorf("Start calls = %d, want 3", got)
	}
}

// TestConnectWithRetryReportsContextCancellation is the distinction that makes
// the error useful: an orderly shutdown must not look like a broker problem.
func TestConnectWithRetryReportsContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	starter := &fakeStarter{failures: 1000}
	starter.onCall = func(attempt int) {
		if attempt == 2 {
			cancel()
		}
	}

	err := ConnectWithRetry(ctx, starter, RetryConfig{InitialBackoff: time.Millisecond})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestConnectWithRetryCancelDuringBackoff covers the other cancellation point:
// the wait between attempts, not the attempt itself.
func TestConnectWithRetryCancelDuringBackoff(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	starter := &fakeStarter{failures: 1000}

	done := make(chan error, 1)
	go func() {
		done <- ConnectWithRetry(ctx, starter, RetryConfig{InitialBackoff: time.Hour})
	}()

	// The first attempt fails immediately, so the goroutine is parked in the
	// hour-long backoff by the time this fires.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ConnectWithRetry ignored cancellation during backoff")
	}
}

// TestConnectWithRetryDefaultsAreUsable pins the zero-value config: it must
// not busy-loop, and it must not reject a nil Logger.
func TestConnectWithRetryDefaultsAreUsable(t *testing.T) {
	t.Parallel()

	starter := &fakeStarter{}
	if err := ConnectWithRetry(context.Background(), starter, RetryConfig{}); err != nil {
		t.Fatalf("ConnectWithRetry with zero config: %v", err)
	}
}

// TestConnectWithRetryClampsMaxBelowInitial pins the clamp that keeps the
// backoff monotonic when a caller configures the two bounds inverted.
func TestConnectWithRetryClampsMaxBelowInitial(t *testing.T) {
	t.Parallel()

	starter := &fakeStarter{failures: 2}
	start := time.Now()
	err := ConnectWithRetry(context.Background(), starter, RetryConfig{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     time.Millisecond, // below InitialBackoff
	})
	if err != nil {
		t.Fatalf("ConnectWithRetry: %v", err)
	}
	// Two backoffs, each clamped up to InitialBackoff rather than down to
	// MaxBackoff, so the total cannot be under one InitialBackoff.
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Errorf("elapsed %v — MaxBackoff below InitialBackoff was not raised", elapsed)
	}
}

// TestConnectWithRetryDrivesALifecycle proves the helper actually fits the
// type it was written for, rather than only the test fake.
func TestConnectWithRetryDrivesALifecycle(t *testing.T) {
	t.Parallel()

	broker := newMockBroker(t)
	client := NewTCPClient(newIntegrationConfig(broker.URL(), "connect-with-retry"))
	lc := NewLifecycle(LifecycleConfig{FlapWindow: -1}, client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := ConnectWithRetry(ctx, lc, RetryConfig{InitialBackoff: time.Millisecond}); err != nil {
		t.Fatalf("ConnectWithRetry: %v", err)
	}
	if err := lc.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
