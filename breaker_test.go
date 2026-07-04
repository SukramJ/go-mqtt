// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-mqtt authors.

package mqtt

// Unit tests for the Breaker circuit-breaking Publisher decorator
// (breaker.go): default config, the closed -> open -> half-open -> closed
// (and half-open -> open) state machine driven by a fake Publisher and a
// fake clock (cfg.now), the neutral-error classification that must
// neither trip nor reset the failure counter, OnStateChange sequencing
// and lock-safety, and a concurrency race sweep. The final test drives a
// real TCPClient against the in-package mockBroker (see
// adapter_integration_test.go / qos_test.go for the harness pattern) to
// prove the wrapping holds end to end against a genuine ack timeout.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// fakePublisher is a scriptable Publisher stand-in: each call pulls the
// next queued result (or the last one, if the queue is exhausted) and
// counts invocations so tests can assert the wrapped Publisher was (or
// was not) reached.
type fakePublisher struct {
	mu      sync.Mutex
	results []error
	calls   int
}

func (f *fakePublisher) Publish(ctx context.Context, topic string, payload []byte, qos QoS, retain bool, opts ...PublishOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.results) == 0 {
		return nil
	}
	if len(f.results) == 1 {
		return f.results[0]
	}
	next := f.results[0]
	f.results = f.results[1:]
	return next
}

func (f *fakePublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakePublisher) setResults(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = errs
}

// fakeClock is a manually-advanced clock seam for cfg.now.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// publishOnce calls Publish with a background context and no payload
// options, the shape every test below needs.
func publishOnce(t *testing.T, b *Breaker) error {
	t.Helper()
	return b.Publish(context.Background(), "t/topic", []byte("x"), QoS1, false)
}

// TestNewBreakerAppliesDefaults proves the zero-value BreakerConfig is
// usable: threshold 5, 30s recovery, a single half-open probe slot.
func TestNewBreakerAppliesDefaults(t *testing.T) {
	t.Parallel()

	b := NewBreaker(&fakePublisher{}, BreakerConfig{})
	if b.cfg.FailureThreshold != 5 {
		t.Fatalf("FailureThreshold = %d, want 5", b.cfg.FailureThreshold)
	}
	if b.cfg.RecoveryTimeout != 30*time.Second {
		t.Fatalf("RecoveryTimeout = %v, want 30s", b.cfg.RecoveryTimeout)
	}
	if b.cfg.HalfOpenMax != 1 {
		t.Fatalf("HalfOpenMax = %d, want 1", b.cfg.HalfOpenMax)
	}
	if b.cfg.now == nil {
		t.Fatal("now = nil, want time.Now fallback")
	}
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("initial State() = %v, want closed", got)
	}
}

// TestNewBreakerRejectsNegativeConfigTheSameAsZero proves negative tuning
// values fall back to the same defaults as the zero value (the guard is
// `<= 0`, not `== 0`).
func TestNewBreakerRejectsNegativeConfigTheSameAsZero(t *testing.T) {
	t.Parallel()

	b := NewBreaker(&fakePublisher{}, BreakerConfig{FailureThreshold: -1, RecoveryTimeout: -time.Second, HalfOpenMax: -3})
	if b.cfg.FailureThreshold != 5 || b.cfg.RecoveryTimeout != 30*time.Second || b.cfg.HalfOpenMax != 1 {
		t.Fatalf("cfg = %+v, want the same defaults as the zero value", b.cfg)
	}
}

// TestClosedOpensAfterExactlyThresholdConsecutiveFailures proves the
// circuit trips on the FailureThreshold-th countable failure, not before,
// and that an intervening success resets the streak so the count starts
// over.
func TestClosedOpensAfterExactlyThresholdConsecutiveFailures(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 3, now: clk.Now})

	fp.setResults(errAckTimeout)
	for i := 1; i < 3; i++ {
		if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
			t.Fatalf("publish %d err = %v, want errAckTimeout", i, err)
		}
		if got := b.State(); got != BreakerClosed {
			t.Fatalf("after %d/3 failures State() = %v, want closed", i, got)
		}
	}

	// A success before the threshold is reached resets the streak.
	fp.setResults(nil)
	if err := publishOnce(t, b); err != nil {
		t.Fatalf("reset publish: %v", err)
	}
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("State() after reset success = %v, want closed", got)
	}

	fp.setResults(errAckTimeout)
	for i := 1; i < 3; i++ {
		if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
			t.Fatalf("post-reset publish %d err = %v, want errAckTimeout", i, err)
		}
		if got := b.State(); got != BreakerClosed {
			t.Fatalf("post-reset after %d/3 failures State() = %v, want closed", i, got)
		}
	}
	// Third consecutive countable failure trips the breaker.
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("tripping publish err = %v, want errAckTimeout", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("State() after threshold failures = %v, want open", got)
	}
}

// TestOpenFailsFastWithoutCallingWrappedPublisher proves an open circuit
// short-circuits Publish entirely: ErrCircuitOpen, and the wrapped
// Publisher's call count does not move.
func TestOpenFailsFastWithoutCallingWrappedPublisher(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 1, RecoveryTimeout: time.Minute, now: clk.Now})

	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("tripping publish err = %v, want errAckTimeout", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("State() = %v, want open", got)
	}
	before := fp.callCount()

	for i := range 3 {
		if err := publishOnce(t, b); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("publish %d while open err = %v, want ErrCircuitOpen", i, err)
		}
	}
	if after := fp.callCount(); after != before {
		t.Fatalf("wrapped Publisher call count = %d, want unchanged at %d while open", after, before)
	}
}

// TestOpenAdmitsExactlyOneProbeAfterRecoveryTimeout proves that once the
// clock has advanced past RecoveryTimeout, exactly HalfOpenMax=1 publish
// is admitted as a probe (reaching the wrapped Publisher) while a second,
// concurrent publish still fails fast with ErrCircuitOpen.
func TestOpenAdmitsExactlyOneProbeAfterRecoveryTimeout(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 10 * time.Second, HalfOpenMax: 1, now: clk.Now})

	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("tripping publish err = %v, want errAckTimeout", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("State() = %v, want open", got)
	}

	// Not yet past RecoveryTimeout: still fails fast.
	if err := publishOnce(t, b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("publish before RecoveryTimeout err = %v, want ErrCircuitOpen", err)
	}

	clk.Advance(10*time.Second + time.Millisecond)

	// Block the admitted probe inside the wrapped Publisher so a second,
	// genuinely concurrent publish is guaranteed to observe the
	// half-open state with its single slot already taken.
	release := make(chan struct{})
	entered := make(chan struct{})
	blocking := &blockingPublisher{enter: entered, release: release}
	b2 := NewBreaker(blocking, BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 10 * time.Second, HalfOpenMax: 1, now: clk.Now})
	b2.mu.Lock()
	b2.state = BreakerOpen
	b2.openedAt = clk.Now().Add(-11 * time.Second)
	b2.mu.Unlock()

	probeErrCh := make(chan error, 1)
	go func() { probeErrCh <- publishOnce(t, b2) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("probe never reached the wrapped Publisher")
	}
	if got := b2.State(); got != BreakerHalfOpen {
		t.Fatalf("State() while probe in flight = %v, want half-open", got)
	}

	// Second publish while the sole slot is occupied must fail fast.
	if err := publishOnce(t, b2); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("concurrent publish err = %v, want ErrCircuitOpen", err)
	}

	close(release)
	if err := <-probeErrCh; err != nil {
		t.Fatalf("probe publish err = %v, want nil", err)
	}
}

// blockingPublisher signals entered once Publish is called and then
// blocks until release is closed, letting a test hold a half-open probe
// slot open for a controlled window.
type blockingPublisher struct {
	enter   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingPublisher) Publish(ctx context.Context, topic string, payload []byte, qos QoS, retain bool, opts ...PublishOption) error {
	p.once.Do(func() { close(p.enter) })
	<-p.release
	return nil
}

// TestHalfOpenProbeSuccessCloses proves a successful half-open probe
// closes the circuit again.
func TestHalfOpenProbeSuccessCloses(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 1, RecoveryTimeout: time.Second, now: clk.Now})

	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("tripping publish err = %v, want errAckTimeout", err)
	}

	clk.Advance(time.Second + time.Millisecond)
	fp.setResults(nil)
	if err := publishOnce(t, b); err != nil {
		t.Fatalf("probe publish err = %v, want nil", err)
	}
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("State() after successful probe = %v, want closed", got)
	}

	// A fresh failure streak from closed needs the full threshold again.
	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("post-close publish err = %v, want errAckTimeout", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("State() = %v, want open (threshold 1)", got)
	}
}

// TestHalfOpenProbeFailureReopensWithFreshWindow proves a failed probe
// re-opens the circuit immediately and restarts the RecoveryTimeout
// window from the moment of the failed probe, not the original trip.
func TestHalfOpenProbeFailureReopensWithFreshWindow(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 1, RecoveryTimeout: 10 * time.Second, now: clk.Now})

	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("tripping publish err = %v, want errAckTimeout", err)
	}

	clk.Advance(10*time.Second + time.Millisecond)
	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("failed probe err = %v, want errAckTimeout", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("State() after failed probe = %v, want open", got)
	}

	// Immediately after re-opening, still within the fresh window: fails
	// fast rather than probing again.
	if err := publishOnce(t, b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("publish right after re-open err = %v, want ErrCircuitOpen", err)
	}

	// Advancing only up to (not past) the original trip-plus-timeout mark
	// must still fail fast: the window restarted at the failed-probe
	// time, ~10s+1ms after the original trip.
	clk.Advance(9 * time.Second)
	if err := publishOnce(t, b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("publish before the fresh window elapses err = %v, want ErrCircuitOpen", err)
	}

	// Advance past the fresh window: a new probe is admitted.
	clk.Advance(2 * time.Second)
	fp.setResults(nil)
	if err := publishOnce(t, b); err != nil {
		t.Fatalf("probe after fresh window err = %v, want nil", err)
	}
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("State() after successful second probe = %v, want closed", got)
	}
}

// TestNeutralErrorsDoNotTripOrResetFailureCounter proves
// ErrPacketTooLarge, ErrPacketIDExhausted and a caller-cancelled context
// are neutral: interleaved between countable failures they neither
// advance the circuit toward open nor reset the failure streak. With
// FailureThreshold 5, two countable failures + a neutral error + three
// more countable failures must open on the fifth countable failure (the
// neutral error does not consume or reset a slot).
func TestNeutralErrorsDoNotTripOrResetFailureCounter(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 5, now: clk.Now})

	fp.setResults(errAckTimeout)
	for i := 1; i <= 2; i++ {
		if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
			t.Fatalf("countable failure %d err = %v, want errAckTimeout", i, err)
		}
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, neutral := range []error{ErrPacketTooLarge, ErrPacketIDExhausted, cancelledCtx.Err()} {
		fp.setResults(neutral)
		if err := b.Publish(cancelledCtxOrBackground(cancelledCtx, neutral), "t/topic", []byte("x"), QoS1, false); !errors.Is(err, neutral) {
			t.Fatalf("neutral publish err = %v, want %v", err, neutral)
		}
		if got := b.State(); got != BreakerClosed {
			t.Fatalf("State() after neutral error %v = %v, want closed", neutral, got)
		}
	}

	// Three more countable failures: total countable failures logged is
	// 2 + 3 = 5, hitting the threshold on the fifth (the neutral error in
	// between must not have reset the streak back to zero, nor must it
	// have silently counted toward the threshold on its own).
	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("countable failure 3 err = %v, want errAckTimeout", err)
	}
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("State() after 3rd countable failure = %v, want closed", got)
	}
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("countable failure 4 err = %v, want errAckTimeout", err)
	}
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("State() after 4th countable failure = %v, want closed", got)
	}
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("countable failure 5 err = %v, want errAckTimeout", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("State() after 5th countable failure = %v, want open (neutral error must not have reset the streak)", got)
	}
}

// cancelledCtxOrBackground returns ctx when the failure under test is the
// ctx's own cancellation error (so countableFailure's ctx.Err() branch is
// exercised for real), and context.Background() for every other neutral
// error, so those cases aren't accidentally routed through the
// cancellation branch instead of their own classification path.
func cancelledCtxOrBackground(ctx context.Context, err error) context.Context {
	if errors.Is(err, context.Canceled) {
		return ctx
	}
	return context.Background()
}

// TestNeutralErrorDuringHalfOpenReleasesProbeSlot proves a neutral error
// returned by a half-open probe releases the probe slot immediately,
// without waiting out RecoveryTimeout again, and without moving the
// circuit toward open or closed.
func TestNeutralErrorDuringHalfOpenReleasesProbeSlot(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 1, RecoveryTimeout: time.Second, HalfOpenMax: 1, now: clk.Now})

	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("tripping publish err = %v, want errAckTimeout", err)
	}
	clk.Advance(time.Second + time.Millisecond)

	fp.setResults(ErrPacketTooLarge)
	if err := publishOnce(t, b); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("neutral probe err = %v, want ErrPacketTooLarge", err)
	}
	if got := b.State(); got != BreakerHalfOpen {
		t.Fatalf("State() after neutral probe = %v, want half-open", got)
	}

	// No clock advance: the slot must already be free again.
	fp.setResults(nil)
	if err := publishOnce(t, b); err != nil {
		t.Fatalf("second probe err = %v, want nil", err)
	}
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("State() after successful second probe = %v, want closed", got)
	}
}

// TestReasonErrorWithErrorCodeCountsWithoutErrorCodeDoesNot proves the
// *ReasonError classification branch: a code >= 0x80 (IsError() true)
// counts as a broker-side failure; a code below 0x80 is neutral.
func TestReasonErrorWithErrorCodeCountsWithoutErrorCodeDoesNot(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 1, now: clk.Now})

	// GrantedQoS1 (0x01) is not an error code.
	grantedQoS1 := &ReasonError{Packet: "SUBSCRIBE", Code: protocol.GrantedQoS1}
	if grantedQoS1.Code.IsError() {
		t.Fatal("fixture sanity check failed: GrantedQoS1 must not be an error code")
	}
	fp.setResults(grantedQoS1)
	if err := publishOnce(t, b); !errors.Is(err, error(grantedQoS1)) {
		t.Fatalf("publish err = %v, want the ReasonError instance back unwrapped", err)
	}
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("State() after non-error ReasonCode = %v, want closed (neutral)", got)
	}

	// QuotaExceeded (0x97) is an error code: counts, and with
	// FailureThreshold 1 trips immediately.
	quotaExceeded := &ReasonError{Packet: "PUBLISH", Code: protocol.QuotaExceeded}
	fp.setResults(quotaExceeded)
	if err := publishOnce(t, b); !errors.Is(err, error(quotaExceeded)) {
		t.Fatalf("publish err = %v, want the ReasonError instance back", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("State() after error ReasonCode = %v, want open", got)
	}
}

// TestUnknownErrorCountsAsFailure proves an arbitrary error that matches
// none of the known sentinels or *ReasonError falls through
// countableFailure's final branch and counts as a broker-side failure —
// a false negative there would let a wedged link hammer the AckTimeout
// forever instead of tripping the circuit.
func TestUnknownErrorCountsAsFailure(t *testing.T) {
	t.Parallel()

	fp := &fakePublisher{}
	clk := newFakeClock()
	b := NewBreaker(fp, BreakerConfig{FailureThreshold: 1, now: clk.Now})

	unknown := errors.New("boom: some unclassified transport error")
	fp.setResults(unknown)
	if err := publishOnce(t, b); !errors.Is(err, unknown) {
		t.Fatalf("publish err = %v, want the unknown error back unwrapped", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("State() after an unclassified error = %v, want open (unknown errors must count)", got)
	}
}

// TestOnStateChangeSequenceClosedOpenHalfOpenClosed proves the exact
// (from, to) transition order across a full closed -> open -> half-open
// -> closed cycle, and that the callback runs outside the breaker's lock
// (State() is called from inside the callback; a lock held during the
// callback would deadlock).
func TestOnStateChangeSequenceClosedOpenHalfOpenClosed(t *testing.T) {
	t.Parallel()

	type transition struct{ from, to BreakerState }
	var mu sync.Mutex
	var got []transition

	fp := &fakePublisher{}
	clk := newFakeClock()
	var b *Breaker
	b = NewBreaker(fp, BreakerConfig{
		FailureThreshold: 1,
		RecoveryTimeout:  time.Second,
		now:              clk.Now,
		OnStateChange: func(from, to BreakerState) {
			mu.Lock()
			got = append(got, transition{from, to})
			mu.Unlock()
			// Must not deadlock: the callback runs outside the lock.
			_ = b.State()
		},
	})

	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("tripping publish err = %v, want errAckTimeout", err)
	}
	clk.Advance(time.Second + time.Millisecond)
	fp.setResults(nil)
	if err := publishOnce(t, b); err != nil {
		t.Fatalf("probe publish err = %v, want nil", err)
	}

	want := []transition{
		{BreakerClosed, BreakerOpen},
		{BreakerOpen, BreakerHalfOpen},
		{BreakerHalfOpen, BreakerClosed},
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("transitions = %+v, want %+v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("transition %d = %+v, want %+v (full sequence: %+v)", i, got[i], w, got)
		}
	}
}

// TestOnStateChangeSequenceClosedOpenHalfOpenOpen proves the failed-probe
// variant of the sequence: closed -> open -> half-open -> open.
func TestOnStateChangeSequenceClosedOpenHalfOpenOpen(t *testing.T) {
	t.Parallel()

	type transition struct{ from, to BreakerState }
	var mu sync.Mutex
	var got []transition

	fp := &fakePublisher{}
	clk := newFakeClock()
	var b *Breaker
	b = NewBreaker(fp, BreakerConfig{
		FailureThreshold: 1,
		RecoveryTimeout:  time.Second,
		now:              clk.Now,
		OnStateChange: func(from, to BreakerState) {
			mu.Lock()
			got = append(got, transition{from, to})
			mu.Unlock()
			_ = b.State()
		},
	})

	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("tripping publish err = %v, want errAckTimeout", err)
	}
	clk.Advance(time.Second + time.Millisecond)
	fp.setResults(errAckTimeout)
	if err := publishOnce(t, b); !errors.Is(err, errAckTimeout) {
		t.Fatalf("failed probe err = %v, want errAckTimeout", err)
	}

	want := []transition{
		{BreakerClosed, BreakerOpen},
		{BreakerOpen, BreakerHalfOpen},
		{BreakerHalfOpen, BreakerOpen},
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("transitions = %+v, want %+v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("transition %d = %+v, want %+v (full sequence: %+v)", i, got[i], w, got)
		}
	}
}

// TestBreakerConcurrentPublishNoRace hammers Publish from many goroutines
// against a fake Publisher that flips between success and countable
// failure, using a real wall clock so RecoveryTimeout genuinely elapses
// during the run. The only assertion beyond -race cleanliness is that the
// breaker settles into a valid, readable state afterward — this test's
// job is to prove the locking discipline holds, not to pin a specific
// end state.
func TestBreakerConcurrentPublishNoRace(t *testing.T) {
	t.Parallel()

	// 50% failure density (rather than e.g. 1-in-3) so that, even though
	// calls from different goroutines interleave nondeterministically,
	// runs of >= FailureThreshold consecutive failures are overwhelmingly
	// likely over thousands of calls — this test wants to actually drive
	// the state machine through Open/HalfOpen under contention, not just
	// sit in Closed the whole time.
	var toggle atomic.Int64
	flipping := publisherFunc(func(ctx context.Context, topic string, payload []byte, qos QoS, retain bool, opts ...PublishOption) error {
		if toggle.Add(1)%2 == 0 {
			return errAckTimeout
		}
		return nil
	})

	var transitions atomic.Int64
	b := NewBreaker(flipping, BreakerConfig{
		FailureThreshold: 3,
		RecoveryTimeout:  5 * time.Millisecond,
		HalfOpenMax:      2,
		OnStateChange:    func(from, to BreakerState) { transitions.Add(1) },
	})

	const goroutines = 20
	const perGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				_ = b.Publish(context.Background(), "race/topic", []byte("x"), QoS1, false)
			}
		}()
	}
	wg.Wait()

	switch got := b.State(); got {
	case BreakerClosed, BreakerOpen, BreakerHalfOpen:
		// Any of these is a valid terminal state under concurrent load.
	default:
		t.Fatalf("State() = %v, not a known BreakerState", got)
	}
	t.Logf("observed %d OnStateChange transitions, final state %v", transitions.Load(), b.State())
}

// publisherFunc adapts a plain function to the Publisher interface.
type publisherFunc func(ctx context.Context, topic string, payload []byte, qos QoS, retain bool, opts ...PublishOption) error

func (f publisherFunc) Publish(ctx context.Context, topic string, payload []byte, qos QoS, retain bool, opts ...PublishOption) error {
	return f(ctx, topic, payload, qos, retain, opts...)
}

// TestBreakerStateStringAllValues proves String() covers every declared
// state plus the unknown-value fallback.
func TestBreakerStateStringAllValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state BreakerState
		want  string
	}{
		{BreakerClosed, "closed"},
		{BreakerOpen, "open"},
		{BreakerHalfOpen, "half-open"},
		{BreakerState(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.state.String(); got != tc.want {
			t.Fatalf("BreakerState(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}

// TestBreakerAgainstRealAckTimeout proves the wrapping holds end to end:
// a real TCPClient wrapped in a Breaker (FailureThreshold 2) against the
// in-package mockBroker, forcing two genuine ack-timeout failures via
// DropNextPuback + a short AckTimeout, opens the circuit; the third
// publish then fails fast with ErrCircuitOpen without waiting out another
// AckTimeout.
func TestBreakerAgainstRealAckTimeout(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "breaker-real")
	cfg.AckTimeout = 150 * time.Millisecond
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	breaker := NewBreaker(c, BreakerConfig{FailureThreshold: 2, RecoveryTimeout: time.Minute})

	b.DropNextPuback(2)

	for i := 1; i <= 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := breaker.Publish(ctx, "breaker/real", []byte("x"), QoS1, false)
		cancel()
		if !errors.Is(err, errAckTimeout) {
			t.Fatalf("publish %d err = %v, want errAckTimeout", i, err)
		}
	}
	if got := breaker.State(); got != BreakerOpen {
		t.Fatalf("State() after two ack timeouts = %v, want open", got)
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := breaker.Publish(ctx, "breaker/real", []byte("x"), QoS1, false)
	cancel()
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("third publish took %v, want a fast ErrCircuitOpen well under the %v AckTimeout",
			elapsed, cfg.AckTimeout)
	}
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("third publish err = %v, want ErrCircuitOpen", err)
	}
}
