// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-mqtt authors.

package mqtt

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned by [Breaker.Publish] while the circuit is
// open: the broker link has produced too many consecutive failures and
// callers fail fast instead of each blocking on the AckTimeout.
var ErrCircuitOpen = errors.New("mqtt: circuit open")

// BreakerState is the circuit state of a [Breaker].
type BreakerState int

// Breaker states.
const (
	// BreakerClosed passes every publish through (healthy).
	BreakerClosed BreakerState = iota
	// BreakerOpen fails every publish fast with [ErrCircuitOpen].
	BreakerOpen
	// BreakerHalfOpen lets a bounded number of probe publishes through
	// to test whether the broker recovered.
	BreakerHalfOpen
)

// String returns the lowercase state name.
func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// BreakerConfig tunes a [Breaker]. The zero value is usable: 5
// consecutive failures open the circuit, recovery is probed after 30
// seconds with a single in-flight probe.
type BreakerConfig struct {
	// FailureThreshold is the number of consecutive countable publish
	// failures that opens the circuit. Zero or negative uses 5.
	FailureThreshold int
	// RecoveryTimeout is how long an open circuit rejects publishes
	// before the next attempt is admitted as a half-open probe. Zero
	// or negative uses 30 seconds.
	RecoveryTimeout time.Duration
	// HalfOpenMax bounds the number of concurrent probe publishes in
	// the half-open state; excess publishes keep failing fast. Zero or
	// negative uses 1.
	HalfOpenMax int
	// OnStateChange, when non-nil, is called synchronously after every
	// state transition, outside the breaker's lock. Wire metrics or
	// logging here.
	OnStateChange func(from, to BreakerState)
	// now is the clock seam for tests; nil uses time.Now.
	now func() time.Time
}

// Breaker is a circuit-breaking [Publisher] decorator.
//
// It addresses the degraded-broker case the reconnect loop cannot see:
// the TCP link is up, but the broker stops acknowledging, so every
// QoS >= 1 publish blocks for the full AckTimeout. After
// FailureThreshold consecutive countable failures the circuit opens
// and publishes return [ErrCircuitOpen] immediately; after
// RecoveryTimeout a bounded number of probes is let through, and one
// success closes the circuit again.
//
// Countable failures are broker-side symptoms: acknowledgement
// timeouts, [ErrConnectionLost], [ErrNotConnected] and broker rejects
// ([*ReasonError]). Local conditions — caller context cancellation,
// [ErrPacketTooLarge], [ErrPacketIDExhausted], client-side limit
// violations — never trip the circuit.
//
// A Breaker is safe for concurrent use and adds no overhead beyond one
// mutex acquisition per publish.
type Breaker struct {
	pub Publisher
	cfg BreakerConfig

	mu       sync.Mutex
	state    BreakerState
	failures int
	openedAt time.Time
	probes   int
}

// Compile-time contract: a Breaker is a drop-in Publisher.
var _ Publisher = (*Breaker)(nil)

// NewBreaker wraps pub in a circuit breaker. pub must not be nil.
func NewBreaker(pub Publisher, cfg BreakerConfig) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.RecoveryTimeout <= 0 {
		cfg.RecoveryTimeout = 30 * time.Second
	}
	if cfg.HalfOpenMax <= 0 {
		cfg.HalfOpenMax = 1
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	return &Breaker{pub: pub, cfg: cfg}
}

// State returns the current circuit state.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Publish implements [Publisher] with circuit gating.
func (b *Breaker) Publish(ctx context.Context, topic string, payload []byte, qos QoS, retain bool, opts ...PublishOption) error {
	if admit, transition := b.admit(); !admit {
		return ErrCircuitOpen
	} else if transition != nil {
		transition()
	}
	err := b.pub.Publish(ctx, topic, payload, qos, retain, opts...)
	if fire := b.record(ctx, err); fire != nil {
		fire()
	}
	return err
}

// admit decides whether a publish may proceed. transition carries the
// pending OnStateChange callback (to run outside the lock) when
// admission itself transitioned the state (open → half-open).
func (b *Breaker) admit() (admitted bool, transition func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerClosed:
		return true, nil
	case BreakerOpen:
		if b.cfg.now().Sub(b.openedAt) < b.cfg.RecoveryTimeout {
			return false, nil
		}
		fire := b.transitionLocked(BreakerHalfOpen)
		b.probes = 1
		return true, fire
	case BreakerHalfOpen:
		if b.probes >= b.cfg.HalfOpenMax {
			return false, nil
		}
		b.probes++
		return true, nil
	default:
		return true, nil
	}
}

// record books the publish outcome and returns the pending
// OnStateChange callback, if any.
func (b *Breaker) record(ctx context.Context, err error) func() {
	switch {
	case err == nil:
		return b.recordSuccess()
	case countableFailure(ctx, err):
		return b.recordFailure()
	default:
		// Local / caller-side condition: neutral. A half-open probe
		// slot is released so the next publish may probe again.
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.state == BreakerHalfOpen && b.probes > 0 {
			b.probes--
		}
		return nil
	}
}

func (b *Breaker) recordSuccess() func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	if b.state == BreakerHalfOpen {
		fire := b.transitionLocked(BreakerClosed)
		b.probes = 0
		return fire
	}
	return nil
}

func (b *Breaker) recordFailure() func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerHalfOpen:
		// A failed probe re-opens immediately and restarts the window.
		fire := b.transitionLocked(BreakerOpen)
		b.openedAt = b.cfg.now()
		b.probes = 0
		b.failures = 0
		return fire
	case BreakerClosed:
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			fire := b.transitionLocked(BreakerOpen)
			b.openedAt = b.cfg.now()
			b.failures = 0
			return fire
		}
		return nil
	case BreakerOpen:
		// A straggler that was admitted before the trip; the window
		// keeps its original start.
		return nil
	default:
		return nil
	}
}

// transitionLocked switches the state and returns the OnStateChange
// invocation to run once the lock is released. Callers hold b.mu.
func (b *Breaker) transitionLocked(to BreakerState) func() {
	from := b.state
	if from == to {
		return nil
	}
	b.state = to
	if cb := b.cfg.OnStateChange; cb != nil {
		return func() { cb(from, to) }
	}
	return nil
}

// countableFailure reports whether err is a broker-side symptom that
// should trip the circuit. Caller-driven context cancellation and
// local validation errors stay neutral.
func countableFailure(ctx context.Context, err error) bool {
	switch {
	case errors.Is(err, errAckTimeout),
		errors.Is(err, ErrConnectionLost),
		errors.Is(err, ErrNotConnected):
		return true
	case errors.Is(err, ErrPacketTooLarge), errors.Is(err, ErrPacketIDExhausted):
		return false
	case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		// The caller's own deadline/cancellation surfaced — not a
		// statement about broker health.
		return false
	}
	var re *ReasonError
	if errors.As(err, &re) {
		return re.Code.IsError()
	}
	// Unknown transport-level failure (e.g. a wrapped net error from a
	// mid-publish teardown): count it — false negatives here would keep
	// a wedged link hammering the AckTimeout.
	return true
}
