// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
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
	//
	// Calls are globally serialised and delivered in transition order, so
	// the (from, to) pairs form an unbroken chain: every from equals the
	// previous to, and a gauge driven from `to` can never be left showing
	// a state the breaker has already left. Achieving that ordering means
	// a callback is not necessarily invoked on the goroutine whose publish
	// caused it: transitions are queued, and one goroutine drains the
	// queue while the others carry on, so a publish may deliver another's
	// transition — or see its own delivered by someone else and return
	// before the callback has run. Callbacks never overlap.
	//
	// No breaker lock is held during the call: a callback may read
	// [Breaker.State] or publish through this same Breaker without
	// deadlocking (a transition it causes that way is queued and delivered
	// after it returns, in order). Keep it quick anyway — it runs inline
	// on a publish path.
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
// [ErrPacketTooLarge], [ErrPacketIDExhausted], and client-side limit
// violations (a [protocol.ErrProtocolViolation],
// [protocol.ErrMalformedPacket] or [protocol.ErrStringTooLong] raised
// before any bytes reach the wire) — never trip the circuit.
//
// A Breaker is safe for concurrent use and adds no overhead beyond one
// mutex acquisition per publish.
type Breaker struct {
	pub Publisher
	cfg BreakerConfig

	mu       sync.Mutex
	state    BreakerState
	epoch    uint64
	failures int
	openedAt time.Time
	probes   int
	// pending is the FIFO queue of transitions awaiting their
	// OnStateChange call, in the order they happened. Guarded by mu.
	pending []stateChange
	// draining marks that one goroutine is currently delivering the queue,
	// making it the only one that may. Guarded by mu; see
	// [Breaker.fireStateChanges].
	draining bool
}

// stateChange is one queued (from, to) transition awaiting its
// OnStateChange call.
type stateChange struct {
	from BreakerState
	to   BreakerState
}

// admission is what a single admitted publish carries from admit to
// record: the epoch it was let through under, and whether it holds one of
// the half-open probe slots.
//
// The epoch makes stragglers harmless. A publish blocks for as long as the
// broker takes (up to the full AckTimeout), so its outcome routinely lands
// in a state the breaker has already left — and booking it against the new
// state is wrong in both directions: a neutral outcome from a closed-state
// publish would decrement a half-open probe count it never contributed to
// (admitting more than HalfOpenMax concurrent probes), and a success from
// before the trip would close the circuit on evidence that predates it.
// Every transition bumps the epoch and re-seeds failures/probes
// absolutely, so an outcome whose epoch no longer matches is simply
// discarded.
type admission struct {
	epoch uint64
	probe bool
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
	adm, admitted, transition := b.admit()
	if !admitted {
		return ErrCircuitOpen
	}
	if transition != nil {
		transition()
	}
	err := b.pub.Publish(ctx, topic, payload, qos, retain, opts...)
	if fire := b.record(ctx, adm, err); fire != nil {
		fire()
	}
	return err
}

// admit decides whether a publish may proceed and, when it does, returns
// the [admission] its outcome must be booked against. transition carries
// the pending OnStateChange callback (to run outside the lock) when
// admission itself transitioned the state (open → half-open).
func (b *Breaker) admit() (adm admission, admitted bool, transition func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerClosed:
		return admission{epoch: b.epoch}, true, nil
	case BreakerOpen:
		if b.cfg.now().Sub(b.openedAt) < b.cfg.RecoveryTimeout {
			return admission{}, false, nil
		}
		fire := b.transitionLocked(BreakerHalfOpen)
		// Absolute, not an increment: entering half-open discards whatever
		// probe accounting the previous state left behind, so a straggler
		// that is about to be dropped for a stale epoch cannot strand a
		// slot it appeared to hold.
		b.probes = 1
		return admission{epoch: b.epoch, probe: true}, true, fire
	case BreakerHalfOpen:
		if b.probes >= b.cfg.HalfOpenMax {
			return admission{}, false, nil
		}
		b.probes++
		return admission{epoch: b.epoch, probe: true}, true, nil
	default:
		return admission{epoch: b.epoch}, true, nil
	}
}

// record books the publish outcome against the state it was admitted in
// and returns the pending OnStateChange callback, if any. An outcome from
// a superseded epoch is discarded without touching failures, probes or
// state.
func (b *Breaker) record(ctx context.Context, adm admission, err error) func() {
	switch {
	case err == nil:
		return b.recordSuccess(adm)
	case countableFailure(ctx, err):
		return b.recordFailure(adm)
	default:
		// Local / caller-side condition: neutral. The half-open probe slot
		// this publish actually holds is released so the next publish may
		// probe again — a publish admitted in another epoch holds none.
		b.mu.Lock()
		defer b.mu.Unlock()
		if adm.epoch != b.epoch {
			return nil
		}
		if adm.probe && b.state == BreakerHalfOpen && b.probes > 0 {
			b.probes--
		}
		return nil
	}
}

func (b *Breaker) recordSuccess(adm admission) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if adm.epoch != b.epoch {
		// Stale evidence: this publish was admitted before the circuit
		// changed state. Closing on it (or clearing the failure streak that
		// accumulated since) would credit the current state with a probe it
		// never ran.
		return nil
	}
	b.failures = 0
	if b.state == BreakerHalfOpen {
		fire := b.transitionLocked(BreakerClosed)
		b.probes = 0
		return fire
	}
	return nil
}

func (b *Breaker) recordFailure(adm admission) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if adm.epoch != b.epoch {
		// Stale evidence: the transition out of the admitting state already
		// re-seeded the accounting this failure would contribute to.
		return nil
	}
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
		// Unreachable while the epoch matches: no publish is admitted in
		// the open state except the one that transitions it to half-open,
		// which bumps the epoch. Kept as the exhaustive-switch arm — and as
		// the historical straggler case, which the epoch guard above now
		// filters out before it can restart the recovery window.
		return nil
	default:
		return nil
	}
}

// transitionLocked switches the state, queues the transition for its
// OnStateChange call and returns the drain to run once the lock is
// released. Callers hold b.mu.
//
// Every real transition bumps the epoch, retiring every publish still in
// flight under the old state (see [admission]).
//
// The queue is what keeps callbacks in transition order. Returning a
// closure that simply calls cb(from, to) was enough to keep the callback
// off b.mu, but not to order two of them: goroutine A can transition
// closed → open, be descheduled before invoking its closure, and let
// goroutine B transition open → half-open and report first — leaving a
// metrics gauge parked on "open" after the breaker has moved past it, and
// a log that reads as if a state was entered twice. Appending under b.mu
// fixes the order at the moment the state actually changes; the drain then
// replays it faithfully.
func (b *Breaker) transitionLocked(to BreakerState) func() {
	from := b.state
	if from == to {
		return nil
	}
	b.state = to
	b.epoch++
	if b.cfg.OnStateChange == nil {
		return nil
	}
	b.pending = append(b.pending, stateChange{from: from, to: to})
	return b.fireStateChanges
}

// fireStateChanges delivers queued transitions FIFO, one at a time, with
// no lock held across the callback — so a callback may safely inspect the
// breaker ([Breaker.State]) or publish through it again.
//
// Exactly one goroutine drains at a time, marked by the draining flag
// rather than a callback mutex: a second goroutine arriving mid-drain
// returns immediately instead of queueing up behind the callback. That
// keeps a slow callback from stalling publishes it has nothing to do with,
// and — because a callback that publishes back through this Breaker
// re-enters here on the same goroutine — makes reentrancy a no-op instead
// of a self-deadlock on a non-reentrant mutex. Whatever it queues is
// delivered by the drain loop already running, still in order.
//
// The consequence, documented on [BreakerConfig.OnStateChange], is that a
// caller may deliver transitions other goroutines queued, and its own may
// be delivered by someone else. The clear of draining shares the lock with
// the emptiness check so no transition can be queued into a queue nobody
// is about to drain.
func (b *Breaker) fireStateChanges() {
	b.mu.Lock()
	if b.draining {
		b.mu.Unlock()
		return
	}
	b.draining = true
	b.mu.Unlock()

	// Normal exits clear draining under the same lock as the emptiness
	// check (see below); this only covers a callback that panics, which
	// would otherwise leave the flag set and silence every later
	// transition. It must not fire on the normal path: by then another
	// goroutine may already be draining, and clearing the flag under it
	// would admit a second concurrent drain.
	drained := false
	defer func() {
		if drained {
			return
		}
		b.mu.Lock()
		b.draining = false
		b.mu.Unlock()
	}()

	for {
		b.mu.Lock()
		if len(b.pending) == 0 {
			b.draining = false
			drained = true
			b.mu.Unlock()
			return
		}
		next := b.pending[0]
		b.pending = b.pending[1:]
		cb := b.cfg.OnStateChange
		b.mu.Unlock()
		cb(next.from, next.to)
	}
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
	case errors.Is(err, protocol.ErrProtocolViolation),
		errors.Is(err, protocol.ErrMalformedPacket),
		errors.Is(err, protocol.ErrStringTooLong):
		// Client-side validation and encode failures (invalid topic, QoS
		// above the broker's Maximum QoS, retain against Retain
		// Available = 0, oversized strings) surface before any bytes
		// reach the wire — they say nothing about broker health, and
		// counting them would let one malformed topic open the circuit
		// for every healthy publish.
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
