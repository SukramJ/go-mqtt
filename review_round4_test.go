// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Regression tests for the round-4 (full-codebase audit) findings: the
// teardown/Connect race that poisoned a healthy new session's quota
// (F-01), the undamped event-driven reconnect storm against a flapping
// broker (F-02), the circuit breaker counting client-side validation
// errors as broker failures (F-03), the read loop crediting a stale
// session's identifier and send quota (F-05), the read/keep-alive loops
// signalling a lost connection during a graceful Disconnect (F-06), the
// breaker booking straggler outcomes against a state they were never
// admitted in (F-07), and the jitter that collapsed the reconnect backoff
// to an immediate retry (F-08).

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
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

// gatedStore wraps a [SessionStore] and holds the first Delete call open
// until the test releases it — the shape of a persistent store whose write
// blocks (fsync, a network round-trip) long enough for the read loop that
// called it to outlive its own connection.
//
// It deliberately does NOT forward the optional containsStore fast path:
// only Delete needs the gate, and the client's All() fallback exercises the
// same code path.
type gatedStore struct {
	SessionStore
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func newGatedStore(inner SessionStore) *gatedStore {
	return &gatedStore{
		SessionStore: inner,
		entered:      make(chan struct{}),
		gate:         make(chan struct{}),
	}
}

func (g *gatedStore) Delete(id uint16, kind StoredKind) error {
	g.once.Do(func() {
		close(g.entered)
		<-g.gate
	})
	return g.SessionStore.Delete(id, kind)
}

// idHeld reports whether the allocator still considers id in use. The
// acquire cursor never rewinds, so a wrongly-freed identifier is invisible
// to Acquire and must be read off the bitmap directly.
func idHeld(a *idAllocator, id uint16) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used[id>>6]&(uint64(1)<<(id&63)) != 0
}

// TestCompleteOutboundBooksAgainstItsOwnSession proves the read loop frees
// the packet identifier and the send-quota permit of a terminal
// acknowledgement against the generations its OWN link was established
// under. Before the fix completeOutbound used the unguarded
// idAllocator.Release / quota.release, so a read loop stalled inside
// store.Delete across a teardown + reconnect freed an identifier the new
// session had already handed to another exchange and credited that
// session's quota one permit past the negotiated Receive Maximum.
func TestCompleteOutboundBooksAgainstItsOwnSession(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "stale-release"})
	gs := newGatedStore(c.store)
	c.store = gs

	// Session A: a single permit, one in-flight QoS 1 PUBLISH holding it.
	c.quota.reset(1)
	l := &link{quotaGen: c.quota.generation(), idGen: c.ids.generation()}
	id, _, err := c.ids.Acquire()
	if err != nil {
		t.Fatalf("acquire id: %v", err)
	}
	if _, err := c.quota.acquire(ctx); err != nil {
		t.Fatalf("acquire permit: %v", err)
	}
	if err := c.store.Save(StoredMessage{ID: id, Kind: StoredPublish}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// The read loop books the PUBACK and stalls inside store.Delete.
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.completeOutbound(l, id, StoredPublish, ackResult{})
	}()
	<-gs.entered

	// Meanwhile the link dropped and the reconnect discarded the session
	// (applySession's reset path), then handed the recycled identifier and
	// the single permit to a fresh exchange.
	if err := c.store.Reset(); err != nil {
		t.Fatalf("reset store: %v", err)
	}
	c.ids.Reset()
	c.quota.reset(1)
	newID, _, err := c.ids.Acquire()
	if err != nil {
		t.Fatalf("new-session acquire id: %v", err)
	}
	if newID != id {
		t.Fatalf("test premise: new session got id %d, want the recycled %d", newID, id)
	}
	if _, err := c.quota.acquire(ctx); err != nil {
		t.Fatalf("new-session acquire permit: %v", err)
	}
	if err := c.store.Save(StoredMessage{ID: newID, Kind: StoredPublish}); err != nil {
		t.Fatalf("new-session save: %v", err)
	}

	close(gs.gate)
	<-done

	if !idHeld(&c.ids, id) {
		t.Fatalf("stale completion freed identifier %d — the new session's exchange still owns it", id)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := c.quota.acquire(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale completion credited the new session's quota past Receive Maximum: acquire returned %v", err)
	}
}

// TestGracefulTeardownFromReadLoopDoesNotSignalLost proves a socket drop
// observed by the read loop while a Disconnect is in flight is treated as
// the graceful shutdown it is. A broker closes the connection the moment it
// reads the DISCONNECT, so the read error routinely beats Disconnect's own
// teardown; before the fix that path hardcoded graceful=false and raised
// the connection-lost signal, driving a [Lifecycle] into a spurious
// reconnect right after an intentional shutdown.
func TestGracefulTeardownFromReadLoopDoesNotSignalLost(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "graceful-race"))
	defer func() { _ = c.Disconnect(context.Background()) }()
	mustConnect(t, c)

	l := c.link.Load()
	if l == nil {
		t.Fatal("no link after connect")
	}
	// Exactly what Disconnect does before its best-effort DISCONNECT write;
	// the broker then drops the socket, so the READ loop (not writeFrame)
	// observes the failure and owns the teardown.
	l.graceful.Store(true)
	b.InjectTCPReset()

	deadline := time.Now().Add(3 * time.Second)
	for c.link.Load() != nil {
		if time.Now().After(deadline) {
			t.Fatal("teardown never cleared the link")
		}
		time.Sleep(time.Millisecond)
	}
	// The lost signal is raised after the link pointer is cleared; give the
	// teardown (and the keep-alive loop's own exit path) room to raise it.
	time.Sleep(100 * time.Millisecond)
	select {
	case <-c.ConnectionLost():
		t.Fatal("graceful teardown signalled a lost connection — a Lifecycle would reconnect after an intentional Disconnect")
	default:
	}
}

// scriptedPublisher is a Publisher whose result is scripted per call index
// and whose calls can be held open, so a test can keep a straggler publish
// in flight across breaker state transitions.
type scriptedPublisher struct {
	mu      sync.Mutex
	calls   int
	results map[int]error
	entered map[int]chan struct{}
	hold    map[int]chan struct{}
}

func newScriptedPublisher() *scriptedPublisher {
	return &scriptedPublisher{
		results: make(map[int]error),
		entered: make(map[int]chan struct{}),
		hold:    make(map[int]chan struct{}),
	}
}

// script sets the result of call n and, when block is true, holds that call
// until release(n).
func (p *scriptedPublisher) script(n int, err error, block bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.results[n] = err
	p.entered[n] = make(chan struct{})
	if block {
		p.hold[n] = make(chan struct{})
	}
}

func (p *scriptedPublisher) release(n int) { close(p.hold[n]) }

func (p *scriptedPublisher) enteredCh(n int) chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.entered[n]
}

func (p *scriptedPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *scriptedPublisher) Publish(context.Context, string, []byte, QoS, bool, ...PublishOption) error {
	p.mu.Lock()
	p.calls++
	n := p.calls
	res, entered, hold := p.results[n], p.entered[n], p.hold[n]
	p.mu.Unlock()
	if entered != nil {
		close(entered)
	}
	if hold != nil {
		<-hold
	}
	return res
}

// newEpochBreaker wires a breaker with a single probe slot, a one-failure
// trip and a fake clock, the shape both straggler scenarios need.
func newEpochBreaker(p Publisher, clk *fakeClock) *Breaker {
	return NewBreaker(p, BreakerConfig{
		FailureThreshold: 1,
		RecoveryTimeout:  10 * time.Second,
		HalfOpenMax:      1,
		now:              clk.Now,
	})
}

// TestBreakerStragglerNeutralOutcomeDoesNotFreeAProbeSlot proves a neutral
// outcome from a publish admitted while the circuit was closed cannot
// release a half-open probe slot it never held. Before the epoch guard it
// decremented b.probes unconditionally, so more than HalfOpenMax probes
// reached the wrapped Publisher concurrently — exactly the stampede
// half-open exists to prevent.
func TestBreakerStragglerNeutralOutcomeDoesNotFreeAProbeSlot(t *testing.T) {
	t.Parallel()

	clk := newFakeClock()
	p := newScriptedPublisher()
	p.script(1, ErrPacketTooLarge, true) // straggler admitted while closed, neutral outcome
	p.script(2, ErrConnectionLost, false)
	p.script(3, nil, true) // the real half-open probe, still in flight
	b := newEpochBreaker(p, clk)

	straggler := make(chan struct{})
	go func() {
		defer close(straggler)
		_ = publishOnce(t, b)
	}()
	<-p.enteredCh(1)

	if err := publishOnce(t, b); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("trip publish: got %v, want ErrConnectionLost", err)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("state after the trip: %v, want open", got)
	}

	clk.Advance(11 * time.Second)
	probe := make(chan struct{})
	go func() {
		defer close(probe)
		_ = publishOnce(t, b)
	}()
	<-p.enteredCh(3)
	if got := b.State(); got != BreakerHalfOpen {
		t.Fatalf("state with a probe in flight: %v, want half-open", got)
	}

	// The straggler now reports its neutral outcome into the half-open
	// state it was never admitted in.
	p.release(1)
	<-straggler

	if err := publishOnce(t, b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second concurrent probe: got %v, want ErrCircuitOpen", err)
	}
	if got := p.count(); got != 3 {
		t.Fatalf("wrapped Publisher reached %d times, want 3 — the straggler freed a probe slot it never held", got)
	}

	p.release(3)
	<-probe
}

// TestBreakerStragglerSuccessDoesNotCloseHalfOpen proves a success from a
// publish admitted before the circuit tripped cannot close it again while
// the real probe is still outstanding. Before the epoch guard that stale
// success ran recordSuccess against the half-open state and closed the
// circuit on evidence that predated the failures which opened it.
func TestBreakerStragglerSuccessDoesNotCloseHalfOpen(t *testing.T) {
	t.Parallel()

	clk := newFakeClock()
	p := newScriptedPublisher()
	p.script(1, nil, true) // straggler admitted while closed, succeeds late
	p.script(2, ErrConnectionLost, false)
	p.script(3, nil, true) // the real probe
	b := newEpochBreaker(p, clk)

	straggler := make(chan struct{})
	go func() {
		defer close(straggler)
		_ = publishOnce(t, b)
	}()
	<-p.enteredCh(1)

	if err := publishOnce(t, b); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("trip publish: got %v, want ErrConnectionLost", err)
	}
	clk.Advance(11 * time.Second)
	probe := make(chan struct{})
	go func() {
		defer close(probe)
		_ = publishOnce(t, b)
	}()
	<-p.enteredCh(3)

	p.release(1)
	<-straggler
	if got := b.State(); got != BreakerHalfOpen {
		t.Fatalf("state after a straggler success: %v, want half-open (the real probe has not reported yet)", got)
	}

	// Only the probe's own outcome may close the circuit.
	p.release(3)
	<-probe
	if got := b.State(); got != BreakerClosed {
		t.Fatalf("state after the probe succeeded: %v, want closed", got)
	}
}

// TestLifecycleJitterNeverCollapsesBackoff proves a Jitter at or above the
// nominal delay still yields a usable wait. Before the clamp the draw was
// d+delta with delta in [-Jitter, +Jitter), so ~40% of the draws at
// d=100ms/Jitter=500ms were zero or negative — time.After fired instantly
// and the reconnect loop hammered the broker with no backoff at all.
func TestLifecycleJitterNeverCollapsesBackoff(t *testing.T) {
	t.Parallel()

	const d = 100 * time.Millisecond
	cfg := LifecycleConfig{
		InitialBackoff: d,
		MaxBackoff:     500 * time.Millisecond,
		Jitter:         500 * time.Millisecond,
	}
	l := NewLifecycle(cfg, &stubConnector{})
	for range 2000 {
		got := l.jittered(d)
		if got <= 0 {
			t.Fatalf("jittered(%v) = %v — a non-positive delay retries immediately", d, got)
		}
		if got < d/2 {
			t.Fatalf("jittered(%v) = %v, below the d/2 floor", d, got)
		}
		if got > d+cfg.Jitter {
			t.Fatalf("jittered(%v) = %v, above d+Jitter", d, got)
		}
	}
}

// TestLifecycleJitterOverflowIsGuarded proves an absurd Jitter cannot panic
// rand.Int63n through an overflowed Jitter*2 span.
func TestLifecycleJitterOverflowIsGuarded(t *testing.T) {
	t.Parallel()

	l := NewLifecycle(LifecycleConfig{Jitter: math.MaxInt64}, &stubConnector{})
	if got := l.jittered(time.Second); got != time.Second {
		t.Fatalf("jittered with an overflowing Jitter = %v, want the nominal 1s", got)
	}
}

// TestNewLifecycleValidatesNegativeAndInvertedConfig proves the defaulting
// treats a negative duration like the zero value (a negative backoff would
// otherwise reach time.After as an immediate fire) and raises an inverted
// MaxBackoff to InitialBackoff.
func TestNewLifecycleValidatesNegativeAndInvertedConfig(t *testing.T) {
	t.Parallel()

	def := DefaultLifecycle()
	neg := NewLifecycle(LifecycleConfig{
		InitialBackoff: -5 * time.Second,
		MaxBackoff:     -1,
		FlapWindow:     -time.Hour,
	}, &stubConnector{})
	if neg.cfg.InitialBackoff != def.InitialBackoff {
		t.Fatalf("InitialBackoff = %v, want the %v default", neg.cfg.InitialBackoff, def.InitialBackoff)
	}
	if neg.cfg.MaxBackoff != def.MaxBackoff {
		t.Fatalf("MaxBackoff = %v, want the %v default", neg.cfg.MaxBackoff, def.MaxBackoff)
	}
	if neg.cfg.FlapWindow != def.FlapWindow {
		t.Fatalf("FlapWindow = %v, want the %v default", neg.cfg.FlapWindow, def.FlapWindow)
	}

	inv := NewLifecycle(LifecycleConfig{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     time.Second,
	}, &stubConnector{})
	if inv.cfg.MaxBackoff != 5*time.Second {
		t.Fatalf("MaxBackoff = %v, want it raised to InitialBackoff (5s)", inv.cfg.MaxBackoff)
	}
}
