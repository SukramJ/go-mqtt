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
//
// The second batch covers the round-4 low-severity findings: a CONNACK
// reporting Session Present = 1 against a Clean Start being accepted
// (F-11), unknown-identifier PUBREC/PUBREL left unanswered or wrongly
// answered with Success (F-12), a Server Keep Alive of 0 being read as
// "absent" instead of "keep-alive off" (F-13), the PINGRESP watchdog
// counter incremented after the write (F-14), a failed connect publishing
// its ConnectResult (F-15), server-to-client-illegal packets being
// warn-logged instead of tearing the connection down (F-16), a stale
// connection-lost token parking the reconnect backoff at MaxBackoff
// (F-17), the session store aliasing the caller's payload buffer (F-19),
// and OnStateChange callbacks firing out of transition order (F-21).

import (
	"bufio"
	"bytes"
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
		FlapWindow:     -1, // flap detection off: every loss counts as stable
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
	// A negative FlapWindow is NOT defaulted: it is the documented "flap
	// detection off" switch and must survive NewLifecycle untouched.
	if neg.cfg.FlapWindow != -time.Hour {
		t.Fatalf("FlapWindow = %v, want the negative disable value preserved", neg.cfg.FlapWindow)
	}
	if zero := NewLifecycle(LifecycleConfig{}, &stubConnector{}); zero.cfg.FlapWindow != def.FlapWindow {
		t.Fatalf("zero FlapWindow = %v, want the %v default", zero.cfg.FlapWindow, def.FlapWindow)
	}

	inv := NewLifecycle(LifecycleConfig{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     time.Second,
	}, &stubConnector{})
	if inv.cfg.MaxBackoff != 5*time.Second {
		t.Fatalf("MaxBackoff = %v, want it raised to InitialBackoff (5s)", inv.cfg.MaxBackoff)
	}
}

// ---------------------------------------------------------------------------
// F-11: CleanStart = 1 + CONNACK Session Present = 1 must close the link
// ---------------------------------------------------------------------------

// TestConnectRefusesSessionPresentAfterCleanStart proves a broker that
// answers a Clean Start CONNECT with Session Present = 1 is refused, on
// both dialects. [MQTT-3.2.2-4] (3.1.1 §3.2.2.2) makes that combination a
// protocol error the client MUST close the connection over; before the fix
// the client accepted it and ran on a session whose broker-side QoS>0
// state it had itself just discarded. On MQTT 5.0 the refusal is preceded
// by a DISCONNECT(0x82) so the broker learns why.
func TestConnectRefusesSessionPresentAfterCleanStart(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		version        ProtocolVersion
		wantDisconnect bool
	}{
		{"v5 sends DISCONNECT 0x82", ProtocolV50, true},
		{"v3.1.1 just closes", ProtocolV311, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newMockBroker(t)
			b.SetSessionPresent(true)
			cfg := newIntegrationConfig(b.URL(), "clean-start-resumed")
			cfg.ProtocolVersion = tc.version
			if !cfg.CleanStart {
				t.Fatal("test premise: the baseline config must request a clean start")
			}
			c := NewTCPClient(cfg)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := c.Connect(ctx)
			if err == nil {
				_ = c.Disconnect(context.Background())
				t.Fatal("Connect accepted Session Present=1 for a Clean Start connect")
			}
			if !errors.Is(err, protocol.ErrProtocolViolation) {
				t.Fatalf("Connect err = %v, want ErrProtocolViolation", err)
			}
			if c.IsConnected() {
				t.Fatal("client reports connected after refusing the CONNACK")
			}
			if _, ok := c.ConnectResult(); ok {
				t.Fatal("a refused connect published its ConnectResult")
			}

			if tc.wantDisconnect {
				if !lcPoll(time.Second, func() bool { return b.DisconnectCount() == 1 }) {
					t.Fatalf("broker saw %d DISCONNECTs, want the 0x82 protocol-error DISCONNECT", b.DisconnectCount())
				}
				return
			}
			// MQTT 3.1.1's DISCONNECT is defined as a CLEAN disconnect that
			// disarms the will, so an abrupt close is the conformant signal.
			time.Sleep(100 * time.Millisecond)
			if got := b.DisconnectCount(); got != 0 {
				t.Fatalf("MQTT 3.1.1 refusal sent %d DISCONNECTs, want 0 (it would disarm the will)", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// F-12: unknown-identifier PUBREC / PUBREL answers
// ---------------------------------------------------------------------------

// injectAckVersion encodes a PUBACK/PUBREC/PUBREL/PUBCOMP for version v
// with reason Success and injects it as if the broker had sent it. It is
// the dual-version sibling of injectAck (adapter_integration_test.go).
func injectAckVersion(t *testing.T, b *mockBroker, v protocol.Version, typ protocol.PacketType, id uint16) {
	t.Helper()
	var buf bytes.Buffer
	ack := &protocol.AckPacket{Version: v, Type: typ, PacketID: id}
	if err := ack.EncodeAck(&buf); err != nil {
		t.Fatalf("encode %s: %v", typ, err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("inject %s: %v", typ, err)
	}
}

// findAck returns the first recorded acknowledgement of type typ for id.
func findAck(acks []mockAck, typ protocol.PacketType, id uint16) (mockAck, bool) {
	for _, a := range acks {
		if a.Type == typ && a.PacketID == id {
			return a, true
		}
	}
	return mockAck{}, false
}

// TestUnknownPubrecIsAnsweredWithPubrel proves a PUBREC for an identifier
// the client holds no state for is answered with a PUBREL — carrying 0x92
// Packet Identifier not found on MQTT 5.0 (§3.6.2.1), bare on MQTT 3.1.1.
// Before the fix the branch only warn-logged, so the broker's QoS 2
// exchange stayed in flight and it retransmitted the PUBREC forever, one
// warn line per retry, with nothing on either side able to end it.
func TestUnknownPubrecIsAnsweredWithPubrel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		version    ProtocolVersion
		wantReason ReasonCode
	}{
		{"v5 answers 0x92", ProtocolV50, protocol.PacketIdentifierNotFound},
		{"v3.1.1 answers bare", ProtocolV311, protocol.Success},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newMockBroker(t)
			cfg := newIntegrationConfig(b.URL(), "pubrec-unknown")
			cfg.ProtocolVersion = tc.version
			c := NewTCPClient(cfg)
			mustConnect(t, c)
			defer func() { _ = c.Disconnect(context.Background()) }()

			const unknownID uint16 = 4242
			injectAckVersion(t, b, tc.version, protocol.Pubrec, unknownID)

			if !lcPoll(2*time.Second, func() bool {
				_, ok := findAck(b.Acks(), protocol.Pubrel, unknownID)
				return ok
			}) {
				t.Fatal("client never answered the unknown-identifier PUBREC — the broker's QoS 2 flow stays stuck")
			}
			got, _ := findAck(b.Acks(), protocol.Pubrel, unknownID)
			if got.ReasonCode != tc.wantReason {
				t.Fatalf("PUBREL reason = %v, want %v", got.ReasonCode, tc.wantReason)
			}
			if !c.IsConnected() {
				t.Fatal("answering an unknown PUBREC must not drop the connection")
			}
		})
	}
}

// TestUnknownPubrelIsAnsweredWithNotFound proves the receiver-side mirror
// image: a PUBREL for an identifier with no dedup record is PUBCOMP'd with
// 0x92, not with Success — Success would assert an exactly-once delivery
// the client has no record of ever making. The completed exchange in the
// same test pins that a genuine PUBREL still gets Success.
func TestUnknownPubrelIsAnsweredWithNotFound(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "pubrel-unknown"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	// A real inbound QoS 2 exchange: the broker's InjectPublish drives
	// PUBLISH → PUBREC → PUBREL → PUBCOMP to completion.
	if err := b.InjectPublish("qos2/known", []byte("x"), 2, false, nil); err != nil {
		t.Fatalf("InjectPublish: %v", err)
	}
	acks := b.Acks()
	if len(acks) == 0 {
		t.Fatal("the completed QoS 2 exchange recorded no acknowledgements")
	}
	known, ok := findAck(acks, protocol.Pubcomp, acks[0].PacketID)
	if !ok {
		t.Fatalf("no PUBCOMP for the completed exchange (acks: %+v)", acks)
	}
	if known.ReasonCode != protocol.Success {
		t.Fatalf("PUBCOMP for a delivered message = %v, want Success", known.ReasonCode)
	}

	const unknownID uint16 = 4243
	injectAckVersion(t, b, protocol.V50, protocol.Pubrel, unknownID)
	if !lcPoll(2*time.Second, func() bool {
		_, ok := findAck(b.Acks(), protocol.Pubcomp, unknownID)
		return ok
	}) {
		t.Fatal("client never answered the unknown-identifier PUBREL")
	}
	got, _ := findAck(b.Acks(), protocol.Pubcomp, unknownID)
	if got.ReasonCode != protocol.PacketIdentifierNotFound {
		t.Fatalf("PUBCOMP reason for an unknown identifier = %v, want 0x92 Packet Identifier not found", got.ReasonCode)
	}
	if !c.IsConnected() {
		t.Fatal("answering an unknown PUBREL must not drop the connection")
	}
}

// ---------------------------------------------------------------------------
// F-13: Server Keep Alive = 0 switches the keep-alive mechanism off
// ---------------------------------------------------------------------------

// TestServerKeepAliveZeroDisablesPings proves a CONNACK carrying Server
// Keep Alive = 0 stops the client pinging at all (§3.1.2.10), rather than
// being read as "the broker did not override anything". Before the fix
// ConnectResult.ServerKeepAlive was a bare duration, so a present zero was
// indistinguishable from an absent property and the client kept pinging a
// broker that had explicitly switched the mechanism off.
func TestServerKeepAliveZeroDisablesPings(t *testing.T) {
	t.Parallel()

	const interval = 20 * time.Millisecond

	b := newMockBroker(t)
	zero := uint16(0)
	b.SetConnackProperties(&protocol.Properties{ServerKeepAlive: &zero})
	c := NewTCPClient(newIntegrationConfig(b.URL(), "keepalive-zero"))
	// The package-test override would otherwise drive pings far faster than
	// the 30s keep-alive floor; a broker-imposed zero must outrank it.
	c.pingInterval = interval
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	res, ok := c.ConnectResult()
	if !ok {
		t.Fatal("no ConnectResult after connect")
	}
	if !res.ServerKeepAliveSet || res.ServerKeepAlive != 0 {
		t.Fatalf("ServerKeepAliveSet=%v ServerKeepAlive=%v, want set with a zero interval",
			res.ServerKeepAliveSet, res.ServerKeepAlive)
	}

	// A control on a second broker proves the window really is long enough
	// for pings to show up when they are supposed to.
	ctrlBroker := newMockBroker(t)
	ctrl := NewTCPClient(newIntegrationConfig(ctrlBroker.URL(), "keepalive-control"))
	ctrl.pingInterval = interval
	mustConnect(t, ctrl)
	defer func() { _ = ctrl.Disconnect(context.Background()) }()

	time.Sleep(10 * interval)

	if got := b.PingCount(); got != 0 {
		t.Fatalf("client sent %d PINGREQs to a broker that set Server Keep Alive = 0", got)
	}
	if got := ctrlBroker.PingCount(); got == 0 {
		t.Fatal("control client sent no PINGREQ — the observation window is too short to prove anything")
	}
	if !c.IsConnected() {
		t.Fatal("connection dropped although keep-alive is off (the watchdog must be inert too)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Publish(ctx, "keepalive/zero", []byte("ok"), QoS0, false); err != nil {
		t.Fatalf("publish on a healthy ping-less link: %v", err)
	}
}

// ---------------------------------------------------------------------------
// F-14: the PINGRESP watchdog counts the ping before it hits the wire
// ---------------------------------------------------------------------------

// pingRaceWriter answers every PINGREQ from inside the write itself — the
// shape of a broker whose PINGRESP the read loop processes between the
// flush and the counter update.
type pingRaceWriter struct {
	l     *link
	fired chan struct{}
	once  sync.Once
}

func (w *pingRaceWriter) Write(p []byte) (int, error) {
	w.l.outstandingPings.Store(0) // exactly what readLoop does for a PINGRESP
	w.once.Do(func() { close(w.fired) })
	return len(p), nil
}

// TestPingCounterIsIncrementedBeforeTheWrite proves a PINGRESP that lands
// while the PINGREQ is still being written is not lost. Before the fix the
// counter was incremented after the flush, so the read loop's Store(0) was
// immediately overwritten by an Add(1) for a ping that had already been
// answered — silently halving the watchdog's documented tolerance of one
// lost PINGRESP, and tripping ping_timeout on a healthy but slow link.
func TestPingCounterIsIncrementedBeforeTheWrite(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "ping-order"})
	l := &link{stop: make(chan struct{}), pingInterval: 50 * time.Millisecond}
	w := &pingRaceWriter{l: l, fired: make(chan struct{})}
	l.w = bufio.NewWriter(w)

	l.wg.Add(1)
	go c.keepAliveLoop(l)

	select {
	case <-w.fired:
	case <-time.After(3 * time.Second):
		close(l.stop)
		l.wg.Wait()
		t.Fatal("keep-alive loop never sent a PINGREQ")
	}
	// Sample well inside the tick interval: the Add is a handful of
	// instructions after the write returns, the next tick 50ms away.
	time.Sleep(10 * time.Millisecond)
	got := l.outstandingPings.Load()
	close(l.stop)
	l.wg.Wait()

	if got != 0 {
		t.Fatalf("outstandingPings = %d after an answered ping, want 0 — the PINGRESP was overwritten by a late Add", got)
	}
}

// ---------------------------------------------------------------------------
// F-15: a connect that dies during session replay publishes no ConnectResult
// ---------------------------------------------------------------------------

// killOnAllStore severs the broker connection the first time the client
// reads the session store during a connect — i.e. before the replay writes
// — so the replay fails and Connect unwinds after the CONNACK was already
// decoded.
type killOnAllStore struct {
	SessionStore
	kill func()
	once sync.Once
}

func (s *killOnAllStore) All() ([]StoredMessage, error) {
	s.once.Do(func() {
		s.kill()
		// Let the reset reach the client's socket so the replay write
		// actually fails instead of landing in the send buffer.
		time.Sleep(150 * time.Millisecond)
	})
	return s.SessionStore.All()
}

// TestFailedReplayKeepsPreviousConnectResult proves a connect that fails
// during the pre-publish session replay does not overwrite the negotiated
// session state of the connection that is (or was) actually live. Before
// the fix c.result was stored right after the CONNACK, so a connect that
// unwound later left ConnectResult() — and with it the Maximum QoS and
// Retain Available gates Publish consults — describing a session that
// never existed.
func TestFailedReplayKeepsPreviousConnectResult(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	sessionA := uint16(10)
	b.SetConnackProperties(&protocol.Properties{ReceiveMaximum: &sessionA})
	b.SetSessionPresent(true)
	cfg := newIntegrationConfig(b.URL(), "replay-failure")
	cfg.CleanStart = false
	cfg.SessionExpirySeconds = 3600
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	if res, ok := c.ConnectResult(); !ok || res.ReceiveMaximum != sessionA {
		t.Fatalf("session A ReceiveMaximum = %v (ok=%v), want %d", res.ReceiveMaximum, ok, sessionA)
	}

	b.InjectTCPReset()
	if !lcPoll(3*time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("teardown never cleared the link")
	}

	// Seed resumable state large enough that the replay write cannot be
	// swallowed by a socket buffer, then arm the store to kill the fresh
	// connection just before the replay runs.
	payload := bytes.Repeat([]byte("x"), 256*1024)
	if err := c.store.Save(StoredMessage{ID: 9, Kind: StoredPublish, Publish: &protocol.PublishPacket{
		Version: protocol.V50, Topic: "replay/fail", Payload: payload, QoS: 1, PacketID: 9,
	}}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	c.store = &killOnAllStore{SessionStore: c.store, kill: b.InjectTCPReset}

	sessionB := uint16(5)
	b.SetConnackProperties(&protocol.Properties{ReceiveMaximum: &sessionB})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err == nil {
		_ = c.Disconnect(context.Background())
		t.Fatal("Connect reported success although the session replay was severed")
	} else if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Connect err = %v, want ErrConnectionLost", err)
	}
	if c.IsConnected() {
		t.Fatal("client reports connected after a failed replay")
	}

	res, ok := c.ConnectResult()
	if !ok {
		t.Fatal("ConnectResult lost the previously established session")
	}
	if res.ReceiveMaximum != sessionA {
		t.Fatalf("ConnectResult ReceiveMaximum = %d, want session A's %d — a failed connect published its own limits",
			res.ReceiveMaximum, sessionA)
	}
}

// ---------------------------------------------------------------------------
// F-16: server-to-client-illegal packets are a protocol error, not a warning
// ---------------------------------------------------------------------------

// TestServerSentClientOnlyPacketIsFatal proves a packet only a client may
// send — a second CONNACK, or a PINGREQ from the broker — tears the
// connection down with DISCONNECT(0x82) instead of being warn-logged and
// read past. A peer that sends one has a desynchronised state machine, so
// every frame after it is suspect; every comparable §4.13 violation in the
// read loop already closes the connection.
func TestServerSentClientOnlyPacketIsFatal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		frame []byte
	}{
		{"second CONNACK", []byte{byte(protocol.Connack) << 4, 0x03, 0x00, 0x00, 0x00}},
		{"server PINGREQ", []byte{byte(protocol.Pingreq) << 4, 0x00}},
		{"server SUBSCRIBE", []byte{byte(protocol.Subscribe)<<4 | 0x02, 0x02, 0x00, 0x01}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newMockBroker(t)
			c := NewTCPClient(newIntegrationConfig(b.URL(), "illegal-packet"))
			defer func() { _ = c.Disconnect(context.Background()) }()
			mustConnect(t, c)

			if err := b.InjectRawFrame(tc.frame); err != nil {
				t.Fatalf("inject: %v", err)
			}
			if !lcPoll(3*time.Second, func() bool { return !c.IsConnected() }) {
				t.Fatal("client kept reading after a packet the broker must never send")
			}
			if !lcPoll(time.Second, func() bool { return b.DisconnectCount() == 1 }) {
				t.Fatalf("broker saw %d DISCONNECTs, want the 0x82 protocol-error DISCONNECT", b.DisconnectCount())
			}
			select {
			case <-c.ConnectionLost():
			default:
				t.Fatal("teardown raised no connection-lost signal — a Lifecycle would never reconnect")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// F-17: a stale connection-lost token must not drive the first reconnect
// ---------------------------------------------------------------------------

// TestLifecycleDrainsStaleConnectionLostToken proves a token buffered
// before Start — left by a caller-managed connect, or by a previous
// Lifecycle over the same client — is discarded instead of firing the
// lost branch on the loop's first select. Before the fix that spurious
// reconnect hit the connector's idempotency short-circuit and parked the
// backoff at MaxBackoff, so the next genuine drop waited out the full
// ceiling.
func TestLifecycleDrainsStaleConnectionLostToken(t *testing.T) {
	t.Parallel()

	s := &stableConnector{lost: make(chan struct{}, 1)}
	s.lost <- struct{}{} // buffered before Start: a session this Lifecycle never owned
	lc := NewLifecycle(LifecycleConfig{
		InitialBackoff: 10 * time.Second, // the timer path must play no part
		MaxBackoff:     30 * time.Second,
		Jitter:         -1,
		FlapWindow:     -1, // flap detection off: a loss event would reconnect immediately
	}, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := lc.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = lc.Stop(context.Background()) }()

	time.Sleep(200 * time.Millisecond)
	if got := s.connects.Load(); got != 1 {
		t.Fatalf("connector was connected %d times, want 1 — a stale lost token drove a spurious reconnect", got)
	}

	// A token arriving after the first connect is real and must still be
	// acted on immediately.
	s.lost <- struct{}{}
	if !lcPoll(2*time.Second, func() bool { return s.connects.Load() >= 2 }) {
		t.Fatalf("a genuine loss event did not reconnect (connects=%d)", s.connects.Load())
	}
}

// ---------------------------------------------------------------------------
// F-19: the session store must own its copy of the payload
// ---------------------------------------------------------------------------

// TestStoredPublishDoesNotAliasCallerPayload proves a QoS>0 message kept
// for session replay is stored as a private copy: a caller that reuses its
// scratch buffer after Publish returns cannot rewrite the bytes the DUP
// resend puts on the wire. Before the fix the stored packet was a shallow
// copy, so the replayed PUBLISH carried whatever the caller's buffer held
// at reconnect time — a wrong payload under the original packet
// identifier, invisible to every acknowledgement.
func TestStoredPublishDoesNotAliasCallerPayload(t *testing.T) {
	t.Parallel()

	const original = "original-payload"
	const corrupted = "CORRUPTED-BUFFER"

	b := newMockBroker(t)
	b.SetSessionPresent(true)
	cfg := newIntegrationConfig(b.URL(), "payload-ownership")
	cfg.CleanStart = false
	cfg.SessionExpirySeconds = 3600
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	payload := []byte(original)
	correlation := []byte(original)
	b.DropNextPuback(1) // the exchange stays unacknowledged, so it is stored
	pubErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		pubErr <- c.Publish(ctx, "copy/t", payload, QoS1, false, WithCorrelationData(correlation))
	}()
	if !lcPoll(2*time.Second, func() bool { return len(b.Published()) >= 1 }) {
		t.Fatal("the QoS 1 PUBLISH never reached the broker")
	}

	// The documented contract: the buffers are the caller's again as soon
	// as Publish has put the frame on the wire.
	copy(payload, corrupted)
	copy(correlation, corrupted)

	b.InjectTCPReset()
	select {
	case err := <-pubErr:
		if err == nil {
			t.Fatal("Publish reported success although the connection was severed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish never unblocked after the connection dropped")
	}
	if !lcPoll(3*time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("teardown never cleared the link")
	}

	before := len(b.Published())
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()
	if !lcPoll(3*time.Second, func() bool { return len(b.Published()) > before }) {
		t.Fatal("the resumed session never replayed the stored PUBLISH")
	}
	replay := b.Published()[before]
	if !replay.Dup {
		t.Fatalf("replayed PUBLISH for %s has DUP=0", replay.Topic)
	}
	if got := string(replay.Payload); got != original {
		t.Fatalf("replayed payload = %q, want %q — the store aliased the caller's buffer", got, original)
	}
	if replay.Properties == nil {
		t.Fatal("replayed PUBLISH lost its property block")
	}
	if got := string(replay.Properties.CorrelationData); got != original {
		t.Fatalf("replayed correlation data = %q, want %q — the store aliased the caller's buffer", got, original)
	}
}

// ---------------------------------------------------------------------------
// F-21: OnStateChange callbacks are delivered in transition order
// ---------------------------------------------------------------------------

// TestOnStateChangeCallbacksKeepTransitionOrder proves two transitions
// whose callbacks are invoked in the wrong order still reach OnStateChange
// as an unbroken chain. The invocation order below is exactly what two
// goroutines produce when the one that transitioned first is descheduled
// between changing the state (under b.mu) and running its callback (after
// the unlock): before the fix each closure carried its own captured pair
// and fired it blindly, so a metrics gauge was left showing a state the
// breaker had already left.
func TestOnStateChangeCallbacksKeepTransitionOrder(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var seen []stateChange
	b := NewBreaker(&fakePublisher{}, BreakerConfig{
		OnStateChange: func(from, to BreakerState) {
			mu.Lock()
			seen = append(seen, stateChange{from: from, to: to})
			mu.Unlock()
		},
	})

	b.mu.Lock()
	fireOpen := b.transitionLocked(BreakerOpen)
	b.mu.Unlock()
	b.mu.Lock()
	fireHalfOpen := b.transitionLocked(BreakerHalfOpen)
	b.mu.Unlock()
	if fireOpen == nil || fireHalfOpen == nil {
		t.Fatal("transitionLocked returned no callback for a real transition")
	}

	// The later transition reports first — the interleaving the fix must
	// absorb.
	fireHalfOpen()
	fireOpen()

	mu.Lock()
	got := append([]stateChange(nil), seen...)
	mu.Unlock()

	want := []stateChange{
		{from: BreakerClosed, to: BreakerOpen},
		{from: BreakerOpen, to: BreakerHalfOpen},
	}
	if len(got) != len(want) {
		t.Fatalf("observed %d callbacks (%+v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("callback %d = %v→%v, want %v→%v (transitions delivered out of order)",
				i, got[i].from, got[i].to, want[i].from, want[i].to)
		}
	}
}

// togglePublisher fails or succeeds according to a flag the test flips, so
// a breaker can be driven through many transitions concurrently.
type togglePublisher struct{ fail atomic.Bool }

func (p *togglePublisher) Publish(context.Context, string, []byte, QoS, bool, ...PublishOption) error {
	if p.fail.Load() {
		return ErrConnectionLost
	}
	return nil
}

// TestOnStateChangeChainUnderConcurrency drives a breaker through many
// transitions from several goroutines at once and asserts the observed
// callback sequence is chain-consistent: every from equals the previous
// to, with no duplicate or skipped state. This is the probabilistic
// counterpart to the deterministic ordering test above.
func TestOnStateChangeChainUnderConcurrency(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var seen []stateChange
	p := &togglePublisher{}
	b := NewBreaker(p, BreakerConfig{
		FailureThreshold: 1,
		RecoveryTimeout:  time.Nanosecond, // every open state is probed at once
		HalfOpenMax:      2,
		OnStateChange: func(from, to BreakerState) {
			mu.Lock()
			seen = append(seen, stateChange{from: from, to: to})
			mu.Unlock()
		},
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_ = b.Publish(ctx, "chain/t", []byte("x"), QoS1, false)
			}
		}()
	}
	for range 20 {
		p.fail.Store(!p.fail.Load())
		time.Sleep(time.Millisecond)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 2 {
		t.Fatalf("only %d transitions observed — the sweep did not exercise the breaker", len(seen))
	}
	prev := BreakerClosed
	for i, sc := range seen {
		if sc.from != prev {
			t.Fatalf("callback %d = %v→%v, but the previous callback left the breaker in %v — callbacks are out of order",
				i, sc.from, sc.to, prev)
		}
		if sc.from == sc.to {
			t.Fatalf("callback %d reports a no-op transition %v→%v", i, sc.from, sc.to)
		}
		prev = sc.to
	}
}

// TestOnStateChangeCallbackMayPublish proves the ordered delivery keeps
// the callback lock-free: a callback that publishes back through the same
// Breaker — the natural "announce the outage" wiring — completes instead
// of deadlocking, and the transition its own publish causes is delivered
// afterwards, in order. A callback mutex held across the call would
// self-deadlock here; the drain flag makes the re-entry a no-op.
func TestOnStateChangeCallbackMayPublish(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var seen []stateChange
	p := &togglePublisher{}
	var b *Breaker
	var reentered atomic.Bool
	b = NewBreaker(p, BreakerConfig{
		FailureThreshold: 1,
		RecoveryTimeout:  time.Hour, // stay open; only the first trip matters
		OnStateChange: func(from, to BreakerState) {
			mu.Lock()
			seen = append(seen, stateChange{from: from, to: to})
			mu.Unlock()
			if reentered.CompareAndSwap(false, true) {
				// Publishing from inside the callback must not deadlock.
				_ = b.Publish(context.Background(), "breaker/state", []byte(to.String()), QoS1, false)
			}
		},
	})

	p.fail.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Publish(context.Background(), "chain/t", []byte("x"), QoS1, false)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publish never returned — a callback that publishes deadlocked the breaker")
	}
	if !reentered.Load() {
		t.Fatal("the callback never ran")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("observed %d transitions (%+v), want 1 (closed→open; the nested publish is rejected fast)", len(seen), seen)
	}
	if seen[0] != (stateChange{from: BreakerClosed, to: BreakerOpen}) {
		t.Fatalf("transition = %v→%v, want closed→open", seen[0].from, seen[0].to)
	}
	if got := b.State(); got != BreakerOpen {
		t.Fatalf("state = %v, want open", got)
	}
}
