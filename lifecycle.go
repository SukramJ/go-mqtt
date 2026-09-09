// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Connector is the narrow lifecycle contract a broker adapter must
// satisfy. `Connect` establishes the session (including the LWT
// handshake); `Disconnect` unregisters the session gracefully.
type Connector interface {
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
}

// ConnectionNotifier is an optional capability a [Connector] may also
// satisfy to drive the reconnect loop event-driven instead of purely
// timer-polled. [ConnectionLost] returns a channel that receives a value
// whenever the underlying session drops. [Lifecycle.loop] selects on it
// so a detected drop triggers an immediate reconnect attempt rather than
// waiting out the (possibly [LifecycleConfig.MaxBackoff]) idle-probe
// timer. [TCPClient] implements this via its buffered, non-blocking
// ConnectionLost channel.
type ConnectionNotifier interface {
	ConnectionLost() <-chan struct{}
}

// LifecycleConfig governs the reconnect loop.
type LifecycleConfig struct {
	// InitialBackoff is the delay before the first reconnect attempt and
	// the value the backoff resets to after a successful connect. Zero or
	// negative uses 1 second.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential growth. Zero or negative uses 30
	// seconds; a value below InitialBackoff is raised to it.
	MaxBackoff time.Duration
	// Jitter is the maximum absolute deviation applied to every delay: the
	// effective wait is drawn from [d-Jitter, d+Jitter) and then clamped to
	// a floor of d/2, so a Jitter at or above the nominal delay cannot
	// collapse it into an immediate retry (which would defeat the
	// exponential growth entirely). Zero or negative disables jitter.
	Jitter time.Duration
	// FlapWindow is the minimum uptime an established connection must
	// have reached for a detected connection loss to trigger an
	// immediate reconnect with a reset backoff. A connection that drops
	// sooner counts as flapping — a broker that accepts the CONNECT and
	// then closes the socket (a ClientID takeover fight, a connection
	// limit, a draining load balancer) — and each consecutive flap
	// doubles the pre-reconnect delay from InitialBackoff up to
	// MaxBackoff instead of hammering the broker at full dial speed.
	// Zero uses 10 seconds. A negative value disables flap detection
	// entirely — every detected loss reconnects immediately with the
	// backoff reset (the pre-1.3.0 behavior) — which is also what tests
	// pin, since a positive window near zero is not decidable on
	// platforms with a coarse monotonic clock.
	FlapWindow time.Duration
	Logger     *slog.Logger
}

// DefaultLifecycle returns the MVP default timings: 1s → 30s
// exponential backoff with ±500ms jitter and a 10s flap window.
func DefaultLifecycle() LifecycleConfig {
	return LifecycleConfig{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		Jitter:         500 * time.Millisecond,
		FlapWindow:     10 * time.Second,
	}
}

// Lifecycle drives a [Connector] with automatic reconnect. It is
// intentionally transport-agnostic: paho, nhooyr, or any future
// adapter just implements Connector.
type Lifecycle struct {
	cfg       LifecycleConfig
	connector Connector

	mu        sync.Mutex
	started   bool
	cancel    context.CancelFunc
	onConnect []func(context.Context)
	loopDone  chan struct{}
}

// NewLifecycle constructs a lifecycle around connector.
func NewLifecycle(cfg LifecycleConfig, connector Connector) *Lifecycle {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	// Defaults apply to a negative value exactly as to the zero value: a
	// negative duration would otherwise reach the loop unvalidated, where
	// time.After fires instantly and the "backoff" is a busy reconnect
	// spin.
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = DefaultLifecycle().InitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultLifecycle().MaxBackoff
	}
	if cfg.FlapWindow == 0 {
		// Unlike the backoffs, a negative FlapWindow is meaningful: it is
		// the documented "flap detection off" switch, so only the unset
		// zero value takes the default.
		cfg.FlapWindow = DefaultLifecycle().FlapWindow
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		// An inverted pair would clamp every delay back down to the ceiling
		// on the first doubling, so the backoff could never exceed a value
		// the caller already considered too small.
		cfg.MaxBackoff = cfg.InitialBackoff
	}
	return &Lifecycle{cfg: cfg, connector: connector}
}

// OnConnect registers a callback fired on every successful (re)connect.
// Typical use: `bridge.AnnounceOnline` + resubscribe.
//
// Contract: callbacks run synchronously, in registration order, on the
// goroutine that performed the connect — [Lifecycle.Start]'s caller
// goroutine for the first connect, the reconnect loop's own goroutine for
// every later one. The ctx handed to a callback is the loop's run context,
// so it is cancelled when the lifecycle stops. Three consequences a
// callback must respect:
//
//   - A panic is not contained. Nothing recovers it, so it unwinds the
//     reconnect-loop goroutine and takes the process down. Recover inside
//     the callback if the work can panic.
//   - Blocking freezes reconnection. The loop cannot reach its next
//     select while a callback runs, so a callback that waits on I/O of
//     unbounded duration stalls every subsequent reconnect attempt.
//     Hand slow work to a goroutine (and honour the ctx there).
//   - Calling [Lifecycle.Stop] from inside a callback deadlocks. Stop
//     waits for the reconnect loop to exit, and the loop cannot exit while
//     it is running the callback. Signal a shutdown to another goroutine
//     instead of calling Stop inline.
func (l *Lifecycle) OnConnect(fn func(context.Context)) {
	l.mu.Lock()
	l.onConnect = append(l.onConnect, fn)
	l.mu.Unlock()
}

// Start boots the reconnect loop and returns once the first connect
// has succeeded (or ctx was cancelled). Subsequent drops reconnect
// in the background.
//
// ctx governs the WHOLE reconnect loop, not just the initial connect:
// cancelling it permanently stops reconnection (without disconnecting
// an established session — use [Lifecycle.Stop] for an orderly
// shutdown). Pass a context that lives as long as reconnection should,
// typically the application's run context — never a short-lived
// timeout context, or the loop silently dies with it.
func (l *Lifecycle) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return errors.New("mqtt.lifecycle: already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.started = true
	l.mu.Unlock()

	// Drain a connection-lost token buffered before this lifecycle ever
	// connected. A [ConnectionNotifier]'s channel is buffered and survives
	// the session that filled it: a [TCPClient] this Lifecycle is adopting
	// after a caller-managed connect, or one a previous Lifecycle drove,
	// hands over an already-signalled channel. loop()'s first select would
	// then fire the lost branch immediately and reconnect an entirely
	// healthy session — which returns ErrAlreadyConnected, and that answer
	// parks the backoff at MaxBackoff, so the next genuine drop waits out
	// the full ceiling before anything happens.
	//
	// The drain sits exactly here — after runCtx exists, immediately before
	// the first connectOnce — because that instant is what makes "stale"
	// decidable: a token present before this Lifecycle has connected even
	// once can only describe a session it does not own, while a token that
	// arrives during or after connectOnce describes the session it just
	// established and MUST be kept. Draining after the connect would
	// swallow that real one (a broker that accepts the CONNECT and drops
	// the socket immediately); draining before runCtx exists would be the
	// same instant but leave the cancel path half-built.
	if n, ok := l.connector.(ConnectionNotifier); ok {
		select {
		case <-n.ConnectionLost():
		default:
		}
	}

	// First connect is synchronous so the caller can decide whether
	// to proceed on a hard failure.
	if err := l.connectOnce(runCtx); err != nil {
		cancel()
		l.mu.Lock()
		l.started = false
		l.mu.Unlock()
		return err
	}
	done := make(chan struct{})
	l.mu.Lock()
	if !l.started {
		// Stop ran while the first connect was in flight: it saw no loopDone
		// to wait for and its Disconnect hit a not-yet-connected session.
		// Tear the just-established session down instead of leaving it
		// connected with no reconnect loop behind a Stop that reported
		// success. (Only Stop clears started — a mere runCtx cancellation
		// must not disconnect an established session, per the Start
		// contract.)
		l.mu.Unlock()
		_ = l.connector.Disconnect(ctx)
		return errors.New("mqtt.lifecycle: stopped during start")
	}
	l.loopDone = done
	l.mu.Unlock()
	go func() {
		defer close(done)
		l.loop(runCtx)
	}()
	return nil
}

// Stop cancels the loop and disconnects the session.
func (l *Lifecycle) Stop(ctx context.Context) error {
	l.mu.Lock()
	cancel := l.cancel
	done := l.loopDone
	l.started = false
	l.cancel = nil
	l.loopDone = nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	// Wait for the reconnect loop to exit before disconnecting so a
	// concurrent connectOnce can't race the connector teardown.
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	if l.connector != nil {
		return l.connector.Disconnect(ctx)
	}
	return nil
}

func (l *Lifecycle) loop(ctx context.Context) {
	// Type-assert the connector's optional event-driven capability once.
	// A nil channel is fine: a receive on it blocks forever, so a
	// connector that does not implement [ConnectionNotifier] falls back
	// to pure timer polling with no extra branching.
	var lost <-chan struct{}
	if n, ok := l.connector.(ConnectionNotifier); ok {
		lost = n.ConnectionLost()
	}

	backoff := l.cfg.InitialBackoff
	// lastSuccess tracks when the current session was established; Start's
	// synchronous first connect succeeded immediately before this loop was
	// spawned. flapStreak counts consecutive connections that died within
	// FlapWindow of being established — evidence of a flapping broker that
	// accepts the CONNECT and then drops the socket, a failure mode the
	// connect-error backoff below never sees because every Connect succeeds.
	lastSuccess := time.Now()
	flapStreak := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-lost:
			if l.cfg.FlapWindow > 0 && time.Since(lastSuccess) < l.cfg.FlapWindow {
				// The link died within FlapWindow of coming up. An
				// immediate retry with a reset backoff would reconnect a
				// flapping broker at full dial speed, forever — the event
				// channel re-arms on every drop, so exponential backoff
				// could never engage. Wait out a delay that doubles with
				// each consecutive flap instead.
				flapStreak++
				delay := l.cfg.InitialBackoff
				for i := 1; i < flapStreak && delay < l.cfg.MaxBackoff; i++ {
					delay *= 2
				}
				if delay > l.cfg.MaxBackoff {
					delay = l.cfg.MaxBackoff
				}
				l.cfg.Logger.Warn("mqtt.reconnect_flap",
					slog.Int("streak", flapStreak),
					slog.Duration("delay", delay))
				select {
				case <-ctx.Done():
					return
				case <-time.After(l.jittered(delay)):
				}
			} else {
				// The adapter detected an established, previously stable
				// socket dropped. Reconnect promptly instead of waiting
				// out the timer — which, after an idle probe, may be a
				// full MaxBackoff away — and reset the backoff to
				// InitialBackoff so a link that just blipped
				// re-establishes fast rather than at MaxBackoff.
				flapStreak = 0
				backoff = l.cfg.InitialBackoff
			}
		case <-time.After(l.jittered(backoff)):
		}
		if err := l.connectOnce(ctx); err != nil {
			// "already connected" is the connector's idempotency
			// signal: the underlying socket is still healthy, the
			// last Connect() succeeded, nothing to do. Silence the
			// warn-level noise and keep the backoff at MaxBackoff
			// so the next idle probe is far in the future. Only
			// real failures (dial / connack) should drive the
			// exponential growth — those carry a different error.
			if errors.Is(err, ErrAlreadyConnected) {
				backoff = l.cfg.MaxBackoff
				continue
			}
			l.cfg.Logger.Warn("mqtt.reconnect", slog.String("err", err.Error()))
			backoff *= 2
			if backoff > l.cfg.MaxBackoff {
				backoff = l.cfg.MaxBackoff
			}
			continue
		}
		lastSuccess = time.Now()
		backoff = l.cfg.InitialBackoff
	}
}

func (l *Lifecycle) connectOnce(ctx context.Context) error {
	if err := l.connector.Connect(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	cbs := make([]func(context.Context), len(l.onConnect))
	copy(cbs, l.onConnect)
	l.mu.Unlock()
	for _, cb := range cbs {
		cb(ctx)
	}
	return nil
}

// jittered spreads d over [d-Jitter, d+Jitter) and clamps the result to a
// floor of d/2. Without the floor a Jitter at or above d turns a large
// share of the draws into a zero or negative delay — time.After fires
// immediately — so the reconnect loop hammers the broker at dial speed and
// the exponential growth never takes effect.
func (l *Lifecycle) jittered(d time.Duration) time.Duration {
	j := l.cfg.Jitter
	if j <= 0 || j > math.MaxInt64/2 {
		// A jitter past half the duration range would overflow the j*2
		// span below into a non-positive value, which panics rand.Int63n.
		return d
	}
	delta := time.Duration(rand.Int63n(int64(j * 2))) //nolint:gosec // jitter only
	out := d + delta - j
	if floor := d / 2; out < floor {
		out = floor
	}
	return out
}

// Starter is the part of [Lifecycle] that [ConnectWithRetry] drives. It is an
// interface rather than a *Lifecycle so a consumer can substitute a fake in
// its own tests without standing up a broker.
type Starter interface {
	// Start performs the initial connect and boots whatever background
	// reconnection the implementation provides.
	Start(ctx context.Context) error
}

// RetryConfig tunes [ConnectWithRetry]. The zero value is usable and matches
// [LifecycleConfig]'s defaults: 1 second growing to 30.
type RetryConfig struct {
	// InitialBackoff is the delay before the second attempt. Zero or
	// negative uses 1 second.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential growth. Zero or negative uses 30
	// seconds; a value below InitialBackoff is raised to it.
	MaxBackoff time.Duration
	// MaxAttempts bounds the number of Start calls. Zero or negative means
	// retry until ctx is done — the right choice for a daemon, whose whole
	// job is to keep trying.
	MaxAttempts int
	// Logger receives a warning per failed attempt. Nil disables logging.
	Logger *slog.Logger
}

// ConnectWithRetry calls s.Start until it succeeds, ctx is done, or
// cfg.MaxAttempts is exhausted, backing off exponentially between attempts.
//
// It exists because [Lifecycle.Start] deliberately makes exactly one connect
// attempt and reports its outcome: the caller gets to decide whether a broker
// that is not there at boot is fatal. A daemon almost never wants that — it
// wants to come up and keep trying — but a tool that must fail loudly does,
// and the single-attempt contract is what lets both exist. This helper is the
// daemon half, written once instead of in every consumer.
//
// Once Start returns nil, reconnection is the Lifecycle's own business; this
// function does not participate in it and returns immediately.
//
// The error returned on ctx cancellation is ctx.Err(), so a caller can tell an
// orderly shutdown from a broker that never appeared. When MaxAttempts is
// exhausted the last Start error is returned, wrapped so [errors.Is] against
// the underlying cause still works.
func ConnectWithRetry(ctx context.Context, s Starter, cfg RetryConfig) error {
	initial := cfg.InitialBackoff
	if initial <= 0 {
		initial = time.Second
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}
	if maxBackoff < initial {
		maxBackoff = initial
	}

	backoff := initial
	for attempt := 1; ; attempt++ {
		err := s.Start(ctx)
		if err == nil {
			return nil
		}
		// A cancelled context outranks the Start error: the caller stopped
		// waiting, and reporting the connect failure instead would make an
		// orderly shutdown look like a broker problem.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if cfg.MaxAttempts > 0 && attempt >= cfg.MaxAttempts {
			return fmt.Errorf("mqtt: connect gave up after %d attempts: %w", attempt, err)
		}
		if cfg.Logger != nil {
			cfg.Logger.Warn("mqtt.connect_retry",
				slog.Int("attempt", attempt),
				slog.Duration("retry_in", backoff),
				slog.String("err", err.Error()))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
}
