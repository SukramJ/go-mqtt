// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Regression tests for the round-3 adversarial-hardening findings: the
// Connect/Disconnect TOCTOU, session-epoch (generation) guarding of quota
// and packet-id releases, resumed-session replay ordering, the
// Lifecycle Stop-during-Start race, Unsubscribe clobbering a newer
// registration, ctx-aware CONNECT/CONNACK deadlines, write deadlines on a
// wedged socket, credential redaction, TLS ServerName defaulting, will
// topic validation, and the blocked-handler QoS 2 staleness guard.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// TestConcurrentConnectEstablishesSingleLink proves Connect is serialised:
// N concurrent calls yield exactly one success, N-1 ErrAlreadyConnected,
// and a single TCP connection — never two live links sharing one session.
func TestConcurrentConnectEstablishesSingleLink(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "concurrent-connect"))
	defer func() { _ = c.Disconnect(context.Background()) }()

	const n = 8
	var ok, already atomic.Int32
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			switch err := c.Connect(ctx); {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrAlreadyConnected):
				already.Add(1)
			default:
				t.Errorf("Connect: %v", err)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != 1 || already.Load() != n-1 {
		t.Fatalf("got %d successes and %d ErrAlreadyConnected, want 1 and %d", ok.Load(), already.Load(), n-1)
	}
	if got := b.ConnCount(); got != 1 {
		t.Fatalf("broker accepted %d connections, want 1", got)
	}
}

// TestQuotaStaleReleaseIsNoOp proves a release from before a reset (the
// stalled-Publish-across-a-reconnect interleaving) does not over-credit
// the send quota past the freshly seeded ceiling.
func TestQuotaStaleReleaseIsNoOp(t *testing.T) {
	t.Parallel()

	q := newQuota(1)
	gen, err := q.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	q.fail()         // connection drops
	q.reset(3)       // reconnect re-seeds the ceiling absolutely
	q.releaseAt(gen) // stale goroutine unwinds

	q.mu.Lock()
	avail := q.avail
	q.mu.Unlock()
	if avail != 3 {
		t.Fatalf("avail = %d after a stale releaseAt, want 3 (no over-credit)", avail)
	}

	// A current-generation releaseAt still credits.
	gen2, err := q.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	q.releaseAt(gen2)
	q.mu.Lock()
	avail = q.avail
	q.mu.Unlock()
	if avail != 3 {
		t.Fatalf("avail = %d after a current releaseAt, want 3", avail)
	}
}

// TestIDAllocatorStaleReleaseIsNoOp proves ReleaseAt from before a Reset
// cannot free a packet identifier the new session has handed to another
// exchange.
func TestIDAllocatorStaleReleaseIsNoOp(t *testing.T) {
	t.Parallel()

	var a idAllocator
	id1, gen1, err := a.Acquire()
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	a.Reset()
	id2, _, err := a.Acquire() // the cursor rewound: id2 == id1
	if err != nil {
		t.Fatalf("Acquire after Reset: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("expected the rewound cursor to reuse id %d, got %d", id1, id2)
	}

	a.ReleaseAt(id1, gen1) // stale goroutine unwinds: must NOT free id2

	a.mu.Lock()
	stillUsed := a.used[id2>>6]&(uint64(1)<<(id2&63)) != 0
	a.mu.Unlock()
	if !stillUsed {
		t.Fatal("stale ReleaseAt freed a packet identifier owned by the new session")
	}
}

// TestUnsubscribeKeepsNewerRegistration proves the token-checked removal:
// a Subscribe that re-registered the filter while an UNSUBSCRIBE was in
// flight must survive the Unsubscribe's late removal.
func TestUnsubscribeKeepsNewerRegistration(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "unsub-token"})
	token1 := c.addSubscription("a/b", protocol.SubscribeOptions{QoS: 1}, func(*Message) {})
	// A concurrent Subscribe supersedes the registration mid-flight.
	token2 := c.addSubscription("a/b", protocol.SubscribeOptions{QoS: 2}, func(*Message) {})

	c.removeSubscriptionIfCurrent("a/b", token1) // the older Unsubscribe resolves late
	if snap, ok := c.snapshotSubscription("a/b"); !ok || snap.token != token2 {
		t.Fatalf("newer registration clobbered: ok=%v token=%d want %d", ok, snap.token, token2)
	}

	c.removeSubscriptionIfCurrent("a/b", token2) // the rightful owner removes
	if _, ok := c.snapshotSubscription("a/b"); ok {
		t.Fatal("current-token removal did not remove the registration")
	}
}

// TestResumedSessionReplayPrecedesNewPublish proves stored QoS>0 state is
// re-sent before any concurrently issued fresh PUBLISH can reach the wire
// ([MQTT-4.4.0-1]): the link pointer is published only after the replay.
func TestResumedSessionReplayPrecedesNewPublish(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	b.SetSessionPresent(true)
	cfg := newIntegrationConfig(b.URL(), "replay-order")
	cfg.CleanStart = false
	cfg.SessionExpirySeconds = 300
	c := NewTCPClient(cfg)

	// Seed an unacknowledged QoS 1 PUBLISH as if it survived a drop.
	stored := &protocol.PublishPacket{
		Version: protocol.V50, Topic: "replay/t", Payload: []byte("old"),
		QoS: 1, PacketID: 7,
	}
	if err := c.store.Save(StoredMessage{ID: 7, Kind: StoredPublish, Publish: stored}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	// Hammer Publish concurrently with Connect; it fails fast with
	// ErrNotConnected until the replay is on the wire.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pubDone := make(chan error, 1)
	go func() {
		for {
			err := c.Publish(ctx, "new/t", []byte("new"), QoS1, false)
			if err == nil || ctx.Err() != nil {
				pubDone <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()
	if err := <-pubDone; err != nil {
		t.Fatalf("concurrent Publish never succeeded: %v", err)
	}

	pubs := b.Published()
	if len(pubs) < 2 {
		t.Fatalf("broker recorded %d PUBLISHes, want at least the replay and the fresh one", len(pubs))
	}
	if !pubs[0].Dup || pubs[0].Topic != "replay/t" {
		t.Fatalf("first frame on the wire = %+v, want the DUP replay of replay/t", pubs[0])
	}
}

// gateConnector blocks Connect until the gate closes, so a test can land
// Stop inside Start's synchronous first connect deterministically.
type gateConnector struct {
	gate        chan struct{}
	entered     chan struct{}
	enterOnce   sync.Once
	disconnects atomic.Int32
}

func (g *gateConnector) Connect(context.Context) error {
	g.enterOnce.Do(func() { close(g.entered) })
	<-g.gate
	return nil
}

func (g *gateConnector) Disconnect(context.Context) error {
	g.disconnects.Add(1)
	return nil
}

// TestLifecycleStopDuringStartDisconnects proves a Stop that lands while
// Start's synchronous first connect is in flight does not leave the
// session connected with no reconnect loop: Start tears the
// just-established session down and reports the stop.
func TestLifecycleStopDuringStartDisconnects(t *testing.T) {
	t.Parallel()

	g := &gateConnector{gate: make(chan struct{}), entered: make(chan struct{})}
	l := NewLifecycle(DefaultLifecycle(), g)

	startErr := make(chan error, 1)
	go func() { startErr <- l.Start(context.Background()) }()

	select {
	case <-g.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Start never reached the connector")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	close(g.gate) // the in-flight Connect now succeeds — too late

	select {
	case err := <-startErr:
		if err == nil {
			t.Fatal("Start returned nil after Stop intervened; the session would be orphaned")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start never returned")
	}
	if !lcPoll(time.Second, func() bool { return g.disconnects.Load() >= 2 }) {
		t.Fatalf("connector.Disconnect called %d time(s); want the compensating teardown from Start", g.disconnects.Load())
	}
}

// TestConnectCancelledContextAbortsConnackWait proves cancelling the
// caller's ctx unblocks Connect promptly while it waits for a CONNACK a
// wedged broker never sends — instead of running out the full DialTimeout
// and possibly establishing a session on a dead context.
func TestConnectCancelledContextAbortsConnackWait(t *testing.T) {
	t.Parallel()

	// A listener that accepts and then never responds.
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close() //nolint:gocritic // bounded: one connection per test
		}
	}()

	cfg := newIntegrationConfig("tcp://"+ln.Addr().String(), "ctx-cancel")
	cfg.DialTimeout = 10 * time.Second // long enough that only ctx can unblock us fast
	c := NewTCPClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	connErr := make(chan error, 1)
	go func() { connErr <- c.Connect(ctx) }()
	time.Sleep(150 * time.Millisecond) // let Connect reach the CONNACK wait
	start := time.Now()
	cancel()

	select {
	case err := <-connErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Connect err = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("Connect took %v to honour the cancellation", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect ignored the cancelled context")
	}
	if c.IsConnected() {
		t.Fatal("client connected on a cancelled context")
	}
}

// TestWriteFrameDeadlineTearsDownWedgedSocket proves a socket whose peer
// stops reading cannot wedge writeFrame (and with it sendMu, every
// Publish and the keep-alive watchdog) indefinitely: the write deadline
// fires and the link is torn down.
func TestWriteFrameDeadlineTearsDownWedgedSocket(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe() // synchronous: a write blocks until read
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "wedged"})
	c.cfg.AckTimeout = 200 * time.Millisecond
	l := &link{conn: client, w: bufio.NewWriter(client), stop: make(chan struct{})}
	c.link.Store(l)

	pkt := &protocol.PublishPacket{
		Version: protocol.V50, Topic: "t", QoS: 0,
		Payload: bytes.Repeat([]byte{'x'}, 64<<10), // >> bufio's 4 KiB buffer
	}
	start := time.Now()
	err := c.writeFrame(l, pkt.Encode)
	if err == nil {
		t.Fatal("writeFrame succeeded against a peer that never reads")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("writeFrame took %v, want the ~200ms write deadline to fire", elapsed)
	}
	if !isStopping(l) {
		t.Fatal("link not torn down after the write deadline fired")
	}
	if c.IsConnected() {
		t.Fatal("client still reports connected after the teardown")
	}
	select {
	case <-c.ConnectionLost():
	default:
		t.Fatal("no connection-lost signal after a non-graceful write teardown")
	}
}

// TestConnectLogsRedactedBrokerURL proves credentials embedded in the
// BrokerURL userinfo never reach the structured log stream.
func TestConnectLogsRedactedBrokerURL(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	addr := strings.TrimPrefix(b.URL(), "tcp://")

	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(lockedWriter{w: &buf, mu: &mu}, nil))

	cfg := newIntegrationConfig("tcp://user:sup3rs3cret@"+addr, "redact")
	cfg.Logger = logger
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if strings.Contains(out, "sup3rs3cret") {
		t.Fatalf("log leaked the URL password:\n%s", out)
	}
	if !strings.Contains(out, "xxxxx") {
		t.Fatalf("log does not carry the redacted URL marker:\n%s", out)
	}
}

// lockedWriter serialises writes so the slog handler is safe against the
// test goroutine reading the buffer.
type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// TestTLSConfigIgnoredWarnsOnPlainScheme proves an explicitly configured
// TLSConfig combined with a non-TLS scheme — an invisible plaintext
// downgrade — is surfaced with a warning.
func TestTLSConfigIgnoredWarnsOnPlainScheme(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)

	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(lockedWriter{w: &buf, mu: &mu}, nil))

	cfg := newIntegrationConfig(b.URL(), "tls-ignored") // tcp://
	cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	cfg.Logger = logger
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if !strings.Contains(out, "mqtt.tcp.tls_config_ignored") {
		t.Fatalf("no tls_config_ignored warning for a TLSConfig on a tcp:// URL:\n%s", out)
	}
}

// TestDialDefaultsServerNameForPinnedRootCAs proves the natural CA-pinning
// config — RootCAs set, ServerName empty — completes a verified handshake
// (the hostname is filled in from the broker URL) instead of failing and
// steering the operator toward InsecureSkipVerify. The caller's config
// must stay unmutated (it is cloned).
func TestDialDefaultsServerNameForPinnedRootCAs(t *testing.T) {
	t.Parallel()

	cert, err := tls.X509KeyPair([]byte(selfSignedCertPEM), []byte(selfSignedKeyPEM))
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(selfSignedCertPEM)) {
		t.Fatal("failed to add the self-signed cert to the pool")
	}

	b := tlsMockBroker(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	addr := b.listener.Addr().String()

	callerCfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12} // no ServerName
	cfg := newIntegrationConfig("tls://"+addr, "pinned-roots")
	cfg.TLSConfig = callerCfg
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	if callerCfg.ServerName != "" {
		t.Fatalf("caller's TLSConfig was mutated: ServerName = %q", callerCfg.ServerName)
	}
}

// TestConnectValidatesWillTopic proves an invalid will topic fails Connect
// fast with a clear error instead of a broker-rejected malformed CONNECT
// turning into a silent reconnect loop.
func TestConnectValidatesWillTopic(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		will *Will
	}{
		{"wildcard topic", &Will{Topic: "state/#", Payload: []byte("offline")}},
		{"empty topic", &Will{Topic: "", Payload: []byte("offline")}},
		{"wildcard response topic", &Will{Topic: "state/x", ResponseTopic: "reply/+"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "will-validate", Will: tc.will}
			c := NewTCPClient(cfg)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := c.Connect(ctx); !errors.Is(err, protocol.ErrProtocolViolation) {
				t.Fatalf("Connect err = %v, want ErrProtocolViolation", err)
			}
		})
	}
}

// TestBlockedHandlerDoesNotPoisonNextSession proves the post-dispatch
// staleness guard: a QoS 2 MessageHandler that blocks across a connection
// teardown must not record its dedup entry into the (possibly reset)
// session store once it returns — that entry would swallow a future
// inbound QoS 2 PUBLISH reusing the identifier as a duplicate.
func TestBlockedHandlerDoesNotPoisonNextSession(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "blocked-handler")
	c := NewTCPClient(cfg)
	c.pingInterval = 50 * time.Millisecond // fast watchdog so the dead socket is noticed
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	entered := make(chan struct{})
	release := make(chan struct{})
	sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	if _, err := c.Subscribe(sctx, "q2/block", QoS2, func(*Message) {
		close(entered)
		<-release
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// The QoS 2 PUBLISH parks the read loop inside the handler; the mock's
	// InjectPublish times out waiting for a PUBREC that cannot come yet.
	go func() { _ = b.InjectPublish("q2/block", []byte("x"), 2, false, nil) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never entered")
	}

	// Kill the socket while the handler blocks; the keep-alive watchdog's
	// failing PINGREQ write tears the link down.
	b.InjectTCPReset()
	if !lcPoll(3*time.Second, func() bool { return !c.IsConnected() }) {
		t.Fatal("client never noticed the dead socket")
	}

	close(release) // the handler returns into a torn-down link

	// The dedup entry must NOT have been saved.
	time.Sleep(150 * time.Millisecond)
	msgs, err := c.store.All()
	if err != nil {
		t.Fatalf("store.All: %v", err)
	}
	for _, m := range msgs {
		if m.Kind == StoredInboundID {
			t.Fatalf("stale handler recorded dedup state into the next session: %+v", m)
		}
	}
}
