// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package e2e

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// proxy is a transparent TCP relay in front of a real broker address. It
// lets a test simulate a dropped connection — a broker crash, a network
// partition — against a real broker without touching the broker itself:
// the client under test dials the proxy instead of the broker, and
// [proxy.Sever] abruptly closes every connection currently relayed while
// leaving the listener running, so a client that reconnects to the same
// proxy address afterward gets a fresh relayed connection to the (still
// healthy) real broker.
type proxy struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	conns  map[*proxyConn]struct{}
	delay  time.Duration
	closed bool
	wg     sync.WaitGroup
}

// proxyConn is one relayed connection: the client-facing socket accepted
// by the proxy's listener and the upstream socket dialed to the real
// broker on its behalf.
type proxyConn struct {
	client, upstream net.Conn
	once             sync.Once
}

func (pc *proxyConn) close() {
	pc.once.Do(func() {
		_ = pc.client.Close()
		_ = pc.upstream.Close()
	})
}

// newProxy starts a proxy listening on 127.0.0.1 (an ephemeral port) that
// relays every accepted connection to target (a bare host:port, see
// [brokerHostPort]). It is torn down automatically via t.Cleanup.
func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy: listen: %v", err)
	}
	p := &proxy{ln: ln, target: target, conns: make(map[*proxyConn]struct{})}
	p.wg.Add(1)
	go p.acceptLoop()
	t.Cleanup(p.stop)
	return p
}

// URL returns the tcp:// address the proxy listens on — pass this as a
// [mqtt.TCPConfig.BrokerURL] in place of the real broker's address.
func (p *proxy) URL() string { return "tcp://" + p.ln.Addr().String() }

func (p *proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.relay(c)
	}
}

func (p *proxy) relay(client net.Conn) {
	defer p.wg.Done()
	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	pc := &proxyConn{client: client, upstream: upstream}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		pc.close()
		return
	}
	delay := p.delay
	p.conns[pc] = struct{}{}
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.conns, pc)
		p.mu.Unlock()
		pc.close()
	}()

	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, client)
		done <- struct{}{}
	}()
	go func() {
		if delay > 0 {
			relayDelayed(client, upstream, delay)
		} else {
			_, _ = io.Copy(client, upstream)
		}
		done <- struct{}{}
	}()
	<-done
}

// relayDelayed copies from src to dst like io.Copy, but sleeps d before
// writing each chunk it reads. This proxy has no protocol awareness — the
// delay is per-Read, not per-frame — but that is good enough to give a
// test a wide, deterministic window in which to sever a connection
// mid-exchange (e.g. after a PUBLISH left the client but before its
// PUBREC/PUBACK arrives) instead of racing a sub-millisecond localhost
// round trip.
func relayDelayed(dst io.Writer, src io.Reader, d time.Duration) {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			time.Sleep(d)
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr != nil {
			return
		}
	}
}

// SetServerToClientDelay delays every byte relayed from the upstream
// broker to the client by d before writing it onward. It only takes
// effect for connections accepted after the call, so set it before the
// client under test (re)connects through the proxy.
func (p *proxy) SetServerToClientDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

// Sever abruptly closes every connection currently relayed through the
// proxy, simulating a broker crash or a network partition: the client
// sees its socket die exactly as it would for either. The listener keeps
// running, so a client that reconnects to the same proxy URL afterward
// gets a fresh relayed connection.
func (p *proxy) Sever() {
	p.mu.Lock()
	victims := make([]*proxyConn, 0, len(p.conns))
	for pc := range p.conns {
		victims = append(victims, pc)
	}
	p.mu.Unlock()
	for _, pc := range victims {
		pc.close()
	}
}

// stop shuts the proxy down for good: the listener and every relayed
// connection. Registered via t.Cleanup by newProxy.
func (p *proxy) stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	victims := make([]*proxyConn, 0, len(p.conns))
	for pc := range p.conns {
		victims = append(victims, pc)
	}
	p.mu.Unlock()

	_ = p.ln.Close()
	for _, pc := range victims {
		pc.close()
	}
	p.wg.Wait()
}

// --- TLS -----------------------------------------------------------------

// certsDir returns MQTT_E2E_CERTS_DIR, skipping the test when it is
// unset. There is no dial probe here: the directory is local filesystem
// state produced by `make e2e-certs`, not a network address.
func certsDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv(envCertsDir)
	if dir == "" {
		t.Skipf("%s not set, skipping", envCertsDir)
	}
	return dir
}

// clientTLSConfig loads the e2e CA (see e2e/gencert) into a RootCAs pool
// and returns a *tls.Config pinned to it for serverName. Passing a
// deliberately wrong serverName is how connect_test.go exercises the
// "TLS fails with wrong ServerName" case: [mqtt.NewClientTLSConfig]'s own
// doc comment warns that tls.Client does not infer ServerName from the
// dialed address, so a mismatched one must fail verification rather than
// silently connect.
func clientTLSConfig(t *testing.T, serverName string) *tls.Config {
	t.Helper()
	dir := certsDir(t)
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		t.Fatalf("read ca.pem: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("ca.pem: no certificates parsed")
	}
	return &tls.Config{
		RootCAs:    pool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}
}
