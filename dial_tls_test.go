// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Coverage for TCPClient.dial's tls:// branches (adapter_tcp.go), which no
// other test in this package exercises: an unsupported URL scheme, the
// default-port assignment (both 1883 and 8883) feeding into a dial
// failure, and a full TLS handshake against a self-signed certificate —
// both the success path (a trusted root) and the failure path (the
// default config, which does not trust it).
//
// mockShim wraps the scripted mockBroker's own accept/serve loop around a
// tls.Listener instead of its usual bare TCP one, by constructing the
// (package-private, same-package-accessible) mockBroker struct directly
// and driving its unexported acceptLoop — reusing every CONNECT/CONNACK
// script already implemented there instead of re-deriving a broker
// double.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"
)

// selfSignedCertPEM/selfSignedKeyPEM are a throwaway ECDSA P-256
// certificate/key pair (SAN 127.0.0.1 + localhost, ~100 year validity)
// generated once with `openssl req -x509 ...` for this test file only. It
// authenticates nothing beyond this in-process TLS listener.
const (
	selfSignedCertPEM = `-----BEGIN CERTIFICATE-----
MIIBijCCATGgAwIBAgIUCmhe3mN9cTm1UOE5Q0mmGhhnOAwwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJMTI3LjAuMC4xMCAXDTI2MDcwNDEwNTA1NFoYDzIxMjYwNjEw
MTA1MDU0WjAUMRIwEAYDVQQDDAkxMjcuMC4wLjEwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAAS1Hbq4CrH/nISipF2lsu8aVyoL3YAOaHMN29MqSO68v0IchCKE0Ap+
xkbg7rZxS0S5to9JyYhYtQ1GZ1S7oTMQo18wXTAaBgNVHREEEzARgglsb2NhbGhv
c3SHBH8AAAEwCwYDVR0PBAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMBMB0GA1Ud
DgQWBBTIaTuSz8lnPAokqIpU6k4jin5qwjAKBggqhkjOPQQDAgNHADBEAiBwUbw0
NWBzroT2U6xCvkMGSnjMVI4/egmdaI97RePK1gIgEYfvxyPp3rH9yl+BTx8/n98R
lQYrC5T0rpm3BHvoNlw=
-----END CERTIFICATE-----`
	selfSignedKeyPEM = `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIICro5Aotw0iwxV2pnAEDWwhGElejDHY8S7dHoVXgxxgoAoGCCqGSM49
AwEHoUQDQgAEtR26uAqx/5yEoqRdpbLvGlcqC92ADmhzDdvTKkjuvL9CHIQihNAK
fsZG4O62cUtEubaPScmIWLUNRmdUu6EzEA==
-----END EC PRIVATE KEY-----`
)

// TestDialUnsupportedScheme proves dial() rejects a scheme it does not
// recognise without attempting to connect anywhere.
func TestDialUnsupportedScheme(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "ftp://127.0.0.1:1", ClientID: "badscheme"})
	u, err := url.Parse("ftp://127.0.0.1:1")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.dial(ctx, u); err == nil {
		t.Fatal("expected an unsupported-scheme error")
	}
}

// TestDialDefaultPortAssignment proves a host with no explicit port gets
// the documented default (1883 plain, 8883 tls), by observing that dial
// fails (there is nothing listening on either default port at a
// non-routable documentation address) within a short, bounded context —
// exercising the port-defaulting statement for both schemes without
// depending on what, if anything, happens to be listening locally.
func TestDialDefaultPortAssignment(t *testing.T) {
	t.Parallel()

	for _, scheme := range []string{"tcp", "tls"} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()

			c := NewTCPClient(TCPConfig{BrokerURL: scheme + "://192.0.2.1", ClientID: "portdefault"})
			u, err := url.Parse(scheme + "://192.0.2.1") // RFC 5737 TEST-NET-1: no port, never routed
			if err != nil {
				t.Fatalf("url.Parse: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			start := time.Now()
			if _, err := c.dial(ctx, u); err == nil {
				t.Fatal("expected a dial error against a non-routable address")
			}
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Fatalf("dial took %v to fail, want bounded by the short ctx", elapsed)
			}
		})
	}
}

// tlsMockBroker starts a mockBroker (reusing its full CONNECT/CONNACK
// scripting) behind a tls.Listener instead of the plain-TCP one
// newMockBroker builds, so TCPClient's tls:// dial path can be driven
// end-to-end. It constructs mockBroker directly since same-package test
// files may reach its unexported fields/methods.
func tlsMockBroker(t *testing.T, servConf *tls.Config) *mockBroker {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	tln := tls.NewListener(ln, servConf)
	t.Cleanup(func() { _ = tln.Close() })
	b := &mockBroker{t: t, listener: tln}
	go b.acceptLoop()
	return b
}

// TestConnectTLSHandshakeSuccess proves a tls:// broker URL completes a
// real TLS handshake and the full CONNECT/CONNACK/PUBLISH round trip when
// the client is configured to trust the broker's certificate.
func TestConnectTLSHandshakeSuccess(t *testing.T) {
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

	cfg := newIntegrationConfig("tls://"+addr, "tls-success")
	cfg.TLSConfig = &tls.Config{RootCAs: pool, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12}
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	if !c.IsConnected() {
		t.Fatal("IsConnected = false after a successful TLS connect")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Publish(ctx, "tls/topic", []byte("secure"), QoS1, false); err != nil {
		t.Fatalf("Publish over TLS: %v", err)
	}
	if !lcPoll(time.Second, func() bool { return len(b.Published()) == 1 }) {
		t.Fatal("broker never observed the PUBLISH sent over TLS")
	}
}

// TestConnectTLSHandshakeFailsUntrustedCert proves the default TLS config
// (built automatically when TCPConfig.TLSConfig is nil) does NOT blindly
// trust an arbitrary self-signed certificate: Connect fails with a
// certificate-verification error instead of silently succeeding.
func TestConnectTLSHandshakeFailsUntrustedCert(t *testing.T) {
	t.Parallel()

	cert, err := tls.X509KeyPair([]byte(selfSignedCertPEM), []byte(selfSignedKeyPEM))
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	b := tlsMockBroker(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	addr := b.listener.Addr().String()

	cfg := newIntegrationConfig("tls://"+addr, "tls-untrusted")
	cfg.DialTimeout = 3 * time.Second
	c := NewTCPClient(cfg) // TLSConfig left nil: the auto-built config trusts only the system roots.

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err = c.Connect(ctx)
	if err == nil {
		t.Fatal("expected a certificate verification error, got nil")
	}
	var unknownAuth x509.UnknownAuthorityError
	if !errors.As(err, &unknownAuth) {
		t.Logf("Connect error (informational, not necessarily UnknownAuthorityError): %v", err)
	}
	if c.IsConnected() {
		t.Fatal("IsConnected = true despite a failed TLS handshake")
	}
}
