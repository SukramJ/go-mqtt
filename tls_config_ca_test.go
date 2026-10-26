// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// otherCAPEM returns a freshly generated self-signed CA certificate that
// the TLS mock broker's certificate does NOT chain to.
func otherCAPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "unrelated-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func writeCAFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return path
}

func TestNewClientTLSConfigWithCAEmptyMatchesBase(t *testing.T) {
	t.Parallel()
	for _, insecure := range []bool{false, true} {
		cfg, err := NewClientTLSConfigWithCA("broker.example.com", "", insecure)
		if err != nil {
			t.Fatalf("insecure=%v: unexpected error: %v", insecure, err)
		}
		if cfg.RootCAs != nil {
			t.Errorf("insecure=%v: RootCAs set without a CA file; the system roots must stay in effect", insecure)
		}
		if cfg.InsecureSkipVerify != insecure {
			t.Errorf("insecure=%v: InsecureSkipVerify = %v", insecure, cfg.InsecureSkipVerify)
		}
		if cfg.ServerName != "broker.example.com" || cfg.MinVersion != tls.VersionTLS12 {
			t.Errorf("insecure=%v: ServerName=%q MinVersion=%#x, want NewClientTLSConfig's values",
				insecure, cfg.ServerName, cfg.MinVersion)
		}
	}
}

func TestNewClientTLSConfigWithCALoadsBundle(t *testing.T) {
	t.Parallel()
	// Comment text around the blocks is common in CA bundles and must not
	// be mistaken for an error.
	bundle := "# private broker CA\n" + selfSignedCertPEM + "\n# second CA\n" + otherCAPEM(t)
	cfg, err := NewClientTLSConfigWithCA("127.0.0.1", writeCAFile(t, bundle), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs not set from the CA file")
	}
	//nolint:staticcheck // Subjects is fine for counting pool entries in a test
	if got := len(cfg.RootCAs.Subjects()); got != 2 {
		t.Errorf("pool holds %d certificates, want 2", got)
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify set although verification against the CA was requested")
	}
	if cfg.ServerName != "127.0.0.1" || cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("ServerName=%q MinVersion=%#x, want NewClientTLSConfig's values", cfg.ServerName, cfg.MinVersion)
	}
}

func TestNewClientTLSConfigWithCARejectsInsecure(t *testing.T) {
	t.Parallel()
	cfg, err := NewClientTLSConfigWithCA("127.0.0.1", writeCAFile(t, selfSignedCertPEM), true)
	if !errors.Is(err, ErrInsecureWithCA) {
		t.Fatalf("err = %v, want ErrInsecureWithCA", err)
	}
	if cfg != nil {
		t.Error("a config was returned alongside the error")
	}
}

func TestNewClientTLSConfigWithCABadFiles(t *testing.T) {
	t.Parallel()
	corrupt := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not DER")}))
	cases := []struct {
		name    string
		content string // ignored when missing is set
		missing bool
		want    string
	}{
		{name: "missing", missing: true},
		{name: "empty", content: "", want: "holds no PEM certificate"},
		{name: "no PEM", content: "just some text\n", want: "holds no PEM certificate"},
		{name: "private key", content: selfSignedCertPEM + "\n" + selfSignedKeyPEM, want: `PEM block 2 is "EC PRIVATE KEY"`},
		{name: "corrupt certificate", content: corrupt, want: "PEM block 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "absent.pem")
			if !tc.missing {
				path = writeCAFile(t, tc.content)
			}
			cfg, err := NewClientTLSConfigWithCA("127.0.0.1", path, false)
			if err == nil {
				t.Fatal("expected an error, got a config")
			}
			if cfg != nil {
				t.Error("a config was returned alongside the error")
			}
			if tc.missing && !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("err = %v, want it to wrap fs.ErrNotExist", err)
			}
			if !strings.Contains(err.Error(), path) {
				t.Errorf("err = %q does not name the file %q", err, path)
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestConnectTLSWithCAFile proves the CA bundle is what the handshake
// actually verifies against: the broker's certificate is accepted when it
// is in the bundle and refused when the bundle holds only an unrelated CA
// — the system roots are not consulted in either case.
func TestConnectTLSWithCAFile(t *testing.T) {
	t.Parallel()
	cert, err := tls.X509KeyPair([]byte(selfSignedCertPEM), []byte(selfSignedKeyPEM))
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}

	t.Run("trusted", func(t *testing.T) {
		t.Parallel()
		b := tlsMockBroker(t, &tls.Config{Certificates: []tls.Certificate{cert}})
		tlsCfg, err := NewClientTLSConfigWithCA("127.0.0.1", writeCAFile(t, selfSignedCertPEM), false)
		if err != nil {
			t.Fatalf("NewClientTLSConfigWithCA: %v", err)
		}
		cfg := newIntegrationConfig("tls://"+b.listener.Addr().String(), "tls-ca-trusted")
		cfg.TLSConfig = tlsCfg
		c := NewTCPClient(cfg)
		mustConnect(t, c)
		defer func() { _ = c.Disconnect(context.Background()) }()
	})

	t.Run("untrusted", func(t *testing.T) {
		t.Parallel()
		b := tlsMockBroker(t, &tls.Config{Certificates: []tls.Certificate{cert}})
		tlsCfg, err := NewClientTLSConfigWithCA("127.0.0.1", writeCAFile(t, otherCAPEM(t)), false)
		if err != nil {
			t.Fatalf("NewClientTLSConfigWithCA: %v", err)
		}
		cfg := newIntegrationConfig("tls://"+b.listener.Addr().String(), "tls-ca-untrusted")
		cfg.TLSConfig = tlsCfg
		cfg.DialTimeout = 3 * time.Second
		c := NewTCPClient(cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		err = c.Connect(ctx)
		if _, ok := errors.AsType[x509.UnknownAuthorityError](err); !ok {
			t.Fatalf("Connect err = %v, want an x509.UnknownAuthorityError", err)
		}
		if c.IsConnected() {
			t.Fatal("IsConnected = true despite a certificate outside the CA bundle")
		}
	})
}
