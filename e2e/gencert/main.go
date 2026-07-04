// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

// Package main implements gencert, a standalone generator for the
// throwaway CA and TLS server certificate the e2e test brokers (see
// ../testdata/mosquitto.conf) use for their TLS listener.
//
// Usage:
//
//	go run ./e2e/gencert -out e2e/testdata/certs
//
// It writes three files under -out:
//
//	ca.pem      self-signed CA certificate (0644)
//	server.pem  server leaf certificate signed by the CA, SAN
//	            localhost/127.0.0.1/::1 (0644)
//	server.key  server private key, PKCS#8 PEM (0600)
//
// gencert is idempotent: if all three files already exist it does
// nothing and exits 0, so `make e2e-certs` is safe to run repeatedly
// (e.g. as an e2e-up dependency) without generating a fresh, mutually
// distrusting CA/cert pair on every invocation.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// certValidity is deliberately short: these certs exist only for the
// lifetime of a local/CI e2e run, never for a long-lived deployment.
const certValidity = 24 * time.Hour

// clockSkew backdates NotBefore so a few minutes of clock drift
// between the host generating the cert and the container serving it
// doesn't make the cert look "not yet valid".
const clockSkew = 5 * time.Minute

func main() {
	outDir := flag.String("out", "", "output directory for ca.pem, server.pem, server.key (required)")
	flag.Parse()

	if *outDir == "" {
		slog.Error("missing required flag", "flag", "-out")
		os.Exit(1)
	}

	if err := run(*outDir); err != nil {
		slog.Error("gencert failed", "error", err)
		os.Exit(1)
	}
}

func run(outDir string) error {
	caPath := filepath.Join(outDir, "ca.pem")
	certPath := filepath.Join(outDir, "server.pem")
	keyPath := filepath.Join(outDir, "server.key")

	if filesExist(caPath, certPath, keyPath) {
		slog.Info("e2e certs already present, skipping generation", "dir", outDir)
		return nil
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil { //nolint:gosec // must stay traversable by whatever uid the docker bind-mount reads it as
		return fmt.Errorf("create output dir %q: %w", outDir, err)
	}

	caCert, caKey, err := generateCA()
	if err != nil {
		return fmt.Errorf("generate CA: %w", err)
	}

	serverDER, serverKey, err := generateServerCert(caCert, caKey)
	if err != nil {
		return fmt.Errorf("generate server cert: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return fmt.Errorf("marshal server private key: %w", err)
	}

	if err := writePEM(caPath, "CERTIFICATE", caCert.Raw, 0o644); err != nil {
		return err
	}
	if err := writePEM(certPath, "CERTIFICATE", serverDER, 0o644); err != nil {
		return err
	}
	if err := writePEM(keyPath, "PRIVATE KEY", keyDER, 0o600); err != nil {
		return err
	}

	slog.Info("generated e2e TLS certs", "dir", outDir)
	return nil
}

// filesExist reports whether every path exists and is non-empty.
func filesExist(paths ...string) bool {
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil || info.Size() == 0 {
			return false
		}
	}
	return true
}

// generateCA creates a fresh, self-signed CA certificate and key.
func generateCA() (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate CA key: %w", err)
	}

	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "go-mqtt e2e test CA",
			Organization: []string{"go-mqtt e2e"},
		},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.Add(certValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("self-sign CA: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA cert: %w", err)
	}
	return cert, key, nil
}

// generateServerCert creates a server leaf certificate, signed by
// caCert/caKey, valid for localhost/127.0.0.1/::1.
func generateServerCert(caCert *x509.Certificate, caKey *ecdsa.PrivateKey) ([]byte, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate server key: %w", err)
	}

	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-clockSkew),
		NotAfter:     now.Add(certValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("sign server cert: %w", err)
	}
	return der, key, nil
}

// randSerial returns a random, positive 128-bit certificate serial.
func randSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}
	return serial, nil
}

// writePEM PEM-encodes der under the given block type and writes it
// to path. perm is caller-controlled: ca.pem/server.pem are
// intentionally world-readable (0644) so a container bind-mounting
// e2e/testdata can read them regardless of the uid it runs as, while
// server.key is written 0600.
func writePEM(path, blockType string, der []byte, perm os.FileMode) error {
	block := &pem.Block{Type: blockType, Bytes: der}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), perm); err != nil { //nolint:gosec // perm is an explicit, intentional argument at each call site
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}
