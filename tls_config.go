// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// NewClientTLSConfig builds the *tls.Config to hand to
// [TCPConfig.TLSConfig] for a tls:// broker connection.
//
// serverName is mandatory: tls.Client(conn, cfg) does NOT infer
// ServerName from the dialed address, so a config built without it
// fails certificate-hostname verification for every broker — a gap
// that is easy to "fix" by reaching for InsecureSkipVerify instead of
// setting ServerName. Making it a required parameter (it is copied
// verbatim into the returned config) is what keeps that trap out of
// reach: there is no way to build a config here that forgets it.
// Callers assembling a *tls.Config by hand are covered too — the dial
// fills an empty ServerName in from the broker URL's hostname before
// handshaking (see [TCPClient.dial]) — but that fallback is a safety
// net, not the documented path.
//
// insecureSkipVerify must only be true when the operator has
// explicitly opted in (config key MQTT_SSL_INSECURE) for a broker with
// a self-signed certificate the operator controls. It must never
// default to true — doing so would silently accept any certificate,
// including one presented by a man-in-the-middle.
func NewClientTLSConfig(serverName string, insecureSkipVerify bool) *tls.Config {
	return &tls.Config{
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
		//nolint:gosec // explicit, documented opt-in via MQTT_SSL_INSECURE; never the default
		InsecureSkipVerify: insecureSkipVerify,
	}
}

// ErrInsecureWithCA is returned by [NewClientTLSConfigWithCA] when it is
// asked both to trust a specific CA and to skip verification altogether.
var ErrInsecureWithCA = errors.New("mqtt: tls: CA file given together with insecureSkipVerify")

// NewClientTLSConfigWithCA extends [NewClientTLSConfig] with an optional
// CA bundle, for brokers whose certificate chains to a private CA rather
// than to one in the system trust store.
//
// caFile names a PEM file of one or more CA certificates. If it is
// empty, the call is equivalent to NewClientTLSConfig(serverName,
// insecureSkipVerify) and never fails. Otherwise the bundle becomes the
// config's RootCAs, which in crypto/tls takes the place of the system
// roots: the broker is then trusted only if it chains to the bundle.
//
// Every problem with the bundle is reported instead of degrading to the
// system roots: the file is unreadable, it holds no certificate, a
// CERTIFICATE block does not parse, or it contains another kind of PEM
// block (typically a private key, a sign that the wrong file was named).
// Text outside PEM blocks is ignored, so commented bundles load fine.
// Build the config at startup, so that a wrong path stops the process
// with an error naming it rather than becoming a handshake failure on
// every reconnect.
//
// insecureSkipVerify keeps its contract from NewClientTLSConfig (an
// explicit operator opt-in, never a default). Together with a caFile it
// is rejected with [ErrInsecureWithCA]: verification would be switched
// off, so the named CA would never be consulted.
func NewClientTLSConfigWithCA(serverName, caFile string, insecureSkipVerify bool) (*tls.Config, error) {
	if caFile == "" {
		return NewClientTLSConfig(serverName, insecureSkipVerify), nil
	}
	if insecureSkipVerify {
		return nil, ErrInsecureWithCA
	}
	roots, err := readCABundle(caFile)
	if err != nil {
		return nil, err
	}
	cfg := NewClientTLSConfig(serverName, false)
	cfg.RootCAs = roots
	return cfg, nil
}

// readCABundle parses every PEM block of the file at path into a fresh
// pool, failing on the first block that is not a valid certificate.
func readCABundle(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the path is the operator's explicit CA choice
	if err != nil {
		return nil, fmt.Errorf("mqtt: tls: CA file: %w", err)
	}
	roots := x509.NewCertPool()
	n := 0
	for rest := data; ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		n++
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("mqtt: tls: CA file %s: PEM block %d is %q, want CERTIFICATE", path, n, block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("mqtt: tls: CA file %s: PEM block %d: %w", path, n, err)
		}
		roots.AddCert(cert)
	}
	if n == 0 {
		return nil, fmt.Errorf("mqtt: tls: CA file %s holds no PEM certificate", path)
	}
	return roots, nil
}
