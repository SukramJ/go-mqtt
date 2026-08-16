// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import "crypto/tls"

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
