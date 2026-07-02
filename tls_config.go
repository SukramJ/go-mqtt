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
// setting ServerName. NewClientTLSConfig always derives ServerName
// from the configured broker host so that trap does not exist.
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
