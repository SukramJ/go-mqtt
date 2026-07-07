// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// Client-side defaults. All are overridable through [TCPConfig].
const (
	// defaultMaxPacketSize is the inbound frame cap (and advertised
	// Maximum Packet Size) used when [TCPConfig.MaximumPacketSize] is zero.
	defaultMaxPacketSize uint32 = 1 << 20 // 1 MiB
	// keepAliveFloor is the minimum keep-alive the client will request
	// (MQTT spec §3.1.2.10 permits any value; a very small one wastes
	// bandwidth on a bridge). A broker-imposed Server Keep Alive may still
	// drive the ping schedule below this floor.
	keepAliveFloor = 30 * time.Second
	// defaultDialTimeout bounds the TCP/TLS dial plus the CONNECT/CONNACK
	// round-trip.
	defaultDialTimeout = 10 * time.Second
	// defaultAckTimeout bounds how long Publish/Subscribe wait for the
	// broker acknowledgement before giving up.
	defaultAckTimeout = 20 * time.Second
	// defaultMaxInflight / defaultReceiveMaximum is the 16-bit ceiling the
	// spec assigns when a Receive Maximum is absent (§3.1.2.11.3).
	defaultReceiveMaximum = 65535
)

// TCPConfig wires a [TCPClient] against a real broker. The zero value is
// not usable on its own — at least BrokerURL and ClientID must be set —
// but every timing/limit field has a sane default applied by
// [NewTCPClient].
type TCPConfig struct {
	// BrokerURL is the broker endpoint: tcp://host[:1883] or
	// tls://host[:8883] (mqtt://, ssl://, mqtts:// are accepted aliases).
	BrokerURL string
	// ClientID is the MQTT client identifier. An empty identifier on an
	// MQTT 5.0 link asks the broker to assign one (surfaced on
	// [ConnectResult.AssignedClientID]).
	ClientID string
	// Username is the optional CONNECT user name.
	Username string
	// Password is the optional CONNECT password. On an MQTT 3.1.1 link a
	// password without a username is rejected by the codec.
	Password string

	// ProtocolVersion selects the wire dialect. The zero value selects
	// MQTT 5.0 ([ProtocolV50]).
	ProtocolVersion ProtocolVersion
	// CleanStart is the CONNECT Clean Start bit (MQTT 3.1.1 Clean Session).
	// When true the broker discards any prior session and the client resets
	// its own stored QoS>0 state.
	CleanStart bool
	// SessionExpirySeconds is the MQTT 5.0 Session Expiry Interval (0x11)
	// requested in CONNECT. Zero requests a session that ends when the
	// connection closes (treated as a clean start for local session state).
	SessionExpirySeconds uint32
	// ReceiveMaximum is the number of unacknowledged QoS>0 PUBLISHes the
	// client will accept concurrently, advertised to the broker (MQTT 5.0
	// 0x21). Zero advertises nothing (the 65535 default).
	ReceiveMaximum uint16
	// MaximumPacketSize is both the largest packet the client will accept
	// (advertised in CONNECT, enforced by the read loop) and the read-frame
	// cap. Zero selects 1 MiB.
	MaximumPacketSize uint32
	// TopicAliasMaximum is the highest inbound topic alias the client
	// accepts, advertised to the broker (MQTT 5.0 0x22). Zero disables
	// inbound topic aliasing.
	TopicAliasMaximum uint16
	// UserProperties are MQTT 5.0 User Property pairs (0x26) attached to the
	// CONNECT.
	UserProperties []UserProperty

	// KeepAlive is the requested keep-alive interval; values below the 30s
	// floor are raised to it. A broker Server Keep Alive overrides the ping
	// schedule even below the floor.
	KeepAlive time.Duration
	// DialTimeout bounds the dial and CONNECT/CONNACK round-trip (default
	// 10s).
	DialTimeout time.Duration
	// AckTimeout bounds each Publish/Subscribe acknowledgement wait (default
	// 20s).
	AckTimeout time.Duration
	// MaxInflight caps concurrent in-flight outbound QoS>0 sends on an MQTT
	// 3.1.1 link (which has no Receive Maximum). Zero selects 65535.
	MaxInflight uint16

	// TLSConfig is used for tls:// endpoints (tls://, ssl://, mqtts://).
	// When nil a minimal config verifying the broker hostname is built
	// automatically. A supplied config is cloned before use and, when its
	// ServerName is empty, the hostname from BrokerURL is filled in — so a
	// CA-pinning config (RootCAs only) verifies the broker identity instead
	// of failing the handshake. On a non-TLS scheme (tcp://, mqtt://) a
	// configured TLSConfig is NOT used; Connect logs a warning because the
	// connection proceeds in plaintext despite transport security having
	// been configured.
	TLSConfig *tls.Config
	// Will is the optional Last Will and Testament published by the broker
	// if the connection drops without a clean DISCONNECT.
	Will *Will
	// Logger receives structured diagnostics. Defaults to [slog.Default].
	Logger *slog.Logger
}

// subscription is one registered filter: the wire options it was
// subscribed with (replayed verbatim on reconnect so the delivery
// guarantee is preserved) and the handler inbound matches route to. token
// is a monotonic stamp assigned on every registration so a failed
// SUBSCRIBE's rollback can tell whether a concurrent Subscribe for the same
// filter has since superseded it (and must not be clobbered).
type subscription struct {
	filter  string
	handler MessageHandler
	options protocol.SubscribeOptions
	token   uint64
}

// ackResult is delivered to a Publish/Subscribe waiter when its
// acknowledgement (PUBACK/PUBCOMP/SUBACK/UNSUBACK, or a QoS 2 PUBREC
// failure) arrives, or when the connection drops. A non-nil err means the
// exchange did not complete; otherwise code carries the broker reason code
// and reason its optional Reason String.
type ackResult struct {
	err    error
	reason string
	code   ReasonCode
}

// ackClass is the acknowledgement family a waiter expects. Packet
// identifiers are shared across PUBLISH/SUBSCRIBE/UNSUBSCRIBE, so without
// the class a broker could complete an in-flight QoS>0 Publish with a
// forged SUBACK carrying the same identifier — reporting success for a
// message it never acknowledged (and stranding the stored entry, the
// identifier and the send-quota permit). PUBACK, PUBREC-error and PUBCOMP
// collapse into one class: a QoS 2 waiter is legitimately resolved by
// either.
type ackClass uint8

const (
	ackClassPublish ackClass = iota + 1
	ackClassSuback
	ackClassUnsuback
)

// String returns a short label for structured log fields.
func (a ackClass) String() string {
	switch a {
	case ackClassPublish:
		return "publish-ack"
	case ackClassSuback:
		return "suback"
	case ackClassUnsuback:
		return "unsuback"
	default:
		return "unknown"
	}
}

// waiter is one registered acknowledgement waiter: the buffered result
// channel and the acknowledgement class that may legitimately resolve it.
type waiter struct {
	ch   chan ackResult
	want ackClass
}

// link is the mutable per-connection state. Every field is owned by one
// connection; a reconnect builds a fresh link and swaps it in atomically,
// so the read and keep-alive loops never observe a half-torn-down
// connection through shared nil-able fields.
//
// aliases (the inbound topic-alias table) is touched only by the read loop
// and therefore needs no lock. sendMu guards buf and w so concurrent
// writers (Publish, the keep-alive ping, read-loop acks) never interleave
// a half-encoded frame on the wire.
type link struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	sendMu sync.Mutex
	buf    bytes.Buffer

	stop      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	// graceful is set by Disconnect before its best-effort DISCONNECT
	// write, so a write failure inside writeFrame tears the link down
	// without signalling a lost connection (which would trigger a spurious
	// lifecycle reconnect after an intentional shutdown).
	graceful atomic.Bool

	aliases map[uint16]string

	outboundMax      uint32 // broker Maximum Packet Size; 0 = unlimited
	inboundMax       uint32 // our advertised cap, wired to ReadFrame
	pingInterval     time.Duration
	outstandingPings atomic.Int32
}

// TCPClient is a reconnecting MQTT 3.1.1 / 5.0 client over TCP or TLS. It
// implements both [Client] (Publish + Subscribe/Unsubscribe) and
// [Connector] (Connect + Disconnect) so a [Lifecycle] can drive its full
// lifecycle, plus [ConnectionNotifier] for event-driven reconnects.
//
// Lock ordering. The client holds several independent short-lived locks;
// to stay deadlock-free they are always acquired in this order and no lock
// is ever held across a blocking channel receive or a socket write:
//
//	connMu → quota.mu → ids.mu → store.mu → waitersMu → link.sendMu
//
// connMu is outermost and long-lived by design: it serialises Connect and
// Disconnect end-to-end (including the dial and the CONNACK round-trip) so
// two concurrent Connect calls can never establish two live links sharing
// one session state. It is never taken by the read/keep-alive loops or the
// publish path. In practice the remaining critical sections do not nest:
// the read loop signals a waiter (waitersMu) and then, separately, writes
// a reply (sendMu); the publish path acquires quota, then an id, saves to
// the store, registers a waiter, and only then writes. link.sendMu is a
// leaf — nothing else is acquired while it is held. subsMu (the
// subscription slice) is independent of all the above; the read loop
// copies the matching handlers out from under it before invoking them.
type TCPClient struct {
	cfg     TCPConfig
	logger  *slog.Logger
	version protocol.Version

	// connMu serialises Connect and Disconnect (see the lock-order note
	// above).
	connMu sync.Mutex

	inboundMax uint32
	// pingInterval, when non-zero, overrides the keep-alive-derived ping
	// schedule. It exists only so package tests can drive the watchdog
	// faster than the 30s keep-alive floor allows.
	pingInterval time.Duration

	link atomic.Pointer[link]

	ids   idAllocator
	store SessionStore
	quota *quota

	subsMu sync.RWMutex
	subs   []subscription
	subSeq uint64 // monotonic registration stamp, guarded by subsMu

	waitersMu sync.Mutex
	waiters   map[uint16]waiter

	lostCh chan struct{}

	result      atomic.Pointer[ConnectResult]
	connectedAt atomic.Pointer[time.Time]
}

// Compile-time confirmation that TCPClient satisfies every contract a
// bridge composes it for.
var (
	_ Client             = (*TCPClient)(nil)
	_ Connector          = (*TCPClient)(nil)
	_ ConnectionNotifier = (*TCPClient)(nil)
)

// NewTCPClient constructs a client from cfg, applying defaults for any
// unset timing/limit field. It does not dial; call [TCPClient.Connect].
func NewTCPClient(cfg TCPConfig) *TCPClient {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ProtocolVersion == 0 {
		cfg.ProtocolVersion = ProtocolV50
	}
	if cfg.KeepAlive < keepAliveFloor {
		cfg.KeepAlive = keepAliveFloor
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = defaultAckTimeout
	}
	inboundMax := cfg.MaximumPacketSize
	if inboundMax == 0 {
		inboundMax = defaultMaxPacketSize
	}
	return &TCPClient{
		cfg:        cfg,
		logger:     cfg.Logger,
		version:    cfg.ProtocolVersion,
		inboundMax: inboundMax,
		store:      newMemStore(),
		quota:      newQuota(0),
		waiters:    make(map[uint16]waiter),
		lostCh:     make(chan struct{}, 1),
	}
}

// ConnectionLost returns a channel that receives a value whenever the
// client detects its connection dropped. It is buffered (size 1) so a drop
// is never missed by a lifecycle loop that reacts to it.
func (c *TCPClient) ConnectionLost() <-chan struct{} { return c.lostCh }

// IsConnected reports whether the client currently holds a link.
func (c *TCPClient) IsConnected() bool { return c.link.Load() != nil }

// LastConnectedAt returns the wall-clock instant of the most recent
// successful CONNECT, or the zero time when none has happened.
func (c *TCPClient) LastConnectedAt() time.Time {
	if p := c.connectedAt.Load(); p != nil {
		return *p
	}
	return time.Time{}
}

// ConnectResult returns the negotiated session state from the most recent
// successful connect. The bool is false before the first connect.
func (c *TCPClient) ConnectResult() (ConnectResult, bool) {
	if p := c.result.Load(); p != nil {
		return *p, true
	}
	return ConnectResult{}, false
}

// Connect implements [Connector]. It dials, sends CONNECT, waits for
// CONNACK, applies the negotiated limits, replays any resumable session
// state and prior subscriptions, and starts the read and keep-alive loops.
// A live link makes it a no-op that returns a wrapped [ErrAlreadyConnected]
// so [Lifecycle] treats it as idempotent. Connect and Disconnect are
// serialised against each other (connMu), so concurrent calls cannot
// establish two live links sharing one session state.
func (c *TCPClient) Connect(ctx context.Context) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.link.Load() != nil {
		return fmt.Errorf("mqtt/tcp: %w", ErrAlreadyConnected)
	}

	u, err := url.Parse(c.cfg.BrokerURL)
	if err != nil {
		return fmt.Errorf("mqtt/tcp: bad broker url: %w", err)
	}
	if c.cfg.Will != nil {
		// Fail fast on an invalid will topic instead of letting the broker
		// reject (or drop the connection over) a malformed CONNECT on every
		// reconnect attempt — a silent reconnect loop with no useful error.
		if err := protocol.ValidateTopicName(c.cfg.Will.Topic); err != nil {
			return fmt.Errorf("mqtt/tcp: will topic: %w", err)
		}
		if c.version == protocol.V50 && c.cfg.Will.ResponseTopic != "" {
			if err := protocol.ValidateTopicName(c.cfg.Will.ResponseTopic); err != nil {
				return fmt.Errorf("mqtt/tcp: will response topic: %w", err)
			}
		}
	}

	// One DialTimeout budget covers the dial, the TLS handshake AND the
	// CONNECT/CONNACK round-trip, matching the field's documented contract.
	deadline := time.Now().Add(c.cfg.DialTimeout)
	dialCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	conn, err := c.dial(dialCtx, u)
	if err != nil {
		return fmt.Errorf("mqtt/tcp: dial: %w", err)
	}

	// The absolute deadline bounds every pre-session read AND write: a
	// broker that accepts TCP but never reads cannot park the CONNECT flush
	// (or a mid-encode bufio auto-flush) forever. The AfterFunc additionally
	// propagates a caller ctx cancellation: it poisons the deadline so a
	// blocked write/read unwinds promptly instead of running out the full
	// DialTimeout on a context the caller has abandoned.
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(dialCtx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	bw := bufio.NewWriter(conn)
	if err := c.buildConnectPacket().Encode(bw); err != nil {
		_ = conn.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: send connect: %w", connectAbortCause(dialCtx, err))
	}

	br := bufio.NewReader(conn)
	frame, err := protocol.ReadFrame(br, c.inboundMax)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: read connack: %w", connectAbortCause(dialCtx, err))
	}
	if frame.PacketType() != protocol.Connack {
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: expected CONNACK, got %s", frame.PacketType())
	}
	ack, err := protocol.DecodeConnack(c.version, frame.Body)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if ack.ReasonCode.IsError() {
		_ = conn.Close()
		return &ReasonError{Packet: "CONNECT", Code: ack.ReasonCode, Reason: reasonStringOf(ack.Properties)}
	}
	if err := validateConnackLimits(c.version, ack); err != nil {
		// The broker advertised a §3.2.2.3 Protocol Error (Receive Maximum or
		// Maximum Packet Size of 0). Refuse the session with a best-effort
		// DISCONNECT(0x82) rather than proceeding with a corrupt send quota or
		// packet-size limit.
		dp := &protocol.DisconnectPacket{Version: c.version, ReasonCode: protocol.ProtocolErrorReason}
		_ = dp.Encode(bw)
		_ = bw.Flush()
		_ = conn.Close()
		return err
	}
	if !stop() {
		// The caller's ctx was cancelled (or the deadline expired) while the
		// CONNACK was in flight: the deadline may already be poisoned and the
		// caller has moved on. Never establish a session on a dead context.
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: connect aborted: %w", context.Cause(dialCtx))
	}
	_ = conn.SetDeadline(time.Time{})

	result := c.buildConnectResult(ack)
	c.result.Store(&result)

	l := &link{
		conn:         conn,
		r:            br,
		w:            bw,
		stop:         make(chan struct{}),
		aliases:      make(map[uint16]string),
		outboundMax:  result.MaximumPacketSize,
		inboundMax:   c.inboundMax,
		pingInterval: c.effectivePingInterval(result),
	}

	c.applySession(result)

	// Replay stored QoS>0 state and prior subscriptions BEFORE publishing
	// the link pointer: the spec requires unacknowledged PUBLISH/PUBREL
	// packets to be re-sent ahead of any new traffic on a resumed session
	// ([MQTT-4.4.0-1] / v5 §4.4), and a concurrent Publish passes the link
	// check the instant the pointer is stored. Until then it keeps failing
	// fast with ErrNotConnected, per the documented contract.
	c.replaySession(l)
	c.replaySubscriptions(l)
	if isStopping(l) {
		// A replay write failed and tore the link down before it was ever
		// published. Surface the failure instead of storing a dead link the
		// loops would never clear.
		return fmt.Errorf("mqtt/tcp: %w: connection lost during session replay", ErrConnectionLost)
	}

	c.link.Store(l)
	now := time.Now()
	c.connectedAt.Store(&now)

	l.wg.Add(2)
	go c.readLoop(l)
	go c.keepAliveLoop(l)

	c.logger.Info("mqtt.tcp.connected",
		slog.String("broker", u.Redacted()),
		slog.String("version", c.version.String()),
		slog.Bool("session_present", result.SessionPresent))
	return nil
}

// connectAbortCause maps a socket error during the CONNECT/CONNACK
// round-trip back to the context cause when the caller's ctx (or the
// DialTimeout deadline) aborted it — the AfterFunc in Connect surfaces a
// cancellation as a poisoned deadline, which would otherwise read as an
// opaque i/o timeout.
func connectAbortCause(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return err
}

// applySession decides whether the resumed session state survives this
// connect. A clean start, a zero MQTT 5.0 Session Expiry, or a broker that
// did not resume the session (Session Present = 0) all discard the stored
// QoS>0 state, free the packet identifiers and resize the send quota from
// scratch. Otherwise the store and identifiers are kept for replay.
//
// The quota is seeded with the negotiated ceiling minus the number of
// QoS>0 messages still in flight: on a resumed session those are exactly
// the StoredPublish/StoredPubrel entries replaySession is about to put back
// on the wire, and they must keep counting against Receive Maximum until
// their acks arrive (§4.9). On a reset session the store was just cleared,
// so the in-flight count is zero and the quota starts at the full ceiling.
func (c *TCPClient) applySession(result ConnectResult) {
	reset := c.cfg.CleanStart ||
		(c.version == protocol.V50 && c.cfg.SessionExpirySeconds == 0) ||
		!result.SessionPresent
	if reset {
		_ = c.store.Reset()
		c.ids.Reset()
	}
	size := c.quotaSize(result) - c.inflightPermits()
	if size < 0 {
		size = 0
	}
	c.quota.reset(size)
}

// quotaSize is the number of concurrent in-flight outbound QoS>0 sends the
// broker permits: its advertised Receive Maximum on MQTT 5.0, the
// configured MaxInflight on MQTT 3.1.1.
func (c *TCPClient) quotaSize(result ConnectResult) int {
	if c.version == protocol.V50 {
		return int(result.ReceiveMaximum)
	}
	if c.cfg.MaxInflight != 0 {
		return int(c.cfg.MaxInflight)
	}
	return defaultReceiveMaximum
}

// inflightPermits counts the outbound QoS>0 exchanges still awaiting a
// terminal acknowledgement — the StoredPublish and StoredPubrel entries,
// each of which holds a send-quota permit. StoredInboundID (receiver-side
// state) does not count. Used to seed the quota on (re)connect so the
// resumed-session replay set stays accounted for against Receive Maximum.
func (c *TCPClient) inflightPermits() int {
	msgs, err := c.store.All()
	if err != nil {
		return 0
	}
	n := 0
	for _, m := range msgs {
		if m.Kind == StoredPublish || m.Kind == StoredPubrel {
			n++
		}
	}
	return n
}

// replaySession retransmits the resumable QoS>0 state in ascending Seq
// order before any new traffic: an unacknowledged PUBLISH is resent with
// the DUP flag set, a PUBREL leg is resent as-is. Completions arriving for
// these have no waiter and are logged at debug by the read loop.
func (c *TCPClient) replaySession(l *link) {
	msgs, err := c.store.All()
	if err != nil {
		c.logger.Warn("mqtt.tcp.replay_load", slog.String("err", err.Error()))
		return
	}
	for _, m := range msgs {
		switch m.Kind {
		case StoredPublish:
			if m.Publish == nil {
				continue
			}
			p := *m.Publish
			p.Dup = true
			if err := c.writeFrame(l, p.Encode); err != nil {
				c.logger.Warn("mqtt.tcp.replay_publish",
					slog.Uint64("packet_id", uint64(m.ID)), slog.String("err", err.Error()))
			}
		case StoredPubrel:
			ack := &protocol.AckPacket{Version: c.version, Type: protocol.Pubrel, PacketID: m.ID}
			if err := c.writeFrame(l, ack.EncodeAck); err != nil {
				c.logger.Warn("mqtt.tcp.replay_pubrel",
					slog.Uint64("packet_id", uint64(m.ID)), slog.String("err", err.Error()))
			}
		case StoredInboundID:
			// Receiver-side exactly-once state: nothing to retransmit.
		}
	}
}

// replaySubscriptions re-sends every registered subscription on a fresh
// connection. Without this a CleanStart=true reconnect (the common bridge
// configuration) silently loses every SUBSCRIBE from the previous socket
// and inbound command topics stop being delivered even though the broker
// happily accepts publishes to them.
//
// It is fire-and-log: the SUBACK is awaited in a detached goroutine that
// only surfaces a rejection, so a slow or hostile broker cannot stall the
// connect path, and the identifier is returned once the ack (or the
// connection drop) resolves.
func (c *TCPClient) replaySubscriptions(l *link) {
	c.subsMu.RLock()
	subs := make([]subscription, len(c.subs))
	copy(subs, c.subs)
	c.subsMu.RUnlock()

	for _, s := range subs {
		id, gen, err := c.ids.Acquire()
		if err != nil {
			c.logger.Warn("mqtt.tcp.resubscribe", slog.String("filter", s.filter), slog.String("err", err.Error()))
			continue
		}
		pkt := &protocol.SubscribePacket{
			Version:       c.version,
			PacketID:      id,
			Subscriptions: []protocol.Subscription{{Filter: s.filter, Options: s.options}},
		}
		ch := c.registerWaiter(id, ackClassSuback)
		if err := c.writeFrame(l, pkt.Encode); err != nil {
			c.removeWaiter(id, ch)
			c.ids.ReleaseAt(id, gen)
			c.logger.Warn("mqtt.tcp.resubscribe", slog.String("filter", s.filter), slog.String("err", err.Error()))
			continue
		}
		l.wg.Add(1)
		go c.awaitResubscribe(l, id, gen, s.filter, ch)
	}
}

// awaitResubscribe consumes the SUBACK for a replayed subscription and logs
// a rejection, then frees the identifier (generation-checked: a session
// reset in between already freed it, possibly to a new owner). A connection
// drop resolves it via the failed waiter or the link's stop channel.
func (c *TCPClient) awaitResubscribe(l *link, id uint16, gen uint64, filter string, ch chan ackResult) {
	defer l.wg.Done()
	var res ackResult
	select {
	case res = <-ch:
	case <-l.stop:
		c.removeWaiter(id, ch)
		c.ids.ReleaseAt(id, gen)
		return
	}
	c.ids.ReleaseAt(id, gen)
	if res.err != nil {
		return
	}
	if res.code.IsError() {
		c.logger.Warn("mqtt.tcp.resubscribe_rejected",
			slog.String("filter", filter), slog.String("reason", res.code.String()))
	}
}

// Disconnect implements [Connector]. It sends a best-effort DISCONNECT,
// tears the link down gracefully (no connection-lost signal), and waits —
// bounded by ctx — for the read and keep-alive loops to exit. It is
// serialised against Connect (connMu), so it can no longer report success
// while a concurrent Connect is mid-handshake and about to establish a
// fresh link.
func (c *TCPClient) Disconnect(ctx context.Context) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	l := c.link.Load()
	if l == nil {
		return nil
	}
	l.graceful.Store(true)
	dp := &protocol.DisconnectPacket{Version: c.version, ReasonCode: protocol.NormalDisconnection}
	_ = c.writeFrame(l, dp.Encode)
	c.teardownLink(l, true)

	done := make(chan struct{})
	go func() { l.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	c.logger.Info("mqtt.tcp.disconnected")
	return nil
}

// writeFrame encodes one packet fully into the link's scratch buffer under
// sendMu, enforces the negotiated outbound size limit, then writes and
// flushes it in a single pass so a partial write never leaves a truncated
// fixed header on the wire. A nil link (already disconnected) yields
// [ErrNotConnected].
//
// Every write is bounded by an AckTimeout write deadline: a broker (or a
// half-open NAT path) that stops reading fills the kernel send buffer and
// would otherwise park the writer — holding sendMu — for the TCP
// retransmission timeout (~15 min), freezing every Publish AND the
// keep-alive watchdog (whose PINGREQ blocks on the same mutex). A failed
// write or flush tears the link down here rather than in the callers: the
// deadline may have expired mid-write, leaving a truncated frame on the
// wire and a poisoned bufio.Writer, so the link is unusable for any
// subsequent frame. Pre-wire failures (encode errors, ErrPacketTooLarge)
// leave the link intact.
func (c *TCPClient) writeFrame(l *link, encode func(io.Writer) error) error {
	if l == nil {
		return ErrNotConnected
	}
	l.sendMu.Lock()
	l.buf.Reset()
	if err := encode(&l.buf); err != nil {
		l.sendMu.Unlock()
		return err
	}
	if l.outboundMax != 0 && uint32(l.buf.Len()) > l.outboundMax { //nolint:gosec // buf length is non-negative
		l.sendMu.Unlock()
		return ErrPacketTooLarge
	}
	// l.conn is nil only for the bare links package tests construct; a
	// dialed link always carries its socket.
	if l.conn != nil {
		_ = l.conn.SetWriteDeadline(time.Now().Add(c.cfg.AckTimeout))
	}
	_, err := l.w.Write(l.buf.Bytes())
	if err == nil {
		err = l.w.Flush()
	}
	if err == nil && l.conn != nil {
		_ = l.conn.SetWriteDeadline(time.Time{})
	}
	// sendMu is a leaf lock: release it before teardownLink acquires
	// waitersMu and quota.mu.
	l.sendMu.Unlock()
	if err != nil {
		c.teardownLink(l, l.graceful.Load())
		return err
	}
	return nil
}

// teardownLink closes a connection exactly once. It stops the loops, closes
// the socket, clears the link pointer if it is still current, fails every
// in-flight waiter with [ErrConnectionLost] and marks the quota failed so
// parked sends unblock immediately. The stored session state, inbound
// dedup set and in-flight identifiers are deliberately KEPT so a resumed
// session can complete them. When graceful is false (a detected drop, not a
// caller Disconnect) it signals the connection-lost channel so a lifecycle
// loop reconnects.
//
// When l is not (or no longer) the current link — it failed during the
// pre-publish session replay, or a newer connection has since been
// established — only the socket is closed: failing the shared waiters and
// quota, or signalling a lost connection, would sabotage the healthy
// current link. connMu makes that state unreachable through Connect/
// Disconnect themselves; the guard is defence in depth for the replay
// window, where the link exists but was never published.
func (c *TCPClient) teardownLink(l *link, graceful bool) {
	l.closeOnce.Do(func() {
		close(l.stop)
		if l.conn != nil {
			_ = l.conn.Close()
		}
		if !c.link.CompareAndSwap(l, nil) {
			return
		}
		c.failAllWaiters()
		c.quota.fail()
		if !graceful {
			select {
			case c.lostCh <- struct{}{}:
			default:
			}
		}
	})
}

// registerWaiter installs a buffered result channel for the packet
// identifier so the read loop can hand the acknowledgement back to the
// blocked Publish/Subscribe caller. want records which acknowledgement
// class may resolve the waiter (see [ackClass]).
func (c *TCPClient) registerWaiter(id uint16, want ackClass) chan ackResult {
	ch := make(chan ackResult, 1)
	c.waitersMu.Lock()
	c.waiters[id] = waiter{ch: ch, want: want}
	c.waitersMu.Unlock()
	return ch
}

// removeWaiter drops the waiter for id, but only while ch is still the
// registered channel. Safe to call for an already-removed id. The channel
// check keeps a stale goroutine — one whose waiter was already failed and
// cleared by a teardown, after which a reconnected session may have handed
// the same identifier to a new request — from deleting the new owner's
// waiter and stranding that request until its AckTimeout.
func (c *TCPClient) removeWaiter(id uint16, ch chan ackResult) {
	c.waitersMu.Lock()
	if w, ok := c.waiters[id]; ok && w.ch == ch {
		delete(c.waiters, id)
	}
	c.waitersMu.Unlock()
}

// signalWaiter delivers res to the waiter for id (removing it) and reports
// whether one was registered. The send is non-blocking: the channel is
// buffered and each waiter is signalled at most once.
//
// got is the acknowledgement class carrying the signal. A registered waiter
// expecting a different class is a protocol violation (e.g. a SUBACK
// carrying the packet identifier of an in-flight QoS>0 PUBLISH); the waiter
// is left registered — it resolves through the real acknowledgement, its
// AckTimeout/ctx, or the connection dropping — and the mismatch is
// warn-logged. It still reports true so callers do not double-log an
// unknown identifier.
func (c *TCPClient) signalWaiter(id uint16, got ackClass, res ackResult) bool {
	c.waitersMu.Lock()
	w, ok := c.waiters[id]
	if ok && w.want == got {
		delete(c.waiters, id)
	}
	c.waitersMu.Unlock()
	if !ok {
		return false
	}
	if w.want != got {
		c.logger.Warn("mqtt.tcp.ack_class_mismatch",
			slog.Uint64("packet_id", uint64(id)),
			slog.String("got", got.String()), slog.String("want", w.want.String()))
		return true
	}
	select {
	case w.ch <- res:
	default:
	}
	return true
}

// failAllWaiters resolves every outstanding waiter with [ErrConnectionLost]
// and clears the table. Called from teardownLink so no Publish/Subscribe
// parks forever on a dead socket.
func (c *TCPClient) failAllWaiters() {
	c.waitersMu.Lock()
	for id, w := range c.waiters {
		select {
		case w.ch <- ackResult{err: ErrConnectionLost}:
		default:
		}
		delete(c.waiters, id)
	}
	c.waitersMu.Unlock()
}

// addSubscription registers or replaces (in place, preserving order) the
// handler and wire options for filter, and returns the fresh monotonic
// token stamped on the registration so the caller can later detect whether
// its registration is still current.
func (c *TCPClient) addSubscription(filter string, options protocol.SubscribeOptions, handler MessageHandler) uint64 {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	c.subSeq++
	token := c.subSeq
	for i := range c.subs {
		if c.subs[i].filter == filter {
			c.subs[i].options = options
			c.subs[i].handler = handler
			c.subs[i].token = token
			return token
		}
	}
	c.subs = append(c.subs, subscription{filter: filter, handler: handler, options: options, token: token})
	return token
}

// snapshotSubscription returns the current registration for filter, if
// any — the undo state for a provisional [TCPClient.addSubscription]
// that a failed SUBSCRIBE round-trip must roll back.
func (c *TCPClient) snapshotSubscription(filter string) (subscription, bool) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for i := range c.subs {
		if c.subs[i].filter == filter {
			return c.subs[i], true
		}
	}
	return subscription{}, false
}

// restoreSubscription undoes a provisional addSubscription (identified by
// token) after a failed SUBSCRIBE: it reinstates the previous registration
// (or removes the entry) ONLY while the provisional registration is still
// the current one. A concurrent Subscribe for the same filter that
// registered after ours bumps the token, so its registration is left intact
// — rolling ours back would delete a subscription the broker has accepted
// and is delivering to, silently dropping its inbound messages.
func (c *TCPClient) restoreSubscription(filter string, prev subscription, existed bool, token uint64) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	idx := -1
	for i := range c.subs {
		if c.subs[i].filter == filter {
			idx = i
			break
		}
	}
	if idx == -1 || c.subs[idx].token != token {
		// Superseded by a later Subscribe (or already gone): do not clobber.
		return
	}
	if existed {
		c.subs[idx].options = prev.options
		c.subs[idx].handler = prev.handler
		c.subs[idx].token = prev.token
		return
	}
	c.subs = append(c.subs[:idx], c.subs[idx+1:]...)
}

// removeSubscriptionIfCurrent drops the registration for filter only while
// its token still matches the snapshot the caller took before its
// UNSUBSCRIBE hit the wire. This mirrors restoreSubscription's guard: a
// concurrent Subscribe for the same filter that registered after the
// snapshot bumped the token, and its live registration — which the broker
// is delivering to — must not be clobbered by the older Unsubscribe
// resolving late. A residual window remains for a Subscribe whose token was
// bumped before the snapshot but whose SUBSCRIBE frame serialised after the
// UNSUBSCRIBE on the wire; closing it fully would require stamping tokens
// under link.sendMu at write time, which is not worth the coupling.
func (c *TCPClient) removeSubscriptionIfCurrent(filter string, token uint64) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for i := range c.subs {
		if c.subs[i].filter == filter {
			if c.subs[i].token != token {
				return
			}
			c.subs = append(c.subs[:i], c.subs[i+1:]...)
			return
		}
	}
}

// buildConnectPacket assembles the CONNECT for the configured version,
// including the MQTT 5.0 property block advertising the client's limits and
// the Last Will and Testament.
func (c *TCPClient) buildConnectPacket() *protocol.ConnectPacket {
	pkt := &protocol.ConnectPacket{
		Version:    c.version,
		ClientID:   c.cfg.ClientID,
		KeepAlive:  c.keepAliveSeconds(),
		Username:   c.cfg.Username,
		Password:   c.cfg.Password,
		CleanStart: c.cfg.CleanStart,
	}
	if c.cfg.Will != nil {
		pkt.Will = buildWill(c.version, c.cfg.Will)
	}
	if c.version == protocol.V50 {
		props := &protocol.Properties{}
		if c.cfg.SessionExpirySeconds != 0 {
			v := c.cfg.SessionExpirySeconds
			props.SessionExpiryInterval = &v
		}
		if c.cfg.ReceiveMaximum != 0 {
			v := c.cfg.ReceiveMaximum
			props.ReceiveMaximum = &v
		}
		maxPkt := c.inboundMax
		props.MaximumPacketSize = &maxPkt
		if c.cfg.TopicAliasMaximum != 0 {
			v := c.cfg.TopicAliasMaximum
			props.TopicAliasMaximum = &v
		}
		props.UserProperties = c.cfg.UserProperties
		pkt.Properties = props
	}
	return pkt
}

// keepAliveSeconds is the CONNECT keep-alive value in whole seconds.
func (c *TCPClient) keepAliveSeconds() uint16 {
	s := c.cfg.KeepAlive / time.Second
	if s > 0xFFFF {
		return 0xFFFF
	}
	return uint16(s) //nolint:gosec // clamped to uint16 range above
}

// buildWill translates the public [Will] into the wire form, attaching the
// MQTT 5.0 will property block only on a v5 link.
func buildWill(v protocol.Version, w *Will) *protocol.Will {
	pw := &protocol.Will{
		Topic:   w.Topic,
		Payload: w.Payload,
		QoS:     byte(w.QoS),
		Retain:  w.Retain,
	}
	if v != protocol.V50 {
		return pw
	}
	props := &protocol.Properties{
		ContentType:     w.ContentType,
		ResponseTopic:   w.ResponseTopic,
		CorrelationData: w.CorrelationData,
		UserProperties:  w.UserProperties,
	}
	if w.PayloadFormatUTF8 {
		one := byte(1)
		props.PayloadFormat = &one
	}
	if w.MessageExpirySeconds != 0 {
		v := w.MessageExpirySeconds
		props.MessageExpiryInterval = &v
	}
	if w.DelayIntervalSeconds != 0 {
		v := w.DelayIntervalSeconds
		props.WillDelayInterval = &v
	}
	pw.Properties = props
	return pw
}

// validateConnackLimits rejects the MQTT 5.0 CONNACK limits the spec
// declares a Protocol Error the client MUST refuse: a Receive Maximum of 0
// (§3.2.2.3.3) — which would starve the send quota and hang every QoS>0
// Publish — and a Maximum Packet Size of 0 (§3.2.2.3.6). Both surface an
// error wrapping [protocol.ErrProtocolViolation]. On an MQTT 3.1.1 link (no
// property block) it is always a no-op.
func validateConnackLimits(v protocol.Version, ack *protocol.ConnackPacket) error {
	if v != protocol.V50 || ack.Properties == nil {
		return nil
	}
	p := ack.Properties
	if p.ReceiveMaximum != nil && *p.ReceiveMaximum == 0 {
		return fmt.Errorf("mqtt/tcp: %w: broker CONNACK Receive Maximum is 0", protocol.ErrProtocolViolation)
	}
	if p.MaximumPacketSize != nil && *p.MaximumPacketSize == 0 {
		return fmt.Errorf("mqtt/tcp: %w: broker CONNACK Maximum Packet Size is 0", protocol.ErrProtocolViolation)
	}
	return nil
}

// buildConnectResult decodes the negotiated session state from the CONNACK,
// filling in the spec defaults for every limit the broker left unset.
func (c *TCPClient) buildConnectResult(ack *protocol.ConnackPacket) ConnectResult {
	res := ConnectResult{
		SessionPresent:  ack.SessionPresent,
		ReasonCode:      ack.ReasonCode,
		ReceiveMaximum:  defaultReceiveMaximum,
		MaximumQoS:      QoS2,
		RetainAvailable: true,
	}
	p := ack.Properties
	if p == nil {
		return res
	}
	res.AssignedClientID = p.AssignedClientID
	if p.ServerKeepAlive != nil {
		res.ServerKeepAlive = time.Duration(*p.ServerKeepAlive) * time.Second
	}
	if p.ReceiveMaximum != nil {
		res.ReceiveMaximum = *p.ReceiveMaximum
	}
	if p.MaximumQoS != nil {
		res.MaximumQoS = QoS(*p.MaximumQoS)
	}
	if p.RetainAvailable != nil {
		res.RetainAvailable = *p.RetainAvailable != 0
	}
	if p.MaximumPacketSize != nil {
		res.MaximumPacketSize = *p.MaximumPacketSize
	}
	if p.TopicAliasMaximum != nil {
		res.TopicAliasMaximum = *p.TopicAliasMaximum
	}
	res.UserProperties = p.UserProperties
	return res
}

// effectivePingInterval is the interval between PINGREQs. A broker Server
// Keep Alive wins even below the client floor (spec §3.2.2.3.15 MUST);
// otherwise it is half the requested keep-alive. The package-test override
// takes precedence over both.
func (c *TCPClient) effectivePingInterval(result ConnectResult) time.Duration {
	if c.pingInterval > 0 {
		return c.pingInterval
	}
	ka := c.cfg.KeepAlive
	if result.ServerKeepAlive > 0 {
		ka = result.ServerKeepAlive
	}
	return ka / 2
}

// reasonStringOf extracts the optional Reason String property, empty when
// absent.
func reasonStringOf(p *protocol.Properties) string {
	if p == nil {
		return ""
	}
	return p.ReasonString
}

// dial opens the TCP (optionally TLS) connection for u, defaulting the port
// to 1883 (plain) or 8883 (TLS) when the URL omits it.
func (c *TCPClient) dial(ctx context.Context, u *url.URL) (net.Conn, error) {
	host := u.Host
	tlsScheme := false
	switch u.Scheme {
	case "tls", "ssl", "mqtts":
		tlsScheme = true
	case "tcp", "mqtt", "":
	default:
		return nil, fmt.Errorf("mqtt/tcp: unsupported scheme %q", u.Scheme)
	}
	if u.Port() == "" {
		port := "1883"
		if tlsScheme {
			port = "8883"
		}
		host = net.JoinHostPort(u.Hostname(), port)
	}

	if !tlsScheme && c.cfg.TLSConfig != nil {
		// The scheme selects the transport, so the configured TLSConfig is
		// unused and the connection — including the CONNECT credentials —
		// crosses the wire in plaintext. Make the trust downgrade visible
		// instead of silently ignoring an explicit security configuration.
		c.logger.Warn("mqtt.tcp.tls_config_ignored",
			slog.String("scheme", u.Scheme),
			slog.String("hint", "use tls://, ssl:// or mqtts:// to enable TLS"))
	}

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}
	if !tlsScheme {
		return conn, nil
	}

	// Clone the caller's config so per-connection adjustments never mutate
	// shared state across reconnects; Clone(nil) is nil, covering the
	// build-a-default case. An empty ServerName is filled from the broker
	// URL: tls.Client does not infer it from the dialed address, and the
	// natural CA-pinning config (RootCAs only) would otherwise fail every
	// handshake with an error that steers operators toward
	// InsecureSkipVerify — the exact trap NewClientTLSConfig documents as
	// closed. With InsecureSkipVerify set this still restores SNI for
	// virtual-hosting brokers, with no verification downside.
	tlsCfg := c.cfg.TLSConfig.Clone()
	if tlsCfg == nil {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if tlsCfg.ServerName == "" {
		tlsCfg.ServerName = u.Hostname()
	}
	tlsConn := tls.Client(conn, tlsCfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tlsConn, nil
}

// isStopping reports whether the link has begun tearing down, so the read
// and keep-alive loops can suppress warnings for the expected close.
func isStopping(l *link) bool {
	select {
	case <-l.stop:
		return true
	default:
		return false
	}
}
