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

	// TLSConfig is used for tls:// endpoints. When nil a minimal config
	// verifying the broker hostname is built automatically.
	TLSConfig *tls.Config
	// Will is the optional Last Will and Testament published by the broker
	// if the connection drops without a clean DISCONNECT.
	Will *Will
	// Logger receives structured diagnostics. Defaults to [slog.Default].
	Logger *slog.Logger
}

// subscription is one registered filter: the wire options it was
// subscribed with (replayed verbatim on reconnect so the delivery
// guarantee is preserved) and the handler inbound matches route to.
type subscription struct {
	filter  string
	handler MessageHandler
	options protocol.SubscribeOptions
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
//	quota.mu → ids.mu → store.mu → waitersMu → link.sendMu
//
// In practice these critical sections do not nest: the read loop signals a
// waiter (waitersMu) and then, separately, writes a reply (sendMu); the
// publish path acquires quota, then an id, saves to the store, registers a
// waiter, and only then writes. link.sendMu is a leaf — nothing else is
// acquired while it is held. subsMu (the subscription slice) is
// independent of all the above; the read loop copies the matching handlers
// out from under it before invoking them.
type TCPClient struct {
	cfg     TCPConfig
	logger  *slog.Logger
	version protocol.Version

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

	waitersMu sync.Mutex
	waiters   map[uint16]chan ackResult

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
		waiters:    make(map[uint16]chan ackResult),
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
// so [Lifecycle] treats it as idempotent.
func (c *TCPClient) Connect(ctx context.Context) error {
	if c.link.Load() != nil {
		return fmt.Errorf("mqtt/tcp: %w", ErrAlreadyConnected)
	}

	u, err := url.Parse(c.cfg.BrokerURL)
	if err != nil {
		return fmt.Errorf("mqtt/tcp: bad broker url: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
	defer cancel()
	conn, err := c.dial(dialCtx, u)
	if err != nil {
		return fmt.Errorf("mqtt/tcp: dial: %w", err)
	}

	bw := bufio.NewWriter(conn)
	if err := c.buildConnectPacket().Encode(bw); err != nil {
		_ = conn.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: send connect: %w", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(c.cfg.DialTimeout))
	br := bufio.NewReader(conn)
	frame, err := protocol.ReadFrame(br, c.inboundMax)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: read connack: %w", err)
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
	_ = conn.SetReadDeadline(time.Time{})

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
	c.link.Store(l)
	now := time.Now()
	c.connectedAt.Store(&now)

	c.replaySession(l)
	c.replaySubscriptions(l)

	l.wg.Add(2)
	go c.readLoop(l)
	go c.keepAliveLoop(l)

	c.logger.Info("mqtt.tcp.connected",
		slog.String("broker", c.cfg.BrokerURL),
		slog.String("version", c.version.String()),
		slog.Bool("session_present", result.SessionPresent))
	return nil
}

// applySession decides whether the resumed session state survives this
// connect. A clean start, a zero MQTT 5.0 Session Expiry, or a broker that
// did not resume the session (Session Present = 0) all discard the stored
// QoS>0 state, free the packet identifiers and resize the send quota from
// scratch. Otherwise the store and identifiers are kept for replay and
// only the quota is resized to the freshly negotiated ceiling.
func (c *TCPClient) applySession(result ConnectResult) {
	reset := c.cfg.CleanStart ||
		(c.version == protocol.V50 && c.cfg.SessionExpirySeconds == 0) ||
		!result.SessionPresent
	if reset {
		_ = c.store.Reset()
		c.ids.Reset()
	}
	c.quota.reset(c.quotaSize(result))
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
		id, err := c.ids.Acquire()
		if err != nil {
			c.logger.Warn("mqtt.tcp.resubscribe", slog.String("filter", s.filter), slog.String("err", err.Error()))
			continue
		}
		pkt := &protocol.SubscribePacket{
			Version:       c.version,
			PacketID:      id,
			Subscriptions: []protocol.Subscription{{Filter: s.filter, Options: s.options}},
		}
		ch := c.registerWaiter(id)
		if err := c.writeFrame(l, pkt.Encode); err != nil {
			c.removeWaiter(id)
			c.ids.Release(id)
			c.logger.Warn("mqtt.tcp.resubscribe", slog.String("filter", s.filter), slog.String("err", err.Error()))
			continue
		}
		l.wg.Add(1)
		go c.awaitResubscribe(l, id, s.filter, ch)
	}
}

// awaitResubscribe consumes the SUBACK for a replayed subscription and logs
// a rejection, then frees the identifier. A connection drop resolves it via
// the failed waiter or the link's stop channel.
func (c *TCPClient) awaitResubscribe(l *link, id uint16, filter string, ch chan ackResult) {
	defer l.wg.Done()
	var res ackResult
	select {
	case res = <-ch:
	case <-l.stop:
		c.removeWaiter(id)
		c.ids.Release(id)
		return
	}
	c.ids.Release(id)
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
// bounded by ctx — for the read and keep-alive loops to exit.
func (c *TCPClient) Disconnect(ctx context.Context) error {
	l := c.link.Load()
	if l == nil {
		return nil
	}
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
func (c *TCPClient) writeFrame(l *link, encode func(io.Writer) error) error {
	if l == nil {
		return ErrNotConnected
	}
	l.sendMu.Lock()
	defer l.sendMu.Unlock()
	l.buf.Reset()
	if err := encode(&l.buf); err != nil {
		return err
	}
	if l.outboundMax != 0 && uint32(l.buf.Len()) > l.outboundMax { //nolint:gosec // buf length is non-negative
		return ErrPacketTooLarge
	}
	if _, err := l.w.Write(l.buf.Bytes()); err != nil {
		return err
	}
	return l.w.Flush()
}

// teardownLink closes a connection exactly once. It stops the loops, closes
// the socket, clears the link pointer if it is still current, fails every
// in-flight waiter with [ErrConnectionLost] and marks the quota failed so
// parked sends unblock immediately. The stored session state, inbound
// dedup set and in-flight identifiers are deliberately KEPT so a resumed
// session can complete them. When graceful is false (a detected drop, not a
// caller Disconnect) it signals the connection-lost channel so a lifecycle
// loop reconnects.
func (c *TCPClient) teardownLink(l *link, graceful bool) {
	l.closeOnce.Do(func() {
		close(l.stop)
		_ = l.conn.Close()
		c.link.CompareAndSwap(l, nil)
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
// blocked Publish/Subscribe caller.
func (c *TCPClient) registerWaiter(id uint16) chan ackResult {
	ch := make(chan ackResult, 1)
	c.waitersMu.Lock()
	c.waiters[id] = ch
	c.waitersMu.Unlock()
	return ch
}

// removeWaiter drops the waiter for id. Safe to call for an already-removed
// id.
func (c *TCPClient) removeWaiter(id uint16) {
	c.waitersMu.Lock()
	delete(c.waiters, id)
	c.waitersMu.Unlock()
}

// signalWaiter delivers res to the waiter for id (removing it) and reports
// whether one was registered. The send is non-blocking: the channel is
// buffered and each waiter is signalled at most once.
func (c *TCPClient) signalWaiter(id uint16, res ackResult) bool {
	c.waitersMu.Lock()
	ch, ok := c.waiters[id]
	if ok {
		delete(c.waiters, id)
	}
	c.waitersMu.Unlock()
	if ok {
		select {
		case ch <- res:
		default:
		}
	}
	return ok
}

// failAllWaiters resolves every outstanding waiter with [ErrConnectionLost]
// and clears the table. Called from teardownLink so no Publish/Subscribe
// parks forever on a dead socket.
func (c *TCPClient) failAllWaiters() {
	c.waitersMu.Lock()
	for id, ch := range c.waiters {
		select {
		case ch <- ackResult{err: ErrConnectionLost}:
		default:
		}
		delete(c.waiters, id)
	}
	c.waitersMu.Unlock()
}

// addSubscription registers or replaces (in place, preserving order) the
// handler and wire options for filter.
func (c *TCPClient) addSubscription(filter string, options protocol.SubscribeOptions, handler MessageHandler) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for i := range c.subs {
		if c.subs[i].filter == filter {
			c.subs[i].options = options
			c.subs[i].handler = handler
			return
		}
	}
	c.subs = append(c.subs, subscription{filter: filter, handler: handler, options: options})
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

// restoreSubscription undoes a provisional addSubscription after a failed
// SUBSCRIBE: it reinstates the previous registration when one existed
// (in place, preserving order) or removes the entry entirely.
func (c *TCPClient) restoreSubscription(filter string, prev subscription, existed bool) {
	if existed {
		c.addSubscription(prev.filter, prev.options, prev.handler)
		return
	}
	c.removeSubscription(filter)
}

// removeSubscription drops the registration for filter, if any.
func (c *TCPClient) removeSubscription(filter string) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for i := range c.subs {
		if c.subs[i].filter == filter {
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

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}
	if !tlsScheme {
		return conn, nil
	}

	tlsCfg := c.cfg.TLSConfig
	if tlsCfg == nil {
		tlsCfg = &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}
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
