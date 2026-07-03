// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

// Package mqtt provides the MQTT transport for the daemon: a TCP/TLS
// adapter, publish/subscribe plumbing, and a reconnecting lifecycle
// around an inverter-to-broker connection.
package mqtt

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
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

// TCPConfig wires a [TCPClient] against a real broker.
type TCPConfig struct {
	BrokerURL    string // tcp://host:1883 or tls://host:8883
	ClientID     string
	Username     string
	Password     string
	KeepAlive    time.Duration // floor: 30s per SPEC §18.1
	DialTimeout  time.Duration // default 10s
	AckTimeout   time.Duration // PUBACK wait, default 20s
	TLSConfig    *tls.Config
	WillTopic    string
	WillPayload  []byte
	WillRetain   bool
	CleanSession bool
	Logger       *slog.Logger
}

// subscriberEntry stores the handler and the QoS level a filter was
// originally subscribed at. The QoS is replayed on reconnect so every
// filter is restored at the same delivery guarantee the caller
// requested, rather than being silently upgraded/downgraded to a fixed
// level.
type subscriberEntry struct {
	handler MessageHandler
	qos     QoS
}

// TCPClient is a pure-Go MQTT 3.1.1 client used by the Bridge's
// Lifecycle.
//
// It implements both [Client] (Publish + Subscribe/Unsubscribe) and
// [Connector] (Connect + Disconnect) so the bridge composes one
// object for the full lifecycle.
type TCPClient struct {
	cfg    TCPConfig
	logger *slog.Logger

	mu     sync.Mutex
	conn   net.Conn
	writer *bufio.Writer
	reader *bufio.Reader

	nextID atomic.Uint32

	ackMu sync.Mutex
	acks  map[uint16]chan struct{}

	subMu       sync.RWMutex
	subscribers map[string]subscriberEntry

	sendMu sync.Mutex // serialises frame writes

	// pingInterval is how often keepAliveLoop sends a PINGREQ; it also
	// bounds the PINGRESP watchdog window. Defaults to KeepAlive/2 in
	// NewTCPClient; tests override it directly to avoid the 30s floor.
	pingInterval time.Duration
	// outstandingPings counts PINGREQs sent since the last PINGRESP was
	// observed: keepAliveLoop increments it after each ping, readLoop
	// resets it to 0 on any PINGRESP. The socket is declared dead only
	// once it reaches pingTimeoutThreshold — i.e. two consecutive pings
	// (≈ one full KeepAlive at the default pingInterval) go unanswered —
	// so a single delayed or dropped PINGRESP (a GC pause, a scheduler
	// stall on a CPU-throttled host, a momentary network blip) does not
	// trip a spurious `ping_timeout` + reconnect. A genuinely half-open
	// socket is still detected, just one pingInterval later.
	outstandingPings atomic.Int32

	stop    chan struct{}
	stopped atomic.Bool
	wg      sync.WaitGroup

	// connectedAt holds the wall-clock instant of the most recent
	// successful TCP+CONNECT round-trip. Cleared on Disconnect.
	// Read by the diagnostics health probe; never the hot path.
	connectedAt atomic.Pointer[time.Time]

	// lostCh is signalled (non-blocking, buffered) when the read or
	// keep-alive loop detects the connection dropped, so an event-driven
	// reconnect loop can react immediately instead of polling. Consumers
	// that don't read it incur a single harmless buffered send.
	lostCh chan struct{}
}

// ConnectionLost returns a channel that receives a value whenever the client
// detects its connection dropped (read/keep-alive failure). Buffered (size 1)
// so a drop is never missed; drained by the consumer's reconnect loop.
func (c *TCPClient) ConnectionLost() <-chan struct{} { return c.lostCh }

// IsConnected reports whether the client currently holds an active
// MQTT session. Used by the diagnostics health probe to derive the
// `mqtt` health component.
func (c *TCPClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil && !c.stopped.Load()
}

// LastConnectedAt returns the timestamp of the most recent successful
// connect, or the zero time when no connect has happened yet.
func (c *TCPClient) LastConnectedAt() time.Time {
	p := c.connectedAt.Load()
	if p == nil {
		return time.Time{}
	}
	return *p
}

// NewTCPClient constructs a new client.
func NewTCPClient(cfg TCPConfig) *TCPClient {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.KeepAlive < 30*time.Second {
		cfg.KeepAlive = 30 * time.Second
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = 20 * time.Second
	}
	return &TCPClient{
		cfg:          cfg,
		logger:       cfg.Logger,
		acks:         make(map[uint16]chan struct{}),
		subscribers:  make(map[string]subscriberEntry),
		stop:         make(chan struct{}),
		lostCh:       make(chan struct{}, 1),
		pingInterval: cfg.KeepAlive / 2,
	}
}

// Connect implements [Connector]. It dials, sends CONNECT, waits for
// CONNACK, and starts the read pump + keep-alive loop.
func (c *TCPClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.conn != nil {
		c.mu.Unlock()
		return errors.New("mqtt/tcp: already connected")
	}
	c.mu.Unlock()

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

	pkt := &protocol.ConnectPacket{
		ClientID:     c.cfg.ClientID,
		KeepAlive:    uint16(c.cfg.KeepAlive.Seconds()), //nolint:gosec // clamped above
		Username:     c.cfg.Username,
		Password:     c.cfg.Password,
		CleanSession: c.cfg.CleanSession,
		WillTopic:    c.cfg.WillTopic,
		WillPayload:  c.cfg.WillPayload,
		WillRetain:   c.cfg.WillRetain,
	}
	bw := bufio.NewWriter(conn)
	if err := pkt.Encode(bw); err != nil {
		_ = conn.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = conn.Close()
		return err
	}

	_ = conn.SetReadDeadline(time.Now().Add(c.cfg.DialTimeout))
	br := bufio.NewReader(conn)
	frame, err := protocol.ReadFrame(br)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: read connack: %w", err)
	}
	if frame.PacketType() != protocol.PacketConnack {
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: unexpected packet %d instead of CONNACK", frame.PacketType())
	}
	ack, err := protocol.DecodeConnack(frame.Body)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if ack.ReturnCode != 0 {
		_ = conn.Close()
		return fmt.Errorf("mqtt/tcp: CONNACK return code %d", ack.ReturnCode)
	}
	_ = conn.SetReadDeadline(time.Time{})

	c.mu.Lock()
	c.conn = conn
	c.writer = bw
	c.reader = br
	c.stop = make(chan struct{})
	stopCh := c.stop
	c.stopped.Store(false)
	c.outstandingPings.Store(0)
	c.mu.Unlock()
	now := time.Now()
	c.connectedAt.Store(&now)

	c.wg.Add(2)
	go c.readLoop(stopCh)
	go c.keepAliveLoop(stopCh)

	// Replay any prior subscriptions on reconnect. Without this, a
	// CleanSession=true client (the typical daemon configuration)
	// loses every SUBSCRIBE on the previous socket and the new
	// session starts with an empty filter set — HA's
	// `set_temperature` / `set_mode` / `set_profile` commands
	// arrive at the broker but are never delivered to the daemon.
	c.subMu.RLock()
	type filterQoS struct {
		filter string
		qos    QoS
	}
	subs := make([]filterQoS, 0, len(c.subscribers))
	for f, entry := range c.subscribers {
		subs = append(subs, filterQoS{filter: f, qos: entry.qos})
	}
	c.subMu.RUnlock()
	for _, s := range subs {
		pkt := &protocol.SubscribePacket{PacketID: c.nextPacketID(), TopicFilter: s.filter, QoS: byte(s.qos)}
		if err := c.writeFrame(pkt); err != nil {
			c.logger.Warn("mqtt.tcp.resubscribe",
				slog.String("filter", s.filter),
				slog.String("err", err.Error()))
		}
	}

	c.logger.Info("mqtt.tcp.connected", slog.String("broker", c.cfg.BrokerURL))
	return nil
}

// Disconnect implements [Connector]. It sends DISCONNECT, closes the
// socket, and waits for the goroutines to exit.
func (c *TCPClient) Disconnect(ctx context.Context) error {
	c.mu.Lock()
	conn := c.conn
	if conn == nil {
		c.mu.Unlock()
		return nil
	}
	if c.stopped.CompareAndSwap(false, true) {
		close(c.stop)
	}
	c.conn = nil
	c.mu.Unlock()

	// Best effort: a graceful DISCONNECT.
	c.sendMu.Lock()
	_ = protocol.EncodeDisconnect(c.writer)
	_ = c.writer.Flush()
	c.sendMu.Unlock()
	_ = conn.Close()

	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	c.logger.Info("mqtt.tcp.disconnected")
	return nil
}

// Publish implements [Publisher]. QoS 0 is fire-and-forget; QoS 1
// waits for PUBACK up to cfg.AckTimeout.
func (c *TCPClient) Publish(ctx context.Context, topic string, payload []byte, qos QoS, retain bool) error {
	if qos > QoS1 {
		return fmt.Errorf("mqtt/tcp: unsupported QoS %d", qos)
	}
	pkt := &protocol.PublishPacket{Topic: topic, Payload: payload, QoS: byte(qos), Retain: retain}
	if qos == 0 {
		return c.writeFrame(pkt)
	}

	pkt.PacketID = c.nextPacketID()
	// Register the ack channel BEFORE the PUBLISH hits the wire —
	// otherwise the broker can answer faster than the registration
	// runs (loopback paths do this routinely) and the PUBACK
	// arrives at the read loop while c.acks is still empty.
	ch := make(chan struct{})
	c.ackMu.Lock()
	c.acks[pkt.PacketID] = ch
	c.ackMu.Unlock()
	defer c.removeAck(pkt.PacketID)

	if err := c.writeFrame(pkt); err != nil {
		return err
	}

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.cfg.AckTimeout):
		return fmt.Errorf("mqtt/tcp: PUBACK timeout (id=%d)", pkt.PacketID)
	}
}

// Subscribe implements [Subscriber]. Only one handler per topic
// filter — re-subscribing replaces the previous handler.
//
// See [MessageHandler] for the non-blocking contract handler must
// satisfy: it runs synchronously in the read loop, so a slow handler
// delays PUBACK/PINGRESP handling and can trip the keep-alive watchdog.
func (c *TCPClient) Subscribe(ctx context.Context, filter string, qos QoS, handler MessageHandler) error {
	pkt := &protocol.SubscribePacket{PacketID: c.nextPacketID(), TopicFilter: filter, QoS: byte(qos)}
	if err := c.writeFrame(pkt); err != nil {
		return err
	}
	c.subMu.Lock()
	c.subscribers[filter] = subscriberEntry{handler: handler, qos: qos}
	c.subMu.Unlock()
	_ = ctx
	return nil
}

// Unsubscribe implements [Subscriber].
func (c *TCPClient) Unsubscribe(ctx context.Context, filter string) error {
	pkt := &protocol.UnsubscribePacket{PacketID: c.nextPacketID(), TopicFilter: filter}
	if err := c.writeFrame(pkt); err != nil {
		return err
	}
	c.subMu.Lock()
	delete(c.subscribers, filter)
	c.subMu.Unlock()
	_ = ctx
	return nil
}

// --- internals ---

func (c *TCPClient) dial(ctx context.Context, u *url.URL) (net.Conn, error) {
	host := u.Host
	if u.Port() == "" {
		switch u.Scheme {
		case "tls", "ssl", "mqtts":
			host = net.JoinHostPort(u.Hostname(), "8883")
		default:
			host = net.JoinHostPort(u.Hostname(), "1883")
		}
	}
	dialer := &net.Dialer{}
	switch u.Scheme {
	case "tcp", "mqtt", "":
		return dialer.DialContext(ctx, "tcp", host)
	case "tls", "ssl", "mqtts":
		tcpConn, err := dialer.DialContext(ctx, "tcp", host)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(tcpConn, c.cfg.TLSConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = tcpConn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	return nil, fmt.Errorf("mqtt/tcp: unsupported scheme %q", u.Scheme)
}

type frameEncoder interface {
	Encode(w io.Writer) error
}

func (c *TCPClient) writeFrame(pkt frameEncoder) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.Lock()
	writer := c.writer
	c.mu.Unlock()
	if writer == nil {
		return errors.New("mqtt/tcp: not connected")
	}
	if err := pkt.Encode(writer); err != nil {
		return err
	}
	return writer.Flush()
}

func (c *TCPClient) nextPacketID() uint16 {
	for {
		v := c.nextID.Add(1)
		id := uint16(v & 0xFFFF) //nolint:gosec // ringed at 16-bit on purpose
		if id == 0 {
			continue
		}
		return id
	}
}

func (c *TCPClient) removeAck(id uint16) {
	c.ackMu.Lock()
	delete(c.acks, id)
	c.ackMu.Unlock()
}

func (c *TCPClient) readLoop(stop <-chan struct{}) {
	defer c.wg.Done()
	for {
		select {
		case <-stop:
			return
		default:
		}
		c.mu.Lock()
		reader := c.reader
		c.mu.Unlock()
		if reader == nil {
			return
		}
		frame, err := protocol.ReadFrame(reader)
		if err != nil {
			if !c.stopped.Load() {
				c.logger.Warn("mqtt.tcp.read", slog.String("err", err.Error()))
				// Tear down the broken socket so the lifecycle's
				// reconnect loop can establish a fresh connection.
				// Without this, `c.conn` stays non-nil after the
				// remote side closes the TCP socket, the next
				// [Connect] returns `mqtt/tcp: already connected`,
				// and the daemon's subscriptions silently die —
				// HA's `set_temperature` / `set_mode` /
				// `set_profile` MQTT commands stop reaching the
				// service-method handler.
				c.handleConnectionLost()
			}
			return
		}
		switch frame.PacketType() { //nolint:exhaustive // outbound-only packet types never reach the read path
		case protocol.PacketPublish:
			ib, err := protocol.DecodePublish(frame.Header, frame.Body)
			if err != nil {
				c.logger.Warn("mqtt.tcp.malformed_publish", slog.String("err", err.Error()))
				continue
			}
			c.dispatch(ib)
			if ib.QoS == 1 {
				c.sendMu.Lock()
				_ = protocol.EncodePuback(c.writer, ib.PacketID)
				_ = c.writer.Flush()
				c.sendMu.Unlock()
			}
		case protocol.PacketPuback:
			if p, err := protocol.DecodePuback(frame.Body); err == nil {
				c.ackMu.Lock()
				if ch, ok := c.acks[p.PacketID]; ok {
					close(ch)
					delete(c.acks, p.PacketID)
				}
				c.ackMu.Unlock()
			}
		case protocol.PacketPingresp:
			// Heartbeat ack: the broker is alive, so reset the
			// outstanding-ping counter keepAliveLoop bumps per PINGREQ.
			c.outstandingPings.Store(0)
		case protocol.PacketSuback:
			// Subscribe/unsubscribe calls return as soon as the frame
			// is on the wire (non-blocking in our MVP — no caller
			// waits on this SUBACK), but a rejected filter is still
			// worth surfacing: without this, HA's `set_temperature` /
			// `set_mode` / `set_profile` command topics could silently
			// never be delivered because the broker refused the
			// subscription (bad ACL, disallowed filter, ...) and
			// nothing would ever say so.
			sub, err := protocol.DecodeSuback(frame.Body)
			if err != nil {
				c.logger.Warn("mqtt.tcp.malformed_suback", slog.String("err", err.Error()))
				continue
			}
			for _, rc := range sub.ReturnCodes {
				if rc == protocol.SubackFailure {
					c.logger.Warn("mqtt.tcp.subscribe_rejected",
						slog.Uint64("packet_id", uint64(sub.PacketID)))
					break
				}
			}
		case protocol.PacketUnsuback:
			// non-blocking in our MVP; the unsubscribe call returns as
			// soon as the frame is on the wire.
		}
	}
}

// pingTimeoutThreshold is how many consecutive unanswered PINGREQs the
// keep-alive watchdog tolerates before declaring the socket dead. Two
// (≈ one full KeepAlive at the default pingInterval of KeepAlive/2)
// rides out a single delayed or dropped PINGRESP — a GC pause, a
// scheduler stall on a CPU-throttled host, a momentary network blip —
// without a spurious `ping_timeout` + reconnect, while still catching a
// genuinely half-open socket within 2×pingInterval.
const pingTimeoutThreshold = 2

func (c *TCPClient) keepAliveLoop(stop <-chan struct{}) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// Watchdog: PINGREQs from previous ticks that never drew a
			// PINGRESP mean the socket is half-open — the broker or
			// network vanished without a TCP FIN/RST, so readLoop stays
			// blocked in ReadFrame forever and never trips
			// handleConnectionLost. Once pingTimeoutThreshold pings go
			// unanswered, declare the connection lost so the lifecycle
			// reconnects. Without this, publishes silently time out on a
			// dead socket until a manual restart.
			if c.outstandingPings.Load() >= pingTimeoutThreshold {
				c.logger.Warn("mqtt.tcp.ping_timeout")
				c.handleConnectionLost()
				return
			}
			c.sendMu.Lock()
			c.mu.Lock()
			writer := c.writer
			c.mu.Unlock()
			if writer == nil {
				c.sendMu.Unlock()
				return
			}
			if err := protocol.EncodePingReq(writer); err != nil {
				c.sendMu.Unlock()
				c.logger.Warn("mqtt.tcp.ping", slog.String("err", err.Error()))
				c.handleConnectionLost()
				return
			}
			if err := writer.Flush(); err != nil {
				c.sendMu.Unlock()
				c.logger.Warn("mqtt.tcp.ping", slog.String("err", err.Error()))
				c.handleConnectionLost()
				return
			}
			// Count this ping as outstanding only after it is on the
			// wire; a PINGRESP resets the counter to 0.
			c.outstandingPings.Add(1)
			c.sendMu.Unlock()
		}
	}
}

// handleConnectionLost tears down the in-flight TCP socket after the
// read or keep-alive loop has detected a remote-side close. Resets
// `c.conn` / `c.reader` / `c.writer` to nil so the next
// [TCPClient.Connect] call dials a fresh socket instead of returning
// `mqtt/tcp: already connected`. Idempotent — concurrent callers
// converge on the same nil-state.
//
// Without this, a connection lost mid-flight (broker restart, NAT
// timeout, transient network glitch) leaves the lifecycle's
// reconnect loop spinning on `already connected` errors forever:
// the daemon's MQTT subscriptions silently expire and HA's
// `set_temperature` / `set_mode` / `set_profile` commands stop
// being delivered.
func (c *TCPClient) handleConnectionLost() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.reader = nil
	c.writer = nil
	if c.stopped.CompareAndSwap(false, true) {
		close(c.stop)
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	// Wake any event-driven reconnect loop (non-blocking; coalesces).
	select {
	case c.lostCh <- struct{}{}:
	default:
	}
}

// dispatch routes an inbound PUBLISH to its matching [MessageHandler].
//
// It runs synchronously, inline in [TCPClient.readLoop] — the same
// goroutine that also decodes PUBACK/PINGRESP and feeds the PINGRESP
// watchdog in keepAliveLoop. A handler that blocks (network calls,
// waiting on a channel, heavy computation, ...) therefore stalls the
// whole read pump: PUBACKs stop being processed, PINGRESPs stop being
// observed, and the keep-alive watchdog can then declare the
// connection lost (`mqtt.tcp.ping_timeout`) even though the socket is
// perfectly healthy. See [MessageHandler] for the contract this
// implies for every Subscribe callback.
func (c *TCPClient) dispatch(ib *protocol.InboundPublish) {
	c.subMu.RLock()
	var handler MessageHandler
	for filter, entry := range c.subscribers {
		if topicMatches(filter, ib.Topic) {
			handler = entry.handler
			break
		}
	}
	c.subMu.RUnlock()
	if handler != nil {
		handler(ib.Topic, ib.Payload, ib.Retain)
	}
}

// topicMatches implements the minimal MQTT wildcard rules the
// bridge relies on: `+` matches one level, `#` matches multiple.
func topicMatches(filter, topic string) bool {
	if filter == topic {
		return true
	}
	fp, tp := 0, 0
	for fp < len(filter) && tp < len(topic) {
		fc, tc := filter[fp], topic[tp]
		switch fc {
		case '#':
			return true
		case '+':
			for tp < len(topic) && topic[tp] != '/' {
				tp++
			}
			fp++
		default:
			if fc != tc {
				return false
			}
			fp++
			tp++
		}
	}
	return fp == len(filter) && tp == len(topic)
}

// Confirm TCPClient satisfies both bridge contracts at compile time.
var (
	_ Client    = (*TCPClient)(nil)
	_ Connector = (*TCPClient)(nil)
)
