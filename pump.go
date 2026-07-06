// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"log/slog"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// pingTimeoutThreshold is how many consecutive unanswered PINGREQs the
// keep-alive watchdog tolerates before declaring the socket dead. Two
// (≈ one full keep-alive at the default ping interval of KeepAlive/2) rides
// out a single delayed or dropped PINGRESP — a GC pause, a scheduler stall
// on a CPU-throttled host, a momentary network blip — without a spurious
// ping_timeout + reconnect, while still catching a genuinely half-open
// socket within 2×pingInterval.
const pingTimeoutThreshold = 2

// readLoop decodes inbound frames until the socket errors or the link
// stops. It runs the single synchronous dispatch goroutine: inbound PUBLISH
// handlers, PUBACK/PUBREC/PUBCOMP session transitions, SUBACK/UNSUBACK
// waiter signalling, the PINGRESP watchdog reset, and the reaction to a
// broker DISCONNECT/AUTH all happen here, in order.
func (c *TCPClient) readLoop(l *link) {
	defer l.wg.Done()
	for {
		frame, err := protocol.ReadFrame(l.r, l.inboundMax)
		if err != nil {
			if !isStopping(l) {
				c.logger.Warn("mqtt.tcp.read", slog.String("err", err.Error()))
			}
			c.teardownLink(l, false)
			return
		}
		if isStopping(l) {
			// The link was torn down (keep-alive watchdog, server DISCONNECT,
			// caller Disconnect) while this frame was in flight — and a
			// reconnect may already have swapped in a new link that shares
			// c.ids, c.quota and c.store. A bufio.Reader can still surface a
			// pipelined PUBACK/PUBCOMP from the now-dead socket here; handling
			// it would run completeOutbound/handlePubrec against the NEW link's
			// allocator and quota (double-freeing a reused packet id,
			// over-crediting the send quota past Receive Maximum). Drop it and
			// exit — teardownLink has already run for this link.
			return
		}
		if err := frame.ValidateFlags(); err != nil {
			c.logger.Warn("mqtt.tcp.malformed_frame", slog.String("err", err.Error()))
			c.protocolError(l, protocol.MalformedPacketReason)
			return
		}
		if !c.handleFrame(l, frame) {
			return
		}
	}
}

// handleFrame dispatches one decoded frame. It returns false when the read
// loop must exit (the connection was torn down).
func (c *TCPClient) handleFrame(l *link, frame protocol.Frame) bool {
	switch frame.PacketType() {
	case protocol.Publish:
		return c.handlePublish(l, frame)
	case protocol.Puback:
		return c.handleAck(l, frame, protocol.Puback)
	case protocol.Pubrec:
		return c.handleAck(l, frame, protocol.Pubrec)
	case protocol.Pubrel:
		return c.handleInboundPubrel(l, frame)
	case protocol.Pubcomp:
		return c.handleAck(l, frame, protocol.Pubcomp)
	case protocol.Suback:
		return c.handleSuback(l, frame)
	case protocol.Unsuback:
		return c.handleUnsuback(l, frame)
	case protocol.Pingresp:
		l.outstandingPings.Store(0)
	case protocol.Disconnect:
		c.handleServerDisconnect(l, frame)
		return false
	case protocol.Auth:
		// This client does not participate in enhanced authentication; a
		// server-initiated AUTH is a protocol error here.
		c.logger.Warn("mqtt.tcp.unexpected_auth")
		c.protocolError(l, protocol.ProtocolErrorReason)
		return false
	default:
		// CONNECT/CONNACK/SUBSCRIBE/UNSUBSCRIBE/PINGREQ are client→server
		// only; receiving one means the peer is confused.
		c.logger.Warn("mqtt.tcp.unexpected_packet", slog.String("type", frame.PacketType().String()))
	}
	return true
}

// handlePublish decodes an inbound PUBLISH, resolves any topic alias,
// dispatches it to matching handlers and drives the receiver-side QoS
// handshake. It returns false when a topic-alias violation forced the
// connection down.
func (c *TCPClient) handlePublish(l *link, frame protocol.Frame) bool {
	pub, err := protocol.DecodePublish(c.version, frame.Header, frame.Body)
	if err != nil {
		// A Malformed Packet is fatal (§4.13.1): close the connection with
		// reason 0x81 rather than logging and reading on. Swallowing it would
		// leave a QoS 1/2 malformed PUBLISH unacknowledged (its packet id was
		// never decoded), so a broker that keeps it unacked retransmits it
		// forever — a warn-log livelock instead of a clean teardown. This
		// mirrors the topic-alias-violation path below.
		c.logger.Warn("mqtt.tcp.malformed_publish", slog.String("err", err.Error()))
		c.protocolError(l, protocol.MalformedPacketReason)
		return false
	}

	topic, ok := c.resolveTopicAlias(l, pub)
	if !ok {
		c.protocolError(l, protocol.TopicAliasInvalid)
		return false
	}
	pub.Topic = topic

	switch pub.QoS {
	case 0:
		c.dispatch(toMessage(pub))
	case 1:
		c.dispatch(toMessage(pub))
		// dispatch runs handlers synchronously and a handler may block
		// across a teardown + reconnect; do not ack on a link that has
		// since died (the write would fail anyway, this just skips the
		// noise).
		if isStopping(l) {
			return false
		}
		c.sendAck(l, protocol.Puback, pub.PacketID)
	case 2:
		return c.handleInboundQoS2(l, pub)
	}
	return true
}

// resolveTopicAlias applies MQTT 5.0 inbound topic aliasing (§3.3.2.3.4).
// A first publish on an alias registers topic→alias; a later publish with
// an empty topic resolves through the table. An alias of zero, an alias
// above the advertised maximum, or an unknown alias is a protocol
// violation and reports ok=false. On MQTT 3.1.1 (no properties) the topic
// is returned unchanged.
func (c *TCPClient) resolveTopicAlias(l *link, pub *protocol.PublishPacket) (string, bool) {
	if pub.Properties == nil || pub.Properties.TopicAlias == nil {
		return pub.Topic, true
	}
	alias := *pub.Properties.TopicAlias
	if alias == 0 || alias > c.cfg.TopicAliasMaximum {
		c.logger.Warn("mqtt.tcp.topic_alias_invalid", slog.Uint64("alias", uint64(alias)))
		return "", false
	}
	if pub.Topic == "" {
		topic, known := l.aliases[alias]
		if !known {
			c.logger.Warn("mqtt.tcp.topic_alias_unknown", slog.Uint64("alias", uint64(alias)))
			return "", false
		}
		return topic, true
	}
	l.aliases[alias] = pub.Topic
	return pub.Topic, true
}

// handleInboundQoS2 implements the exactly-once receiver (method A): the
// first PUBLISH for an identifier is dispatched, recorded and PUBREC'd; a
// duplicate re-sends only the PUBREC without re-dispatching. It returns
// false when the link died while the handler ran.
func (c *TCPClient) handleInboundQoS2(l *link, pub *protocol.PublishPacket) bool {
	if c.storeContains(pub.PacketID, StoredInboundID) {
		// Already delivered; re-acknowledge without re-dispatching.
		c.sendAck(l, protocol.Pubrec, pub.PacketID)
		return true
	}
	c.dispatch(toMessage(pub))
	if isStopping(l) {
		// The handler blocked across a teardown — and possibly a reconnect
		// whose applySession reset the shared store. The message was
		// delivered, but recording its dedup entry now would poison the NEW
		// session's state: a fresh inbound QoS 2 PUBLISH reusing this
		// identifier would be swallowed as a duplicate. Skip the record and
		// the moot PUBREC; the broker re-sends on the next connection and
		// the application-level dup is the documented at-least-once cost of
		// a blocking handler.
		return false
	}
	_ = c.store.Save(StoredMessage{ID: pub.PacketID, Kind: StoredInboundID})
	c.sendAck(l, protocol.Pubrec, pub.PacketID)
	return true
}

// handleInboundPubrel completes the receiver-side QoS 2 handshake: PUBCOMP
// the identifier and drop its dedup record. An unknown identifier is still
// PUBCOMP'd (the peer must be released either way). A PUBREL that cannot be
// decoded is a Malformed Packet and fatal (§4.13.1) — reading on would
// leave the peer's QoS 2 flow and our dedup entry stuck forever.
func (c *TCPClient) handleInboundPubrel(l *link, frame protocol.Frame) bool {
	ack, err := protocol.DecodeAck(c.version, protocol.Pubrel, frame.Body)
	if err != nil {
		c.logger.Warn("mqtt.tcp.malformed_pubrel", slog.String("err", err.Error()))
		c.protocolError(l, protocol.MalformedPacketReason)
		return false
	}
	_ = c.store.Delete(ack.PacketID, StoredInboundID)
	c.sendAck(l, protocol.Pubcomp, ack.PacketID)
	return true
}

// handleAck processes an inbound PUBACK (QoS 1 terminal), PUBREC (QoS 2
// intermediate) or PUBCOMP (QoS 2 terminal) for an outbound publish,
// advancing the session state machine and signalling the waiter on a
// terminal transition. An acknowledgement that cannot be decoded is a
// Malformed Packet and fatal (§4.13.1): reading on would strand the
// in-flight exchange's stored entry, packet identifier and send-quota
// permit until the next session reset — a permanent Receive Maximum leak
// on a long-lived connection.
func (c *TCPClient) handleAck(l *link, frame protocol.Frame, t protocol.PacketType) bool {
	ack, err := protocol.DecodeAck(c.version, t, frame.Body)
	if err != nil {
		c.logger.Warn("mqtt.tcp.malformed_ack",
			slog.String("type", t.String()), slog.String("err", err.Error()))
		c.protocolError(l, protocol.MalformedPacketReason)
		return false
	}
	reason := reasonStringOf(ack.Properties)
	switch t {
	case protocol.Puback:
		c.completeOutbound(ack.PacketID, StoredPublish, ackResult{code: ack.ReasonCode, reason: reason})
	case protocol.Pubcomp:
		c.completeOutbound(ack.PacketID, StoredPubrel, ackResult{code: ack.ReasonCode, reason: reason})
	case protocol.Pubrec:
		c.handlePubrec(l, ack, reason)
	default:
		// Unreachable: handleAck is only called for the three ack types.
	}
	return true
}

// completeOutbound finalises a terminal outbound QoS>0 exchange: it drops
// the stored entry, frees the identifier, releases the send-quota permit the
// in-flight message held, and signals the waiter. When the entry is absent
// the acknowledgement is unknown (warn); when present but unwaited it is a
// resumed-session completion (debug) — in which case the permit released
// here is the one the original publishAcked goroutine intentionally left
// held when the send outlived its caller (ctx cancel / ack timeout / a
// connection drop that resumed the session).
func (c *TCPClient) completeOutbound(id uint16, kind StoredKind, res ackResult) {
	if !c.storeContains(id, kind) {
		c.logger.Warn("mqtt.tcp.ack_unknown_id",
			slog.Uint64("packet_id", uint64(id)), slog.String("kind", kind.String()))
		return
	}
	_ = c.store.Delete(id, kind)
	c.ids.Release(id)
	c.quota.release()
	if !c.signalWaiter(id, ackClassPublish, res) {
		c.logger.Debug("mqtt.tcp.ack_replayed", slog.Uint64("packet_id", uint64(id)))
	}
}

// handlePubrec advances a QoS 2 exchange past the PUBREC leg. A success
// code atomically supersedes the stored PUBLISH with a PUBREL and resends
// it (autonomously, so a resumed session with no waiter still completes); a
// failure code aborts the exchange and reports a *ReasonError to the
// waiter. A duplicate PUBREC in the PUBREL state just re-sends the PUBREL.
func (c *TCPClient) handlePubrec(l *link, ack *protocol.AckPacket, reason string) {
	id := ack.PacketID
	if c.storeContains(id, StoredPublish) {
		if ack.ReasonCode.IsError() {
			// Terminal abort of the exchange: drop the stored PUBLISH, free
			// the id and release the send-quota permit it held.
			_ = c.store.Delete(id, StoredPublish)
			c.ids.Release(id)
			c.quota.release()
			if !c.signalWaiter(id, ackClassPublish, ackResult{code: ack.ReasonCode, reason: reason}) {
				c.logger.Debug("mqtt.tcp.pubrec_error_replayed", slog.Uint64("packet_id", uint64(id)))
			}
			return
		}
		// Success: the PUBLISH is superseded by its PUBREL leg. The permit is
		// NOT released here — it carries over to the StoredPubrel and is freed
		// only when the terminal PUBCOMP arrives (completeOutbound).
		_ = c.store.Save(StoredMessage{ID: id, Kind: StoredPubrel})
		_ = c.store.Delete(id, StoredPublish)
		c.sendAck(l, protocol.Pubrel, id)
		return
	}
	if c.storeContains(id, StoredPubrel) {
		// Duplicate PUBREC: resend the PUBREL only.
		c.sendAck(l, protocol.Pubrel, id)
		return
	}
	c.logger.Warn("mqtt.tcp.pubrec_unknown_id", slog.Uint64("packet_id", uint64(id)))
}

// handleSuback signals the Subscribe waiter with the first filter's reason
// code (this client sends one filter per SUBSCRIBE). A SUBACK that cannot
// be decoded is a fatal Malformed Packet (§4.13.1) — the in-flight
// Subscribe would otherwise idle out its full AckTimeout against a peer
// already known to be confused.
func (c *TCPClient) handleSuback(l *link, frame protocol.Frame) bool {
	sp, err := protocol.DecodeSuback(c.version, frame.Body)
	if err != nil {
		c.logger.Warn("mqtt.tcp.malformed_suback", slog.String("err", err.Error()))
		c.protocolError(l, protocol.MalformedPacketReason)
		return false
	}
	res := ackResult{reason: reasonStringOf(sp.Properties)}
	if len(sp.ReasonCodes) > 0 {
		res.code = sp.ReasonCodes[0]
	}
	if !c.signalWaiter(sp.PacketID, ackClassSuback, res) {
		c.logger.Warn("mqtt.tcp.suback_unknown_id", slog.Uint64("packet_id", uint64(sp.PacketID)))
	}
	return true
}

// handleUnsuback signals the Unsubscribe waiter with the first filter's
// reason code (v5; v3 carries none, treated as success). A malformed
// UNSUBACK is fatal, mirroring handleSuback.
func (c *TCPClient) handleUnsuback(l *link, frame protocol.Frame) bool {
	up, err := protocol.DecodeUnsuback(c.version, frame.Body)
	if err != nil {
		c.logger.Warn("mqtt.tcp.malformed_unsuback", slog.String("err", err.Error()))
		c.protocolError(l, protocol.MalformedPacketReason)
		return false
	}
	res := ackResult{reason: reasonStringOf(up.Properties)}
	if len(up.ReasonCodes) > 0 {
		res.code = up.ReasonCodes[0]
	}
	if !c.signalWaiter(up.PacketID, ackClassUnsuback, res) {
		c.logger.Warn("mqtt.tcp.unsuback_unknown_id", slog.Uint64("packet_id", uint64(up.PacketID)))
	}
	return true
}

// handleServerDisconnect logs the broker's DISCONNECT reason and treats it
// as a lost connection so the lifecycle reconnects.
func (c *TCPClient) handleServerDisconnect(l *link, frame protocol.Frame) {
	reason := protocol.NormalDisconnection
	if dp, err := protocol.DecodeDisconnect(c.version, frame.Body); err == nil {
		reason = dp.ReasonCode
	}
	c.logger.Warn("mqtt.tcp.server_disconnect", slog.String("reason", reason.String()))
	c.teardownLink(l, false)
}

// protocolError tears the connection down as lost, preceded on MQTT 5.0 by
// a best-effort DISCONNECT carrying reason. On MQTT 3.1.1 no DISCONNECT is
// sent: the v3 packet has no reason code and is defined as a CLEAN
// disconnect that makes the broker discard the Last Will without
// publishing it ([MQTT-3.14.4-3]) — abruptly closing the socket is the
// conformant error signal and keeps the LWT armed for this abnormal
// teardown.
func (c *TCPClient) protocolError(l *link, reason protocol.ReasonCode) {
	if c.version == protocol.V50 {
		dp := &protocol.DisconnectPacket{Version: c.version, ReasonCode: reason}
		_ = c.writeFrame(l, dp.Encode)
	}
	c.teardownLink(l, false)
}

// sendAck writes a PUBACK/PUBREC/PUBREL/PUBCOMP with a success reason code,
// logging (but not tearing down on) a transient write failure — the read
// loop will observe the socket error on its next read.
func (c *TCPClient) sendAck(l *link, t protocol.PacketType, id uint16) {
	ack := &protocol.AckPacket{Version: c.version, Type: t, PacketID: id}
	if err := c.writeFrame(l, ack.EncodeAck); err != nil && !isStopping(l) {
		c.logger.Warn("mqtt.tcp.send_ack",
			slog.String("type", t.String()), slog.String("err", err.Error()))
	}
}

// storeContains reports whether an entry for (id, kind) is present. It runs
// on the read-loop hot path (once per inbound ack and QoS 2 PUBLISH), so it
// prefers a store's O(1) [containsStore.Contains] over the O(n log n)
// snapshot-and-sort of [SessionStore.All]; a store that does not implement
// the fast path falls back to a linear scan.
func (c *TCPClient) storeContains(id uint16, kind StoredKind) bool {
	if cs, ok := c.store.(containsStore); ok {
		return cs.Contains(id, kind)
	}
	msgs, err := c.store.All()
	if err != nil {
		return false
	}
	for _, m := range msgs {
		if m.ID == id && m.Kind == kind {
			return true
		}
	}
	return false
}

// dispatch routes msg to every subscription whose filter matches, in
// registration order. Matching handlers are copied out from under the
// subscription lock before they run, so a handler is free to (re)subscribe
// without deadlocking, while preserving the synchronous-in-read-loop
// contract documented on [MessageHandler].
func (c *TCPClient) dispatch(msg *Message) {
	c.subsMu.RLock()
	handlers := make([]MessageHandler, 0, len(c.subs))
	for i := range c.subs {
		if protocol.MatchTopic(c.subs[i].filter, msg.Topic) {
			handlers = append(handlers, c.subs[i].handler)
		}
	}
	c.subsMu.RUnlock()
	for _, h := range handlers {
		h(msg)
	}
}

// toMessage projects a decoded PUBLISH (topic already alias-resolved) into
// the public [Message], lifting the MQTT 5.0 properties into typed fields.
func toMessage(p *protocol.PublishPacket) *Message {
	m := &Message{
		Topic:   p.Topic,
		Payload: p.Payload,
		QoS:     QoS(p.QoS),
		Retain:  p.Retain,
		Dup:     p.Dup,
	}
	pr := p.Properties
	if pr == nil {
		return m
	}
	m.ContentType = pr.ContentType
	m.ResponseTopic = pr.ResponseTopic
	m.CorrelationData = pr.CorrelationData
	m.SubscriptionIdentifiers = pr.SubscriptionIdentifiers
	m.UserProperties = pr.UserProperties
	if pr.MessageExpiryInterval != nil {
		m.MessageExpirySeconds = *pr.MessageExpiryInterval
	}
	if pr.PayloadFormat != nil {
		m.PayloadFormatUTF8 = *pr.PayloadFormat == 1
	}
	return m
}

// keepAliveLoop pings on the negotiated interval and runs the PINGRESP
// watchdog. Unanswered PINGREQs from previous ticks mean the socket is
// half-open — the peer vanished without a FIN/RST, so readLoop would block
// in ReadFrame forever — and once pingTimeoutThreshold accumulate the
// connection is declared lost so the lifecycle reconnects. A zero interval
// disables keep-alive entirely.
func (c *TCPClient) keepAliveLoop(l *link) {
	defer l.wg.Done()
	if l.pingInterval <= 0 {
		<-l.stop
		return
	}
	ticker := time.NewTicker(l.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			if l.outstandingPings.Load() >= pingTimeoutThreshold {
				c.logger.Warn("mqtt.tcp.ping_timeout")
				c.teardownLink(l, false)
				return
			}
			if err := c.writeFrame(l, protocol.EncodePingReq); err != nil {
				if !isStopping(l) {
					c.logger.Warn("mqtt.tcp.ping", slog.String("err", err.Error()))
				}
				c.teardownLink(l, false)
				return
			}
			l.outstandingPings.Add(1)
		}
	}
}
