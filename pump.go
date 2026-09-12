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
			// A caller Disconnect is a completely normal way to reach this
			// branch: the broker closes the socket the moment it reads our
			// DISCONNECT, so the read error races Disconnect's own teardown.
			// Honour l.graceful or that race raises a lost-connection signal
			// for an intentional shutdown and a [Lifecycle] reconnects.
			c.teardownLink(l, l.graceful.Load())
			return
		}
		if isStopping(l) {
			// The link was torn down (keep-alive watchdog, server DISCONNECT,
			// caller Disconnect) while this frame was in flight — and a
			// reconnect may already have swapped in a new link that shares
			// c.ids, c.quota and c.store. A bufio.Reader can still surface a
			// pipelined PUBACK/PUBCOMP from the now-dead socket here; handling
			// it would run completeOutbound/handlePubrec against the NEW
			// session's store and waiter table, dropping a stored PUBLISH the
			// new session owns and resolving its waiter with a stale
			// acknowledgement. (The identifier and quota releases are
			// generation-checked against this link, so those two survive the
			// race on their own — this check is what protects the rest.) Drop
			// the frame and exit — teardownLink has already run for this link.
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
		// only, and a CONNACK is additionally forbidden after the first one
		// ([MQTT-3.2.0-1]). Receiving any of them is a §4.13 Protocol Error,
		// not a frame to log and read past: the peer's state machine is
		// desynchronised from ours, so every subsequent frame it sends is
		// suspect. Tear the connection down with 0x82 like every other
		// protocol violation in this file. (Reserved/unknown packet types
		// never reach here — ValidateFlags rejects them as Malformed Packet
		// in readLoop, which closes the connection with 0x81.)
		c.logger.Warn("mqtt.tcp.unexpected_packet", slog.String("type", frame.PacketType().String()))
		c.protocolError(l, protocol.ProtocolErrorReason)
		return false
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
// the identifier and drop its dedup record. A PUBREL that cannot be decoded
// is a Malformed Packet and fatal (§4.13.1) — reading on would leave the
// peer's QoS 2 flow and our dedup entry stuck forever.
//
// An identifier with no dedup record — the broker retrying a PUBREL whose
// PUBCOMP was lost, or a session this side discarded — is still answered,
// because leaving it unanswered strands the peer's exchange. But it is
// answered with 0x92 Packet Identifier not found (§3.6.2.1), not Success:
// Success asserts the message was delivered exactly once by this client,
// which is precisely what it cannot claim for state it does not have. On an
// MQTT 3.1.1 link the PUBCOMP carries no reason code at all, so the same
// call emits the plain acknowledgement.
func (c *TCPClient) handleInboundPubrel(l *link, frame protocol.Frame) bool {
	ack, err := protocol.DecodeAck(c.version, protocol.Pubrel, frame.Body)
	if err != nil {
		c.logger.Warn("mqtt.tcp.malformed_pubrel", slog.String("err", err.Error()))
		c.protocolError(l, protocol.MalformedPacketReason)
		return false
	}
	if !c.storeContains(ack.PacketID, StoredInboundID) {
		c.logger.Warn("mqtt.tcp.pubrel_unknown_id", slog.Uint64("packet_id", uint64(ack.PacketID)))
		c.sendAckReason(l, protocol.Pubcomp, ack.PacketID, protocol.PacketIdentifierNotFound)
		return true
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
		c.completeOutbound(l, ack.PacketID, StoredPublish, ackResult{code: ack.ReasonCode, reason: reason})
	case protocol.Pubcomp:
		c.completeOutbound(l, ack.PacketID, StoredPubrel, ackResult{code: ack.ReasonCode, reason: reason})
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
//
// The identifier and permit are freed against the generations l was
// established under: a store call can block (a persistent [SessionStore] is
// an explicitly supported extension point) long enough for this read loop to
// outlive its own link, and an unguarded release would then free an
// identifier the reconnected session already handed to another exchange and
// credit that session's quota past the negotiated Receive Maximum.
func (c *TCPClient) completeOutbound(l *link, id uint16, kind StoredKind, res ackResult) {
	if !c.storeContains(id, kind) {
		c.logger.Warn("mqtt.tcp.ack_unknown_id",
			slog.Uint64("packet_id", uint64(id)), slog.String("kind", kind.String()))
		return
	}
	_ = c.store.Delete(id, kind)
	c.ids.ReleaseAt(id, l.idGen)
	c.quota.releaseAt(l.quotaGen)
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
			// the id and release the send-quota permit it held — both against
			// l's session generations, so a read loop that stalled in the
			// store across a reconnect cannot corrupt the new session's
			// accounting (see completeOutbound).
			_ = c.store.Delete(id, StoredPublish)
			c.ids.ReleaseAt(id, l.idGen)
			c.quota.releaseAt(l.quotaGen)
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
	// No state for this identifier: the exchange was completed, aborted or
	// discarded with a previous session. §4.3.3 still requires an answer —
	// the sender keeps the message in flight until its PUBREL arrives, so a
	// warn-and-continue leaves the broker retransmitting this PUBREC (and
	// this branch re-logging) for the connection's lifetime. Release it with
	// 0x92 Packet Identifier not found (§3.6.2.1), the reason code that says
	// exactly what happened; on an MQTT 3.1.1 link the PUBREL carries no
	// reason code and the bare packet unsticks the peer just the same.
	c.logger.Warn("mqtt.tcp.pubrec_unknown_id", slog.Uint64("packet_id", uint64(id)))
	c.sendAckReason(l, protocol.Pubrel, id, protocol.PacketIdentifierNotFound)
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
	// A broker DISCONNECT arriving while our own Disconnect is in flight is
	// part of the graceful shutdown, not a drop to reconnect from.
	c.teardownLink(l, l.graceful.Load())
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
	c.teardownLink(l, l.graceful.Load())
}

// sendAck writes a PUBACK/PUBREC/PUBREL/PUBCOMP with a success reason code.
func (c *TCPClient) sendAck(l *link, t protocol.PacketType, id uint16) {
	c.sendAckReason(l, t, id, protocol.Success)
}

// sendAckReason writes a PUBACK/PUBREC/PUBREL/PUBCOMP carrying reason,
// logging (but not tearing down on) a transient write failure — the read
// loop will observe the socket error on its next read. On an MQTT 3.1.1
// link the encoder omits the reason code (the dialect has none), so the
// same call emits the plain two-byte acknowledgement body.
func (c *TCPClient) sendAckReason(l *link, t protocol.PacketType, id uint16, reason protocol.ReasonCode) {
	ack := &protocol.AckPacket{Version: c.version, Type: t, PacketID: id, ReasonCode: reason}
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

// dispatch routes msg to the subscriptions it belongs to. Matching handlers
// are copied out from under the subscription lock before they run, so a
// handler is free to (re)subscribe without deadlocking, while preserving the
// synchronous-in-read-loop contract documented on [MessageHandler].
//
// A PUBLISH carrying MQTT 5.0 Subscription Identifiers (§3.3.4) says which
// subscriptions it was forwarded for, and those are then the only ones it
// reaches. Everything else falls back to matching the topic against every
// registered filter, which is all a v3.1.1 link or an identifier-less
// subscription offers.
//
// The distinction is not cosmetic. A broker sends one copy of a PUBLISH per
// matching subscription, so a client holding two overlapping filters gets
// two copies — and re-matching each copy against every filter invokes both
// handlers twice over, running a handler twice per published message. That
// was measured against Mosquitto 2.1.2 on both dialects, and for a consumer
// whose handler performs a write it means the write happens twice with
// nothing in any log. Identifiers turn the broker's fan-out into something
// the client can attribute; see [WithSubscriptionID].
//
// An identifier the client does not know is dropped rather than broadened
// into a topic match: it names a subscription this process did not register
// — a session resumed from a previous run, say — and guessing a handler for
// it would deliver a message to code that never asked for it.
func (c *TCPClient) dispatch(msg *Message) {
	c.subsMu.RLock()
	handlers := make([]MessageHandler, 0, len(c.subs))
	stamped := 0
	if ids := msg.SubscriptionIdentifiers; len(ids) > 0 {
		for i := range c.subs {
			if c.subs[i].subID != 0 && containsID(ids, c.subs[i].subID) {
				handlers = append(handlers, c.subs[i].handler)
			}
		}
	} else {
		// No identifier means this message was forwarded for no
		// identified subscription: §3.3.4 requires the server to include
		// the identifier of EVERY subscription it forwarded a PUBLISH
		// for. So a topic match against a stamped subscription would
		// deliver a copy the broker did not send for it — which is the
		// doubling that identifiers exist to remove, reintroduced through
		// the fallback. Only unstamped subscriptions are candidates here.
		for i := range c.subs {
			if c.subs[i].subID != 0 {
				stamped++
				continue
			}
			if protocol.MatchTopic(c.subs[i].filter, msg.Topic) {
				handlers = append(handlers, c.subs[i].handler)
			}
		}
	}
	c.subsMu.RUnlock()
	if len(handlers) == 0 && stamped > 0 {
		// Failing closed is deliberate: a doubled command is worse than a
		// dropped one, because the doubling is invisible and the drop is
		// this line. It is also the signature of a broker or intermediary
		// that accepted a Subscription Identifier and did not stamp the
		// messages it forwarded — which no compliant server does, and
		// which nothing else in the client can detect.
		c.logger.Warn("mqtt.tcp.unstamped_publish_dropped",
			slog.String("topic", msg.Topic),
			slog.Int("stamped_subscriptions", stamped))
	}
	for _, h := range handlers {
		h(msg)
	}
}

// containsID reports whether id is among the identifiers a PUBLISH carried.
// A linear scan because the list is one entry in every case a broker
// produces except a shared subscription overlap, and allocating a set per
// inbound message on the read loop would cost more than it saves.
func containsID(ids []uint32, id uint32) bool {
	for _, got := range ids {
		if got == id {
			return true
		}
	}
	return false
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
// connection is declared lost so the lifecycle reconnects.
//
// A zero interval disables keep-alive entirely — the broker imposed a
// Server Keep Alive of 0 (§3.1.2.10), see
// [TCPClient.effectivePingInterval]. The loop is still started and simply
// parks until teardown, so the link's wait-group accounting is the same on
// every connection; with no PINGREQ ever sent the watchdog below is inert.
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
				c.teardownLink(l, l.graceful.Load())
				return
			}
			// Count the ping BEFORE it goes on the wire. The read loop
			// answers a PINGRESP with Store(0); counting afterwards loses
			// every PINGRESP that lands between the flush and the Add — the
			// Store(0) is overwritten by an Add(1) for a ping that was in
			// fact already answered, so the watchdog reaches its threshold
			// after a single lost PINGRESP instead of the two it documents.
			// Counting first can only overstate by one tick (the ping the
			// write is about to fail on), and that path tears the link down
			// anyway, so the counter never outlives it.
			l.outstandingPings.Add(1)
			if err := c.writeFrame(l, protocol.EncodePingReq); err != nil {
				if !isStopping(l) {
					c.logger.Warn("mqtt.tcp.ping", slog.String("err", err.Error()))
				}
				// writeFrame already tore the link down honouring l.graceful;
				// repeating it here is the closeOnce no-op that keeps the exit
				// path uniform.
				c.teardownLink(l, l.graceful.Load())
				return
			}
		}
	}
}
