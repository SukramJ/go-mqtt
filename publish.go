// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// errAckTimeout is returned when an acknowledgement does not arrive within
// the configured AckTimeout.
var errAckTimeout = errors.New("mqtt/tcp: timed out waiting for acknowledgement")

// Publish implements [Publisher]. QoS 0 is fire-and-forget; QoS 1 blocks
// until PUBACK; QoS 2 blocks until PUBCOMP. A disconnected client fails
// fast with [ErrNotConnected]; a connection lost mid-flight fails with
// [ErrConnectionLost]; a broker failure reason code yields a *[ReasonError].
// Each acknowledgement wait is bounded by ctx and the configured
// AckTimeout.
func (c *TCPClient) Publish(ctx context.Context, topic string, payload []byte, qos QoS, retain bool, opts ...PublishOption) error {
	if err := protocol.ValidateTopicName(topic); err != nil {
		return err
	}
	if qos > QoS2 {
		return fmt.Errorf("%w: unsupported QoS %d", protocol.ErrProtocolViolation, qos)
	}

	var po publishOptions
	for _, o := range opts {
		o(&po)
	}
	// The Response Topic is a topic name a responder publishes its reply to,
	// so [MQTT-3.3.2-14] forbids wildcard characters in it. A wildcarded
	// Response Topic is a §3.3.2.3.5 Protocol Error a conformant broker answers
	// with a DISCONNECT that tears down the whole connection (and every
	// in-flight QoS exchange with it); reject it locally like the Maximum QoS /
	// Retain Available limits below. It is only carried on an MQTT 5.0 link.
	if c.version == protocol.V50 && po.responseTopic != "" {
		if err := protocol.ValidateTopicName(po.responseTopic); err != nil {
			return fmt.Errorf("invalid response topic: %w", err)
		}
	}

	l := c.link.Load()
	if l == nil {
		return ErrNotConnected
	}

	// Honour the broker's advertised limits locally rather than transmitting
	// a PUBLISH the broker will reject with a DISCONNECT: a QoS above the
	// negotiated Maximum QoS violates [MQTT-3.2.2-11] (broker replies 0x9B),
	// and a retained PUBLISH when Retain Available = 0 violates
	// [MQTT-3.2.2-14] (broker replies 0x9A). On an MQTT 3.1.1 link (and any
	// broker that advertised neither) these default to QoS 2 / retain-on, so
	// the checks are no-ops.
	if res, ok := c.ConnectResult(); ok {
		if qos > res.MaximumQoS {
			return fmt.Errorf("%w: QoS %d exceeds the broker's Maximum QoS %d", protocol.ErrProtocolViolation, qos, res.MaximumQoS)
		}
		if retain && !res.RetainAvailable {
			return fmt.Errorf("%w: broker does not support retained messages", protocol.ErrProtocolViolation)
		}
	}

	pkt := &protocol.PublishPacket{
		Version:    c.version,
		Topic:      topic,
		Payload:    payload,
		QoS:        byte(qos),
		Retain:     retain,
		Properties: buildPublishProps(c.version, &po),
	}

	if qos == QoS0 {
		return c.writeFrame(l, pkt.Encode)
	}
	return c.publishAcked(ctx, l, pkt)
}

// publishAcked runs the QoS 1/2 flow-control path: reserve a send permit and
// a packet identifier, persist the PUBLISH for replay, register the waiter,
// put it on the wire, and block for the terminal acknowledgement.
func (c *TCPClient) publishAcked(ctx context.Context, l *link, pkt *protocol.PublishPacket) error {
	if err := c.quota.acquire(ctx); err != nil {
		return err
	}
	// The permit is now owned by the in-flight message, not by this
	// goroutine. It is released at the terminal acknowledgement (in the read
	// loop's completeOutbound / handlePubrec) or explicitly on a pre-send
	// failure below. It is deliberately NOT released when the message is kept
	// for session replay (ctx cancel, ack timeout, connection lost): the
	// QoS>0 PUBLISH is still unacknowledged at the broker, so the permit must
	// stay held or a fresh publish could push the outstanding count past
	// Receive Maximum (§4.9 → broker DISCONNECT 0x93).

	id, err := c.ids.Acquire()
	if err != nil {
		c.quota.release()
		return err
	}
	pkt.PacketID = id

	stored := *pkt
	if err := c.store.Save(StoredMessage{ID: id, Kind: StoredPublish, Publish: &stored}); err != nil {
		c.ids.Release(id)
		c.quota.release()
		return err
	}

	ch := c.registerWaiter(id)
	if err := c.writeFrame(l, pkt.Encode); err != nil {
		// The frame never (completely) reached the broker: drop the stored
		// state, free the identifier and release the permit — there is
		// nothing in flight and nothing to replay.
		c.removeWaiter(id)
		_ = c.store.Delete(id, StoredPublish)
		c.ids.Release(id)
		c.quota.release()
		return err
	}

	res, err := c.waitAck(ctx, ch)
	if err != nil {
		// ctx cancellation or ack timeout: the exchange is still in flight,
		// so the identifier, stored PUBLISH and the held send-quota permit
		// all survive. The permit is released by completeOutbound if the
		// broker later acks on this connection, or dropped together with the
		// stored state when a clean-start reconnect resets the session.
		c.removeWaiter(id)
		return err
	}
	if res.err != nil {
		// Connection lost: the waiter was already failed and cleared, and the
		// stored PUBLISH/identifier/permit are kept for replay. The permit is
		// re-accounted by applySession, which seeds the reconnect quota with
		// Receive Maximum minus the still-in-flight count.
		return res.err
	}
	if res.code.IsError() {
		// The read loop already dropped the stored state, freed the id and
		// released the permit.
		return &ReasonError{Packet: "PUBLISH", Code: res.code, Reason: res.reason}
	}
	return nil
}

// Subscribe implements [Subscriber]. It sends a single-filter SUBSCRIBE and
// blocks until the SUBACK (bounded by ctx and AckTimeout). A granted QoS
// below the requested one is not an error; a rejection (reason >= 0x80)
// yields a *[ReasonError]. On success the handler is registered so it
// survives reconnects (resubscribe replay).
func (c *TCPClient) Subscribe(ctx context.Context, filter string, qos QoS, handler MessageHandler, opts ...SubscribeOption) (SubscribeResult, error) {
	if err := protocol.ValidateTopicFilter(filter); err != nil {
		return SubscribeResult{}, err
	}
	if qos > QoS2 {
		return SubscribeResult{}, fmt.Errorf("%w: unsupported QoS %d", protocol.ErrProtocolViolation, qos)
	}
	l := c.link.Load()
	if l == nil {
		return SubscribeResult{}, ErrNotConnected
	}

	var so subscribeOptions
	for _, o := range opts {
		o(&so)
	}
	options := protocol.SubscribeOptions{
		QoS:               byte(qos),
		NoLocal:           so.noLocal,
		RetainAsPublished: so.retainAsPublished,
		RetainHandling:    byte(so.retainHandling),
	}

	// Register the handler BEFORE the SUBSCRIBE hits the wire. The broker
	// may deliver matching messages — most notably the retained-message
	// replay — immediately after the SUBACK, often in the same TCP flush.
	// The read loop dispatches those frames concurrently with this caller,
	// so a post-SUBACK registration loses that race and the first messages
	// are silently dropped for want of a handler. Rolled back on failure.
	prev, replaced := c.snapshotSubscription(filter)
	token := c.addSubscription(filter, options, handler)

	res, err := c.requestAck(ctx, l, "SUBSCRIBE", func(id uint16) frameEncoder {
		return &protocol.SubscribePacket{
			Version:       c.version,
			PacketID:      id,
			Subscriptions: []protocol.Subscription{{Filter: filter, Options: options}},
		}
	})
	if err != nil {
		c.restoreSubscription(filter, prev, replaced, token)
		return SubscribeResult{}, err
	}
	return SubscribeResult{GrantedQoS: QoS(res.code), ReasonCode: res.code}, nil
}

// Unsubscribe implements [Subscriber]. It sends a single-filter UNSUBSCRIBE,
// blocks until the UNSUBACK, and removes the local handler registration.
func (c *TCPClient) Unsubscribe(ctx context.Context, filter string) error {
	if err := protocol.ValidateTopicFilter(filter); err != nil {
		return err
	}
	l := c.link.Load()
	if l == nil {
		return ErrNotConnected
	}

	if _, err := c.requestAck(ctx, l, "UNSUBSCRIBE", func(id uint16) frameEncoder {
		return &protocol.UnsubscribePacket{
			Version:  c.version,
			PacketID: id,
			Filters:  []string{filter},
		}
	}); err != nil {
		return err
	}
	c.removeSubscription(filter)
	return nil
}

// frameEncoder is any packet that can encode itself onto the wire — the
// shape SUBSCRIBE and UNSUBSCRIBE share for requestAck.
type frameEncoder interface {
	Encode(w io.Writer) error
}

// requestAck sends a SUBSCRIBE/UNSUBSCRIBE built by mk (given the freshly
// allocated packet identifier), waits for its acknowledgement, and returns a
// *ReasonError for a failure reason code. The identifier is always freed
// afterwards — these requests are never replayed, so they hold no session
// state across a reconnect.
func (c *TCPClient) requestAck(ctx context.Context, l *link, packet string, mk func(id uint16) frameEncoder) (ackResult, error) {
	id, err := c.ids.Acquire()
	if err != nil {
		return ackResult{}, err
	}
	ch := c.registerWaiter(id)
	if err := c.writeFrame(l, mk(id).Encode); err != nil {
		c.removeWaiter(id)
		c.ids.Release(id)
		return ackResult{}, err
	}
	res, werr := c.waitAck(ctx, ch)
	if werr != nil {
		// ctx cancel / ack timeout: remove the waiter BEFORE freeing the id.
		// Releasing first would let a concurrent Acquire hand this id to
		// another SUBSCRIBE/UNSUBSCRIBE that registers its own waiter, which
		// this trailing removeWaiter would then delete — stranding that
		// request until its own AckTimeout despite a valid broker ack.
		c.removeWaiter(id)
		c.ids.Release(id)
		return ackResult{}, werr
	}
	c.ids.Release(id)
	if res.err != nil {
		return ackResult{}, res.err
	}
	if res.code.IsError() {
		return ackResult{}, &ReasonError{Packet: packet, Code: res.code, Reason: res.reason}
	}
	return res, nil
}

// waitAck blocks for the terminal acknowledgement on ch, bounded by ctx and
// the configured AckTimeout. A delivered result (including a
// connection-lost result) returns with a nil error; ctx cancellation or the
// timeout returns the corresponding error and no result.
func (c *TCPClient) waitAck(ctx context.Context, ch chan ackResult) (ackResult, error) {
	timer := time.NewTimer(c.cfg.AckTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return ackResult{}, ctx.Err()
	case <-timer.C:
		return ackResult{}, errAckTimeout
	}
}

// buildPublishProps assembles the MQTT 5.0 PUBLISH property block from the
// per-call options, or nil when there is nothing to attach (or on a v3.1.1
// link, which has no property block).
func buildPublishProps(v protocol.Version, po *publishOptions) *protocol.Properties {
	if v != protocol.V50 {
		return nil
	}
	p := &protocol.Properties{
		ContentType:     po.contentType,
		ResponseTopic:   po.responseTopic,
		CorrelationData: po.correlationData,
		UserProperties:  po.userProperties,
	}
	empty := po.contentType == "" && po.responseTopic == "" &&
		len(po.correlationData) == 0 && len(po.userProperties) == 0
	if po.messageExpiry != nil {
		p.MessageExpiryInterval = po.messageExpiry
		empty = false
	}
	if po.payloadFormatUTF8 {
		one := byte(1)
		p.PayloadFormat = &one
		empty = false
	}
	if empty {
		return nil
	}
	return p
}
