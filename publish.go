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
	l := c.link.Load()
	if l == nil {
		return ErrNotConnected
	}

	var po publishOptions
	for _, o := range opts {
		o(&po)
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
	defer c.quota.release()

	id, err := c.ids.Acquire()
	if err != nil {
		return err
	}
	pkt.PacketID = id

	stored := *pkt
	if err := c.store.Save(StoredMessage{ID: id, Kind: StoredPublish, Publish: &stored}); err != nil {
		c.ids.Release(id)
		return err
	}

	ch := c.registerWaiter(id)
	if err := c.writeFrame(l, pkt.Encode); err != nil {
		// The frame never (completely) reached the broker: drop the stored
		// state and free the identifier — there is nothing to replay.
		c.removeWaiter(id)
		_ = c.store.Delete(id, StoredPublish)
		c.ids.Release(id)
		return err
	}

	res, err := c.waitAck(ctx, ch)
	if err != nil {
		// ctx cancellation or ack timeout: the exchange is still in flight,
		// so the identifier and stored PUBLISH survive for session replay.
		c.removeWaiter(id)
		return err
	}
	if res.err != nil {
		// Connection lost: the waiter was already failed and cleared, and
		// the stored PUBLISH/identifier are kept for replay.
		return res.err
	}
	if res.code.IsError() {
		// The read loop already dropped the stored state and freed the id.
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

	res, err := c.requestAck(ctx, l, "SUBSCRIBE", func(id uint16) frameEncoder {
		return &protocol.SubscribePacket{
			Version:       c.version,
			PacketID:      id,
			Subscriptions: []protocol.Subscription{{Filter: filter, Options: options}},
		}
	})
	if err != nil {
		return SubscribeResult{}, err
	}
	c.addSubscription(filter, options, handler)
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
	c.ids.Release(id)
	if werr != nil {
		c.removeWaiter(id)
		return ackResult{}, werr
	}
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
