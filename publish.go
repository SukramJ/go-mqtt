// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"bytes"
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
//
// payload (and any option-supplied byte slice) may be reused once Publish
// returns: a QoS>0 message the client must keep for session replay is
// stored as a private copy. See [Publisher.Publish].
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
	quotaGen, err := c.quota.acquire(ctx)
	if err != nil {
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
	//
	// Every failure-path release below is generation-checked (releaseAt/
	// ReleaseAt): if this goroutine stalls across a connection drop and the
	// reconnect's applySession has already re-accounted the session
	// (quota.reset / ids.Reset), the stale release must be a no-op — an
	// unconditional one would over-credit the send quota past Receive
	// Maximum, or free a packet identifier the new session has handed to
	// another exchange.

	id, idGen, err := c.ids.Acquire()
	if err != nil {
		c.quota.releaseAt(quotaGen)
		return err
	}
	pkt.PacketID = id

	if err := c.store.Save(StoredMessage{ID: id, Kind: StoredPublish, Publish: storedCopy(pkt)}); err != nil {
		c.ids.ReleaseAt(id, idGen)
		c.quota.releaseAt(quotaGen)
		return err
	}

	ch := c.registerWaiter(id, ackClassPublish)
	if err := c.writeFrame(l, pkt.Encode); err != nil {
		c.removeWaiter(id, ch)
		// The stored entry, the identifier and the send-quota permit are one
		// unit: either this failed send owns all three and tears them all
		// down, or a reconnect has already adopted the unit and this
		// goroutine must touch none of it. The quota generation is what
		// distinguishes the two — applySession bumps it (quota.reset) as the
		// last step of re-accounting the session — so it is read once and
		// gates the whole block, rather than each release deciding for
		// itself. In particular the identifier must NOT be freed while the
		// entry survives for replay: the allocator would hand it to a fresh
		// publish while the DUP resend is still using it on the wire.
		//
		// The stamps are the ones this send acquired under, not the link's
		// (l.quotaGen / l.idGen). They normally agree; where they can differ
		// — quota.acquire parked on an exhausted quota and was woken by the
		// reconnect's quota.reset, so the permit belongs to the NEW session
		// while l is the old link — the acquire-time generation is the
		// correct one: booking against l's would skip the release and leak
		// that permit for the new session's lifetime.
		if c.quota.generation() == quotaGen {
			// The frame never (completely) reached the broker and no
			// reconnect has re-accounted the session since: drop the stored
			// state, free the identifier and release the permit — there is
			// nothing in flight and nothing to replay.
			//
			// Residual race, deliberately accepted: applySession can land
			// between this generation read and the Delete below, having
			// already counted the entry as in-flight (inflightPermits runs
			// before quota.reset). The new session then starts one permit
			// short, and — on a resumed session — replaySession may find the
			// entry gone. Closing that window needs the decision and the
			// delete to be one atomic step, which the exported SessionStore
			// contract cannot express (there is no conditional/compare-and-
			// delete), and widening the store interface for a few
			// instructions of exposure is not worth an API break. The
			// failure mode is bounded and benign: at most one lost permit
			// per occurrence, never an over-credit past Receive Maximum.
			_ = c.store.Delete(id, StoredPublish)
			c.ids.ReleaseAt(id, idGen)
			c.quota.releaseAt(quotaGen)
		}
		// Otherwise a teardown + reconnect completed while this goroutine
		// was parked in the failing write: applySession has re-accounted the
		// stored entry (a resumed session counts and replays it as a DUP —
		// permitted by at-least-once semantics even though this caller sees
		// an error; a reset session discarded it), so every piece of cleanup
		// here would clobber state the new session owns.
		return err
	}

	res, err := c.waitAck(ctx, ch)
	if err != nil {
		// ctx cancellation or ack timeout: the exchange is still in flight,
		// so the identifier, stored PUBLISH and the held send-quota permit
		// all survive. The permit is released by completeOutbound if the
		// broker later acks on this connection, or dropped together with the
		// stored state when a clean-start reconnect resets the session.
		c.removeWaiter(id, ch)
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

// storedCopy is the replay copy of an outbound QoS>0 PUBLISH: a shallow
// copy of the packet with every caller-owned byte slice cloned.
//
// A shallow copy alone keeps the caller's buffers alive inside the session
// store. [Publisher.Publish] documents the payload as reusable once the
// call returns, and a caller that does reuse a scratch buffer would then
// silently rewrite the bytes a DUP resend puts on the wire after a
// reconnect — the wrong payload delivered under the original packet
// identifier, which no acknowledgement can detect. The clones are Payload
// and the MQTT 5.0 Correlation Data property, the only caller-provided
// slices reachable from an encoded PUBLISH: the remaining property fields
// are strings (immutable) or slices this package itself allocated —
// WithUserProperties appends into a fresh slice, and the *uint32/*byte
// property values point at option-closure copies.
func storedCopy(pkt *protocol.PublishPacket) *protocol.PublishPacket {
	stored := *pkt
	stored.Payload = bytes.Clone(pkt.Payload)
	if pkt.Properties != nil {
		props := *pkt.Properties
		props.CorrelationData = bytes.Clone(pkt.Properties.CorrelationData)
		stored.Properties = &props
	}
	return &stored
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

	res, err := c.requestAck(ctx, l, "SUBSCRIBE", ackClassSuback, func(id uint16) frameEncoder {
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
// blocks until the UNSUBACK, and removes the local handler registration —
// unless a concurrent Subscribe for the same filter registered a newer
// handler while the UNSUBSCRIBE was in flight; that registration is left
// intact (the broker processed the later SUBSCRIBE after this UNSUBSCRIBE,
// so its subscription is live and removing the handler would silently drop
// its messages).
func (c *TCPClient) Unsubscribe(ctx context.Context, filter string) error {
	if err := protocol.ValidateTopicFilter(filter); err != nil {
		return err
	}
	l := c.link.Load()
	if l == nil {
		return ErrNotConnected
	}

	snap, existed := c.snapshotSubscription(filter)
	if _, err := c.requestAck(ctx, l, "UNSUBSCRIBE", ackClassUnsuback, func(id uint16) frameEncoder {
		return &protocol.UnsubscribePacket{
			Version:  c.version,
			PacketID: id,
			Filters:  []string{filter},
		}
	}); err != nil {
		return err
	}
	if existed {
		c.removeSubscriptionIfCurrent(filter, snap.token)
	}
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
// afterwards (generation-checked, so a session reset in between — which
// already freed it, possibly to a new owner — turns the release into a
// no-op) — these requests are never replayed, so they hold no session state
// across a reconnect.
func (c *TCPClient) requestAck(ctx context.Context, l *link, packet string, want ackClass, mk func(id uint16) frameEncoder) (ackResult, error) {
	id, gen, err := c.ids.Acquire()
	if err != nil {
		return ackResult{}, err
	}
	ch := c.registerWaiter(id, want)
	if err := c.writeFrame(l, mk(id).Encode); err != nil {
		c.removeWaiter(id, ch)
		c.ids.ReleaseAt(id, gen)
		return ackResult{}, err
	}
	res, werr := c.waitAck(ctx, ch)
	if werr != nil {
		// ctx cancel / ack timeout: remove the waiter BEFORE freeing the id.
		// Releasing first would let a concurrent Acquire hand this id to
		// another SUBSCRIBE/UNSUBSCRIBE that registers its own waiter, which
		// this trailing removeWaiter would then delete — stranding that
		// request until its own AckTimeout despite a valid broker ack.
		c.removeWaiter(id, ch)
		c.ids.ReleaseAt(id, gen)
		return ackResult{}, werr
	}
	c.ids.ReleaseAt(id, gen)
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
