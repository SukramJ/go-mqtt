// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Ack-waiting edge cases publish.go's requestAck/publishAcked share with
// no other test in this package: input validation that fails before any
// I/O, a broker-rejected PUBACK/PUBREC surfacing as *ReasonError, a
// duplicate PUBREC once the exchange has already moved to the PUBREL
// state, packet-identifier exhaustion, a SessionStore.Save failure, an
// oversized frame rejected locally, and — via a broker double that never
// acknowledges anything — ctx cancellation, ack timeout and connection
// loss while a SUBSCRIBE/UNSUBSCRIBE is still in flight.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// ---------------------------------------------------------------------------
// Input validation (fails before touching the connection)
// ---------------------------------------------------------------------------

func TestPublishInvalidTopicNameRejected(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "badtopic"})
	err := c.Publish(context.Background(), "has/+/wildcard", []byte("x"), QoS0, false)
	if !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("err = %v, want ErrProtocolViolation", err)
	}
}

func TestPublishInvalidQoSRejected(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "badqos"})
	err := c.Publish(context.Background(), "a/b", []byte("x"), QoS(3), false)
	if !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("err = %v, want ErrProtocolViolation", err)
	}
}

func TestSubscribeInvalidFilterAndQoSRejected(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "badsub"})
	if _, err := c.Subscribe(context.Background(), "", QoS0, func(*Message) {}); !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("empty filter: err = %v, want ErrProtocolViolation", err)
	}
	if _, err := c.Subscribe(context.Background(), "a/b", QoS(3), func(*Message) {}); !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("bad QoS: err = %v, want ErrProtocolViolation", err)
	}
}

func TestUnsubscribeInvalidFilterRejected(t *testing.T) {
	t.Parallel()

	c := NewTCPClient(TCPConfig{BrokerURL: "tcp://127.0.0.1:1", ClientID: "badunsub"})
	if err := c.Unsubscribe(context.Background(), "bad/#/tail"); !errors.Is(err, protocol.ErrProtocolViolation) {
		t.Fatalf("err = %v, want ErrProtocolViolation", err)
	}
}

// ---------------------------------------------------------------------------
// Broker-rejected PUBACK/PUBREC -> *ReasonError
// ---------------------------------------------------------------------------

// injectAckWithReason builds and injects a PUBACK/PUBREC/PUBCOMP carrying an
// explicit (typically failure) reason code and optional reason string.
func injectAckWithReason(t *testing.T, b *mockBroker, typ protocol.PacketType, id uint16, code protocol.ReasonCode, reason string) {
	t.Helper()
	ack := &protocol.AckPacket{
		Version: protocol.V50, Type: typ, PacketID: id, ReasonCode: code,
		Properties: &protocol.Properties{ReasonString: reason},
	}
	var buf bytes.Buffer
	if err := ack.EncodeAck(&buf); err != nil {
		t.Fatalf("encode %s: %v", typ, err)
	}
	if err := b.InjectRawFrame(buf.Bytes()); err != nil {
		t.Fatalf("inject %s: %v", typ, err)
	}
}

func TestPublishRejectedPubackYieldsReasonError(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "puback-reject")
	cfg.AckTimeout = 4 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	b.DropNextPuback(1)
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errCh <- c.Publish(ctx, "puback/reject", []byte("x"), QoS1, false)
	}()
	if !lcPoll(time.Second, func() bool { return len(b.Published()) >= 1 }) {
		t.Fatal("PUBLISH never reached the broker")
	}
	msgs, _ := c.store.All()
	if len(msgs) != 1 {
		t.Fatalf("stored entries = %d, want 1", len(msgs))
	}
	injectAckWithReason(t, b, protocol.Puback, msgs[0].ID, protocol.QuotaExceeded, "too busy")

	select {
	case err := <-errCh:
		var re *ReasonError
		if !errors.As(err, &re) {
			t.Fatalf("err = %v, want *ReasonError", err)
		}
		if re.Packet != "PUBLISH" || re.Code != protocol.QuotaExceeded || re.Reason != "too busy" {
			t.Fatalf("ReasonError = %+v", re)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish never returned")
	}
}

func TestPublishRejectedPubrecYieldsReasonError(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "pubrec-reject")
	cfg.AckTimeout = 4 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	b.DropNextPubrec(1)
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errCh <- c.Publish(ctx, "pubrec/reject", []byte("y"), QoS2, false)
	}()
	if !lcPoll(time.Second, func() bool { return len(b.Published()) >= 1 }) {
		t.Fatal("PUBLISH never reached the broker")
	}
	msgs, _ := c.store.All()
	if len(msgs) != 1 {
		t.Fatalf("stored entries = %d, want 1", len(msgs))
	}
	injectAckWithReason(t, b, protocol.Pubrec, msgs[0].ID, protocol.NotAuthorized, "")

	select {
	case err := <-errCh:
		var re *ReasonError
		if !errors.As(err, &re) {
			t.Fatalf("err = %v, want *ReasonError", err)
		}
		if re.Packet != "PUBLISH" || re.Code != protocol.NotAuthorized {
			t.Fatalf("ReasonError = %+v", re)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish never returned")
	}
}

// TestQoS2DuplicatePubrecInPubrelStateResendsPubrel proves a second PUBREC
// for an identifier already advanced to the PUBREL state (its own PUBCOMP
// still outstanding) just re-sends the PUBREL rather than restarting the
// exchange.
func TestQoS2DuplicatePubrecInPubrelStateResendsPubrel(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	cfg := newIntegrationConfig(b.URL(), "pubrec-dup")
	cfg.AckTimeout = 4 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	// Keep the exchange parked in the PUBREL state (rather than
	// completing near-instantly over loopback) by dropping the first
	// PUBCOMP, so there is a window to inject the duplicate PUBREC.
	b.DropNextPubcomp(1)

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		errCh <- c.Publish(ctx, "pubrec/dup", []byte("z"), QoS2, false)
	}()

	var id uint16
	if !lcPoll(2*time.Second, func() bool {
		msgs, _ := c.store.All()
		for _, m := range msgs {
			if m.Kind == StoredPubrel {
				id = m.ID
				return true
			}
		}
		return false
	}) {
		t.Fatal("client never reached the PUBREL state")
	}

	// A second, duplicate success PUBREC for the same identifier: the
	// client must resend PUBREL (observed as the broker's PUBCOMP still
	// completing the exchange), not restart from PUBLISH.
	injectAckWithReason(t, b, protocol.Pubrec, id, protocol.Success, "")

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish never completed after the duplicate PUBREC")
	}
}

// ---------------------------------------------------------------------------
// publishAcked internal failure branches
// ---------------------------------------------------------------------------

func TestPublishIDExhaustionSurfacesError(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "id-exhausted"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	for i := range c.ids.used {
		c.ids.used[i] = ^uint64(0) // whitebox: mark every identifier in use
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Publish(ctx, "exhausted/topic", []byte("x"), QoS1, false); !errors.Is(err, ErrPacketIDExhausted) {
		t.Fatalf("err = %v, want ErrPacketIDExhausted", err)
	}
}

// failingStore is a [SessionStore] whose Save always fails, so the
// publishAcked cleanup path (release the id, return the error) can be
// exercised deterministically.
type failingStore struct{ saveErr error }

func (s *failingStore) Save(StoredMessage) error        { return s.saveErr }
func (s *failingStore) Delete(uint16, StoredKind) error { return nil }
func (s *failingStore) All() ([]StoredMessage, error)   { return nil, nil }
func (s *failingStore) Reset() error                    { return nil }

func TestPublishStoreSaveFailureReturnsErrAndReleasesID(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	c := NewTCPClient(newIntegrationConfig(b.URL(), "store-fail"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	wantErr := errors.New("store: boom")
	c.store = &failingStore{saveErr: wantErr} // whitebox: same-package field swap

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Publish(ctx, "store/fail", []byte("x"), QoS1, false); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

func TestPublishWriteFrameTooLargeReleasesState(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	maxSize := uint32(10)
	b.SetConnackProperties(&protocol.Properties{MaximumPacketSize: &maxSize})
	c := NewTCPClient(newIntegrationConfig(b.URL(), "toolarge"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	payload := []byte(strings.Repeat("x", 100))
	if err := c.Publish(ctx, "toolarge/topic", payload, QoS1, false); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("err = %v, want ErrPacketTooLarge", err)
	}
	// The id and quota permit must have been released: a follow-up
	// publish tiny enough to fit under the same 10-byte ceiling still
	// works on the same connection.
	if err := c.Publish(ctx, "a", nil, QoS1, false); err != nil {
		t.Fatalf("follow-up publish: %v", err)
	}
}

func TestSubscribeWriteFrameTooLarge(t *testing.T) {
	t.Parallel()

	b := newMockBroker(t)
	maxSize := uint32(10)
	b.SetConnackProperties(&protocol.Properties{MaximumPacketSize: &maxSize})
	c := NewTCPClient(newIntegrationConfig(b.URL(), "sub-toolarge"))
	mustConnect(t, c)
	defer func() { _ = c.Disconnect(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	longFilter := strings.Repeat("a/", 100) + "b"
	if _, err := c.Subscribe(ctx, longFilter, QoS0, func(*Message) {}); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("err = %v, want ErrPacketTooLarge", err)
	}
}

// ---------------------------------------------------------------------------
// requestAck (SUBSCRIBE/UNSUBSCRIBE): ctx-cancel, and connection loss,
// while still waiting on an ack a broker double that never answers.
// ---------------------------------------------------------------------------

// silentBroker accepts exactly one connection, replies to CONNECT with a
// success CONNACK, and then never acknowledges anything else — letting
// tests deterministically exercise the ctx-cancel/timeout/connection-loss
// paths of an ack wait without racing a real broker's near-instant reply.
type silentBroker struct {
	ln     net.Listener
	connCh chan net.Conn
}

func newSilentBroker(t *testing.T, version protocol.Version) *silentBroker {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	sb := &silentBroker{ln: ln, connCh: make(chan net.Conn, 1)}
	t.Cleanup(func() { _ = ln.Close() })
	go sb.serve(version)
	return sb
}

func (s *silentBroker) serve(version protocol.Version) {
	conn, err := s.ln.Accept()
	if err != nil {
		return
	}
	s.connCh <- conn
	br := bufio.NewReader(conn)
	if _, err := protocol.ReadFrame(br, 1<<20); err != nil { // CONNECT
		return
	}
	body := []byte{0x00, 0x00}
	if version == protocol.V50 {
		body = []byte{0x00, 0x00, 0x00}
	}
	frame := append([]byte{byte(protocol.Connack) << 4, byte(len(body))}, body...)
	if _, err := conn.Write(frame); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, br) // swallow everything else, never ack
}

func (s *silentBroker) URL() string { return "tcp://" + s.ln.Addr().String() }

// resetConn abruptly closes the (single) accepted connection.
func (s *silentBroker) resetConn(t *testing.T) {
	t.Helper()
	select {
	case conn := <-s.connCh:
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = conn.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("silentBroker: no connection accepted")
	}
}

func TestSubscribeCtxCancelWhileWaitingForAck(t *testing.T) {
	t.Parallel()

	sb := newSilentBroker(t, protocol.V50)
	cfg := newIntegrationConfig(sb.URL(), "sub-ctxcancel")
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(ctx, "never/acked", QoS0, func(*Message) {})
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the SUBSCRIBE actually go out
	start := time.Now()
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("cancel took %v to take effect", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe never returned after ctx cancel")
	}
}

func TestUnsubscribeCtxCancelWhileWaitingForAck(t *testing.T) {
	t.Parallel()

	sb := newSilentBroker(t, protocol.V50)
	cfg := newIntegrationConfig(sb.URL(), "unsub-ctxcancel")
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.Unsubscribe(ctx, "never/acked") }()
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Unsubscribe never returned after ctx cancel")
	}
}

func TestSubscribeConnectionLostWhileWaitingForAck(t *testing.T) {
	t.Parallel()

	sb := newSilentBroker(t, protocol.V50)
	cfg := newIntegrationConfig(sb.URL(), "sub-connlost")
	cfg.AckTimeout = 5 * time.Second
	c := NewTCPClient(cfg)
	mustConnect(t, c)

	errCh := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(context.Background(), "never/acked", QoS0, func(*Message) {})
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	sb.resetConn(t)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConnectionLost) {
			t.Fatalf("err = %v, want ErrConnectionLost", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("connection loss took %v to surface", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe never returned after the connection reset")
	}
}
