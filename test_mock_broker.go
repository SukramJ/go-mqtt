// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// mockBroker is a scripted MQTT broker test double used by this package's
// own tests (lifecycle, adapter). It is an expect/respond stand-in, NOT a
// real broker state machine: it parses just enough of CONNECT/SUBSCRIBE/
// UNSUBSCRIBE (which the protocol package deliberately never decodes,
// since a client only ever sends them) to record what was requested, and
// answers with scriptable, canned responses. It understands both MQTT
// 3.1.1 and MQTT 5.0 on the wire.
//
// It is kept in a non-"_test.go" file (like its predecessor) so it is
// reachable from every test file in the package without visibility
// tricks, while remaining entirely unexported so nothing here is part of
// the module's public API.
//
// All accept/reply plumbing is goroutine-safe: the accept loop, the
// per-connection read loop and every Inject*/scripting call may run
// concurrently.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

// mockMaxRemainingLength bounds the frames the broker will read from a
// client, mirroring the cap a real broker would advertise.
const mockMaxRemainingLength = 16 * 1024 * 1024

// mockAckTimeout bounds how long InjectPublish waits for the client to
// complete the QoS 1/2 handshake before giving up.
const mockAckTimeout = 5 * time.Second

// errMockNotConnected is returned by Inject* calls made while no client is
// connected.
var errMockNotConnected = errors.New("mockbroker: no active connection")

// mockPublished is a recorded inbound PUBLISH (client -> broker).
type mockPublished struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Dup        bool
	Properties *protocol.Properties
}

// mockBroker is a multi-connection scripted MQTT broker listening on a
// random local port.
type mockBroker struct {
	t        *testing.T
	listener net.Listener

	mu  sync.Mutex
	cur *mockLink // the most recently accepted connection, nil once closed

	connCount atomic.Int32

	// CONNACK scripting.
	rejectConnect  mockOneShotByte
	sessionPresent atomic.Bool
	connackProps   atomic.Pointer[protocol.Properties]

	// CONNECT recording (last connection seen).
	connMu           sync.Mutex
	lastVersion      protocol.Version
	lastCleanStart   bool
	lastConnectProps *protocol.Properties

	// SUBSCRIBE scripting + recording.
	subAck    mockSubAckPolicy
	subFrames atomic.Int32
	subMu     sync.Mutex
	subs      []protocol.Subscription

	// Inbound PUBLISH recording + ack-flow scripting.
	pubMu           sync.Mutex
	published       []mockPublished
	dropNextPuback  atomic.Int32
	dupNextPuback   atomic.Bool
	dropNextPubrec  atomic.Int32
	dropNextPubcomp atomic.Int32

	// PINGREQ scripting + counting.
	dropPings     atomic.Bool
	dropNextPings atomic.Int32
	pingCount     atomic.Int32
}

// newMockBroker starts a listener on a random local port. The listener is
// closed automatically when t finishes.
func newMockBroker(t *testing.T) *mockBroker {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("mockbroker: listen: %v", err)
	}
	b := &mockBroker{t: t, listener: ln}
	go b.acceptLoop()
	t.Cleanup(func() { _ = ln.Close() })
	return b
}

// URL returns the tcp:// URL the broker is listening on.
func (b *mockBroker) URL() string { return "tcp://" + b.listener.Addr().String() }

// ConnCount returns the total number of accepted TCP connections.
func (b *mockBroker) ConnCount() int { return int(b.connCount.Load()) }

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

// mockLink is one accepted client connection.
type mockLink struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	writeMu sync.Mutex

	version protocol.Version // set once the CONNECT is parsed

	idMu   sync.Mutex
	nextID uint16

	ackMu    sync.Mutex
	pubacks  map[uint16]chan struct{}
	pubcomps map[uint16]chan struct{}
}

func newMockLink(conn net.Conn) *mockLink {
	return &mockLink{
		conn:     conn,
		br:       bufio.NewReader(conn),
		bw:       bufio.NewWriter(conn),
		pubacks:  make(map[uint16]chan struct{}),
		pubcomps: make(map[uint16]chan struct{}),
	}
}

// acceptLoop loops forever accepting connections until the listener closes.
func (b *mockBroker) acceptLoop() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.connCount.Add(1)
		l := newMockLink(conn)
		b.mu.Lock()
		b.cur = l
		b.mu.Unlock()
		go b.serve(l)
	}
}

// currentLink returns the most recently accepted connection, or nil if
// none is active.
func (b *mockBroker) currentLink() *mockLink {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cur
}

// serve is the per-connection read loop: it decodes every inbound frame
// and drives the scripted CONNACK/SUBACK/UNSUBACK/PUBACK/PUBREC/PUBCOMP/
// PINGRESP replies.
func (b *mockBroker) serve(l *mockLink) {
	defer func() {
		b.mu.Lock()
		if b.cur == l {
			b.cur = nil
		}
		b.mu.Unlock()
		_ = l.conn.Close()
	}()

	connected := false
	for {
		frame, err := protocol.ReadFrame(l.br, mockMaxRemainingLength)
		if err != nil {
			return
		}
		pt := frame.PacketType()
		if !connected && pt != protocol.Connect {
			b.t.Errorf("mockbroker: expected CONNECT, got %s", pt)
			return
		}

		switch pt {
		case protocol.Connect:
			info, err := decodeMockConnect(frame.Body)
			if err != nil {
				b.t.Errorf("mockbroker: decode CONNECT: %v", err)
				return
			}
			l.version = info.version
			connected = true
			b.recordConnect(info)
			if err := l.sendRaw(byte(protocol.Connack)<<4, b.buildConnack(info.version)); err != nil {
				return
			}

		case protocol.Subscribe:
			info, err := decodeMockSubscribe(l.version, frame.Body)
			if err != nil {
				b.t.Errorf("mockbroker: decode SUBSCRIBE: %v", err)
				return
			}
			b.recordSubscriptions(info.subs)
			if err := l.sendRaw(byte(protocol.Suback)<<4, b.buildSuback(l.version, info.packetID, info.subs)); err != nil {
				return
			}

		case protocol.Unsubscribe:
			info, err := decodeMockUnsubscribe(l.version, frame.Body)
			if err != nil {
				b.t.Errorf("mockbroker: decode UNSUBSCRIBE: %v", err)
				return
			}
			body := buildMockUnsuback(l.version, info.packetID, len(info.filters))
			if err := l.sendRaw(byte(protocol.Unsuback)<<4, body); err != nil {
				return
			}

		case protocol.Publish:
			pub, err := protocol.DecodePublish(l.version, frame.Header, frame.Body)
			if err != nil {
				b.t.Errorf("mockbroker: decode PUBLISH: %v", err)
				return
			}
			b.recordPublished(pub)
			switch pub.QoS {
			case 1:
				b.replyPuback(l, pub.PacketID)
			case 2:
				b.replyPubrec(l, pub.PacketID)
			}

		case protocol.Pubrel:
			ack, err := protocol.DecodeAck(l.version, protocol.Pubrel, frame.Body)
			if err != nil {
				b.t.Errorf("mockbroker: decode PUBREL: %v", err)
				return
			}
			b.replyPubcomp(l, ack.PacketID)

		case protocol.Puback:
			ack, err := protocol.DecodeAck(l.version, protocol.Puback, frame.Body)
			if err != nil {
				b.t.Errorf("mockbroker: decode PUBACK: %v", err)
				return
			}
			l.resolve(l.pubacks, ack.PacketID)

		case protocol.Pubrec:
			ack, err := protocol.DecodeAck(l.version, protocol.Pubrec, frame.Body)
			if err != nil {
				b.t.Errorf("mockbroker: decode PUBREC: %v", err)
				return
			}
			// The broker is the original publisher of an injected QoS 2
			// message: reply PUBREL right away and wait for PUBCOMP.
			rel := &protocol.AckPacket{Version: l.version, Type: protocol.Pubrel, PacketID: ack.PacketID}
			if err := l.send(rel.EncodeAck); err != nil {
				return
			}

		case protocol.Pubcomp:
			ack, err := protocol.DecodeAck(l.version, protocol.Pubcomp, frame.Body)
			if err != nil {
				b.t.Errorf("mockbroker: decode PUBCOMP: %v", err)
				return
			}
			l.resolve(l.pubcomps, ack.PacketID)

		case protocol.Pingreq:
			b.pingCount.Add(1)
			if b.dropPings.Load() || mockTakeCounter(&b.dropNextPings) {
				continue
			}
			if err := l.send(protocol.EncodePingResp); err != nil {
				return
			}

		case protocol.Disconnect:
			return

		default:
			// Unhandled packet types (e.g. a stray AUTH) are ignored: this
			// is a scripted double, not a spec-complete broker.
		}
	}
}

// ---------------------------------------------------------------------------
// Wire helpers local to the mock (the protocol package keeps its encoder/
// cursor internals private, and never decodes CONNECT/SUBSCRIBE/
// UNSUBSCRIBE since only a client sends them).
// ---------------------------------------------------------------------------

// send encodes via fn into a scratch buffer, then writes it to the
// connection under the link's write lock in a single call.
func (l *mockLink) send(fn func(io.Writer) error) error {
	var buf bytes.Buffer
	if err := fn(&buf); err != nil {
		return err
	}
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	if _, err := l.bw.Write(buf.Bytes()); err != nil {
		return err
	}
	return l.bw.Flush()
}

// sendRaw writes a fixed-header/body frame the mock hand-assembled
// (CONNACK/SUBACK/UNSUBACK/AUTH — packets the protocol package only
// decodes, never encodes, since a client never sends them).
func (l *mockLink) sendRaw(header byte, body []byte) error {
	return l.send(func(w io.Writer) error { return writeMockFrame(w, header, body) })
}

func writeMockFrame(w io.Writer, header byte, body []byte) error {
	var hdr bytes.Buffer
	hdr.WriteByte(header)
	hdr.Write(encodeMockVarint(len(body)))
	if _, err := w.Write(hdr.Bytes()); err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	_, err := w.Write(body)
	return err
}

func encodeMockVarint(n int) []byte {
	v := uint32(n) //nolint:gosec // mock-only; bodies are test-sized
	var out []byte
	for {
		d := byte(v & 0x7F)
		v >>= 7
		if v > 0 {
			d |= 0x80
		}
		out = append(out, d)
		if v == 0 {
			return out
		}
	}
}

func writeMockU16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

func writeMockU32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func writeMockString(buf *bytes.Buffer, s string) {
	writeMockU16(buf, uint16(len(s))) //nolint:gosec // mock-only; test strings are short
	buf.WriteString(s)
}

// mCursor is a minimal bounds-checked reader over a packet body, private
// to this file. It exists only because the protocol package's own cursor
// is unexported and CONNECT/SUBSCRIBE/UNSUBSCRIBE have no public decoder
// (a client only ever encodes them).
type mCursor struct {
	buf []byte
	pos int
}

func newMCursor(b []byte) *mCursor { return &mCursor{buf: b} }

func (c *mCursor) remaining() int { return len(c.buf) - c.pos }

var errMockTruncated = errors.New("mockbroker: truncated field")

func (c *mCursor) readByte() (byte, error) {
	if c.remaining() < 1 {
		return 0, errMockTruncated
	}
	v := c.buf[c.pos]
	c.pos++
	return v, nil
}

func (c *mCursor) readUint16() (uint16, error) {
	if c.remaining() < 2 {
		return 0, errMockTruncated
	}
	v := binary.BigEndian.Uint16(c.buf[c.pos : c.pos+2])
	c.pos += 2
	return v, nil
}

func (c *mCursor) readUint32() (uint32, error) {
	if c.remaining() < 4 {
		return 0, errMockTruncated
	}
	v := binary.BigEndian.Uint32(c.buf[c.pos : c.pos+4])
	c.pos += 4
	return v, nil
}

func (c *mCursor) readVarint() (uint32, error) {
	var value uint32
	for i := range 4 {
		b, err := c.readByte()
		if err != nil {
			return 0, err
		}
		value |= uint32(b&0x7F) << (7 * i)
		if b&0x80 == 0 {
			return value, nil
		}
	}
	return 0, errMockTruncated
}

func (c *mCursor) readString() (string, error) {
	n, err := c.readUint16()
	if err != nil {
		return "", err
	}
	if c.remaining() < int(n) {
		return "", errMockTruncated
	}
	s := string(c.buf[c.pos : c.pos+int(n)])
	c.pos += int(n)
	return s, nil
}

func (c *mCursor) readBinary() ([]byte, error) {
	n, err := c.readUint16()
	if err != nil {
		return nil, err
	}
	if c.remaining() < int(n) {
		return nil, errMockTruncated
	}
	out := make([]byte, n)
	copy(out, c.buf[c.pos:c.pos+int(n)])
	c.pos += int(n)
	return out, nil
}

// decodeMockProperties reads an MQTT 5.0 property block (varint length
// prefix + that many bytes) and populates every property this codec
// knows about. Unlike the protocol package's own decoder it does not
// enforce the per-packet-type allow-list or reject duplicates — this is a
// lenient test double, not a spec conformance checker — but it must still
// recognise every property's value shape correctly or it could not skip
// past properties the caller does not care about.
func decodeMockProperties(c *mCursor) (*protocol.Properties, error) {
	length, err := c.readVarint()
	if err != nil {
		return nil, err
	}
	n := int(length)
	if n > c.remaining() {
		return nil, fmt.Errorf("mockbroker: property length %d overruns packet", n)
	}
	sub := newMCursor(c.buf[c.pos : c.pos+n])
	c.pos += n
	if n == 0 {
		return nil, nil //nolint:nilnil // absent property block is a legitimate nil-nil result
	}

	p := &protocol.Properties{}
	for sub.remaining() > 0 {
		id, err := sub.readByte()
		if err != nil {
			return nil, err
		}
		if err := decodeMockProperty(sub, id, p); err != nil {
			return nil, err
		}
	}
	return p, nil
}

//nolint:cyclop,gocyclo // mechanical one-case-per-property-id switch, mirrors protocol.Properties' own field list
func decodeMockProperty(c *mCursor, id byte, p *protocol.Properties) error {
	switch id {
	case 0x01:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.PayloadFormat = &v
	case 0x02:
		v, err := c.readUint32()
		if err != nil {
			return err
		}
		p.MessageExpiryInterval = &v
	case 0x03:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ContentType = s
	case 0x08:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ResponseTopic = s
	case 0x09:
		v, err := c.readBinary()
		if err != nil {
			return err
		}
		p.CorrelationData = v
	case 0x0B:
		v, err := c.readVarint()
		if err != nil {
			return err
		}
		p.SubscriptionIdentifiers = append(p.SubscriptionIdentifiers, v)
	case 0x11:
		v, err := c.readUint32()
		if err != nil {
			return err
		}
		p.SessionExpiryInterval = &v
	case 0x12:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.AssignedClientID = s
	case 0x13:
		v, err := c.readUint16()
		if err != nil {
			return err
		}
		p.ServerKeepAlive = &v
	case 0x15:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.AuthMethod = s
	case 0x16:
		v, err := c.readBinary()
		if err != nil {
			return err
		}
		p.AuthData = v
	case 0x17:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.RequestProblemInfo = &v
	case 0x18:
		v, err := c.readUint32()
		if err != nil {
			return err
		}
		p.WillDelayInterval = &v
	case 0x19:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.RequestResponseInfo = &v
	case 0x1A:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ResponseInfo = s
	case 0x1C:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ServerReference = s
	case 0x1F:
		s, err := c.readString()
		if err != nil {
			return err
		}
		p.ReasonString = s
	case 0x21:
		v, err := c.readUint16()
		if err != nil {
			return err
		}
		p.ReceiveMaximum = &v
	case 0x22:
		v, err := c.readUint16()
		if err != nil {
			return err
		}
		p.TopicAliasMaximum = &v
	case 0x23:
		v, err := c.readUint16()
		if err != nil {
			return err
		}
		p.TopicAlias = &v
	case 0x24:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.MaximumQoS = &v
	case 0x25:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.RetainAvailable = &v
	case 0x26:
		k, err := c.readString()
		if err != nil {
			return err
		}
		v, err := c.readString()
		if err != nil {
			return err
		}
		p.UserProperties = append(p.UserProperties, protocol.UserProperty{Key: k, Value: v})
	case 0x27:
		v, err := c.readUint32()
		if err != nil {
			return err
		}
		p.MaximumPacketSize = &v
	case 0x28:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.WildcardSubAvailable = &v
	case 0x29:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.SubIDAvailable = &v
	case 0x2A:
		v, err := c.readByte()
		if err != nil {
			return err
		}
		p.SharedSubAvailable = &v
	default:
		return fmt.Errorf("mockbroker: unknown property 0x%02X", id)
	}
	return nil
}

// ---------------------------------------------------------------------------
// CONNECT / CONNACK
// ---------------------------------------------------------------------------

// mockConnectInfo is everything the mock recovers from a client CONNECT.
type mockConnectInfo struct {
	version    protocol.Version
	cleanStart bool
	properties *protocol.Properties
}

// decodeMockConnect hand-parses a CONNECT body — the inverse of
// [protocol.ConnectPacket.Encode], which this package never provides a
// decoder for since a client only ever sends CONNECT.
func decodeMockConnect(body []byte) (mockConnectInfo, error) {
	c := newMCursor(body)

	if _, err := c.readString(); err != nil { // protocol name "MQTT"
		return mockConnectInfo{}, err
	}
	level, err := c.readByte()
	if err != nil {
		return mockConnectInfo{}, err
	}
	v := protocol.Version(level)
	if !v.Valid() {
		return mockConnectInfo{}, fmt.Errorf("mockbroker: unsupported CONNECT level %d", level)
	}

	flags, err := c.readByte()
	if err != nil {
		return mockConnectInfo{}, err
	}
	hasUser := flags&0x80 != 0
	hasPass := flags&0x40 != 0
	hasWill := flags&0x04 != 0
	cleanStart := flags&0x02 != 0

	if _, err := c.readUint16(); err != nil { // keep alive
		return mockConnectInfo{}, err
	}

	var props *protocol.Properties
	if v == protocol.V50 {
		props, err = decodeMockProperties(c)
		if err != nil {
			return mockConnectInfo{}, err
		}
	}

	if _, err := c.readString(); err != nil { // client identifier, not recorded
		return mockConnectInfo{}, err
	}

	if hasWill {
		if v == protocol.V50 {
			if _, err := decodeMockProperties(c); err != nil { // will properties, not recorded
				return mockConnectInfo{}, err
			}
		}
		if _, err := c.readString(); err != nil { // will topic
			return mockConnectInfo{}, err
		}
		if _, err := c.readBinary(); err != nil { // will payload
			return mockConnectInfo{}, err
		}
	}
	if hasUser {
		if _, err := c.readString(); err != nil {
			return mockConnectInfo{}, err
		}
	}
	if hasPass {
		if _, err := c.readBinary(); err != nil {
			return mockConnectInfo{}, err
		}
	}

	return mockConnectInfo{version: v, cleanStart: cleanStart, properties: props}, nil
}

func (b *mockBroker) recordConnect(info mockConnectInfo) {
	b.connMu.Lock()
	b.lastVersion = info.version
	b.lastCleanStart = info.cleanStart
	b.lastConnectProps = info.properties
	b.connMu.Unlock()
}

// ProtocolVersion returns the protocol level of the most recent CONNECT.
func (b *mockBroker) ProtocolVersion() protocol.Version {
	b.connMu.Lock()
	defer b.connMu.Unlock()
	return b.lastVersion
}

// CleanStart returns the Clean Start (v5) / Clean Session (v3) bit of the
// most recent CONNECT.
func (b *mockBroker) CleanStart() bool {
	b.connMu.Lock()
	defer b.connMu.Unlock()
	return b.lastCleanStart
}

// ConnectProperties returns the CONNECT property block of the most recent
// connection, nil for MQTT 3.1.1 or an empty v5 block.
func (b *mockBroker) ConnectProperties() *protocol.Properties {
	b.connMu.Lock()
	defer b.connMu.Unlock()
	return b.lastConnectProps
}

// mockOneShotByte is a scriptable "next call only" byte value, used by
// RejectNextConnect.
type mockOneShotByte struct {
	mu    sync.Mutex
	armed bool
	value byte
}

func (o *mockOneShotByte) arm(v byte) {
	o.mu.Lock()
	o.armed, o.value = true, v
	o.mu.Unlock()
}

func (o *mockOneShotByte) take() (byte, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.armed {
		return 0, false
	}
	o.armed = false
	return o.value, true
}

// RejectNextConnect makes the next CONNACK carry reason as a non-success
// return/reason code, then revert to accepting.
func (b *mockBroker) RejectNextConnect(reason byte) { b.rejectConnect.arm(reason) }

// SetSessionPresent sets the CONNACK Session Present bit used for every
// subsequent connect, until changed again.
func (b *mockBroker) SetSessionPresent(v bool) { b.sessionPresent.Store(v) }

// SetConnackProperties sets the MQTT 5.0 CONNACK properties (a subset:
// ReceiveMaximum, ServerKeepAlive, AssignedClientID, TopicAliasMaximum,
// MaximumPacketSize) attached to every subsequent v5 CONNACK. Pass nil to
// stop attaching any. Ignored on an MQTT 3.1.1 link.
func (b *mockBroker) SetConnackProperties(p *protocol.Properties) { b.connackProps.Store(p) }

// buildConnack assembles the CONNACK body for protocol version v.
func (b *mockBroker) buildConnack(v protocol.Version) []byte {
	var body bytes.Buffer
	var ackFlags byte
	if b.sessionPresent.Load() {
		ackFlags = 0x01
	}
	body.WriteByte(ackFlags)

	reason := byte(protocol.Success)
	if r, ok := b.rejectConnect.take(); ok {
		reason = r
	}
	body.WriteByte(reason)

	if v == protocol.V50 {
		appendMockConnackProps(&body, b.connackProps.Load())
	}
	return body.Bytes()
}

// appendMockConnackProps writes the scriptable CONNACK property subset
// (see [mockBroker.SetConnackProperties]) as a length-prefixed block.
func appendMockConnackProps(body *bytes.Buffer, p *protocol.Properties) {
	var props bytes.Buffer
	if p != nil {
		if p.ReceiveMaximum != nil {
			props.WriteByte(0x21)
			writeMockU16(&props, *p.ReceiveMaximum)
		}
		if p.ServerKeepAlive != nil {
			props.WriteByte(0x13)
			writeMockU16(&props, *p.ServerKeepAlive)
		}
		if p.AssignedClientID != "" {
			props.WriteByte(0x12)
			writeMockString(&props, p.AssignedClientID)
		}
		if p.TopicAliasMaximum != nil {
			props.WriteByte(0x22)
			writeMockU16(&props, *p.TopicAliasMaximum)
		}
		if p.MaximumPacketSize != nil {
			props.WriteByte(0x27)
			writeMockU32(&props, *p.MaximumPacketSize)
		}
	}
	body.Write(encodeMockVarint(props.Len()))
	body.Write(props.Bytes())
}

// ---------------------------------------------------------------------------
// SUBSCRIBE / SUBACK, UNSUBSCRIBE / UNSUBACK
// ---------------------------------------------------------------------------

type mockSubscribeInfo struct {
	packetID uint16
	subs     []protocol.Subscription
}

// decodeMockSubscribe hand-parses a SUBSCRIBE body — the inverse of
// [protocol.SubscribePacket.Encode].
func decodeMockSubscribe(v protocol.Version, body []byte) (mockSubscribeInfo, error) {
	c := newMCursor(body)
	pid, err := c.readUint16()
	if err != nil {
		return mockSubscribeInfo{}, err
	}
	if v == protocol.V50 {
		if _, err := decodeMockProperties(c); err != nil {
			return mockSubscribeInfo{}, err
		}
	}
	var subs []protocol.Subscription
	for c.remaining() > 0 {
		filter, err := c.readString()
		if err != nil {
			return mockSubscribeInfo{}, err
		}
		opt, err := c.readByte()
		if err != nil {
			return mockSubscribeInfo{}, err
		}
		sub := protocol.Subscription{Filter: filter, Options: protocol.SubscribeOptions{QoS: opt & 0x03}}
		if v == protocol.V50 {
			sub.Options.NoLocal = opt&0x04 != 0
			sub.Options.RetainAsPublished = opt&0x08 != 0
			sub.Options.RetainHandling = (opt >> 4) & 0x03
		}
		subs = append(subs, sub)
	}
	if len(subs) == 0 {
		return mockSubscribeInfo{}, errors.New("mockbroker: SUBSCRIBE with no filters")
	}
	return mockSubscribeInfo{packetID: pid, subs: subs}, nil
}

type mockUnsubscribeInfo struct {
	packetID uint16
	filters  []string
}

// decodeMockUnsubscribe hand-parses an UNSUBSCRIBE body — the inverse of
// [protocol.UnsubscribePacket.Encode].
func decodeMockUnsubscribe(v protocol.Version, body []byte) (mockUnsubscribeInfo, error) {
	c := newMCursor(body)
	pid, err := c.readUint16()
	if err != nil {
		return mockUnsubscribeInfo{}, err
	}
	if v == protocol.V50 {
		if _, err := decodeMockProperties(c); err != nil {
			return mockUnsubscribeInfo{}, err
		}
	}
	var filters []string
	for c.remaining() > 0 {
		f, err := c.readString()
		if err != nil {
			return mockUnsubscribeInfo{}, err
		}
		filters = append(filters, f)
	}
	if len(filters) == 0 {
		return mockUnsubscribeInfo{}, errors.New("mockbroker: UNSUBSCRIBE with no filters")
	}
	return mockUnsubscribeInfo{packetID: pid, filters: filters}, nil
}

func (b *mockBroker) recordSubscriptions(subs []protocol.Subscription) {
	b.subFrames.Add(1)
	b.subMu.Lock()
	b.subs = append(b.subs, subs...)
	b.subMu.Unlock()
}

// SubscribeCount returns the total number of SUBSCRIBE frames received
// across every connection.
func (b *mockBroker) SubscribeCount() int { return int(b.subFrames.Load()) }

// Subscriptions returns every filter+options pair received, across every
// SUBSCRIBE frame and every connection, in receive order.
func (b *mockBroker) Subscriptions() []protocol.Subscription {
	b.subMu.Lock()
	defer b.subMu.Unlock()
	out := make([]protocol.Subscription, len(b.subs))
	copy(out, b.subs)
	return out
}

// mockSubAckPolicy scripts the reason code the mock grants for every
// filter in the next (and every subsequent, until changed) SUBACK.
type mockSubAckPolicy struct {
	mu       sync.Mutex
	reject   bool
	reason   byte
	override bool
	qos      byte
}

func (p *mockSubAckPolicy) setReject(reason byte) {
	p.mu.Lock()
	p.reject, p.reason, p.override = true, reason, false
	p.mu.Unlock()
}

func (p *mockSubAckPolicy) setGrant(qos byte) {
	p.mu.Lock()
	p.override, p.qos, p.reject = true, qos, false
	p.mu.Unlock()
}

func (p *mockSubAckPolicy) resolve(requested byte) byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.reject:
		return p.reason
	case p.override:
		return p.qos
	default:
		return requested
	}
}

// RejectSubscribe makes every filter in subsequent SUBACKs come back with
// reason (a failure code, >= 0x80), until [mockBroker.GrantQoS] or another
// call to RejectSubscribe changes it.
func (b *mockBroker) RejectSubscribe(reason byte) { b.subAck.setReject(reason) }

// GrantQoS overrides the granted QoS in subsequent SUBACKs to qos
// (instead of echoing each filter's requested QoS), until
// [mockBroker.RejectSubscribe] or another call to GrantQoS changes it.
func (b *mockBroker) GrantQoS(qos byte) { b.subAck.setGrant(qos) }

// buildSuback assembles the SUBACK body: packet id, an empty (v5)
// property block, then one resolved reason/QoS byte per subscription.
func (b *mockBroker) buildSuback(v protocol.Version, pid uint16, subs []protocol.Subscription) []byte {
	var body bytes.Buffer
	writeMockU16(&body, pid)
	if v == protocol.V50 {
		body.WriteByte(0x00)
	}
	for _, s := range subs {
		body.WriteByte(b.subAck.resolve(s.Options.QoS))
	}
	return body.Bytes()
}

// buildMockUnsuback assembles the UNSUBACK body: a bare packet id for
// MQTT 3.1.1, or packet id + empty property block + one Success reason
// code per filter for MQTT 5.0.
func buildMockUnsuback(v protocol.Version, pid uint16, filterCount int) []byte {
	var body bytes.Buffer
	writeMockU16(&body, pid)
	if v == protocol.V50 {
		body.WriteByte(0x00)
		for range filterCount {
			body.WriteByte(byte(protocol.Success))
		}
	}
	return body.Bytes()
}

// ---------------------------------------------------------------------------
// Inbound PUBLISH (client -> broker) and its QoS 1/2 acks
// ---------------------------------------------------------------------------

func (b *mockBroker) recordPublished(p *protocol.PublishPacket) {
	b.pubMu.Lock()
	b.published = append(b.published, mockPublished{
		Topic: p.Topic, Payload: p.Payload, QoS: p.QoS,
		Retain: p.Retain, Dup: p.Dup, Properties: p.Properties,
	})
	b.pubMu.Unlock()
}

// Published returns every inbound PUBLISH received, in receive order.
func (b *mockBroker) Published() []mockPublished {
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	out := make([]mockPublished, len(b.published))
	copy(out, b.published)
	return out
}

// DropNextPuback makes the broker swallow the next n PUBACKs it would
// otherwise send for inbound QoS 1 PUBLISHes, so a client's resend path
// can be exercised.
func (b *mockBroker) DropNextPuback(n int) { b.dropNextPuback.Store(int32(n)) } //nolint:gosec // test-only small counts

// DuplicateNextPuback makes the broker send the next PUBACK for an
// inbound QoS 1 PUBLISH twice, so a client's duplicate-ack handling can
// be exercised.
func (b *mockBroker) DuplicateNextPuback() { b.dupNextPuback.Store(true) }

// DropNextPubrec makes the broker swallow the next n PUBRECs it would
// otherwise send for inbound QoS 2 PUBLISHes.
func (b *mockBroker) DropNextPubrec(n int) { b.dropNextPubrec.Store(int32(n)) } //nolint:gosec // test-only small counts

// DropNextPubcomp makes the broker swallow the next n PUBCOMPs it would
// otherwise send after an inbound PUBREL.
func (b *mockBroker) DropNextPubcomp(n int) { b.dropNextPubcomp.Store(int32(n)) } //nolint:gosec // test-only small counts

func (b *mockBroker) replyPuback(l *mockLink, id uint16) {
	if mockTakeCounter(&b.dropNextPuback) {
		return
	}
	ack := &protocol.AckPacket{Version: l.version, Type: protocol.Puback, PacketID: id}
	_ = l.send(ack.EncodeAck)
	if b.dupNextPuback.CompareAndSwap(true, false) {
		_ = l.send(ack.EncodeAck)
	}
}

func (b *mockBroker) replyPubrec(l *mockLink, id uint16) {
	if mockTakeCounter(&b.dropNextPubrec) {
		return
	}
	ack := &protocol.AckPacket{Version: l.version, Type: protocol.Pubrec, PacketID: id}
	_ = l.send(ack.EncodeAck)
}

func (b *mockBroker) replyPubcomp(l *mockLink, id uint16) {
	if mockTakeCounter(&b.dropNextPubcomp) {
		return
	}
	ack := &protocol.AckPacket{Version: l.version, Type: protocol.Pubcomp, PacketID: id}
	_ = l.send(ack.EncodeAck)
}

// mockTakeCounter atomically decrements c if positive, reporting whether
// it did (i.e. whether the caller should skip its normal action once).
func mockTakeCounter(c *atomic.Int32) bool {
	for {
		n := c.Load()
		if n <= 0 {
			return false
		}
		if c.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

// ---------------------------------------------------------------------------
// Outbound injection (broker -> client)
// ---------------------------------------------------------------------------

func (l *mockLink) allocID() uint16 {
	l.idMu.Lock()
	defer l.idMu.Unlock()
	l.nextID++
	if l.nextID == 0 {
		l.nextID = 1
	}
	return l.nextID
}

func (l *mockLink) register(m map[uint16]chan struct{}, id uint16) chan struct{} {
	ch := make(chan struct{}, 1)
	l.ackMu.Lock()
	m[id] = ch
	l.ackMu.Unlock()
	return ch
}

func (l *mockLink) resolve(m map[uint16]chan struct{}, id uint16) {
	l.ackMu.Lock()
	ch := m[id]
	delete(m, id)
	l.ackMu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// InjectPublish sends a broker-initiated PUBLISH to the connected client
// and drives the QoS 1/2 handshake to completion: QoS 1 waits for PUBACK;
// QoS 2 waits for PUBREC, replies PUBREL (handled by [mockBroker.serve]),
// then waits for PUBCOMP. QoS 0 returns as soon as the frame is written.
// Returns an error if no client is connected, the write fails, or the
// handshake does not complete within [mockAckTimeout].
func (b *mockBroker) InjectPublish(topic string, payload []byte, qos byte, retain bool, props *protocol.Properties) error {
	l := b.currentLink()
	if l == nil {
		return errMockNotConnected
	}

	var id uint16
	var wait chan struct{}
	if qos > 0 {
		id = l.allocID()
		switch qos {
		case 1:
			wait = l.register(l.pubacks, id)
		default:
			wait = l.register(l.pubcomps, id)
		}
	}

	pkt := &protocol.PublishPacket{
		Version: l.version, Topic: topic, Payload: payload,
		QoS: qos, Retain: retain, PacketID: id, Properties: props,
	}
	if err := l.send(pkt.Encode); err != nil {
		return err
	}
	if qos == 0 {
		return nil
	}

	select {
	case <-wait:
		return nil
	case <-time.After(mockAckTimeout):
		return fmt.Errorf("mockbroker: timed out waiting for the QoS %d ack of PUBLISH id=%d", qos, id)
	}
}

// InjectDisconnect sends a broker-initiated DISCONNECT with reason to the
// connected client.
func (b *mockBroker) InjectDisconnect(reason protocol.ReasonCode) error {
	l := b.currentLink()
	if l == nil {
		return errMockNotConnected
	}
	pkt := &protocol.DisconnectPacket{Version: l.version, ReasonCode: reason}
	return l.send(pkt.Encode)
}

// InjectAuth sends a minimal AUTH packet (reason code Continue
// Authentication, no properties) to the connected client, so a client
// under test can be observed rejecting enhanced authentication.
func (b *mockBroker) InjectAuth() error {
	l := b.currentLink()
	if l == nil {
		return errMockNotConnected
	}
	return l.sendRaw(byte(protocol.Auth)<<4, []byte{byte(protocol.ContinueAuthentication)})
}

// InjectRawFrame writes raw bytes verbatim to the connected client,
// bypassing all packet encoding — for testing how a client reacts to a
// malformed or unexpected frame.
func (b *mockBroker) InjectRawFrame(raw []byte) error {
	l := b.currentLink()
	if l == nil {
		return errMockNotConnected
	}
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	if _, err := l.bw.Write(raw); err != nil {
		return err
	}
	return l.bw.Flush()
}

// InjectTCPReset abruptly closes the active connection (with SO_LINGER 0
// where the connection is a *net.TCPConn, so the client observes a real
// reset rather than an orderly FIN) to simulate a vanished peer.
func (b *mockBroker) InjectTCPReset() {
	b.mu.Lock()
	l := b.cur
	b.cur = nil
	b.mu.Unlock()
	if l == nil {
		return
	}
	if tc, ok := l.conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = l.conn.Close()
}

// ---------------------------------------------------------------------------
// PINGREQ / PINGRESP
// ---------------------------------------------------------------------------

// DropPings toggles whether the broker ignores every PINGREQ, simulating
// a half-open socket for the PINGRESP watchdog.
func (b *mockBroker) DropPings(v bool) { b.dropPings.Store(v) }

// DropNextPings makes the broker swallow the next n PINGREQ frames, then
// answer normally again.
func (b *mockBroker) DropNextPings(n int) { b.dropNextPings.Store(int32(n)) } //nolint:gosec // test-only small counts

// PingCount returns the total number of PINGREQ frames received.
func (b *mockBroker) PingCount() int { return int(b.pingCount.Load()) }
