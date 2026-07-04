// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/SukramJ/go-mqtt/protocol"
)

// StoredKind classifies a [StoredMessage] held in a [SessionStore]. It
// distinguishes the three QoS>0 flow-control artefacts the client must
// persist across a reconnect to complete an in-flight exchange.
type StoredKind uint8

// Stored message kinds.
const (
	// StoredPublish is an outbound QoS 1 or QoS 2 PUBLISH that has been
	// sent but not yet fully acknowledged (awaiting PUBACK, or PUBREC
	// before the PUBREL leg). On a resumed session it is replayed with the
	// DUP flag set.
	StoredPublish StoredKind = iota + 1
	// StoredPubrel is the PUBREL leg of an outbound QoS 2 exchange: the
	// PUBREC has been received, so the original PUBLISH is superseded and
	// only the PUBREL must be resent until PUBCOMP arrives.
	StoredPubrel
	// StoredInboundID records an inbound QoS 2 packet identifier that has
	// been delivered to the application and PUBREC'd, pending the peer's
	// PUBREL (exactly-once receiver state, method A).
	StoredInboundID
)

// String returns a short lower-case label for the kind, suitable for
// structured log fields.
func (k StoredKind) String() string {
	switch k {
	case StoredPublish:
		return "publish"
	case StoredPubrel:
		return "pubrel"
	case StoredInboundID:
		return "inbound-id"
	default:
		return fmt.Sprintf("StoredKind(%d)", uint8(k))
	}
}

// StoredMessage is one entry of persisted QoS>0 session state. ID is the
// MQTT packet identifier the entry is keyed by (together with Kind); Seq
// is a store-assigned monotonic sequence that fixes replay order; Publish
// is the packet to (re)transmit for a [StoredPublish] entry and is nil for
// the identifier-only kinds.
type StoredMessage struct {
	Publish *protocol.PublishPacket
	Seq     uint64
	ID      uint16
	Kind    StoredKind
}

// SessionStore persists the QoS>0 flow-control state of a single MQTT
// session so an in-flight exchange survives a reconnect that resumes the
// session (CONNACK Session Present = 1). Implementations MUST be safe for
// concurrent use: the read loop, the keep-alive loop and application
// Publish calls all touch the store from different goroutines.
type SessionStore interface {
	// Save inserts or replaces the entry keyed by (m.ID, m.Kind). The
	// store owns the sequence number: a new key is assigned the next
	// monotonic Seq, an existing key keeps its original Seq so an update
	// (e.g. a DUP re-save) does not jump the entry to the back of the
	// replay order.
	Save(m StoredMessage) error
	// Delete removes the entry keyed by (id, kind). Deleting an absent key
	// is a no-op.
	Delete(id uint16, kind StoredKind) error
	// All returns every stored entry ordered by ascending Seq — i.e. in
	// the order entries were first saved, which is the order they must be
	// replayed on a resumed session.
	All() ([]StoredMessage, error)
	// Reset discards all entries and restarts the sequence counter. Called
	// when the broker reports Session Present = 0, or on a clean start.
	Reset() error
}

// storeKey identifies a stored entry by packet identifier and kind. The
// same identifier can legitimately carry both a [StoredInboundID]
// (receiver state) and outbound state, so kind is part of the key.
type storeKey struct {
	id   uint16
	kind StoredKind
}

// memStore is the default in-memory [SessionStore]: a mutex-guarded map
// with a monotonic sequence counter. It is constructor-internal in v1.0;
// there is no configuration hook to substitute a persistent store yet.
type memStore struct {
	msgs map[storeKey]StoredMessage
	mu   sync.Mutex
	seq  uint64
}

var _ SessionStore = (*memStore)(nil)

// newMemStore returns an empty in-memory session store.
func newMemStore() *memStore {
	return &memStore{msgs: make(map[storeKey]StoredMessage)}
}

// Save implements [SessionStore].
func (s *memStore) Save(m StoredMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := storeKey{id: m.ID, kind: m.Kind}
	if prev, ok := s.msgs[k]; ok {
		m.Seq = prev.Seq
	} else {
		s.seq++
		m.Seq = s.seq
	}
	s.msgs[k] = m
	return nil
}

// Delete implements [SessionStore].
func (s *memStore) Delete(id uint16, kind StoredKind) error {
	s.mu.Lock()
	delete(s.msgs, storeKey{id: id, kind: kind})
	s.mu.Unlock()
	return nil
}

// All implements [SessionStore].
func (s *memStore) All() ([]StoredMessage, error) {
	s.mu.Lock()
	out := make([]StoredMessage, 0, len(s.msgs))
	for _, m := range s.msgs {
		out = append(out, m)
	}
	s.mu.Unlock()
	slices.SortFunc(out, func(a, b StoredMessage) int {
		return cmp.Compare(a.Seq, b.Seq)
	})
	return out, nil
}

// Reset implements [SessionStore].
func (s *memStore) Reset() error {
	s.mu.Lock()
	s.msgs = make(map[storeKey]StoredMessage)
	s.seq = 0
	s.mu.Unlock()
	return nil
}

// packetIDSpace is the number of MQTT packet identifiers (0..65535).
// Identifier 0 is reserved and never allocated.
const packetIDSpace = 1 << 16

// idAllocator hands out unique non-zero MQTT packet identifiers for
// outbound PUBLISH (QoS>0), SUBSCRIBE and UNSUBSCRIBE. The [1024]uint64
// bitmap tracks the 65536-bit in-use set; next is the rotating cursor so
// consecutive Acquire calls tend to return distinct identifiers even after
// a release, which keeps a slow-to-ack broker from immediately reusing an
// identifier the peer may still be processing.
type idAllocator struct {
	used [1024]uint64
	mu   sync.Mutex
	next uint16
}

// Acquire reserves and returns a free non-zero packet identifier. It scans
// forward from the internal cursor, wrapping around, and skips identifier
// 0 and any already in use. When every non-zero identifier is taken it
// returns [ErrPacketIDExhausted].
func (a *idAllocator) Acquire() (uint16, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for range packetIDSpace {
		id := a.next
		a.next++ // uint16 wraps 65535 -> 0
		if id == 0 {
			continue
		}
		word, bit := id>>6, uint64(1)<<(id&63)
		if a.used[word]&bit == 0 {
			a.used[word] |= bit
			return id, nil
		}
	}
	return 0, ErrPacketIDExhausted
}

// Release returns id to the free set. Releasing 0 or an already-free
// identifier is a no-op.
func (a *idAllocator) Release(id uint16) {
	if id == 0 {
		return
	}
	a.mu.Lock()
	a.used[id>>6] &^= uint64(1) << (id & 63)
	a.mu.Unlock()
}

// Reset frees every identifier and rewinds the cursor. Used when a session
// is discarded (clean start, or Session Present = 0).
func (a *idAllocator) Reset() {
	a.mu.Lock()
	a.used = [1024]uint64{}
	a.next = 0
	a.mu.Unlock()
}

// quota is a counting semaphore bounding the number of concurrently
// in-flight outbound QoS>0 sends to the broker's Receive Maximum. Beyond
// the usual acquire/release it supports two connection-lifecycle
// operations: reset(n) resizes the permit ceiling when a reconnect
// advertises a different Receive Maximum, and fail() unblocks every waiter
// at once so a dropped connection does not leave a Publish parked forever.
//
// The wake channel is the broadcast primitive: it is closed (and replaced)
// on every state change so waiters — which select on it together with the
// caller's context — re-evaluate. Capturing the channel under the mutex
// before selecting closes the lost-wakeup window.
type quota struct {
	wake   chan struct{}
	mu     sync.Mutex
	avail  int
	failed bool
}

// newQuota returns a quota seeded with n permits.
func newQuota(n int) *quota {
	return &quota{avail: n, wake: make(chan struct{})}
}

// acquire takes one permit, blocking until one is available. It returns
// early with ctx.Err() if ctx is cancelled, and with [ErrConnectionLost]
// if fail() is invoked while it waits (or has already been invoked). A
// permit that is immediately available is granted without consulting ctx.
func (q *quota) acquire(ctx context.Context) error {
	for {
		q.mu.Lock()
		if q.failed {
			q.mu.Unlock()
			return ErrConnectionLost
		}
		if q.avail > 0 {
			q.avail--
			q.mu.Unlock()
			return nil
		}
		wake := q.wake
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			// State changed (release/reset/fail); re-evaluate.
		}
	}
}

// release returns one permit and wakes any waiters.
func (q *quota) release() {
	q.mu.Lock()
	q.avail++
	q.broadcast()
	q.mu.Unlock()
}

// reset sets the permit ceiling to n and clears any failed state, then
// wakes all waiters. It is an absolute reset used on (re)connect: permits
// already checked out by in-flight sends are NOT subtracted from n. When n
// shrinks below the number of outstanding permits, those outstanding
// sends still call release() and will push avail above the new ceiling
// until they drain, so the smaller ceiling only takes full effect once the
// pre-reset in-flight sends complete. Callers therefore reset only while
// no send holds a permit (connection re-established, waiters already
// failed) so this transient over-commit cannot occur.
func (q *quota) reset(n int) {
	q.mu.Lock()
	q.avail = n
	q.failed = false
	q.broadcast()
	q.mu.Unlock()
}

// fail marks the quota failed and wakes every waiter so each returns
// [ErrConnectionLost]. Used on connection loss to fail parked sends fast
// instead of waiting for their context or an ack that will never come. A
// subsequent reset clears the failed state.
func (q *quota) fail() {
	q.mu.Lock()
	q.failed = true
	q.broadcast()
	q.mu.Unlock()
}

// broadcast wakes all current waiters by closing the wake channel and
// installing a fresh one. The caller must hold q.mu.
func (q *quota) broadcast() {
	close(q.wake)
	q.wake = make(chan struct{})
}
