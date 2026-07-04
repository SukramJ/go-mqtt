// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SukramJ/go-mqtt/protocol"
)

func assertNoReceive(t *testing.T, ch <-chan error, d time.Duration) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("acquire returned early: %v", err)
	case <-time.After(d):
	}
}

func assertReceive(t *testing.T, ch <-chan error, d time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		t.Fatal("acquire did not return in time")
		return nil
	}
}

func TestIDAllocatorExhaustion(t *testing.T) {
	t.Parallel()
	a := &idAllocator{}
	seen := make(map[uint16]bool)
	for i := range packetIDSpace - 1 { // 65535 non-zero identifiers
		id, err := a.Acquire()
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if id == 0 {
			t.Fatal("allocator handed out reserved identifier 0")
		}
		if seen[id] {
			t.Fatalf("duplicate identifier %d", id)
		}
		seen[id] = true
	}
	if len(seen) != packetIDSpace-1 {
		t.Fatalf("expected %d unique identifiers, got %d", packetIDSpace-1, len(seen))
	}
	if _, err := a.Acquire(); !errors.Is(err, ErrPacketIDExhausted) {
		t.Fatalf("expected ErrPacketIDExhausted, got %v", err)
	}

	// Freeing exactly one identifier makes it (and only it) allocatable.
	a.Release(1234)
	id, err := a.Acquire()
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	if id != 1234 {
		t.Fatalf("expected freed identifier 1234, got %d", id)
	}
	if _, err := a.Acquire(); !errors.Is(err, ErrPacketIDExhausted) {
		t.Fatalf("expected ErrPacketIDExhausted after refill, got %v", err)
	}
}

func TestIDAllocatorWrapSkipsZero(t *testing.T) {
	t.Parallel()
	a := &idAllocator{next: 65534}
	id1, err1 := a.Acquire()
	id2, err2 := a.Acquire()
	id3, err3 := a.Acquire() // cursor wraps through 0, which must be skipped
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("acquire errors: %v %v %v", err1, err2, err3)
	}
	if id1 != 65534 || id2 != 65535 || id3 != 1 {
		t.Fatalf("wrap sequence wrong: got %d, %d, %d; want 65534, 65535, 1", id1, id2, id3)
	}
}

func TestIDAllocatorReset(t *testing.T) {
	t.Parallel()
	a := &idAllocator{}
	first, err := a.Acquire()
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	for range 100 {
		if _, err := a.Acquire(); err != nil {
			t.Fatalf("acquire: %v", err)
		}
	}
	a.Reset()
	again, err := a.Acquire()
	if err != nil {
		t.Fatalf("acquire after reset: %v", err)
	}
	if again != first {
		t.Fatalf("after reset expected identifier %d, got %d", first, again)
	}
}

func TestIDAllocatorConcurrent(t *testing.T) {
	t.Parallel()
	a := &idAllocator{}
	var mu sync.Mutex
	held := make(map[uint16]bool)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				id, err := a.Acquire()
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				mu.Lock()
				if held[id] {
					t.Errorf("identifier %d handed out while still held", id)
				}
				held[id] = true
				mu.Unlock()

				mu.Lock()
				delete(held, id)
				mu.Unlock()
				a.Release(id)
			}
		}()
	}
	wg.Wait()
}

func TestMemStoreOrderingAndReset(t *testing.T) {
	t.Parallel()
	s := newMemStore()
	ids := []uint16{7, 3, 42, 1}
	for _, id := range ids {
		if err := s.Save(StoredMessage{ID: id, Kind: StoredPublish}); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	all, err := s.All()
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if len(all) != len(ids) {
		t.Fatalf("expected %d entries, got %d", len(ids), len(all))
	}
	for i, m := range all {
		if m.ID != ids[i] {
			t.Fatalf("entry %d: got id %d, want %d (Seq order must match save order)", i, m.ID, ids[i])
		}
		if m.Seq != uint64(i+1) {
			t.Fatalf("entry %d: Seq %d, want %d", i, m.Seq, i+1)
		}
		if i > 0 && all[i-1].Seq >= m.Seq {
			t.Fatalf("Seq not strictly ascending at %d: %d >= %d", i, all[i-1].Seq, m.Seq)
		}
	}

	if err := s.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	all, _ = s.All()
	if len(all) != 0 {
		t.Fatalf("after reset expected 0 entries, got %d", len(all))
	}
	if err := s.Save(StoredMessage{ID: 9, Kind: StoredPublish}); err != nil {
		t.Fatalf("save after reset: %v", err)
	}
	all, _ = s.All()
	if all[0].Seq != 1 {
		t.Fatalf("Seq did not restart after reset: got %d", all[0].Seq)
	}
}

func TestMemStoreKeyByIDAndKind(t *testing.T) {
	t.Parallel()
	s := newMemStore()
	if err := s.Save(StoredMessage{ID: 5, Kind: StoredPublish}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(StoredMessage{ID: 5, Kind: StoredInboundID}); err != nil {
		t.Fatal(err)
	}
	all, _ := s.All()
	if len(all) != 2 {
		t.Fatalf("same id, different kind must coexist: got %d entries", len(all))
	}
	if err := s.Delete(5, StoredPublish); err != nil {
		t.Fatal(err)
	}
	all, _ = s.All()
	if len(all) != 1 || all[0].Kind != StoredInboundID {
		t.Fatalf("delete removed the wrong entry: %+v", all)
	}
	// Deleting an absent key is a no-op.
	if err := s.Delete(999, StoredPubrel); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
}

func TestMemStoreSavePreservesSeqOnUpdate(t *testing.T) {
	t.Parallel()
	s := newMemStore()
	if err := s.Save(StoredMessage{ID: 1, Kind: StoredPublish}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(StoredMessage{ID: 2, Kind: StoredPublish}); err != nil {
		t.Fatal(err)
	}
	pub := &protocol.PublishPacket{Version: protocol.V50, Topic: "t", QoS: 1, PacketID: 1}
	if err := s.Save(StoredMessage{ID: 1, Kind: StoredPublish, Publish: pub}); err != nil {
		t.Fatal(err)
	}
	all, _ := s.All()
	if all[0].ID != 1 || all[0].Seq != 1 {
		t.Fatalf("update must keep original Seq/order: %+v", all[0])
	}
	if all[0].Publish != pub {
		t.Fatal("update did not replace the stored payload")
	}
	// The store owns Seq: an incoming Seq is ignored.
	if err := s.Save(StoredMessage{ID: 3, Kind: StoredPublish, Seq: 999}); err != nil {
		t.Fatal(err)
	}
	all, _ = s.All()
	if last := all[len(all)-1]; last.ID != 3 || last.Seq != 3 {
		t.Fatalf("store did not own Seq: %+v", last)
	}
}

func TestQuotaBlockUnblockRelease(t *testing.T) {
	t.Parallel()
	q := newQuota(1)
	ctx := context.Background()
	if err := q.acquire(ctx); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- q.acquire(ctx) }()
	assertNoReceive(t, done, 30*time.Millisecond)
	q.release()
	if err := assertReceive(t, done, time.Second); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestQuotaResetUnblocks(t *testing.T) {
	t.Parallel()
	q := newQuota(0)
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- q.acquire(ctx) }()
	assertNoReceive(t, done, 30*time.Millisecond)

	q.reset(2)
	if err := assertReceive(t, done, time.Second); err != nil {
		t.Fatalf("acquire after reset: %v", err)
	}
	// reset(2) granted two permits; the waiter consumed one, one remains.
	if err := q.acquire(ctx); err != nil {
		t.Fatalf("second permit after reset: %v", err)
	}
	// Now empty again; a fresh reset re-arms.
	done2 := make(chan error, 1)
	go func() { done2 <- q.acquire(ctx) }()
	assertNoReceive(t, done2, 30*time.Millisecond)
	q.reset(1)
	if err := assertReceive(t, done2, time.Second); err != nil {
		t.Fatalf("acquire after second reset: %v", err)
	}
}

func TestQuotaCtxCancel(t *testing.T) {
	t.Parallel()
	q := newQuota(0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.acquire(ctx) }()
	assertNoReceive(t, done, 30*time.Millisecond)
	cancel()
	if err := assertReceive(t, done, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestQuotaFailConnectionLost(t *testing.T) {
	t.Parallel()
	q := newQuota(0)
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- q.acquire(ctx) }()
	assertNoReceive(t, done, 30*time.Millisecond)

	q.fail()
	if err := assertReceive(t, done, time.Second); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("waiter: expected ErrConnectionLost, got %v", err)
	}
	// While failed, acquire fails fast even without waiting.
	if err := q.acquire(ctx); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("acquire while failed: expected ErrConnectionLost, got %v", err)
	}
	// reset clears the failed state and re-arms permits.
	q.reset(1)
	if err := q.acquire(ctx); err != nil {
		t.Fatalf("acquire after reset: %v", err)
	}
}

func TestQuotaAvailableIgnoresCancelledCtx(t *testing.T) {
	t.Parallel()
	q := newQuota(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.acquire(ctx); err != nil {
		t.Fatalf("an immediately available permit must be granted despite a cancelled ctx: %v", err)
	}
}

func TestQuotaConcurrentBound(t *testing.T) {
	t.Parallel()
	const ceiling = 4
	q := newQuota(ceiling)
	ctx := context.Background()

	var inFlight, maxSeen atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if err := q.acquire(ctx); err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				cur := inFlight.Add(1)
				for {
					m := maxSeen.Load()
					if cur <= m || maxSeen.CompareAndSwap(m, cur) {
						break
					}
				}
				if cur > ceiling {
					t.Errorf("in-flight %d exceeds ceiling %d", cur, ceiling)
				}
				inFlight.Add(-1)
				q.release()
			}
		}()
	}
	wg.Wait()
	if maxSeen.Load() > ceiling {
		t.Fatalf("peak concurrency %d exceeded ceiling %d", maxSeen.Load(), ceiling)
	}
}
