// SPDX-License-Identifier: MIT
// Copyright (C) 2026 OpenCCU-Loom authors.

package mqtt

// Small direct-unit gaps in session.go not otherwise reached:
// StoredKind.String()'s full switch (only StoredPublish is exercised
// indirectly by session_test.go) and idAllocator.Release's id==0 no-op.

import "testing"

func TestStoredKindString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		kind StoredKind
		want string
	}{
		{StoredPublish, "publish"},
		{StoredPubrel, "pubrel"},
		{StoredInboundID, "inbound-id"},
		{StoredKind(99), "StoredKind(99)"},
	}
	for _, tt := range cases {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("StoredKind(%d).String() = %q, want %q", tt.kind, got, tt.want)
		}
	}
}

// TestIDAllocatorReleaseZeroIsNoop proves releasing the reserved identifier
// 0 does not touch the bitmap (and, in particular, does not panic on the
// id>>6 shift).
func TestIDAllocatorReleaseZeroIsNoop(t *testing.T) {
	t.Parallel()

	a := &idAllocator{}
	a.Release(0)
	id, err := a.Acquire()
	if err != nil {
		t.Fatalf("acquire after Release(0): %v", err)
	}
	if id == 0 {
		t.Fatal("Release(0) must not make identifier 0 acquirable")
	}
}
