package grpc_server

import (
	"context"
	"testing"
	"time"

	pb "github.com/dsg-uwaterloo/treebeard/api/daos_xr"
	"github.com/dsg-uwaterloo/treebeard/pkg/config"
)

// Regression cover for 2026-08-04: UnionSession used to launch one goroutine
// per block in the batch with no bound beyond the global router-slot
// admission (sized to Treebeard's own max_requests, ~8000 — far too loose to
// protect a single shard's stash from one oversized batch). At batch-size
// 1024 that let one UNION batch inject up to 1024 simultaneous Router.Read
// calls; the shardnode stash climbed monotonically and pinned near 20,000
// resident blocks, collapsing throughput to ~1-3 completions/s. This asserts
// serveUnionBatch never lets more than unionFanout blocks from the SAME batch
// have an outstanding Router call at once, independent of routerSlots (given
// a large, non-binding MaxRequests here) and independent of batch size (the
// batch here is far larger than the fanout limit).
func TestUnionSessionBoundsPerBatchFanout(t *testing.T) {
	const fanoutLimit = 3
	const batchSize = 20

	release := make(chan struct{})
	router := &blockingRouter{release: release}
	// MaxRequests is deliberately large and non-binding: this test isolates
	// the per-batch fanout gate, not the global router-slot gate (that is
	// TestRouterAdmissionHonorsConfiguredMaxRequests's job).
	s := New(router, config.Parameters{MaxRequests: 1000}, "", 64, fanoutLimit, false)

	committed := make([]*pb.ClientRequestPb, batchSize)
	for i := range committed {
		committed[i] = &pb.ClientRequestPb{Key: uint32(i), RequestType: pb.RequestType_READ}
	}
	batch := &pb.BatchRequestPb{BatchId: 1, Committed: committed}

	done := make(chan *pb.BatchResponsePb, 1)
	go func() {
		done <- s.serveUnionBatch(context.Background(), queuedBatch{batch: batch, receivedAt: time.Now()})
	}()

	deadline := time.Now().Add(time.Second)
	for router.active.Load() != fanoutLimit && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := router.active.Load(); got != fanoutLimit {
		t.Fatalf("active Router calls = %d, want the fanout limit %d", got, fanoutLimit)
	}

	close(release)
	resp := <-done

	if got := router.peak.Load(); got > fanoutLimit {
		t.Fatalf("peak concurrent Router calls = %d, exceeded the batch's fanout limit %d", got, fanoutLimit)
	}
	if got := int(s.unionFanoutPeak.Load()); got > fanoutLimit {
		t.Fatalf("adapter-recorded union fanout peak = %d, exceeded the configured limit %d", got, fanoutLimit)
	}
	if got := s.unionFanoutInflight.Load(); got != 0 {
		t.Fatalf("adapter still records %d fanout slots held after the batch completed", got)
	}
	if got := len(resp.Responses); got != batchSize {
		t.Fatalf("response count = %d, want the full batch size %d", got, batchSize)
	}
	for i, r := range resp.Responses {
		if r == nil {
			t.Fatalf("response[%d] is nil — every slot must be answered even under fanout gating", i)
		}
	}
}

// A batch smaller than the fanout limit must not be artificially throttled:
// every block should be able to acquire a slot immediately.
func TestUnionSessionFanoutDoesNotThrottleSmallBatches(t *testing.T) {
	const fanoutLimit = 64
	const batchSize = 4

	release := make(chan struct{})
	router := &blockingRouter{release: release}
	s := New(router, config.Parameters{MaxRequests: 1000}, "", 64, fanoutLimit, false)

	committed := make([]*pb.ClientRequestPb, batchSize)
	for i := range committed {
		committed[i] = &pb.ClientRequestPb{Key: uint32(i), RequestType: pb.RequestType_READ}
	}
	batch := &pb.BatchRequestPb{BatchId: 1, Committed: committed}

	done := make(chan *pb.BatchResponsePb, 1)
	go func() {
		done <- s.serveUnionBatch(context.Background(), queuedBatch{batch: batch, receivedAt: time.Now()})
	}()

	deadline := time.Now().Add(time.Second)
	for router.active.Load() != batchSize && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := router.active.Load(); got != batchSize {
		t.Fatalf("active Router calls = %d, want the full batch dispatched at once (%d), fanout limit should not bind here", got, batchSize)
	}

	close(release)
	<-done
}
