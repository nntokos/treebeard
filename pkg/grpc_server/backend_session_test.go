package grpc_server

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/dsg-uwaterloo/treebeard/api/obistream"
	"github.com/dsg-uwaterloo/treebeard/pkg/config"
)

func TestBackendSessionReadBatchUsesBoundedConcurrency(t *testing.T) {
	const fanout = 3
	const batchSize = 12
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseReads := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseReads()
	router := &blockingRouter{release: release}
	s := New(router, config.Parameters{MaxRequests: 1000}, "", 64, fanout, false)
	committed := make([]*pb.ClientRequestPb, batchSize)
	for i := range committed {
		committed[i] = &pb.ClientRequestPb{RequestId: uint32(i + 1), Key: uint32(i), RequestType: pb.RequestType_READ}
	}
	done := make(chan *pb.BatchResponsePb, 1)
	go func() {
		done <- s.serveBackendBatch(context.Background(), queuedBatch{
			batch: &pb.BatchRequestPb{BatchId: 1, Committed: committed}, receivedAt: time.Now(),
		})
	}()
	deadline := time.Now().Add(time.Second)
	for router.active.Load() != fanout && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := router.active.Load(); got != fanout {
		t.Fatalf("active reads = %d, want %d concurrent reads", got, fanout)
	}
	releaseReads()
	select {
	case response := <-done:
		if len(response.Responses) != batchSize {
			t.Fatalf("response count = %d, want %d", len(response.Responses), batchSize)
		}
		for i, r := range response.Responses {
			if r == nil || r.RequestId != committed[i].RequestId {
				t.Fatalf("response %d does not match its request", i)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("read batch did not complete")
	}
	if got := router.peak.Load(); got > fanout {
		t.Fatalf("peak concurrent reads = %d, limit = %d", got, fanout)
	}
}
