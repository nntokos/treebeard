package grpc_server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/dsg-uwaterloo/treebeard/api/daos_xr"
	routerpb "github.com/dsg-uwaterloo/treebeard/api/router"
	"github.com/dsg-uwaterloo/treebeard/pkg/config"
	"google.golang.org/grpc"
)

type blockingRouter struct {
	active  atomic.Int64
	peak    atomic.Int64
	release <-chan struct{}
}

func (r *blockingRouter) Read(context.Context, *routerpb.ReadRequest, ...grpc.CallOption) (*routerpb.ReadReply, error) {
	active := r.active.Add(1)
	for old := r.peak.Load(); active > old && !r.peak.CompareAndSwap(old, active); old = r.peak.Load() {
	}
	<-r.release
	r.active.Add(-1)
	return &routerpb.ReadReply{Value: ""}, nil
}

func (r *blockingRouter) Write(context.Context, *routerpb.WriteRequest, ...grpc.CallOption) (*routerpb.WriteReply, error) {
	panic("unexpected write")
}

func TestRouterAdmissionHonorsConfiguredMaxRequests(t *testing.T) {
	release := make(chan struct{})
	router := &blockingRouter{release: release}
	s := New(router, config.Parameters{MaxRequests: 2}, "", 64, 64, false)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(key uint32) {
			defer wg.Done()
			_, _, _ = s.access(context.Background(), &pb.ClientRequestPb{Key: key, RequestType: pb.RequestType_READ}, nil)
		}(uint32(i))
	}

	deadline := time.Now().Add(time.Second)
	for router.active.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := router.active.Load(); got != 2 {
		t.Fatalf("active Router calls = %d, want configured limit 2", got)
	}
	close(release)
	wg.Wait()
	if got := router.peak.Load(); got > 2 {
		t.Fatalf("peak Router calls = %d, exceeded configured limit 2", got)
	}
	if got := s.routerInflight.Load(); got != 0 {
		t.Fatalf("adapter still records %d Router calls after completion", got)
	}
	if got := s.routerAdmissionWaits.Load(); got == 0 {
		t.Fatal("expected admission wait evidence under saturation")
	}
}
