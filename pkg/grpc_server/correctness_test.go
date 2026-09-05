package grpc_server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	pb "github.com/dsg-uwaterloo/treebeard/api/daos_xr"
	routerpb "github.com/dsg-uwaterloo/treebeard/api/router"
	"github.com/dsg-uwaterloo/treebeard/pkg/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type scriptedRouter struct {
	readReply  *routerpb.ReadReply
	writeReply *routerpb.WriteReply
}

func (r *scriptedRouter) Read(context.Context, *routerpb.ReadRequest, ...grpc.CallOption) (*routerpb.ReadReply, error) {
	return r.readReply, nil
}

func (r *scriptedRouter) Write(context.Context, *routerpb.WriteRequest, ...grpc.CallOption) (*routerpb.WriteReply, error) {
	return r.writeReply, nil
}

func preloadValue(key uint32, size int) []byte {
	value := make([]byte, size)
	copy(value, preloadMagic)
	binary.LittleEndian.PutUint32(value[len(preloadMagic):], key)
	return value
}

func TestValidateReadValueRequiresConfiguredWidth(t *testing.T) {
	s := New(&scriptedRouter{}, config.Parameters{MaxRequests: 1, BlockSize: 32}, "", 1, 1, false)
	if err := s.validateReadValue(7, make([]byte, 31)); err == nil || !strings.Contains(err.Error(), "want configured block size 32") {
		t.Fatalf("wrong-width read error = %v", err)
	}
}

func TestValidateReadValueChecksPopulationIdentityWhenEnabled(t *testing.T) {
	s := New(&scriptedRouter{}, config.Parameters{MaxRequests: 1, BlockSize: 32}, "", 1, 1, true)
	if err := s.validateReadValue(7, preloadValue(7, 32)); err != nil {
		t.Fatalf("valid population payload rejected: %v", err)
	}
	if err := s.validateReadValue(7, preloadValue(8, 32)); err == nil || !strings.Contains(err.Error(), "payload for key 8") {
		t.Fatalf("wrong-key payload error = %v", err)
	}
}

func TestAccessRejectsEmptyReadAndFailedWrite(t *testing.T) {
	router := &scriptedRouter{
		readReply:  &routerpb.ReadReply{},
		writeReply: &routerpb.WriteReply{Success: false},
	}
	s := New(router, config.Parameters{MaxRequests: 1, BlockSize: 32}, "", 1, 1, true)

	_, _, err := s.access(context.Background(), &pb.ClientRequestPb{Key: 7, RequestType: pb.RequestType_READ}, nil)
	if err == nil || !strings.Contains(err.Error(), "returned 0 bytes") {
		t.Fatalf("empty read error = %v", err)
	}

	_, _, err = s.access(context.Background(), &pb.ClientRequestPb{Key: 7, RequestType: pb.RequestType_WRITE, Value: preloadValue(7, 32)}, nil)
	if err == nil || !strings.Contains(err.Error(), "success=false") {
		t.Fatalf("failed write error = %v", err)
	}
}

func TestDegradedWriteCannotMasqueradeAsCanonicalEmptyAck(t *testing.T) {
	s := New(&scriptedRouter{}, config.Parameters{MaxRequests: 1, BlockSize: 32}, "", 1, 1, true)
	req := &pb.ClientRequestPb{RequestId: 3, Key: 7, RequestType: pb.RequestType_WRITE}
	resp := s.degradeBlock(req, 11, errors.New("write failed"))
	if string(resp.Value) != degradedWriteMarker {
		t.Fatalf("degraded write value = %q, want adapter failure marker", resp.Value)
	}
	if got := s.degradedBlocks.Load(); got != 1 {
		t.Fatalf("degraded blocks = %d, want 1", got)
	}
}

func TestCanceledAccessIsAuditedSeparatelyFromDegradation(t *testing.T) {
	s := New(&scriptedRouter{}, config.Parameters{MaxRequests: 1, BlockSize: 32}, "", 1, 1, true)
	req := &pb.ClientRequestPb{RequestId: 4, Key: 8, RequestType: pb.RequestType_READ}

	s.degradeBlock(req, 12, fmt.Errorf("wrapped context cancellation: %w", context.Canceled))
	s.degradeBlock(req, 13, fmt.Errorf("wrapped grpc cancellation: %w", status.Error(codes.Canceled, "stream closed")))

	if got := s.canceledBlocks.Load(); got != 2 {
		t.Fatalf("canceled blocks = %d, want 2", got)
	}
	if got := s.degradedBlocks.Load(); got != 0 {
		t.Fatalf("degraded blocks = %d, want 0", got)
	}
}
