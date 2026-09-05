// Package grpc_server implements daos-xr BackendIngress and Capability over a
// connected Treebeard Router client.
//
// Block key mapping:   daos-xr uint32 key  →  Treebeard string block id (decimal)
// Value encoding:      daos-xr []byte       →  base64-encoded string in Treebeard Router
//
// UNION mapping: the Router's epochManager (pkg/router/epoch.go) queues every
// Read/Write it receives — regardless of which gRPC call it arrived on — and once
// per epoch_time tick folds everything queued so far into one ShardNode.BatchQuery
// per destination shard. So committed requests admitted within the same
// epoch_time window land in the same epoch queue and get answered by that same
// shard-level batch call: admission ordering, not client-side batching, is what
// gives UNION its atomicity. (Added 2026-07-15.)
//
// UNION fan-out is bounded (2026-08-04): admitting the requests inside one
// epoch window does NOT require dispatching them all as simultaneous
// goroutines. Before this, UnionSession launched one unbounded goroutine per
// block in the batch, gated only by the GLOBAL router-slot admission (sized to
// Treebeard's own max_requests, ~8000 — far too loose to protect a single
// shard's stash from one oversized batch). At batch-size 1024 that let one
// client-issued UNION batch inject up to 1024 simultaneous Router.Read calls.
// Observed effect: the shardnode stash climbed monotonically and pinned near
// 20,000 resident blocks (the non-UNION passthrough baseline, one block per
// Router.Read at a time, oscillates under ~3,000), collapsing throughput to
// ~1-3 completions/s and starving eviction entirely. See unionFanout below.
package grpc_server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	routerpb "github.com/dsg-uwaterloo/treebeard/api/router"
	"github.com/dsg-uwaterloo/treebeard/pkg/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/dsg-uwaterloo/treebeard/api/daos_xr"
)

// Server wraps a Treebeard router client and implements the three daos-xr services.
type Server struct {
	pb.UnimplementedBackendIngressServer
	pb.UnimplementedCapabilityServer

	router    routerpb.RouterClient
	params    config.Parameters
	statsPath string

	// Enables constant-cost validation of the deterministic experiment payload.
	validatePreloadPayloads bool

	// Batches served concurrently per session (2026-08-01). See serveSession.
	serveConcurrency int

	// Router admission is global across sessions and batches. Treebeard's native
	// client bounds outstanding Router RPCs with parameters.max-requests; the XR
	// adapter replaces that client, so it must preserve the same safety invariant.
	// Without this gate, serveConcurrency * UNION batch size could inject tens of
	// thousands of calls and drive the Router's timeout path into an unbounded
	// late-reply goroutine leak. Added 2026-08-03.
	routerSlots              chan struct{}
	routerInflight           atomic.Uint64
	routerInflightPeak       atomic.Uint64
	routerAdmissionWaits     atomic.Uint64
	routerAdmissionWaitNs    atomic.Uint64
	routerAdmissionMaxWaitNs atomic.Uint64

	// Caps how many blocks from ONE UNION batch may have an outstanding
	// Router.Read/Write simultaneously (2026-08-04). Independent of, and much
	// tighter than, routerSlots: routerSlots is a global cross-batch ceiling
	// sized to Treebeard's own max_requests, which does nothing to stop a
	// single large batch from overwhelming one shard's stash on its own. See
	// the package doc "UNION fan-out is bounded" for the incident this fixes.
	unionFanout         int
	unionFanoutSlots    chan struct{}
	unionFanoutInflight atomic.Uint64
	unionFanoutPeak     atomic.Uint64

	// Blocks answered with an EMPTY value because the access failed or panicked
	// (2026-07-29). Nonzero means a backend operation genuinely failed — see
	// degradeBlock. Stream-shutdown cancellation is counted separately because
	// the orchestrator may cancel unused speculative work after all demand was
	// delivered. Both counters are atomic because UnionSession fans out calls.
	degradedBlocks atomic.Uint64
	canceledBlocks atomic.Uint64
}

const degradedWriteMarker = "TREEBEARD_BACKEND_FAILED"

// degradeBlock builds a visibly invalid answer for one request whose access
// failed, and records it. READ failures remain empty so fixed-block validation
// rejects them. WRITE failures carry a private adapter marker because the legacy
// daos-xr protobuf has no outcome field and an empty WRITE value is otherwise a
// canonical success acknowledgement accepted by the population tool.
//
// Containment rationale: a per-block error used to abort the whole session with
// codes.Internal, and a gRPC stream is not resumable — every later request was
// then orphaned and the orchestrator's client hung until --timeout-client instead
// of failing fast. Answering the request visibly invalid lets the orchestrator complete its
// failure path while keeping the session serving.
func (s *Server) degradeBlock(req *pb.ClientRequestPb, recvMs uint64, cause error) *pb.ClientResponsePb {
	label := "degraded"
	n := uint64(0)
	if errors.Is(cause, context.Canceled) || status.Code(cause) == codes.Canceled {
		label = "canceled"
		n = s.canceledBlocks.Add(1)
	} else {
		n = s.degradedBlocks.Add(1)
	}
	value := []byte(nil)
	if req.RequestType == pb.RequestType_WRITE {
		value = []byte(degradedWriteMarker)
	}
	log.Printf("treebeard: key %d %s (%d total this run): %v", req.Key, label, n, cause)
	return &pb.ClientResponsePb{
		RequestId:        req.RequestId,
		RequestType:      req.RequestType,
		Key:              req.Key,
		Value:            value,
		ProxyReceivedMs:  recvMs,
		ProxyRespondedMs: uint64(time.Now().UnixMilli()),
	}
}

// New returns a Server ready to serve gRPC. serveConcurrency is the number of
// batches served in parallel per session (see serveSession); values below 1 are
// clamped to 1, which reproduces the pre-2026-08-01 sequential behaviour.
// unionFanout is the most blocks from a SINGLE UNION batch allowed an
// outstanding Router.Read/Write at once (see the unionFanout field doc);
// values below 1 are clamped to 1, which serialises UNION batches into the
// same one-at-a-time shape as the non-UNION BackendSession path.
func New(router routerpb.RouterClient, params config.Parameters, statsPath string, serveConcurrency int, unionFanout int, validatePreloadPayloads bool) *Server {
	if serveConcurrency < 1 {
		serveConcurrency = 1
	}
	if unionFanout < 1 {
		unionFanout = 1
	}
	routerLimit := params.MaxRequests
	if routerLimit < 1 {
		routerLimit = 1
	}
	s := &Server{
		router:                  router,
		params:                  params,
		statsPath:               statsPath,
		serveConcurrency:        serveConcurrency,
		routerSlots:             make(chan struct{}, routerLimit),
		unionFanout:             unionFanout,
		unionFanoutSlots:        make(chan struct{}, unionFanout),
		validatePreloadPayloads: validatePreloadPayloads,
	}
	s.startStatsWriter()
	return s
}

// acquireUnionFanoutSlot bounds how many blocks from ONE UNION batch may have
// an outstanding Router call simultaneously. See the unionFanout field doc.
// Mirrors acquireRouterSlot's cancellation handling: a waiter released by
// ctx.Done() never consumes a slot.
func (s *Server) acquireUnionFanoutSlot(ctx context.Context) error {
	select {
	case s.unionFanoutSlots <- struct{}{}:
		current := s.unionFanoutInflight.Add(1)
		atomicMax(&s.unionFanoutPeak, current)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) releaseUnionFanoutSlot() {
	s.unionFanoutInflight.Add(^uint64(0))
	<-s.unionFanoutSlots
}

// acquireRouterSlot preserves Treebeard's configured max-requests admission
// limit for every adapter-originated Router RPC. Waiting is deliberately at the
// adapter boundary: queued work remains bounded by the existing batch queues and
// never enters the Router's epoch/goroutine machinery until capacity is available.
// The request context makes cancellation release a waiter without consuming a slot.
func (s *Server) acquireRouterSlot(ctx context.Context) error {
	select {
	case s.routerSlots <- struct{}{}:
		s.recordRouterAdmission()
		return nil
	default:
	}

	s.routerAdmissionWaits.Add(1)
	started := time.Now()
	select {
	case s.routerSlots <- struct{}{}:
		waited := uint64(time.Since(started))
		s.routerAdmissionWaitNs.Add(waited)
		atomicMax(&s.routerAdmissionMaxWaitNs, waited)
		s.recordRouterAdmission()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) recordRouterAdmission() {
	current := s.routerInflight.Add(1)
	atomicMax(&s.routerInflightPeak, current)
}

func (s *Server) releaseRouterSlot() {
	s.routerInflight.Add(^uint64(0))
	<-s.routerSlots
}

func atomicMax(dst *atomic.Uint64, value uint64) {
	for old := dst.Load(); value > old && !dst.CompareAndSwap(old, value); old = dst.Load() {
	}
}

// startStatsWriter publishes a crash-independent admission snapshot once per
// second. The atomic rename means Ansible can fetch the file concurrently without
// ever observing a partial CSV row. Added 2026-08-03.
func (s *Server) startStatsWriter() {
	if s.statsPath == "" {
		return
	}
	write := func() {
		body := fmt.Sprintf(
			"timestamp_ms,router_inflight_limit,router_inflight,router_inflight_peak,router_admission_waits,router_admission_wait_ms_total,router_admission_wait_ms_max,degraded_blocks,canceled_blocks,union_fanout_limit,union_fanout_peak\n%d,%d,%d,%d,%d,%.3f,%.3f,%d,%d,%d,%d\n",
			time.Now().UnixMilli(), cap(s.routerSlots), s.routerInflight.Load(),
			s.routerInflightPeak.Load(), s.routerAdmissionWaits.Load(),
			float64(s.routerAdmissionWaitNs.Load())/float64(time.Millisecond),
			float64(s.routerAdmissionMaxWaitNs.Load())/float64(time.Millisecond),
			s.degradedBlocks.Load(),
			s.canceledBlocks.Load(),
			cap(s.unionFanoutSlots), s.unionFanoutPeak.Load(),
		)
		tmp := s.statsPath + ".tmp"
		if err := os.WriteFile(tmp, []byte(body), 0644); err != nil {
			log.Printf("treebeard: write adapter stats: %v", err)
			return
		}
		if err := os.Rename(tmp, s.statsPath); err != nil {
			log.Printf("treebeard: publish adapter stats: %v", err)
		}
	}
	write()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			write()
		}
	}()
}

// keyToBlock converts a daos-xr uint32 key to Treebeard's string block id.
//
// The "k" prefix is REQUIRED, not cosmetic. Treebeard stores each block's bucket-slot
// metadata as the string "<pos><blockid>" with NO separator, and its reader
// (storage.parseMetadataBlock) recovers <pos> by scanning to the first NON-digit
// character. A purely numeric block id makes "<pos><id>" all digits, so the scan never
// finds a boundary, block[:0]=="" and strconv.Atoi("") fails — surfacing as
// "could not get offset from storage" on any bucket that holds a real (written-back)
// block, which cumulatively wedges reads. Treebeard's own dummy ids ("dummyN") avoid
// this because they start with a letter; prefixing every real id with a non-digit gives
// it the same clean delimiter. The prefix is internal to the Treebeard cluster — the
// adapter matches responses by request_id and never parses the block string back to a
// key — so nothing else needs to change.
func keyToBlock(key uint32) string {
	return "k" + strconv.FormatUint(uint64(key), 10)
}

// blockToKey reverses keyToBlock, stripping the "k" prefix. Used only to
// translate harvested stash blocks (see access) back to daos-xr keys for
// OpportunisticBlockPb -- never applied to the router-facing block string
// itself. (Added 2026-08-03.)
func blockToKey(block string) (uint32, bool) {
	if len(block) < 2 || block[0] != 'k' {
		return 0, false
	}
	key, err := strconv.ParseUint(block[1:], 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(key), true
}

const preloadMagic = "DAOSXR01"

// validateReadValue enforces Treebeard's fixed-block contract on every demand
// read. Experiment runs may additionally validate the deterministic population
// marker and embedded key; this catches a right-sized wrong-block response at
// constant cost without scanning the payload.
func (s *Server) validateReadValue(key uint32, value []byte) error {
	if len(value) != s.params.BlockSize {
		return fmt.Errorf("read key %d returned %d bytes, want configured block size %d", key, len(value), s.params.BlockSize)
	}
	if !s.validatePreloadPayloads {
		return nil
	}
	if len(value) < len(preloadMagic)+4 || string(value[:len(preloadMagic)]) != preloadMagic {
		return fmt.Errorf("read key %d failed DAOS-XR population marker validation", key)
	}
	gotKey := uint32(value[8]) | uint32(value[9])<<8 | uint32(value[10])<<16 | uint32(value[11])<<24
	if gotKey != key {
		return fmt.Errorf("read key %d returned payload for key %d", key, gotKey)
	}
	return nil
}

// encodeValue encodes binary block data as a base64 string for Treebeard.
func encodeValue(b []byte) string {
	return base64.RawStdEncoding.EncodeToString(b)
}

// decodeValue decodes a base64 string from Treebeard back to binary block data.
func decodeValue(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	return base64.RawStdEncoding.DecodeString(s)
}

// accessSafe is access with a panic barrier (2026-07-29). UnionSession fans a
// batch out across goroutines, and a panic in one of those unwinds past its
// caller and takes the whole backend process down — losing not just the batch but
// every subsequent arm of a sweep sharing that backend. Converting it to an error
// keeps the fault inside the one block, which degradeBlock then answers empty.
func (s *Server) accessSafe(ctx context.Context, req *pb.ClientRequestPb, opportunisticBlocks []string) (val []byte, harvested map[string]string, err error) {
	defer func() {
		if r := recover(); r != nil {
			val, harvested, err = nil, nil, fmt.Errorf("panic serving key %d: %v", req.Key, r)
		}
	}()
	return s.access(ctx, req, opportunisticBlocks)
}

// access performs one Treebeard read or write and returns the block value as
// bytes plus any opportunistic candidates the router's shard happened to have
// resident in its stash while folding this request into an epoch (see
// router.proto opportunistic_blocks/opportunistic_served, added 2026-08-03).
func (s *Server) access(ctx context.Context, req *pb.ClientRequestPb, opportunisticBlocks []string) ([]byte, map[string]string, error) {
	if err := s.acquireRouterSlot(ctx); err != nil {
		return nil, nil, fmt.Errorf("router admission for key %d: %w", req.Key, err)
	}
	defer s.releaseRouterSlot()

	block := keyToBlock(req.Key)
	switch req.RequestType {
	case pb.RequestType_READ:
		reply, err := s.router.Read(ctx, &routerpb.ReadRequest{Block: block, OpportunisticBlocks: opportunisticBlocks})
		if err != nil {
			return nil, nil, fmt.Errorf("router.Read block %s: %w", block, err)
		}
		val, err := decodeValue(reply.Value)
		if err != nil {
			return nil, nil, fmt.Errorf("decodeValue block %s: %w", block, err)
		}
		if err := s.validateReadValue(req.Key, val); err != nil {
			return nil, nil, err
		}
		return val, reply.OpportunisticServed, nil

	case pb.RequestType_WRITE:
		reply, err := s.router.Write(ctx, &routerpb.WriteRequest{
			Block:               block,
			Value:               encodeValue(req.Value),
			OpportunisticBlocks: opportunisticBlocks,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("router.Write block %s: %w", block, err)
		}
		if !reply.Success {
			return nil, nil, fmt.Errorf("router.Write block %s returned success=false", block)
		}
		return req.Value, reply.OpportunisticServed, nil

	default:
		return nil, nil, fmt.Errorf("unknown request type %v", req.RequestType)
	}
}

// opportunisticBlockStrings converts the daos-xr keys the orchestrator offered
// as harvest candidates into Treebeard block ids for the router call. (Added
// 2026-08-03.)
func opportunisticBlockStrings(keys []uint32) []string {
	if len(keys) == 0 {
		return nil
	}
	blocks := make([]string, len(keys))
	for i, key := range keys {
		blocks[i] = keyToBlock(key)
	}
	return blocks
}

// mergeHarvest folds one router call's harvested blocks into the batch-wide
// accumulator, keyed by daos-xr key. Blocks that fail to decode or don't map
// back to a well-formed key are dropped -- harvest is best-effort by
// contract. (Added 2026-08-03.)
func (s *Server) mergeHarvest(acc map[uint32][]byte, harvested map[string]string) {
	for block, encoded := range harvested {
		key, ok := blockToKey(block)
		if !ok {
			continue
		}
		if _, exists := acc[key]; exists {
			continue
		}
		val, err := decodeValue(encoded)
		if err != nil {
			continue
		}
		if err := s.validateReadValue(key, val); err != nil {
			continue
		}
		acc[key] = val
	}
}

// opportunisticServedPb flattens a harvest accumulator into the wire's
// repeated OpportunisticBlockPb shape.
func opportunisticServedPb(acc map[uint32][]byte) []*pb.OpportunisticBlockPb {
	if len(acc) == 0 {
		return nil
	}
	served := make([]*pb.OpportunisticBlockPb, 0, len(acc))
	for key, val := range acc {
		served = append(served, &pb.OpportunisticBlockPb{Key: key, Value: val})
	}
	return served
}

// GetCapabilities advertises UNION: the Router's epoch batching (see package doc)
// gives UnionSession real atomic admission, not just client-side concurrency.
// Opportunistic harvest (see access) needs no capability of its own -- the
// daos_xr wire contract carries it unconditionally -- but max_opportunistic_keys
// bounds how many candidates are worth offering per batch.
func (s *Server) GetCapabilities(_ context.Context, _ *pb.CapabilitiesReq) (*pb.CapabilitiesPb, error) {
	return &pb.CapabilitiesPb{
		Capabilities: []pb.BackendCapability{pb.BackendCapability_BACKEND_CAPABILITY_UNION},
		Hints:        map[string]uint32{"max_opportunistic_keys": 4096},
	}, nil
}

// batchQueueDepth bounds each session's queue between the wire reader and the
// service worker. The reader goroutine never stalls on service time (the daos-xr
// scheduler dispatches fire-and-continue since 2026-07-19 and expects adapters to
// keep reading frames); it blocks only when this queue is full — the deliberate
// flood-backpressure backstop, which then propagates to the orchestrator via
// HTTP/2 flow control. Mirrors the orchestrator's own channel_capacity (1024).
const batchQueueDepth = 1024

// batchRecvStream is the shared shape of both session server streams.
type batchRecvStream interface {
	Recv() (*pb.BatchRequestPb, error)
	Context() context.Context
}

// pumpBatches decouples the wire from service: a reader goroutine pulls frames
// into a bounded channel so the handler can serve batches at the backend's pace
// while further requests keep arriving. The goroutine exits on EOF, stream error,
// or stream-context cancellation (the handler returning cancels the context, so
// an early service error cannot leak the reader).
//
// queuedBatch carries the frame transport-ingress time through adapter queueing.
// time.Time retains its monotonic reading for local duration arithmetic while
// UnixMilli supplies the protobuf epoch clock domain.
type queuedBatch struct {
	batch      *pb.BatchRequestPb
	receivedAt time.Time
}

func pumpBatches(stream batchRecvStream) (<-chan queuedBatch, <-chan error) {
	batches := make(chan queuedBatch, batchQueueDepth)
	errc := make(chan error, 1)
	ctx := stream.Context()
	go func() {
		defer close(batches)
		for {
			batch, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				errc <- err
				return
			}
			receivedAt := time.Now()
			select {
			case batches <- queuedBatch{batch: batch, receivedAt: receivedAt}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return batches, errc
}

// sendableStream is the shared shape of both session server streams, adding the
// Send half that serveSession serialises.
type sendableStream interface {
	batchRecvStream
	Send(*pb.BatchResponsePb) error
	SendHeader(metadata.MD) error
}

// serveSession drives one session: it pumps frames off the wire and serves up to
// serveConcurrency batches in parallel, serialising only the Send.
//
// # Why batches must overlap (2026-08-01)
//
// Both handlers used to serve batches strictly sequentially — `for queued := range
// batches { serve; Send }` — so exactly ONE batch was ever in service, no matter how
// many the orchestrator had dispatched. pumpBatches decoupled the *reader* from
// service, but not service from itself, which capped the adapter near
// 1/(per-batch latency) backend ops per second regardless of every tuning knob on
// either side.
//
// That is the wrong shape for Treebeard specifically. Its router (pkg/router/epoch.go)
// queues every Read/Write into the current epoch and, on each tick, fires the whole
// epoch as ONE ShardNode.BatchQuery per shard **in its own goroutine** — epochs never
// block each other, and one fold costs roughly one shard round-trip however many
// requests it carries. The design assumes many callers waiting concurrently; a single
// sequential caller puts one batch's worth of requests into each epoch and pays a full
// round-trip for it, discarding the amortisation the backend exists to provide.
//
// Overlapping batches is a transport/pipelining property, not an ORAM semantic: each
// request still takes the same path and lands in whichever epoch it arrives in. In
// particular it does not blur UNION into the per-block path — UNION's meaning is atomic
// co-admission of one committed set into a single epoch fold (see the package doc),
// which BackendSession still never does.
//
// Out-of-order batch completion is safe: the orchestrator's response router matches on
// request id via its route map, not on arrival order.
func (s *Server) serveSession(
	stream sendableStream,
	serve func(ctx context.Context, queued queuedBatch) *pb.BatchResponsePb,
) error {
	// Flush response headers eagerly. The orchestrator (tonic) blocks on the
	// stream-open RPC until the server sends its response HEADERS frame, but these
	// handlers read before they write and grpc-go withholds headers until the first
	// Send — deadlocking the dial. Sending headers up front unblocks the client.
	if err := stream.SendHeader(nil); err != nil {
		return err
	}
	ctx := stream.Context()
	batches, recvErr := pumpBatches(stream)

	// grpc-go forbids concurrent SendMsg on one stream, so responses funnel through
	// this mutex. It is held only for the marshal/write, never across a backend access.
	var sendMu sync.Mutex
	var sendErr atomic.Pointer[error]

	var wg sync.WaitGroup
	for i := 0; i < s.serveConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for queued := range batches {
				// A failed Send means the stream is gone; stop serving but keep
				// draining so the reader goroutine is never left blocked on the
				// queue (it also exits via ctx.Done once the handler returns).
				if sendErr.Load() != nil {
					continue
				}
				resp := serve(ctx, queued)
				sendMu.Lock()
				err := stream.Send(resp)
				sendMu.Unlock()
				if err != nil {
					sendErr.CompareAndSwap(nil, &err)
				}
			}
		}()
	}
	wg.Wait()

	if err := sendErr.Load(); err != nil {
		return *err
	}
	select {
	case err := <-recvErr:
		return err
	default:
		return nil
	}
}

// BackendSession processes each batch in committed order, translating each item to
// the existing router.Read or router.Write API. Blocks within a batch stay strictly
// sequential — this is the per-block (non-UNION) control path — while batches
// themselves overlap; see serveSession.
func (s *Server) BackendSession(stream pb.BackendIngress_BackendSessionServer) error {
	return s.serveSession(stream, func(ctx context.Context, queued queuedBatch) *pb.BatchResponsePb {
		batch := queued.batch
		recvMs := uint64(queued.receivedAt.UnixMilli())
		responses := make([]*pb.ClientResponsePb, 0, len(batch.Committed))
		opportunisticBlocks := opportunisticBlockStrings(batch.OpportunisticKeys)
		harvested := make(map[uint32][]byte)
		for _, req := range batch.Committed {
			val, opp, err := s.accessSafe(ctx, req, opportunisticBlocks)
			if err != nil {
				responses = append(responses, s.degradeBlock(req, recvMs, err))
				continue
			}
			s.mergeHarvest(harvested, opp)
			responses = append(responses, &pb.ClientResponsePb{
				RequestId:        req.RequestId,
				RequestType:      req.RequestType,
				Key:              req.Key,
				Value:            val,
				ProxyReceivedMs:  recvMs,
				ProxyRespondedMs: uint64(time.Now().UnixMilli()),
			})
		}
		return &pb.BatchResponsePb{BatchId: batch.BatchId, Responses: responses, OpportunisticServed: opportunisticServedPb(harvested)}
	})
}

// UnionSession admits every committed request in a batch into the Router's
// current epoch, bounded to unionFanout concurrent outstanding calls (see the
// unionFanout field doc and package doc "UNION fan-out is bounded"), then
// answers them once all have returned. Batches overlap as well (see
// serveSession), so successive unions fold into successive epochs instead of
// waiting for one another.
func (s *Server) UnionSession(stream pb.BackendIngress_UnionSessionServer) error {
	return s.serveSession(stream, s.serveUnionBatch)
}

// serveUnionBatch is UnionSession's per-batch body. Pulled out to a named
// method (rather than left as an inline closure) so it can be exercised
// directly in tests without a fake gRPC stream — queuedBatch has no stream
// dependency, just a *pb.BatchRequestPb and a receive timestamp.
func (s *Server) serveUnionBatch(ctx context.Context, queued queuedBatch) *pb.BatchResponsePb {
	batch := queued.batch
	recvMs := uint64(queued.receivedAt.UnixMilli())
	responses := make([]*pb.ClientResponsePb, len(batch.Committed))
	errs := make([]error, len(batch.Committed))
	opps := make([]map[string]string, len(batch.Committed))
	opportunisticBlocks := opportunisticBlockStrings(batch.OpportunisticKeys)
	var wg sync.WaitGroup
	for i, req := range batch.Committed {
		wg.Add(1)
		go func(i int, req *pb.ClientRequestPb) {
			defer wg.Done()
			// Bounded fan-out (2026-08-04): see the unionFanout field doc.
			// Acquired here, ahead of accessSafe's own routerSlots gate,
			// because that gate is a global cross-batch ceiling sized to
			// Treebeard's own max_requests (~8000) — far too loose on its
			// own to stop one oversized batch from overwhelming a shard's
			// stash.
			if err := s.acquireUnionFanoutSlot(ctx); err != nil {
				errs[i] = fmt.Errorf("union fan-out admission for key %d: %w", req.Key, err)
				return
			}
			defer s.releaseUnionFanoutSlot()
			val, opp, err := s.accessSafe(ctx, req, opportunisticBlocks)
			if err != nil {
				errs[i] = err
				return
			}
			opps[i] = opp
			responses[i] = &pb.ClientResponsePb{
				RequestId:        req.RequestId,
				RequestType:      req.RequestType,
				Key:              req.Key,
				Value:            val,
				ProxyReceivedMs:  recvMs,
				ProxyRespondedMs: uint64(time.Now().UnixMilli()),
			}
		}(i, req)
	}
	wg.Wait()
	// Degrade the failed slots in place rather than aborting the session. Every
	// slot must be non-nil before Send: a nil element would otherwise be
	// marshalled as a zero-valued response the orchestrator cannot attribute.
	for i, err := range errs {
		if err != nil || responses[i] == nil {
			if err == nil {
				err = errors.New("access returned no response")
			}
			responses[i] = s.degradeBlock(batch.Committed[i], recvMs, err)
		}
	}
	harvested := make(map[uint32][]byte)
	for _, opp := range opps {
		s.mergeHarvest(harvested, opp)
	}
	return &pb.BatchResponsePb{BatchId: batch.BatchId, Responses: responses, OpportunisticServed: opportunisticServedPb(harvested)}
}
