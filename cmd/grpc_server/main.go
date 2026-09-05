// treebeard_grpc — daos-xr BackendIngress adapter for Treebeard.
//
// Connects to a running Treebeard router (Router.Read / Router.Write gRPC),
// and exposes daos_xr BackendIngress + Capability.
//
// Usage: treebeard_grpc --port 4000 --router-addr localhost:8745 [--conf ./configs/default]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"time"

	"github.com/dsg-uwaterloo/treebeard/pkg/config"
	grpcserver "github.com/dsg-uwaterloo/treebeard/pkg/grpc_server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/dsg-uwaterloo/treebeard/api/daos_xr"
	routerpb "github.com/dsg-uwaterloo/treebeard/api/router"
)

func main() {
	port := flag.Int("port", 4000, "daos-xr BackendIngress listen port")
	routerAddr := flag.String("router-addr", "", "Treebeard router address (ip:port); overrides --conf")
	confPath := flag.String("conf", "configs/default", "Treebeard configs directory (for router_endpoints.yaml + parameters.yaml)")
	pidFile := flag.String("pid-file", "", "write PID here on startup (for ansible lifecycle)")
	statsPath := flag.String("stats", "", "path to write treebeard-stats.csv (optional)")
	serveConcurrency := flag.Int("serve-concurrency", 64,
		"batches served in parallel per session; 1 = the pre-2026-08-01 sequential behaviour")
	unionFanoutLimit := flag.Int("union-fanout-limit", 64,
		"blocks from a SINGLE UNION batch allowed an outstanding Router call at once; "+
			"1 serialises UNION batches like the non-UNION path (2026-08-04)")
	validatePreloadPayloads := flag.Bool("validate-preload-payloads", false,
		"require every READ to contain the DAOSXR01 marker and embedded requested key; enable for deterministic experiment populations")
	flag.Parse()

	if *pidFile != "" {
		if err := os.WriteFile(*pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
			log.Fatalf("treebeard_grpc: failed to write pid file: %v", err)
		}
	}

	// Resolve router address: flag wins over config file.
	addr := *routerAddr
	if addr == "" {
		endpoints, err := config.ReadRouterEndpoints(*confPath + "/router_endpoints.yaml")
		if err != nil || len(endpoints) == 0 {
			log.Fatalf("treebeard_grpc: cannot read router endpoints from %s: %v", *confPath, err)
		}
		ep := endpoints[0]
		addr = fmt.Sprintf("%s:%d", ep.IP, ep.Port)
	}

	params, err := config.ReadParameters(*confPath + "/parameters.yaml")
	if err != nil {
		log.Fatalf("treebeard_grpc: cannot read parameters.yaml: %v", err)
	}

	// Connect to the Treebeard router.
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDial()
	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32),
		),
	)
	if err != nil {
		log.Fatalf("treebeard_grpc: router at %s was not ready within 10s: %v", addr, err)
	}
	defer conn.Close()

	routerClient := routerpb.NewRouterClient(conn)
	srv := grpcserver.New(routerClient, params, *statsPath, *serveConcurrency, *unionFanoutLimit, *validatePreloadPayloads)

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("treebeard_grpc: failed to listen on port %d: %v", *port, err)
	}

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
	)
	pb.RegisterBackendIngressServer(grpcSrv, srv)
	pb.RegisterCapabilityServer(grpcSrv, srv)

	log.Printf("treebeard_grpc: listening on :%d (router=%s block_size=%d max_blocks_to_send=%d tree_height=%d serve_concurrency=%d router_inflight_limit=%d union_fanout_limit=%d validate_preload_payloads=%t)",
		*port, addr, params.BlockSize, params.MaxBlocksToSend, params.TreeHeight, *serveConcurrency, params.MaxRequests, *unionFanoutLimit, *validatePreloadPayloads)

	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("treebeard_grpc: serve error: %v", err)
	}
}
