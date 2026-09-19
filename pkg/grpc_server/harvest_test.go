package grpc_server

import (
	"fmt"
	"testing"

	"github.com/dsg-uwaterloo/treebeard/pkg/config"
)

// The defect this file exists for (2026-09-19): every request in a batch used to
// carry the WHOLE offered candidate list, and the router answers each caller with
// the whole epoch's merged harvest, so one batch's harvest crossed the wire once
// per committed block instead of once.
func TestPlanHarvestOffersEachCandidateExactlyOnce(t *testing.T) {
	s := New(&scriptedRouter{}, config.Parameters{MaxRequests: 1, BlockSize: 102400}, "", 1, 1, false)

	keys := make([]uint32, 128)
	for i := range keys {
		keys[i] = uint32(1000 + i)
	}
	plan := s.planHarvest(keys, 128)

	if len(plan) != 128 {
		t.Fatalf("plan has %d parts, want one per committed request (128)", len(plan))
	}
	seen := map[string]int{}
	total := 0
	for _, part := range plan {
		for _, block := range part {
			seen[block]++
			total++
		}
	}
	if total != len(keys) {
		t.Fatalf("offered %d candidate slots, want exactly the %d keys offered", total, len(keys))
	}
	for block, n := range seen {
		if n != 1 {
			t.Fatalf("block %s offered %d times; each candidate must reach the stash once per batch", block, n)
		}
	}
	for _, key := range keys {
		if seen[keyToBlock(key)] != 1 {
			t.Fatalf("key %d was dropped from the plan", key)
		}
	}
}

// The split must actually split: a single part carrying everything would keep the
// straggler and peak-message problems that made splitting preferable to nominating
// one carrier request.
func TestPlanHarvestSpreadsAcrossRequests(t *testing.T) {
	s := New(&scriptedRouter{}, config.Parameters{MaxRequests: 1, BlockSize: 102400}, "", 1, 1, false)

	keys := make([]uint32, 256)
	for i := range keys {
		keys[i] = uint32(i)
	}
	plan := s.planHarvest(keys, 128)

	for i, part := range plan {
		if len(part) != 2 {
			t.Fatalf("part %d carries %d candidates, want an even 2 per request", i, len(part))
		}
	}
}

// Everything the orchestrator offers is forwarded: the harvest size is
// `opportunistic_max` and this adapter adds no bound behind it (2026-09-19).
func TestPlanHarvestForwardsEverythingOffered(t *testing.T) {
	s := New(&scriptedRouter{}, config.Parameters{MaxRequests: 1, BlockSize: 102400}, "", 1, 1, false)

	keys := make([]uint32, 128)
	for i := range keys {
		keys[i] = uint32(i)
	}
	plan := s.planHarvest(keys, 128)

	total := 0
	for _, part := range plan {
		total += len(part)
	}
	if total != len(keys) {
		t.Fatalf("forwarded %d of %d offered candidates; the adapter must not cap what "+
			"the orchestrator asked for", total, len(keys))
	}
	if got := s.harvestOffered.Load(); got != uint64(len(keys)) {
		t.Fatalf("harvest_offered = %d, want %d", got, len(keys))
	}
}

// mergeHarvest is what makes the duplication factor observable. It must count
// every entry the router hands back, and keep each block once.
func TestMergeHarvestCountsDuplicationWithoutRedecoding(t *testing.T) {
	s := New(&scriptedRouter{}, config.Parameters{MaxRequests: 1, BlockSize: 32}, "", 1, 1, false)

	// validateReadValue enforces the fixed block size on harvested blocks too, so
	// the fixtures must be exactly BlockSize bytes or they are dropped before the
	// accumulator sees them.
	block := func(fill byte) string {
		buf := make([]byte, 32)
		for i := range buf {
			buf[i] = fill
		}
		return encodeValue(buf)
	}
	acc := make(map[uint32][]byte)
	reply := map[string]string{
		keyToBlock(7): block('a'),
		keyToBlock(8): block('b'),
	}
	// Three callers in one router epoch each receive the same merged harvest —
	// exactly the broadcast this adapter cannot suppress from its side.
	for i := 0; i < 3; i++ {
		s.mergeHarvest(acc, reply)
	}

	if len(acc) != 2 {
		t.Fatalf("accumulator holds %d blocks, want 2 distinct", len(acc))
	}
	if got := s.harvestReceived.Load(); got != 6 {
		t.Fatalf("harvest_received = %d, want 6 (3 replies x 2 entries)", got)
	}
	if got := s.harvestMerged.Load(); got != 2 {
		t.Fatalf("harvest_merged = %d, want 2 distinct blocks kept", got)
	}
	if fmt.Sprintf("%.1f", float64(s.harvestReceived.Load())/float64(s.harvestMerged.Load())) != "3.0" {
		t.Fatalf("the published duplication factor must be received/merged")
	}
}
