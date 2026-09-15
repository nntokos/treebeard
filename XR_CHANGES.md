# oasis-adapter-og changes

Branch: `oasis-adapter-og`
Origin: `git@github.com:nntokos/treebeard.git` (cloned from upstream, remote renamed)
Upstream: `https://github.com/dsg-uwaterloo/treebeard`
Added: 2026-09-15

## What this fork is, and how it differs from `backends/treebeard`

`treebeard-og` is the **as-published** Treebeard backend: it carries the daos-xr
adapter and the opportunistic stash harvest, and **nothing else**. Every storage /
oramnode correctness fix that `backends/treebeard` (branch `oasis-adapter`) applies on
top of upstream is reverted here, so the ORAM data path is byte-for-byte
`upstream/main`.

The two forks exist so the orchestrator can be evaluated against the backend as its
authors shipped it (`treebeard-og`) and against a repaired backend (`treebeard`),
without either arm silently standing in for the other. They are separate backends end
to end: separate fork checkout, separate ansible playbook tree
(`experiments/treebeard-og/ansible/`), separate `/tmp` build+storage root, separate
experiment tree. They are never run concurrently and never share a result directory.

### Reverted relative to `backends/treebeard` (2026-09-15)

These five files are restored to `upstream/main` verbatim
(`git checkout upstream/main -- <file>`); `git diff upstream/main -- pkg/storage pkg/oramnode`
is empty on this branch.

| File | Fix present in `treebeard`, absent here |
|------|----------------------------------------|
| `pkg/storage/storage.go` | `selectDummyBlock()` — scan for any surviving dummy slot instead of assuming `dummy1` (resp. `dummy1..dummyN` in order) is still valid. Upstream's `BatchReadBucket` miss path returns `nil, err` with `err == nil`. |
| `pkg/storage/storage.go` | `BatchReadBucket` now checks the error from `BatchGetAllMetaData`; upstream discards it. |
| `pkg/storage/storage.go` | `BatchReadBlock` post-read invalidation. Upstream matches a metadata *value* against the physical offset and `HSet`s at that physical position; the fix resolves the physical offset back to its **logical** metadata field first. Upstream also builds its `bucketIDs` slice with `make(..., len)` + `append`, so half of it is zero values. |
| `pkg/storage/storage_helper.go` | `metadataFieldForOffset()` / `batchGetMetadataFieldsForOffsets()` — the helpers the above invalidation fix needs. |
| `pkg/oramnode/server.go` | `earlyReshuffle` write-back. Upstream fires `go o.storageHandler.BatchWriteBucket(...)` and drops the result; the fix makes it synchronous and propagates the error. |
| `pkg/oramnode/server.go` | `ReadPath` returned a stale outer `err` (usually `nil`) on a block-read failure, masking it; the fix wraps `response.err`. |
| `pkg/storage/storage_test.go`, `pkg/storage/storage_helper_test.go` | The unit tests covering the two fixes above. |

**Expected consequence.** This arm can and does surface the upstream faults the fixes
were written for — wrong-slot metadata invalidation, dummy-slot exhaustion, and reshuffle
write-backs whose failure is never observed. Errors of the shape `could not get offset
from storage`, short or empty demand reads, and delivery-validation rejections are
therefore *results* on this arm, not run faults, and must not be "fixed" by importing
patches from `backends/treebeard`. If a run on this arm fails, record it; do not repair
the backend.

### Kept here, identical to `backends/treebeard`

- The whole daos-xr adapter: `cmd/grpc_server/`, `pkg/grpc_server/`, `api/daos_xr*`,
  `Makefile`. This is the compatibility shim, not backend behaviour.
- **Opportunistic stash harvest** (`api/router.proto`, `api/shardnode.proto`,
  `pkg/router/epoch.go`, `pkg/router/server.go`, `pkg/shardnode/server.go`). Kept
  deliberately: `getStashHarvest` is a read-only map lookup under a lock the batch path
  already holds, it adds no path I/O, and it mutates neither the stash nor the position
  map — it reports state the ORAM already produced. It changes what the backend
  *exposes*, not what it *does*, so keeping it costs the "as-published" framing nothing
  while keeping the two arms' adapter surface identical.
  - One caveat, recorded so it is not mistaken for an accident: the harvest change also
    restructured `sendEpochRequestsAndAnswerThem` to buffer every shard's reply for an
    epoch before answering any request, where upstream answered each shard reply as it
    arrived. Within an epoch a caller now waits for the slowest shard rather than its
    own. The 10 s epoch timeout is unchanged. This is present on BOTH forks, so it never
    distinguishes them.
- Protobuf regeneration noise in `api/*/`: protoc-gen-go 1.33 to 1.34.1, protoc 4.25.3
  to 3.21.12, and the `go_package` path `dsg-uwaterloo/oblishard` to
  `dsg-uwaterloo/treebeard`. No wire or semantic change.

Everything below this line is inherited from the `oasis-adapter` branch and applies to
both forks unless a section says otherwise.

---

## Inherited adapter notes (branch `oasis-adapter`, added 2026-07-12)

### New files

| Path | Purpose |
|------|---------|
| `Makefile` | Builds `treebeard_grpc` adapter + upstream cluster binaries |
| `api/daos_xr.proto` | Copy of `crates/proto/proto/daos_xr.proto` with `go_package` added |
| `api/daos_xr/` | Generated gRPC stubs (`make proto`) |
| `cmd/grpc_server/main.go` | `treebeard_grpc` entry point |
| `pkg/grpc_server/server.go` | BackendIngress / Capability / BatchUnionIngress implementations |

## Architecture

```
daos-xr orchestrator
        │
  daos_xr.proto (port 4000)
        │
  treebeard_grpc (adapter)
        │
  router.proto (port 8745)
        │
  Treebeard Router ──► ShardNode ──► OramNode ──► Redis
```

The adapter translates between the two gRPC layers. It does NOT start Treebeard's
internal components — those are managed separately by the experiment ansible playbook.

## Key translation decisions

- **Block id**: daos-xr `uint32 key` → Treebeard `string block` = **`"k" + strconv.FormatUint(key)`**.
  The `"k"` prefix is REQUIRED, not cosmetic. Treebeard stores each block's bucket-slot
  metadata as the string `"<pos><blockid>"` with **no separator**, and its reader
  (`storage.parseMetadataBlock`) recovers `<pos>` by scanning to the first NON-digit
  character. A purely numeric block id makes `"<pos><id>"` all digits → the scan finds no
  boundary → `block[:0] == ""` → `strconv.Atoi("")` fails, surfacing as
  `could not get offset from storage` on any bucket holding a real (written-back) block.
  Empty/dummy buckets parse fine (dummy ids `"dummyN"` start with a letter), so the
  failure is **cumulative** — reads work briefly, then fail more and more as blocks get
  placed, eventually wedging. Prefixing every real id with a non-digit gives it the same
  clean delimiter as the dummies. The prefix is internal to the Treebeard cluster: the
  adapter matches committed responses by `request_id` and echoes the original `req.Key`,
  never parsing the block string back for the demand path. (Added 2026-07-15. As of
  2026-08-03 `blockToKey` in `pkg/grpc_server/server.go` does parse it back, but only for
  harvested opportunistic blocks, which have no `request_id` of their own — see below.)
- **Value encoding**: daos-xr `[]byte` → base64 string in Router.Write; decoded on Router.Read
- **UNION** (`BatchUnionIngress`): committed blocks sent as concurrent goroutines to the
  router so they land within the same Treebeard epoch window (epoch_time ms). This is
  the natural UNION equivalent for Treebeard's epoch-batching architecture. Concurrency
  is bounded per batch by `--union-fanout-limit` (default 64) — see "UNION fan-out
  bound" below; it is not "every block in the batch at once."
- **HARVEST**: advertised unconditionally (no capability needed, per the daos_xr wire
  contract) since 2026-08-03 — see "Opportunistic stash harvest" below for how the
  per-bucket over-read that was deferred here is now exposed at the router gRPC level.
- **Eager stream headers** (`stream.SendHeader(nil)` at the top of `BackendSession`
  and `BatchUnionSession`): REQUIRED, not optional. The daos-xr orchestrator is a
  tonic (Rust) client whose stream-open `.await` blocks until the server flushes its
  HTTP/2 response HEADERS frame. grpc-go withholds those headers until the handler's
  first `Send`, but these handlers `Recv` before they `Send` — so without an eager
  `SendHeader` the dial deadlocks (orchestrator hangs at `dialing backend gRPC ...`,
  never reaching `listening for clients on`). The Rust/tonic backends (cloak,
  path-oram, roram) flush headers eagerly and never hit this; Treebeard is the only
  Go/grpc-go backend, so it must send headers explicitly. **Any future Go backend
  adapter with a read-before-write streaming handler needs the same call.** (Added
  2026-07-14.)

## Syncing with upstream

```bash
git fetch upstream
git rebase upstream/main   # or merge, if preferred
```

Only the files listed above were added; rebase conflicts are not expected.

## Build

```bash
# 1. Generate daos_xr gRPC stubs (one-time, or after proto changes):
make proto

# 2. Build the adapter binary:
make treebeard_grpc

# 3. (Optional) Build upstream cluster binaries:
make cluster_binaries
```

Requires: `go 1.20+`, `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`.
Install Go plugins: `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`

## Single-machine deployment

### 2026-07-16 — workload population

After the adapter becomes ready, the experiment runner resolves the selected XR manifest and writes every addressed key through the canonical `BackendSession` before starting the orchestrator/client cycle. This creates real Treebeard records rather than relying on the dummy-only Redis tree initialization. No upstream Treebeard source or algorithm is changed.

All Treebeard components (Redis, oramnode×3, shardnode, router) run on the same
`[server]` host alongside `treebeard_grpc`. Start order:
1. Redis (port 6379)
2. oramnode replica 0 (bootstrap), then replicas 1 and 2 (join replica 0)
3. shardnode
4. router
5. Wait for Redis to fill (storage init complete)
6. treebeard_grpc adapter

Startup is managed by `experiments/treebeard/ansible/playbooks/tasks/backend_up.yml`.

## Redis storage sizing

Treebeard initialises its ORAM tree into Redis before accepting requests.
Storage entries = `(2^tree_height - 1) × 2` Redis keys (each key = one bucket).
Each bucket holds `Z + S` blocks (default Z=1, S=9 → 10 blocks/bucket).

| block_size | tree_height | leaves   | approx. Redis size |
|-----------|-------------|----------|--------------------|
| 1024      | 21          | 2,097,152 | ~20 GB            |
| 10240     | 18          | 262,144   | ~26 GB            |
| 102400    | 13          | 8,192     | ~820 MB (reduced-scale, see NOTES.md) |

The default block size (bs102400) is run at tree_height=13 (8192 leaves) on a
single machine due to Redis memory limits. This covers a workload of ≤8192 unique
block keys. Full-scale 400k×100KiB requires a distributed Redis setup across
multiple machines with ≥500 GB aggregate RAM.

## Free-flowing session pump (2026-07-19)

The adapter's session handler no longer serves batches inside the stream
callback/read loop. Reading and service are decoupled: the wire reader enqueues
inbound `BatchRequestPb`s into a bounded per-session queue (1,024 batches,
mirroring the orchestrator's channel capacity) and a service worker serves them
at the backend's pace, emitting one response frame per batch as before. The
reader blocks only when the queue is full — the deliberate flood-backpressure
backstop, propagated to the orchestrator via HTTP/2 flow control. Required by
the orchestrator's fire-and-continue dispatch (2026-07-19): an adapter that
stops reading frames while serving re-creates stop-and-wait at the transport.
Per-batch service semantics are unchanged. Adapter-only; no upstream source
touched.


## Adapter ingress timing (2026-07-21)

`pumpBatches` now wraps every received frame with `time.Now()` before its bounded channel send. The embedded monotonic reading remains available for local elapsed timing, while `UnixMilli()` supplies the unchanged response wire fields; current Treebeard adapter stats remain unimplemented. Both session modes now include adapter queue residence in backend-service latency. No Treebeard backend code changed.


## Concurrent batch service (2026-08-01)

Both session handlers served batches **strictly sequentially** — `for queued := range
batches { serve; Send }` — so exactly one batch was ever in service, however many the
orchestrator had dispatched. `pumpBatches` (2026-07-19) decoupled the wire *reader* from
service, but never service from itself, so the adapter was capped near
`1 / per-batch-latency` backend ops/s regardless of any orchestrator or Treebeard knob.

That is the wrong shape for Treebeard in particular. Its router (`pkg/router/epoch.go`)
queues every Read/Write into the current epoch and, on each tick, fires the whole epoch
as one `ShardNode.BatchQuery` per shard **in its own goroutine** (`go
e.sendEpochRequestsAndAnswerThem(...)`) — epochs never block one another, and a fold
costs roughly one shard round-trip however many requests it carries. The architecture
assumes many callers waiting concurrently; a single sequential caller puts one batch's
worth of requests in each epoch and pays a full round-trip for it, discarding exactly the
amortisation Treebeard exists to provide.

**Change:** a new `serveSession` drives both handlers with a bounded per-session worker
pool (`--serve-concurrency`, default 64), serialising only `stream.Send` under a mutex
(grpc-go forbids concurrent `SendMsg` on one stream; the lock is never held across a
backend access). `--serve-concurrency=1` reproduces the old sequential behaviour exactly,
so the change is A/B-testable.

Semantics are unchanged. Overlapping batches is a transport/pipelining property, not an
ORAM one: each request takes the same path and lands in whichever epoch it arrives in.
It does not blur UNION into the per-block path — UNION means atomic co-admission of one
committed set into a single epoch fold, which `BackendSession` still never does, and
blocks within a `BackendSession` batch remain strictly sequential so the non-UNION
control path keeps its meaning. Out-of-order batch completion is safe: the orchestrator's
response router matches on request id via its route map, not arrival order.

Adapter-only; no upstream Treebeard source touched.

## Router admission parity (2026-08-03)

Treebeard's native client wraps Router RPCs in a global semaphore sized by
`parameters.max-requests`. The daos-xr adapter bypasses that client, and initially omitted the
equivalent gate: overlapping worker batches multiplied by UNION fan-out could therefore exceed
the configured backend admission ceiling by an order of magnitude. Under a slow shard fold this
also exercises the Router's 10-second epoch timeout repeatedly; late replies target unbuffered
channels after the epoch waiter has returned, retaining blocked goroutines and amplifying overload.

The adapter now admits at most `max-requests` Router RPCs globally across every stream and batch.
Waiting happens before the Router call and honors stream cancellation. `treebeard-stats.csv` is
published atomically once per second with the configured limit, current and peak in-flight calls,
admission wait count/latency, and degraded-block count. This restores the native client's safety
contract and makes saturation directly distinguishable from a backend crash. Adapter-only; the
upstream Router implementation remains unchanged.

## UNION fan-out bound (2026-08-04)

`UnionSession` used to launch one goroutine per block in a batch with no bound beyond
the global router-slot admission from "Router admission parity" above (sized to
`max-requests`, typically 8000 — a cross-batch, cross-session ceiling that does nothing
to stop a single oversized batch from overwhelming one shard on its own). At
`--batch-size 1024` that let one client-issued UNION batch inject up to 1024
simultaneous `Router.Read` calls.

Observed effect on the daos-xr side (2026-08-04, `experiments/treebeard/v10-0/`):
the shardnode stash climbed monotonically over a run and pinned near 20,000 resident
blocks, versus the non-UNION passthrough baseline's ~2,000-3,000 oscillating peak
under the same workload. A saturated stash makes every ORAM access effectively
O(stash), collapsing completion throughput from Treebeard's proven ~2,050 blocks/s
(measured on the passthrough baseline) to ~1-3/s and starving eviction entirely —
the run times out having served a small fraction of demand. The stash growth also
explains a class of router OOM kills that a memory-only accounting (in-flight
ceiling × batch-size × block_size) did not fully predict: an unbounded burst of
concurrent path reads holds far more live state at once than the batch's own payload
bytes.

Fix: `UnionSession`'s per-batch dispatch is now gated by a second, independent
semaphore (`unionFanoutSlots`, sized by `--union-fanout-limit`, default 64) acquired
ahead of the existing global `routerSlots` gate. This caps how many blocks from ONE
batch may have an outstanding Router call simultaneously, without touching the global
cross-batch ceiling. UNION's atomicity guarantee does not require true simultaneity:
Treebeard's `epochManager` folds by arrival-time window (`epoch_time` ticks), not by
concurrency degree, so bounding fan-out costs nothing UNION actually needs — the
package doc's "real atomic admission" framing was corrected to describe admission
ordering rather than implying every block must be dispatched at once.

A small batch (below the fanout limit) is unaffected: every block still gets a slot
immediately. `treebeard-stats.csv` gained `union_fanout_limit`/`union_fanout_peak`
columns alongside the existing router-admission ones. Tests:
`pkg/grpc_server/union_fanout_test.go`. Adapter-only; no upstream Treebeard source
touched — `UnionSession`/`serveUnionBatch` are entirely in `pkg/grpc_server/server.go`.

## Opportunistic stash harvest (2026-08-03)

Implements the `HARVEST` capability deferred above: the daos-xr orchestrator's
`opportunistic_keys` (offered on every `BatchRequestPb`) are now served from each
shard's resident stash at zero extra path I/O, mirroring the additive
`access_scan`/`access_batch` harvest already shipped in the `path-oram` fork.
Purely additive across three layers, each already ADDITIVE surface (the
`oasis-adapter` adapter, or a proto message this fork already regenerates via
`scripts/generate_protos.sh`):

- **`api/router.proto` / `api/shardnode.proto`** — `ReadRequest`/`WriteRequest` gain
  `opportunistic_blocks` (candidate block ids); `ReadReply`/`WriteReply` and
  `shardnode.RequestBatch`/`ReplyBatch` gain a matching `opportunistic_served` map.
  Regenerated with the existing `make proto_upstream` / `scripts/generate_protos.sh`
  flow — no manual stub edits.
- **`pkg/shardnode/server.go`** — new `getStashHarvest(blocks []string)`: under the
  existing `stashMu` lock (the same one `getBlocksForSend` already reads for
  eviction), it looks up each candidate in `shardNodeFSM.stash` and returns the hits.
  Read-only: no position-map or stash mutation, so it costs one map lookup per
  candidate and no path I/O. Called once per `BatchQuery`, after the batch's own
  reads/writes have resolved (so blocks that batch just fetched are already
  stash-resident and harvestable). `OramReadPathEviction`-equivalent core logic
  (`query`, the raft-replicated stash/position-map state machine) is untouched.
- **`pkg/router/epoch.go`** — each `request` now carries the candidates its caller
  offered. `getShardnodeBatches` routes every candidate to the shard that actually
  hashes to it (`whereToForward(candidate)`, NOT the requester's own block), and only
  into a shard's `RequestBatch` that already has real read/write demand this epoch —
  harvest rides an already-scheduled `BatchQuery` fold, it never triggers an extra one.
  `sendEpochRequestsAndAnswerThem` now buffers every shard's reply for the epoch,
  unions their `opportunistic_served` maps, and hands that union to every request
  answered this round — a candidate can live on a different shard than the one
  serving the caller's own block, so all of an epoch's callers see the whole epoch's
  harvest, not just their own shard's.
- **`pkg/grpc_server/server.go`** (the daos-xr adapter) — `access`/`accessSafe` now
  take the batch's offered blocks (`keyToBlock` per offered key) and return the
  router's per-call harvest. `blockToKey` reverses the `"k"` + decimal mapping
  *only* for harvested blocks (see the block-id note above) since harvested entries
  ride back keyed by block string, not `request_id`. `BackendSession` and
  `UnionSession` both accumulate harvest across every committed request in the batch
  (deduped by key, first writer wins) into `BatchResponsePb.opportunistic_served`.
  `GetCapabilities` hints `max_opportunistic_keys` (4096, matching path-oram); no
  capability bit is needed since harvest rides the common message shape
  unconditionally (see `daos_xr.proto`).

Not handled: harvest candidates are dropped (never forwarded to a shard) when that
shard has no real demand in the epoch, and when a shard reply times out or errors
its opportunistic_served is simply absent from the union — both are the same
best-effort semantics `access_scan` already has in path-oram (offered, never
required; the orchestrator re-offers on a later batch).


## Fixed-block delivery validation (2026-08-11)

The daos-xr adapter now treats Treebeard's configured `block_size` as a runtime
contract. Every demand READ must return exactly that width. The experiment runner
also enables `--validate-preload-payloads`, which checks the deterministic
`DAOSXR01` population marker and little-endian key embedded in each payload;
harvested stash values are subject to the same check before they are exposed to
the orchestrator.

Router WRITE replies are accepted only when `success=true`. This fork's legacy
daos-xr response message has no outcome field, while an empty WRITE value is the
canonical successful acknowledgment. A degraded WRITE therefore carries the
adapter-private `TREEBEARD_BACKEND_FAILED` marker so the backend populator
rejects it instead of counting it as an acknowledged write. Degraded READs remain
empty and are rejected by fixed-width validation and the experiment's final
delivery accounting.

Population can now request exact readback verification. Treebeard experiments
use two complete passes: the first exercises normal remapping/eviction after the
writes, and the second proves the data remains retrievable after that exercise.
The measured workload never starts if any key, outcome, width, or byte differs.
All changes are confined to the daos-xr adapter and experiment tooling; no
Treebeard storage, router, shardnode, or ORAM algorithm source was changed.
