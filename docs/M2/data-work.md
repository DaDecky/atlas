# Data implementation: B1–B13

This branch implements the data owner's work. Local tests use PostgreSQL and
RabbitMQ; they do not establish the final three-machine demo results. The
assistant's real dataset, cross-host timings, Tester Agent scenarios, and the
other owners' leadership/ingestion integrations remain team integration work.

## Coverage

| Item | Implementation | Verification |
|---|---|---|
| B1 | `ShardStore`, `WriteBatch`, PostgreSQL implementation | Shared contract suite on single and replicated stores |
| B2 | 48 virtual shards, SHA-256 key hashing, rendezvous top-two, ConfigMap resource-version CAS | Distribution, input-order stability, stale CAS tests |
| B3 | Repeatable M1 upgrade, expanded source/type checks, byte collation, ingested index, per-shard schemas and bootstrap owner roles | Real PostgreSQL startup with the deployment init script |
| B4 | Three-store empty-copy/refill experiment while writing | Wipe/refill/next-loss and concurrent-refill tests |
| B5 | Fenced dual write; CAS `behind` before accepting degraded writes | Replicated contract, wipe/repair tests |
| B6 | Scatter-gather with eight concurrent shard reads, deadlines/fallback, filter-bound keyset cursor, partial markers | Reference counts, no duplicate pages, second-loss partial reads |
| B7 | Placement Lease, non-serving refill, revision-conditional copy, checksum verification, incremental membership rebalance | Empty-volume recovery, live refill, new-node join |
| B8 | Authenticated Audit events/checksum/location/unprocessed | Independent byte-order SHA-256 and HTTP contract tests |
| B9 | Analytics summary plus browser page with range filter, timeline, severity, area and incomplete marker | Totals match Audit, route authorization and HTML checks |
| B10 | Atomic outbox, `SKIP LOCKED` relay on each store, critical lane before fanout, stable message IDs | Broker failure retry, concurrent claims, priority and crash persistence |
| B11 | Raw parking, reason/retry metadata, due retries, fenced ordered replay and timestamp guard | Raw bytes, pagination, concurrent replay, replica repair |
| B12 | StatefulSet, local-path PVCs, required host anti-affinity, owner bootstrap | Kustomize build plus bootstrap exercised on local PostgreSQL |
| B13 | Canonical JSONL loader, deterministic regional generator, per-node counts/bytes/write metrics | Synthetic dataset tool and node distribution tests |

Local verification results are in [evidence/m2-data](../../evidence/m2-data/README.md).

## Run locally

From the repository root:

```sh
make test
make lint
make build
make integration
make data-integration
```

`data-integration` creates three data nodes plus a fourth temporary node for the
membership-join test. Ports bind to loopback at random ports, credentials are
public test-only values, and the project and its disposable volumes are removed
on exit. No team machine is involved. This test is not a demo cluster.

## Deployment handoff to Adha

Include `deploy/store` in the deployment Kustomization. It supplies three stores,
a headless Service, bootstrap ConfigMap, and an `aggregator-data` service account
with the placement/recovery permissions. Keep the namespace aligned with the
base deployment. The three pods require three schedulable nodes and `local-path`.
Adha's NetworkPolicies must permit Aggregator-to-store, DNS, Kubernetes API, and
the Tester Agent, while denying other application services direct store access.

Create Secret `canonical-store-credentials` with `admin-password` and
`aggregator-password`. Existing volumes keep the original database credentials;
changing a Secret alone does not rotate persisted PostgreSQL roles. Put the
per-node connection configuration in a mounted Secret file, for example:

```json
{
  "nodes": [
    {"name":"ACTUAL_NODE_A","url":"postgres://aggregator:PASSWORD@canonical-store-0.canonical-store:5432/canonical?sslmode=disable"},
    {"name":"ACTUAL_NODE_B","url":"postgres://aggregator:PASSWORD@canonical-store-1.canonical-store:5432/canonical?sslmode=disable"},
    {"name":"ACTUAL_NODE_C","url":"postgres://aggregator:PASSWORD@canonical-store-2.canonical-store:5432/canonical?sslmode=disable"}
  ],
  "placement": {
    "origin":"https://kubernetes.default.svc",
    "namespace":"sistem-informasi-kebencanaan-bnpb",
    "name":"canonical-placement",
    "tokenFile":"/var/run/secrets/kubernetes.io/serviceaccount/token",
    "caFile":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
  }
}
```

**Use actual node names from the scheduling result**, not invented labels or pod
ordinals. Required anti-affinity separates stores but does not fix which ordinal
lands on which host. Confirm the mapping with `kubectl get pods -o wide`.

Generate the initial placement ConfigMap (JSON is valid Kubernetes input):

```sh
cd services/bnpb/aggregator
go run ./cmd/data-tool -mode placement -nodes ACTUAL_NODE_A,ACTUAL_NODE_B,ACTUAL_NODE_C > placement.json
kubectl apply -f placement.json
```

Set `M2_DATA_CONFIG` to the mounted configuration path, select service account
`aggregator-data`, and set `AUDIT_TOKEN` from a Secret. `M2_READ_ONLY=1` starts the
API, relay and recovery planner while source leadership integration is pending.
It does not ingest upstream data. All existing M1 required env values remain
required by the current application config parser. The recovery planner renews
its placement Lease; it does not replace source election.

The configuration file is read at startup. For a new node, add the store endpoint
and actual node name, then roll the Aggregators. New configured healthy nodes
are gradually assigned shard copies by the placement planner. Existing source
writes and reads continue; source leadership's graceful handover is Razi's task.
The simple placement Role allows only the named placement ConfigMap and recovery
Lease; extend it separately for Razi's source Leases.

## Integration handoff to Razi and Adha

Use the [v1 data contract](shard-store.md). Construct `EpochIngest` for an M1
poller only after acquiring and renewing source leadership, and discard it on
lease loss. The runtime deliberately refuses uncoordinated legacy polling in
cluster mode. `Cluster.Seal` seals a majority and records the epoch with placement
CAS. Callers stop on `ErrStaleEpoch` and retry safe batches on CAS conflict.

Satellite code submits events with its opaque stream state and committed offset
using `CommitState`. Parking callers supply stable message IDs, source reference,
raw bytes and failure classification. The source lease holder starts
`RunParkingRetries`; an explicit schema-fix action invokes `Replay`. Only Internal
Ops routes may expose raw bodies. Audit uses its separate token and returns
failed-message metadata without raw bytes. Scopes and Internal Ops HTTP routes
are Adha's ownership and are not implemented here.

The store writes changed events to its outbox automatically. `changed` is a
convenience result, not a second queue to publish. Replica relays may each publish
the same `(hazard_id, revision)`; consumers must deduplicate that identity.
Critical Sensor Pantai events carry `attributes.critical: true`; relay confirms
the dedicated durable `field.alerts` queue before fanout. Queue configuration
must agree with the notifier owner; current declaration is durable with no
additional queue arguments.

## Dataset and measurements

```sh
cd services/bnpb/aggregator
go run ./cmd/data-tool -mode generate -count 10000 -region 'Jawa Barat' -prefix baseline > /tmp/baseline.jsonl
go run ./cmd/data-tool -mode load -config /mounted/data.json -epoch CURRENT_BMKG_EPOCH -input /tmp/baseline.jsonl
go run ./cmd/data-tool -mode generate -count 5000 -region 'Jawa Barat' -prefix regional-spike > /tmp/spike.jsonl
go run ./cmd/data-tool -mode load -config /mounted/data.json -epoch CURRENT_BMKG_EPOCH -input /tmp/spike.jsonl
go run ./cmd/data-tool -mode metrics -config /mounted/data.json
```

The loader accepts canonical HazardEvent JSONL. Convert the assistants' historical
catalog format explicitly once supplied; its columns are not guessed. Distinct
prefixes prevent a spike from being mistaken for idempotent baseline replay.
The loader does not acquire a source lease: run it as the agreed BMKG writer,
not alongside an active BMKG poller. Metrics include physical record counts (including retired non-serving copies),
relation bytes, and PostgreSQL insert/update counters; counters can lag stats
collection and reset on restart. `/internal/data/metrics` exposes the same gauges
with the internal token. Compare canonical counts with Audit, not replica totals.

## Consistency and operational boundaries

Single-instance `CommitState`/checkpoint saves are one SQL transaction. The
replicated implementation is **not a distributed transaction**: it validates the
batch, writes each event to its two holders, and advances source state/checkpoint
last. A crash before the final progress commit causes idempotent replay. Reads
can temporarily see committed events ahead of the source progress. Source
batches serialize on an advisory lock at a serving metadata holder, using a
separate coordination pool so waiting locks cannot consume ingestion capacity; source
epoch checks protect every local commit, and CAS protects degraded placement.

Snapshots are per-shard, not a global cluster snapshot. Keyset pages are ordered
but concurrent corrections can change `occurred_at`; analytics/checksum evidence
must be captured after inputs stabilize. Audit checksum streams sorted references
in bounded pages, preserving byte order and repeated references across sources.
A partial checksum is explicitly marked and must not be compared as a full result.

Refill copies an MVCC snapshot, accepts dual writes on the non-serving target,
keeps higher revisions/offsets, verifies canonical rows and promotes with CAS.
An overlapping failure or placement change makes promotion fail; the planner
retries. Five checksum attempts bound one refill cycle under constant writes.
No successful promotion is claimed if replicas differ. Retired former holders
are excluded from placement and retained physically: a paused former planner
must not truncate a schema that a newer planner might have reassigned. Disk
reclamation needs a separate fenced cleanup procedure; the physical metrics
include these retained rows. Parking recovery markers
are idempotent; recovery can publish duplicates, which consumers must absorb.
The change from the outbox branch `origin/fix/transactional-outbox` was adapted
into the current store transaction code rather than merging old generated
benchmark evidence.

Run the official cluster scenarios, record actual CPU/RAM and timings, load the
real dataset, and capture the report/video evidence before marking the team's
M2 submission complete. Local tests do not prove the final cross-host latency or
all no-intervention recovery requirements.

### M1 canonical-data migration

`data-tool -mode export-m1` reads the existing single-instance store selected by
`DATABASE_URL` and writes canonical JSONL to stdout. Load that file into the
cluster with `-mode load`. The loader switches batches at source boundaries;
use a common development epoch or split files by source when source epochs differ.
Compare per-source Audit counts and checksums before retiring the M1 store.
Export is read-only apart from the standard startup schema migration/freshness
mark; it does not delete the old data. Retire the old single-instance store before
the demo so it does not remain an extra full copy outside canonical placement.

## Verification caveat: existing security baseline

`make security` currently stops on the repository's existing toolchain/dependency
baseline: GO-2026-6859 in Go 1.27.3 (fixed in 1.27.4), and GO-2026-6700 in
`amqp091-go` 1.13.0 (fixed in 1.14.0). These versions predate the data work.
No new application dependency was added. The scan must be rerun after the team
updates the toolchain and the existing AMQP dependency across its modules.
