# M2 ShardStore contract v1 (B1)

Status: **v1, fixed as the integration target for the data implementation on
this branch.** Reliability and ingestion code should implement against this
contract. This records the data owner's decision, not an assertion that other
owners have already integrated it. The implementation runs on the existing
Compose PostgreSQL; no Kubernetes, Tailscale, VM, VPS, or other team device is
needed.

Method signatures, existing fields, error meanings, ordering, and transaction
guarantees below are the v1 compatibility boundary. Replication work must keep
existing callers compiling and passing the contract suite. New optional fields
and separate interfaces may be added; an incompatible change requires a v2
contract and an explicit migration for callers. Internal replica operations are
reserved below and are not required by source adapters.

The Go contract is `ShardStore` in
`services/bnpb/aggregator/internal/aggregator/shard_store.go`. All three streams
work in the Aggregator module and can use its existing `HazardEvent`, `Query`,
and `Response` types. `OpenStore` returns the single-instance implementation.

## Calls and guarantees

| Call | Single-instance behavior |
|---|---|
| `Upsert(ctx, batch)` | Atomically saves the batch; returns only new or changed events. Identical replay does not increment revisions. |
| `CommitCheckpoint(ctx, batch, checkpoint)` | Saves events and the source time checkpoint in one transaction. An older checkpoint cannot move progress backwards. |
| `Checkpoint(ctx, source)` | Reads the committed time checkpoint; nil means no successful checkpoint yet. |
| `CommitState(ctx, batch, state)` | Saves events, an opaque JSON object, and its stream offset in one transaction. Duplicate or older offsets return `ErrStaleOffset` without changes. |
| `LoadState(ctx, source)` | Reads committed state and offset together; returns `pgx.ErrNoRows` before the first commit. |
| `Epoch(ctx, source)` | Reads the durable sealed epoch. Each source starts at epoch 1. |
| `Seal(ctx, source, epoch)` | Advances the source fence monotonically. Repeating the current epoch succeeds; a lower epoch returns `ErrStaleEpoch`. |
| `Read(ctx, query)` | Uses the existing stored read and freshness snapshot. Specify a positive `Limit`. Successful reads have `partial: false` and `unavailable_units: []`. |
| `MarkBehind(ctx, request)` | Reserved for replica coordination. Takes source, epoch, shard, copy, and expected placement generation. B1 returns `ErrSingleInstance`; it never claims a placement update succeeded. |

Each `WriteBatch` supplies one source, its sealed epoch, events from that source,
and a nonzero ingestion time (`Now`). Writes require the exact sealed epoch;
future epochs must be sealed before use. The source row lock is held until
commit or rollback, so a completed seal prevents older writers from committing
another batch. Fences are independent across BMKG, PVMBG, SATELIT, and
SENSOR_PANTAI. Stream offsets must be nonnegative and strictly increase after
the first commit. Reconnect callers should skip `ErrStaleOffset` replays.

The canonical source names are exactly `BMKG`, `PVMBG`, `SATELIT`, and
`SENSOR_PANTAI`. Event identities use the existing source/reference hash; callers
must preserve a stable `HazardID` and `SourceRefID` across replay. Events must
carry valid hazard fields, nonzero observation/ingestion timestamps, and a
non-nil JSON attributes object (an empty object is valid). `Now` marks the
successful batch; an event's `IngestedAt` is preserved from its first insertion.
Only changes to severity, area, coordinates, observation time, or attributes
increment a revision. Source, reference, ID, hazard type, and original ingestion
time are immutable for an existing identity. Delete is not part of v1.

`Read` orders events by `occurred_at` descending, then `hazard_id` ascending.
Its limit is required and should be in the HTTP API's range of 1 to 500. An
empty complete answer has `data: []`, `partial: false`, and
`unavailable_units: []`; per-source freshness is independent of shard
availability. Database failures return an error rather than a successful empty
page. Keyset pagination will be an additive query/response extension in B6.

An existing M1 database is upgraded when opened, preserving its events and
progress. The migration widens the source and hazard-type checks and adds the
source fences and stream-state table. This is enough to store events from the
new adapters; it does not add their HTTP routes or pollers.

## Integration recipes

Razi owns acquiring and renewing leadership. Before a leader's first write, use
`Epoch` to read the durable fence, choose a new epoch, and call `Seal`. In the
single-instance development setup a leader may use the initial epoch 1.
Production takeover still requires the majority-seal protocol; a successful
local `Seal` alone is not a cluster leadership decision. Carry the resulting
epoch on **every** write. `Seal` never renews a Kubernetes lease.

Time-based pollers use `CommitCheckpoint` with the complete accepted batch:

```go
batch := aggregator.WriteBatch{
    Source: "BMKG", Epoch: epoch, Events: events, Now: time.Now().UTC(),
}
changed, err := data.CommitCheckpoint(ctx, batch, fetchedThrough)
```

Adha's satellite adapter resumes from `LoadState`, computes the next state,
then submits events and that state together:

```go
batch := aggregator.WriteBatch{
    Source: "SATELIT", Epoch: epoch, Events: events, Now: time.Now().UTC(),
}
changed, err := data.CommitState(ctx, batch, aggregator.StreamState{
    Offset: lastProcessedOffset, Payload: encodedState,
})
```

The snippets assume a caller in another package inside the Aggregator module;
`data` is a `ShardStore` dependency. No caller opens its own transaction, writes
canonical tables directly, or implements local placement logic. The data owner
owns those details. State belongs to the adapter: it must include everything
needed for deterministic resume, including its watermark. Empty-event batches
are allowed, so heartbeats/offsets and checkpoints can advance without a new
hazard. `Upsert` alone does not advance a time checkpoint or stream offset.

## Errors and retries

| Result | Caller action |
|---|---|
| `ErrStaleEpoch` | Stop writes for that source and return control to leadership. Do not retry with the same epoch. |
| `ErrStaleOffset` | Treat the input as an already-committed replay; reload state before processing newer input. No events or state in the rejected batch were changed. |
| `pgx.ErrNoRows` from `LoadState` | No state committed yet; start from the adapter's initial state. |
| `ErrSingleInstance` | Replica placement is unavailable in B1. Do not acknowledge a single-copy fallback. |
| Invalid input or a database constraint error | Fix/reject the input; do not retry an unchanged invalid batch. Parking/replay is a separate data item. |
| Context cancellation or another database error | Do not advance caller progress. Retry the same batch with backoff if leadership still holds, or reload progress after reconnect. |

Use `errors.Is` for the named sentinel errors. A connection failure during
commit can have an unknown outcome: retry retains source identities, event
contents, checkpoint, and offset. Revision idempotence and the committed-offset
guard make that retry safe. A returned `changed` slice is not a durable
publication queue; delivery guarantees require the outbox from B10.

## Local verification

Run `make integration` from the repository root. The existing harness starts
disposable PostgreSQL and RabbitMQ containers, runs integration tests, then
removes its containers and volumes. It does not use the team's demo machines.

`TestIntegrationShardStoreContract` runs the reusable
`runShardStoreContract(t, open)` suite. A later implementation supplies a fresh
store factory and runs the same suite. It checks revision idempotence,
checkpoint rollback, source fencing, cross-source rejection, atomic state and
event rollback, and concurrent replay. The restart test checks durable state
and fences after reopening the store. The existing M1 tests remain in the run.

## Integration boundaries

The running M1 pollers still use `Save`; that legacy method does not check an
epoch. Adapters must use the new contract to receive fencing. Wiring the pollers
to the real leadership implementation is a later integration step.

This is not the replicated store. Placement, dual writes, majority sealing,
refill, scatter-gather with keyset cursors, partial reads on shard failures,
outbox, parking, Audit, and analytics remain later data items. In particular,
do not treat `MarkBehind` as a successful operation until replicated placement
exists. The next experiment is B4: refill under live writes on three local
PostgreSQL containers, before implementing the full replication design.
