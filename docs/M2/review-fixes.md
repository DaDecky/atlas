# PR 12 reliability fixes

These changes address the 13-point review at `6de8dbc`. They preserve the
ShardStore v1 method signatures and leave the A13–A20 cleanup work to its owner.
The new optional placement `store_id` field binds a copy to a database incarnation.

| Review | Result | Regression coverage |
|---|---|---|
| 1 | Bootstrap persists a random store ID. Startup and recovery bind IDs in placement; a changed ID marks all affected copies behind before resealing/refill. Each copy transaction verifies its expected ID, protecting reads before the planner runs. Missing-table detection remains for partial schema loss. | `TestWipedStoreGoesBehindAfterRecreate`, `TestDataIntegrationRecoveryInitializesFreshStorePublicTables` |
| 2 | Client API accepts all four source freshness values and the WILDFIRE/COASTAL filters. The Aggregator keeps exposing the four sources required by M2. | `TestPublicEnvelopeAcceptsM2Freshness`, `TestM2SourceAndHazardFilters` |
| 3 | Parking tries both eligible copies, marks failures behind, and succeeds with at least one durable copy after the placement update succeeds. | `TestDataIntegrationParkingDegradedCopy` |
| 4 | Failed INSERT and a target-only extra row exercise the actual failed-copy and checksum paths. A checksum mismatch leaves the target refilling. | `TestFailedCopyWriteMarksCopyBehind`, `TestRefillRejectsDifferingDigests` |
| 5 | Each failed publication persists its attempt count. Five failures atomically move the full payload into parking as `publish_failed` and remove the outbox row. Publication failures automatically retry after 30 seconds; undecodable payloads remain parked for repair. Source replay excludes publication parking. | `TestIntegrationOutboxParksPoisonAndRecoversAfterBrokerOutage`, `TestIntegrationOutboxMalformedPayloadDoesNotBlockQueue`, `TestIntegrationOutboxPersistsTimeoutAttempts`, `TestDataIntegrationPublicationParkingDoesNotStarveSourceReplay` |
| 6 | Planner probes, initialization, sealing, refill statements, and copy moves have bounded database calls. A refill can span multiple batches, each operation bounded separately. The renewing planner cycle also has a two-minute limit. | `TestDataIntegrationPlannerDatabaseCallDeadline` |
| 7 | Reads start a second serving copy after 150 ms, or immediately after failure, and cancel the losing attempt. Each copy has a 400 ms deadline; the request retains its 2 s budget and eight shard workers. Metadata uses the same fallback. A healthy not-found answer completes the lookup. | `TestDataIntegrationSlowCopyUsesHealthyPeer`, `TestDataIntegrationHealthyAbsenceDoesNotWaitForSlowPeer` |
| 8 | The two event writes run concurrently under independent deadlines; failed copies are marked as results arrive, before acknowledging success. | `TestDataIntegrationCopyWritesShareDeadline` |
| 9 | Cluster runtime uses an in-memory placement snapshot updated by a ConfigMap watch. CAS uses the cached opaque resourceVersion; successful PUTs update the local snapshot, and conflicts reload it. Disconnects suspend cached reads until reconnection; expired watches reload. | `TestPlacementCacheEliminatesReadGETs`, `TestPlacementCacheWatchCASAndConflict`, `TestPlacementCacheReloadsExpiredWatch` |
| 10 | Recovery collects errors and continues through later shards. Rebalancing waits until required repairs succeed. | `TestDataIntegrationRecoveryContinuesAfterRefillFailure` |
| 11 | Canonical events copy in transactions of up to 100 events without enqueueing history. Refill copies only the donor snapshot's outstanding outbox rows and publication parking for the shard, preserving original publication age and identity. | `TestDataIntegrationRefillCopiesOnlyUnpublishedEvents`, `TestDataIntegrationRefillPreservesParkedPublications` |
| 12 | A per-schema migration version applies CHECK/collation/index changes once under the existing advisory lock. The duplicate collation/index statements are removed. | `TestDataIntegrationRestartDoesNotRebuildConstraints` |
| 13 | A present critical attribute must be boolean; malformed values return an error containing the hazard ID. An absent key remains false. | `TestCriticalAttributeMustBeBoolean`, `TestIntegrationMalformedCriticalRollsBackEventAndOutbox` |

## Deployment and integration

Apply the updated store bootstrap and placement Role, which now permits a
name-filtered ConfigMap watch. Existing databases acquire their identity through
the startup migration; an existing placement without IDs is bound once. Deploy
this update before exercising disk-loss scenarios. A complete volume wipe gets
a fresh ID; copying a database directory must not be used to clone an identity
onto a different logical store.

The first upgrade of an old schema still takes the DDL locks required by its
migration. Later starts preserve constraint OIDs rather than dropping/readding
the constraints. No new application dependency was introduced.

Publication delivery remains at least once. A refill can repeat an outstanding
publication that the donor delivered after the copy snapshot was taken; consumers
must deduplicate `(hazard_id, revision)`. Parking retains failed delivery payloads
and retry state rather than discarding messages during a broker outage. A publish
timeout uses a separate bounded context to persist failure bookkeeping.

The request-count test compares 100 uncached loads (100 GETs) with 100 cached loads
(zero GETs after one initial GET) against an HTTP API fixture. This verifies removal
of per-operation requests, not the final cluster throughput target. Cross-host
latency, Kubernetes rollout, source leadership integration, official dataset and
Tester Agent measurements remain team integration work.

## Verification

From the repository root:

```sh
make lint
make test
make build
make security
make integration
make data-integration
```

The data harness includes the three named regression tests in addition to its
`DataIntegration` cases. It uses disposable PostgreSQL containers initialized by
the deployment bootstrap; the integration harness also runs RabbitMQ. See
`evidence/m2-data/review-fixes/` for the final local run results.

Mutation checks disabled the failed-copy mark and forced the checksum comparison
to succeed, in separate temporary builds. The corresponding regression tests
failed on their behavioral assertions in both cases. The production code was
restored before the final full run.
