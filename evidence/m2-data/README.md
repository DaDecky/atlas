# M2 data verification — 18 November 2026

Local Linux / Go 1.27.3 / PostgreSQL 18.3-alpine / RabbitMQ 4.2.5-alpine.
These are development checks, not official multi-machine demonstration results.
The logs retain test names/results and distribution facts; generated credentials,
container lifecycle noise, and full event payloads are omitted.

- `make lint`: passed across all modules (`lint.txt`).
- `make test`: passed with race detection across all modules (`test.txt`).
- `make build`: passed across all modules (`build.txt`).
- `make integration`: passed against disposable real PostgreSQL and RabbitMQ
  (`integration.txt`). Covers independent checksum calculation, cursor ordering,
  Audit authorization, analytics parity, atomic outbox, priority/retry, crash
  persistence, raw parking, concurrent replay, due retries, and clock rollback.
- `make data-integration`: full replicated suite passed (`replicated-suite.txt`).
  Covers dual write, wipe/refill, next-node loss, reusable contract, live refill,
  automatic recovery, parking recovery, node join and takeover fencing.
- Final focused replicated run after safety/coordination changes passed
  (`replicated-final.txt`): wipe/refill/next loss, physical metrics, unsealed-copy
  exclusion, and eight concurrent writers without starving the ingestion pools.
- Store Kustomization rendered successfully with kustomize v5.8.3. No live
  Kubernetes apply, scheduling, or NetworkPolicy proof is claimed.
- Dataset and placement commands generated valid JSONL and a valid 48-shard
  ConfigMap in local smoke checks. The real assistant dataset was not supplied.
- `go vet ./...` passed for the Aggregator module.

The 150-event synthetic fixture had physical distributions of A=90, B=95 and
C=115 records: 300 total physical records for 150 canonical events and two copies
per event. No node held the full dataset. The fourth-node join test assigned 24
copy units to the new node while retaining the canonical checksum. These are
fixture observations, not throughput or RAM measurements.

`make security` did not pass: the existing baseline contains GO-2026-6859 in
Go 1.27.3 (fixed in 1.27.4) and GO-2026-6700 in amqp091-go 1.13.0 (fixed in
1.14.0). No new application dependency was introduced by this branch.

Remaining team evidence: real historical dataset, actual per-node host resource
use, cross-host latency, production source election/adapter integrations,
Tester Agent scenarios and the report/video. See `docs/M2/data-work.md` for
configuration, contracts, consistency limits, and integration ownership.
