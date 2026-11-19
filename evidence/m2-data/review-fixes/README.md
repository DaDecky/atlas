# PR 12 review-fix verification

Local verification on 19 November 2026, based on PR head `6de8dbc`, using Go
1.27.4 and disposable PostgreSQL 18.3 containers. The integration harness also
starts RabbitMQ. Containers and test volumes were removed by the harnesses.

| Check | Result | Evidence |
|---|---|---|
| `make lint` | Passed, all modules | [lint.txt](lint.txt) |
| `make test` | Passed with race detector | [test.txt](test.txt) |
| `make build` | Passed, all modules | [build.txt](build.txt) |
| `make security` | Passed, dependencies verified and no reported vulnerabilities | [security.txt](security.txt) |
| `make integration` | Passed, including durable outbox timeout attempts and publication parking/retry | [integration.txt](integration.txt) |
| `make data-integration` | Passed, 21 top-level data tests plus contract subtests | [data-integration.txt](data-integration.txt) |
| ConfigMap cache fixture | Passed watch, cache isolation, opaque-version CAS/conflict, HTTP 410 reload and reconnect checks | [placement-cache.txt](placement-cache.txt) |
| Failed-copy mutation | Expected failure: removing `markFailedCopy` made `TestFailedCopyWriteMarksCopyBehind` fail | [mutation-behind.txt](mutation-behind.txt) |
| Refill mutation | Expected failure: forcing digest equality made `TestRefillRejectsDifferingDigests` fail | [mutation-checksum.txt](mutation-checksum.txt) |

The mutation builds were temporary and were restored before the final full run.
The slow-copy test held one store's tables locked for up to three seconds; the
complete read used the healthy copies within the two-second request budget.
The cache fixture counts HTTP requests and verifies zero per-load GETs after
initialization. These are controlled local checks, not live cluster benchmarks.

See [the fix mapping](../../../docs/M2/review-fixes.md) for deployment changes and
remaining team integration boundaries. Existing source leadership, ingestion,
rollout and Tester Agent evidence is not claimed here.
