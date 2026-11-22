# Atlas

Disaster coordination services for BMKG, PVMBG, and BNPB. Compose runs each component in its own container for development, and M2 runs on k3s as described in [Run on k3s (M2)](#run-on-k3s-m2). Auth owns its session database, the Aggregator owns the canonical store, and clients retrieve hazard data through the Client API.

## Run

Install Docker with Compose, Go 1.27.4, GNU Make, Bash, and OpenSSL. On Windows, use WSL or Git Bash. Create `.env` with fresh secrets and a new Ed25519 signing pair.

```sh
make env
```

`make env` overwrites `.env` every run. If the stack ran before, reset its databases with `docker compose down -v` so they accept the new passwords.

Start all services in the foreground with `make dev`, or in the background with `make start`. Then check that they answer.

```sh
make start
curl http://localhost:8080/health
curl http://localhost:8084/health
make logs
make stop      # keeps the database, broker, and consumer state volumes
```

The source mocks listen on `8081` and `8082`, the Client API on `8080`, and Auth on `8084`. All host ports bind to `127.0.0.1`. The Aggregator API is reachable only from the Compose network. Never commit `.env`.

Most examples below read credentials from `.env`. Load it into the shell once.

```sh
set -a; source .env; set +a
```

## Configuration

`.env` holds two kinds of variables. Secrets are required, and `make env` generates them. Config is optional, and each value below is the default.

| Variable | Used by | Meaning | Default |
|---|---|---|---|
| `BMKG_API_KEY` | BMKG, Aggregator | BMKG key sent in `X-BMKG-Key`. | secret |
| `PVMBG_TOKEN` | PVMBG, Aggregator | PVMBG bearer token shaped `pvmbg.<key-id>.<secret>`, with a secret of at least 16 URL-safe characters. | secret |
| `AGGREGATOR_INTERNAL_TOKEN` | Aggregator, Client API | Credential for the Aggregator's internal read API. | secret |
| `AUDIT_TOKEN` | Aggregator | Credential for the Audit API. The assistants give the real one for the exam, and `make env` generates a local one. | secret |
| `AUTH_VALIDATE_TOKEN` | Auth, Client API | Credential for `POST /auth/validate`. | secret |
| `AUTH_ED25519_PRIVATE_KEY_B64` | Auth | Base64 PKCS#8 PEM key that signs access tokens. | secret |
| `AUTH_ED25519_PUBLIC_KEY_B64` | Client API | Base64 public key PEM that verifies access tokens. | secret |
| `MEDIA_CLIENT_ID`, `MEDIA_CLIENT_SECRET` | Auth | Media identity, summary fields only. | `media`, secret |
| `FIELD_TEAM_CLIENT_ID`, `FIELD_TEAM_CLIENT_SECRET` | Auth | Field Team identity, all fields. | `field-team`, secret |
| `INTERNAL_OPS_CLIENT_ID`, `INTERNAL_OPS_CLIENT_SECRET` | Auth | BNPB Internal Ops identity, all fields. The three IDs and secrets must be distinct. | `internal-ops`, secret |
| `CANONICAL_DB_PASSWORD` | PostgreSQL, Aggregator | Password of the `aggregator` role that owns the Canonical Store. | secret |
| `AUTH_DB_PASSWORD` | PostgreSQL, Auth | Password of the `auth` role that owns the Auth Store. | secret |
| `BMKG_DB_PASSWORD`, `PVMBG_DB_PASSWORD` | Each agency database and its mock | Password of the agency's own database server. | secret |
| `RABBITMQ_PASSWORD` | RabbitMQ, Aggregator, consumers | Password of the broker user `atlas`. | secret |
| `POSTGRES_ADMIN_PASSWORD` | PostgreSQL | Superuser password, used only to create the two owner roles. | secret |
| `BMKG_PORT`, `PVMBG_PORT`, `CLIENT_API_PORT`, `AUTH_PORT` | Compose | Loopback host ports. | `8081`, `8082`, `8080`, `8084` |
| `PVMBG_MIN_RESPONSE_DELAY`, `PVMBG_MAX_RESPONSE_DELAY` | PVMBG | Random response delay range, within 500ms to 3s. | `500ms`, `3s` |
| `SOURCE_EVENT_INTERVAL` | BMKG, PVMBG | Time between generated events, 1s to 10s. | `10s` |
| `POLL_INTERVAL` | Aggregator | Time between polls of each source. | `3s` |
| `ACCESS_TOKEN_TTL` | Auth | Access token lifetime. | `60s` |

Other limits are optional environment variables. Each service's `config.go` holds the name, the default, and the range of each, and Compose passes the value from `.env` when it is set.

## Run on k3s (M2)

M2 runs every BNPB component on Kubernetes in the namespace `sistem-informasi-kebencanaan-bnpb`. Compose remains the local development stack. The commands below start from an empty cluster.

### Topology

The demo cluster has three k3s nodes with separate kernels. The node names are part of the deployment: the overlay pins services to `atlas-node-a`, and the data placement map is computed from the three names.

| Node | Kind | k3s role | Physical host | Host CPU / RAM |
|---|---|---|---|---|
| `atlas-node-a` | VM | control plane and worker | TODO | TODO |
| `atlas-node-b` | VM | worker | TODO | TODO |
| `atlas-node-c` | VM | worker | TODO | TODO |

The node IPs are `10.0.0.40` to `10.0.0.42`, and pods use the k3s default `10.42.0.0/16`. The demo overlay places the workloads as follows.

| Component | Replicas | Placement |
|---|---|---|
| Aggregator | 3 | Spread one per node by anti-affinity and a topology spread constraint |
| Canonical Store (`canonical-store`) | 3 | One per node, required by anti-affinity, data on the node's local disk |
| Auth, Client API | 2 each | `atlas-node-a` |
| BMKG, PVMBG, their databases, Auth database, RabbitMQ, consumers, notifier | 1 each | `atlas-node-a` |
| Sensor Pantai | 1 | Scheduler's choice, vendor manifest unmodified |
| VictoriaMetrics, Jaeger | 1 each | `atlas-node-a`, only with telemetry enabled |
| Tester Agent | 1 per node | DaemonSet in its own namespace `tester-agent` |

### Deploy

Install `kubectl` with a context for the cluster, Docker, Go 1.27.4, and Python 3 with the `cryptography` package. Each node needs an SSH alias that can run `sudo k3s ctr`, so the script can import the locally built images.

Deploy the whole system with one command. It builds the images, imports them on each node, applies the `demo` overlay with lease-managed ingestion and telemetry, and waits until every workload is ready.

```sh
make deploy-demo DEPLOY_ARGS="--context <context> --load-vms <ssh-node-a> <ssh-node-b> <ssh-node-c>"
```

`make deploy-demo` is shorthand for `python scripts/deploy-k8s.py --build --overlay demo --enable-ingestion --enable-telemetry` followed by `DEPLOY_ARGS`. The first run creates the ignored file `.env.m2` with fresh credentials, and later runs reuse it. Before the exam, replace `AUDIT_TOKEN`, `PANTAI_AUTH_TOKEN`, and `SATELLITE_TOKEN` in `.env.m2` with the values the assistants give, then run the command again. Credentials become Secrets and settings become ConfigMaps, and no secret appears in a manifest or in the command output.

Install the Tester Agent with the assistants' manifest, unmodified.

```sh
kubectl apply -f https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml
```

Every service is a ClusterIP Service, so reach them with port forwards. The rest of this section assumes these forwards and `set -a; source .env.m2; set +a`.

```sh
ns=sistem-informasi-kebencanaan-bnpb
kubectl -n $ns port-forward svc/client-api 8080:8080 &
kubectl -n $ns port-forward svc/auth 8084:8084 &
kubectl -n $ns port-forward svc/aggregator 8083:8083 &        # Audit API
kubectl -n $ns port-forward svc/sensor-pantai 18080:8080 &
kubectl -n $ns port-forward svc/victoriametrics 8428:8428 &   # dashboard at /vmui
kubectl -n $ns port-forward svc/jaeger 16686:16686 &          # traces
```

### M2 configuration

`.env.m2` uses the same variables as `.env`, described in [Configuration](#configuration), plus the ones below. Optional values are read only when set, and each default is listed in `.env.example`.

| Variable | Used by | Meaning | Default |
|---|---|---|---|
| `AUDIT_TOKEN` | Aggregator | Bearer token of the Audit API. | secret |
| `PANTAI_AUTH_TOKEN` | Sensor Pantai, Aggregator | Shared Sensor Pantai token. | secret |
| `SATELLITE_TOKEN` | Aggregator | Group token for the satellite feed. Without it the satellite adapter stays off. | unset |
| `SOURCE_LEASE_DURATION`, `SOURCE_LEASE_RENEW_DEADLINE`, `SOURCE_LEASE_RETRY_PERIOD` | Aggregator | Ingestion lease timing. A leader that misses the renew deadline stops writing. | `15s`, `10s`, `2s` |
| `SENSOR_POLL_INTERVAL`, `SENSOR_TIMEOUT`, `SENSOR_STALE_AFTER` | Aggregator | Sensor Pantai polling, request timeout, and the age after which its data is stale. | `1s`, `2s`, `5m` |
| `SHARD_DEADLINE`, `READ_HEDGE_DELAY`, `SHARD_READ_WORKERS` | Aggregator | Per-copy read deadline, delay before a hedged read to the second copy, and parallel shard reads. | `400ms`, `150ms`, `8` |
| `REFILL_VERIFY_ATTEMPTS` | Aggregator | Verification attempts per refilled copy. | `5` |
| `DATA_INGEST_POOL_SIZE`, `DATA_QUERY_POOL_SIZE` | Aggregator | Connection pools per Canonical Store node. | `4`, `8` |
| `HAZARD_READ_CACHE_TTL` | Aggregator | First-page read cache, `0s` turns it off. | `1s` |
| `AUDIT_TIMEOUT`, `OPS_CAPACITY` | Aggregator | Audit API request deadline and concurrent Internal Ops requests. | `10s`, `2` |

### Observability

With telemetry enabled, the Aggregator, Auth, and Client API export traces to Jaeger and metrics to VictoriaMetrics over OTLP, with 10% trace sampling. The dashboard `Atlas observability` in VictoriaMetrics shows request rate, 5xx rate, and p95 latency per service, plus satellite lag, capacity rejections by client class, broker messages ready per queue, field report publish-to-ack latency, outbox backlog, copy refills, source polls, the active lease holder and writer epoch, stale writer rejections, and the Sensor Pantai circuit breaker. BMKG, PVMBG, the consumers, and the notifier have no exporter. Their queues are measured from the Aggregator through the broker, and their JSON logs carry the correlation ID like every other service.

```sh
kubectl -n $ns logs deploy/aggregator -f | grep <correlation-id>
```

### M2 test scenarios

Practice scenarios of the Tester Agent cover the injected failures, such as node loss, clock skew, network delay, and mock outages. List them and run one at any time. Freezing a VM is done by hand on its host.

```sh
kubectl exec -n tester-agent daemonset/tester-agent -- /app list
kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice <name>
kubectl logs -n tester-agent daemonset/tester-agent
```

**P1. Distributed storage.** Check where each event lives and that the replicas agree, before and after a node is lost or wiped. `make data-integration` runs the wipe, refill, and node join tests locally.

```sh
curl -H "Authorization: Bearer $AUDIT_TOKEN" 'http://localhost:8083/internal/audit/location?hazard_id=<hazard-id>'
curl -H "Authorization: Bearer $AUDIT_TOKEN" http://localhost:8083/internal/audit/checksum
```

**P2. Leadership and scaling.** Delete the leader pod, or skew a node's clock with the Tester Agent, and watch the lease move in the Aggregator logs and the `source lease` panel. For the load criteria, run k6 together with `loadtest/scale.sh`. The k6 summary reports throughput from complete HTTP 200 responses only. Partial 200s, 429s, and failed requests are counted separately. A port forward is a single tunnel and caps throughput, so for these runs start k6 on a node and set `BASE_URL` and `AUTH_URL` to the ClusterIPs of `client-api` and `auth`, as listed by `kubectl -n $ns get svc`.

```sh
# Criterion 6: 10,000 VUs while the Aggregator goes from 3 to 5 to 2 instances
k6 run -e SCENARIO=sustained -e MEDIA_CLIENT_ID=$MEDIA_CLIENT_ID -e MEDIA_CLIENT_SECRET=$MEDIA_CLIENT_SECRET loadtest/m2.js &
loadtest/scale.sh scale
# Criterion 7: run once with 1 Aggregator and once with 3, then compare the throughput lines
kubectl -n $ns scale deployment/aggregator --replicas=1
k6 run -e SCENARIO=throughput -e MEDIA_CLIENT_ID=$MEDIA_CLIENT_ID -e MEDIA_CLIENT_SECRET=$MEDIA_CLIENT_SECRET loadtest/m2.js
# Criterion 8: a version swap under load fails k6 if any request fails
k6 run -e SCENARIO=rollout -e MEDIA_CLIENT_ID=$MEDIA_CLIENT_ID -e MEDIA_CLIENT_SECRET=$MEDIA_CLIENT_SECRET loadtest/m2.js &
loadtest/scale.sh rollout <new-tag>
```

The `rollout` threshold fails on any request that is neither a 200 nor a controlled 429. `STEP_SECONDS` sets the wait between scale steps, and `VUS`, `DURATION`, and `THINK_TIME` size the k6 run.

**P3. Unreadable source data.** Switch PVMBG to schema 3, 4, or 5. Payloads that break the contract, including oversized ones, are parked with their reason and full body, and Internal Ops can inspect and replay them. Field Team reports are accepted through `POST /field-reports`.

```sh
kubectl -n $ns exec deploy/pvmbg -- wget -qO- --post-data '{"version":4}' http://127.0.0.1:8082/admin/schema-version
curl -H 'Authorization: Bearer <internal-ops-token>' http://localhost:8080/internal-ops/unprocessed
curl -H "Authorization: Bearer $AUDIT_TOKEN" http://localhost:8083/internal/audit/unprocessed
```

**P4. Satellite feed.** With `SATELLITE_TOKEN` set, the Aggregator reads the satellite feed and stores one `SATELIT` event per fire. Compare the Audit API output with a reference file.

```sh
curl -H "Authorization: Bearer $AUDIT_TOKEN" 'http://localhost:8083/internal/audit/events?source=SATELIT&limit=1000' > satellite.json
python scripts/satellite-reference-diff.py --actual satellite.json --reference <reference.json>
```

**P5. Sensor Pantai and the Field Team alert path.** Run the Sensor Pantai practice scenarios through the Tester Agent. The overload scenario floods the Client API as Media while Field Team keeps reading, and must reject only Media.

```sh
k6 run -e SCENARIO=overload -e MEDIA_CLIENT_ID=$MEDIA_CLIENT_ID -e MEDIA_CLIENT_SECRET=$MEDIA_CLIENT_SECRET \
  -e FIELD_TEAM_CLIENT_ID=$FIELD_TEAM_CLIENT_ID -e FIELD_TEAM_CLIENT_SECRET=$FIELD_TEAM_CLIENT_SECRET loadtest/m2.js
```

The alert path has two objectives: no critical event lost, and 99% of critical events delivered within 5 s. `scripts/error-budget.py` takes the expected critical events from Sensor Pantai itself, so an event lost before Atlas stored it still counts. It matches them with the notifier log and reports how much of each budget a window used.

```sh
kubectl -n $ns logs deploy/notifier --since=2h | python scripts/error-budget.py --sensor http://localhost:18080 --since 2026-11-22T10:00:00Z
```

## Source mocks

BMKG (`8081`) and PVMBG (`8082`) are passive. Each starts with 24 deterministic seed records and generates one new record every `SOURCE_EVENT_INTERVAL`. Each agency stores its records in its own PostgreSQL server (`bmkg-db`, `pvmbg-db`), on a network that only that agency joins. A restarted mock keeps its history and, for PVMBG, its schema version. The outage switch is not stored, so a restart always comes back without an outage. The `since` filter includes records at the requested timestamp.

```sh
curl -H "X-BMKG-Key: $BMKG_API_KEY" 'http://localhost:8081/seismic-events?since=2026-09-29T12:00:00Z'
curl -H "X-BMKG-Key: $BMKG_API_KEY" http://localhost:8081/tsunami-warnings
curl -H "Authorization: Bearer $PVMBG_TOKEN" http://localhost:8082/volcanic-reports
curl http://localhost:8082/health      # shows the current schema version
```

## Client API and Auth

Auth serves `POST /auth/login` with `{client_id, client_secret}` and `POST /auth/refresh` with `{refresh_token}`. Both return `{access_token, token_type, expires_in, refresh_token}`. `token_type` is `Bearer`, and `expires_in` is in seconds. Errors use `{"error":{"code":"..."}}`.

Access JWTs are never stored. Auth stores the refresh-token hash and the session version in its own database. The Client API verifies the JWT locally, then calls `POST /auth/validate` with `{session_id, token_version}` using a dedicated internal bearer credential. A valid current session returns `{active:true}`. There is no positive validation cache, so a refresh immediately supersedes earlier access tokens. An unavailable Auth gives 503.

`GET /hazards` accepts the Aggregator filters `source`, `hazard_type`, `severity`, `since`, and `limit`, and returns the freshness of each source in `sources`. That freshness block contains only `source`, `status`, `last_success_at`, and `stale_since`. Unknown envelope fields and dependency diagnostics are not forwarded. A malformed Aggregator response gives 502. Local overload and an Aggregator 429 both give a public 429 with `Retry-After`.

| View | Fields | Allowed clients |
|---|---|---|
| `summary` (default) | `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`, `ingested_at` | All three identities |
| `raw` | All canonical fields, including coordinates, `source_ref_id`, and `attributes` | Field Team and Internal Ops |

## Aggregator internal API

`GET /internal/hazards` supports `source`, `hazard_type`, `severity`, inclusive `since=<RFC3339>`, and `limit` (1 to 500, default 100). Responses contain `data` and the freshness of each source in `sources`. A response is 200 even when one source is unavailable, and that source shows `"status":"unavailable"`. It is 503 only when every requested source is unavailable.

The Aggregator publishes no host port, so query it from inside its container.

```sh
docker compose exec aggregator wget -qO- http://127.0.0.1:8083/health
docker compose exec aggregator sh -c   'wget -qO- --header "Authorization: Bearer $AGGREGATOR_INTERNAL_TOKEN" "http://127.0.0.1:8083/internal/hazards?source=BMKG&limit=20"'
```

BNPB Internal Ops can inspect parked M2 payloads through `GET /internal-ops/unprocessed` with an `ops:read` token. For an item with `raw_chunked: true`, request `?source=<source>&message_id=<message_id>&part=0` on the same endpoint. Base64-decode each response's `data` and follow `next_part` until it is `null`. Concatenate the decoded bytes in order and compare them with the listed `raw_length` and `raw_sha256`.

## Hazard revisions and late tsunami warnings

A new hazard starts at `revision=1`. If severity, area, coordinates, occurrence time, or attributes change, the Aggregator updates the same stored hazard, increments its revision, and publishes its committed snapshot. `hazard_id` and the original `ingested_at` stay the same. Identical polling results do not increment the revision or publish a message. JSON key order and whitespace do not count as changes. Joined warnings are ordered by warning ID.

For example, a seismic event is published as revision 1. A related tsunami warning that arrives three minutes later updates severity and `attributes.tsunami_warnings`, and publishes revision 2. The warning joins only events with `potential_tsunami=true`, within the one-hour warning lookback. Broker messages contain `revision` and use `message_id=<hazard_id>:<revision>`, and retries keep that identity. Revision is visible in the Aggregator API and the raw Client API view. The summary view keeps its specified fields.

Publication uses a bounded in-memory publisher, not a transactional outbox. A crash between save and publish, or exhausted retries, can lose a publication, so a stored revision does not by itself guarantee delivery. The M1 spec does not define late-warning publication, so this policy is a team decision.

## Event consumers

Both consumers have separate durable queues and separate persistent idempotency files. The stored effect is one ledger entry per `(hazard_id, revision)`, so the effect and the duplicate marker are the same durable record. The consumer appends and syncs that record before ACK, and a duplicate adds no record. On restart it reloads complete records and repairs an interrupted final append. Each ledger has one writer. It grows with processed IDs and is not designed for multiple replicas.

The supported M1 outage is 30 seconds, at up to two events per second and payloads up to 32 KiB, with catch-up within 60 seconds after restart. New queues keep messages for one hour, bounded by 10,000 ready messages and 64 MiB of payload. Beyond those bounds, TTL expires old messages, or `reject-publish` returns a publisher NACK. Retries can deliver duplicate copies to other queues. See RabbitMQ's [queue limits](https://www.rabbitmq.com/docs/maxlength) and [message TTL](https://www.rabbitmq.com/docs/ttl). This is at-least-once processing of retained deliveries, with no guarantee for unlimited outages or historical replay.

Consumers reuse an existing queue without redeclaring its arguments, and create only a missing queue with the configured limits. The limits are code defaults that the service environment can override, and a changed value does not alter an existing queue. Compose gives each consumer, including the optional third one, its own state volume.

## Test scenarios

Each problem lists the manual steps for a live demo, then the automated check. `make demo P=Px` runs only that problem's checks on a fresh stack.

### P1. Live schema change

Start the stack and let the Aggregator ingest the original schema. Then switch PVMBG to schema version 2 without restarting anything. Reports created after the switch carry `confidence_level`, and the Aggregator stores it in `attributes`. Earlier reports keep their original shape.

```sh
curl -X POST http://localhost:8082/admin/schema-version -d '{"version":2}'
curl -X POST http://localhost:8082/admin/schema-version -d '{"version":1}'   # stop adding the field
make demo P=P1
```

### P2. Slow source, outage, and load

For the three-second delay, set `PVMBG_MIN_RESPONSE_DELAY=3s` and `PVMBG_MAX_RESPONSE_DELAY=3s` in `.env`, then recreate only PVMBG. Other containers keep running.

```sh
docker compose up -d pvmbg
```

Switch the PVMBG outage on and off at any time. Mode `error` returns 503 at once. Mode `hang` sends no response, like a dead host. Reports keep accumulating during the outage, and the Aggregator catches up after recovery. During the outage, `GET /hazards?source=PVMBG` returns the last stored data and marks PVMBG with `stale_since`.

```sh
curl -X POST http://localhost:8082/admin/outage -d '{"enabled":true,"mode":"error"}'
curl -X POST http://localhost:8082/admin/outage -d '{"enabled":true,"mode":"hang"}'
curl -X POST http://localhost:8082/admin/outage -d '{"enabled":false}'
```

`make demo P=P2 LOAD=1` runs the load test on a fresh stack and samples Client API connections. To run k6 by hand against your own stack, install [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) and set the three-second PVMBG delay first.

```sh
CLIENT_ID=$INTERNAL_OPS_CLIENT_ID CLIENT_SECRET=$INTERNAL_OPS_CLIENT_SECRET k6 run loadtest/p2.js
CLIENT_ID=$INTERNAL_OPS_CLIENT_ID CLIENT_SECRET=$INTERNAL_OPS_CLIENT_SECRET k6 run -e SCENARIO=burst loadtest/p2.js
```

The sustained scenario runs 40 BMKG-only and 20 volcanic virtual users for 90 seconds. Each user keeps its own connection and refreshes its token before the 60-second TTL ends. The burst scenario runs 400 users for 20 seconds, which exceeds Client API capacity and must produce counted 429 responses without unexpected errors.

The demo records timestamped connection samples and requires an interval of at least 60 seconds with 50 or more connections at every sample. A count below 50 or a sampling gap longer than five seconds resets the interval. The samples and k6 summaries are saved under `P2/` in the run's evidence folder, and the report records the longest qualifying interval.

| Variable | Default | Meaning |
|---|---|---|
| `SCENARIO` | `sustained` | `sustained` for P2.1 and P2.2, or `burst` for controlled rejection. |
| `BMKG_VUS`, `VOLCANIC_VUS` | `40`, `20` | Sustained virtual users per request kind. |
| `BURST_VUS` | `400` | Burst virtual users. |
| `DURATION` | `90s` or `20s` | Test length. |
| `THINK_TIME` | `0` | Seconds each user waits between requests. |
| `BASE_URL`, `AUTH_URL` | `http://localhost:8080`, `http://localhost:8084` | Client API and Auth addresses. |
| `SUMMARY_JSON`, `SUMMARY_TEXT` | unset | Files for the full k6 summary and the printed table. |

### P3. Credentials and access tokens

Each mock rejects the other agency's credential.

```sh
curl -i -H "X-BMKG-Key: $PVMBG_TOKEN" http://localhost:8081/seismic-events                 # 403
curl -i -H "Authorization: Bearer $BMKG_API_KEY" http://localhost:8082/volcanic-reports    # 401
```

Log in as Media and request summary data with the returned access token.

```sh
curl -s http://localhost:8084/auth/login \
  -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$MEDIA_CLIENT_ID\",\"client_secret\":\"$MEDIA_CLIENT_SECRET\"}"
curl -H 'Authorization: Bearer <access-token>' 'http://localhost:8080/hazards?source=BMKG&limit=20'
```

Media receives summary fields only, and an explicit raw request gives 403. Field Team and Internal Ops can request `view=raw`.

```sh
curl -i -H 'Authorization: Bearer <media-access-token>' 'http://localhost:8080/hazards?view=raw'
```

When an access token expires, refresh it without logging in again. Refresh rotates the refresh token and increments the session version, so the previous refresh token and access token both fail afterwards.

```sh
curl -s http://localhost:8084/auth/refresh \
  -H 'Content-Type: application/json' \
  -d '{"refresh_token":"<refresh-token>"}'
make demo P=P3
```

### P4. Independent services and flexible storage

Rebuild and restart only the Aggregator. The other containers keep running, and requests that do not need the Aggregator, such as an Auth login or a BMKG read, keep answering during the rebuild.

```sh
docker compose up -d --build --no-deps aggregator
docker compose ps
curl -H "X-BMKG-Key: $BMKG_API_KEY" http://localhost:8081/seismic-events
```

The Canonical Store keeps unmapped fields in the JSONB `attributes` column, so records from before and after the P1 switch sit side by side with no migration. Count both shapes.

```sh
docker compose exec -T postgres psql -U atlas_admin -d canonical -c \
  "SELECT attributes ? 'confidence_level' AS has_confidence, count(*) FROM hazard_events WHERE source='PVMBG' GROUP BY 1"
```

Only the Aggregator and PostgreSQL join the `canonical` network, so no other service can reach the Canonical Store.

```sh
docker network inspect atlas_canonical -f '{{range .Containers}}{{.Name}} {{end}}'
make demo P=P4
```

### P5. Fanout to independent consumers

Stop Consumer A while source events continue, and confirm Consumer B keeps processing and the Aggregator keeps publishing.

```sh
docker compose stop consumer-a
docker compose logs -f consumer-b aggregator
docker compose start consumer-a
docker compose logs -f consumer-a
```

To add a third subscriber without changing Aggregator code, start the optional Consumer C profile. It uses the Consumer B image with its own queue and state volume. A new queue receives events published after it is bound.

```sh
docker compose --profile consumer-c-demo up -d consumer-c-demo
docker compose logs -f consumer-c-demo
docker compose --profile consumer-c-demo stop consumer-c-demo
make demo P=P5
```

## Checks

Each test layer has one job, so no behavior is asserted twice.

| Layer | Command | Proves |
|---|---|---|
| Unit | `make ci` | Logic inside one service, such as mapping rules, scopes, and credentials. CI runs this layer. |
| Integration | `make integration` | One service against real PostgreSQL or RabbitMQ, such as checkpoints, grants, queue limits, and the save/publish crash gap. |
| Demo | `make demo` | Every specification criterion, black-box, on a fresh Compose stack. |
| Load | `make demo LOAD=1` | Demo plus the P2 sustained and overload k6 runs. |

Demo checks are named by the criterion they prove, for example `P2.3 outage serves stale volcanic data and fresh seismic data`. `S1` is the source contract, and `R1` to `R7` are the implementation rules. Every run, full or filtered, writes `demo.md` and `demo.json` to its own `evidence/YYYY-MM-DD_HH-MM-SS_WIB/` folder. `demo.md` is a matrix of every check and criterion, and `demo.json` holds the evidence each check recorded. Stack state, including the generated secrets, stays in the gitignored `.demo/` folder.

For a live demonstration, run one problem at a time. `KEEP=1` keeps the stack between runs to skip the startup.

```sh
make demo P=P1 KEEP=1
make demo P=P3 KEEP=1
make demo P=P2 KEEP=1 LOAD=1
make demo P=P5
```

The suite needs Docker and Go, and it runs on Linux, macOS, and Windows. P3.3 waits for a real 60-second token expiry, so a full run takes about 10 minutes.

## Compose networks

Each network admits only the services that need it. The table lists the members and the purpose of each.

| Network | Members | Purpose |
|---|---|---|
| `auth-api` | Auth, Client API | Auth validation and the published client ports 8084 and 8080. A regular bridge, so published ports work. |
| `internal-api` | Client API, Aggregator | Internal canonical reads. Internal network, with no Aggregator host port. |
| `auth-store` | Auth, PostgreSQL | Internal Auth database access. |
| `canonical` | Aggregator, PostgreSQL | Internal canonical database access. |
| `bmkg-store` | BMKG, `bmkg-db` | Internal BMKG database access. |
| `pvmbg-store` | PVMBG, `pvmbg-db` | Internal PVMBG database access. |
| `sources` | BMKG, PVMBG, Aggregator | Source polling. |
| `events` | RabbitMQ, Aggregator, consumers | Fanout publication and subscriptions. |

The Client API belongs to no database network and receives no database credentials. BNPB's PostgreSQL server is shared infrastructure with separate `auth` and `aggregator` roles, and database grants deny cross-owner connections. Network separation controls reachability, and database grants control ownership on the shared server.
