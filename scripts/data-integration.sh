#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -p atlas-data-test -f "$PWD/compose.data-test.yaml")
trap '"${compose[@]}" down -v >/dev/null' EXIT
"${compose[@]}" up -d --wait
export DATA_TEST_A="postgres://aggregator:local-data-test@$("${compose[@]}" port store-a 5432)/canonical?sslmode=disable"
export DATA_TEST_B="postgres://aggregator:local-data-test@$("${compose[@]}" port store-b 5432)/canonical?sslmode=disable"
export DATA_TEST_C="postgres://aggregator:local-data-test@$("${compose[@]}" port store-c 5432)/canonical?sslmode=disable"
export DATA_TEST_D="postgres://aggregator:local-data-test@$("${compose[@]}" port store-d 5432)/canonical?sslmode=disable"
export DATA_TEST_ADMIN_D="postgres://atlas_admin:local-admin-test@$("${compose[@]}" port store-d 5432)/postgres?sslmode=disable"
cd services/bnpb/aggregator
go test -race -count=1 -timeout=10m -tags=integration -run "${DATA_TEST_RUN:-$(cat ../../../scripts/data-tests.regex)}" -v ./internal/...
