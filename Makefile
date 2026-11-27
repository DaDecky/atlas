.DEFAULT_GOAL := help
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: help env lint fmt test build security ci dev start stop logs integration demo

GOLANGCI_LINT_VERSION := v2.14.0
GOLANGCI_LINT := $(CURDIR)/bin/golangci-lint-$(GOLANGCI_LINT_VERSION)/golangci-lint$(if $(filter Windows_NT,$(OS)),.exe)

define in_modules
@mapfile -d '' -t modules < <( \
	find . -type d \( -name .git -o -name vendor -o -name docs \) -prune \
		-o -type f -name go.mod -print0 \
); \
for module_file in "$${modules[@]}"; do \
	( \
		cd "$$(dirname "$$module_file")"; \
		echo "$@ $$module_file"; \
		$(1) \
	); \
done
endef

help:
	@echo "make env          Write .env from .env.example with fresh secrets"
	@echo "make lint         Check formatting, go vet, and the linters with golangci-lint"
	@echo "make fmt          Format Go source files with gofumpt and goimports"
	@echo "make test         Run tests with race detection"
	@echo "make build        Compile Go packages"
	@echo "make security     Verify dependencies and scan known vulnerabilities"
	@echo "make ci           Run all four checks"
	@echo "make dev          Build and start Compose with foreground logs"
	@echo "make start        Start Compose in the background"
	@echo "make stop         Stop Compose and preserve volumes"
	@echo "make logs         Follow Compose logs"
	@echo "make integration  Test services with disposable PostgreSQL and RabbitMQ"
	@echo "make deploy       Build and deploy Kustomize; pass context/options in DEPLOY_ARGS"
	@echo "make deploy-demo  Deploy the k3s demo with ingestion and telemetry; pass cluster options in DEPLOY_ARGS"
	@echo "make demo         Check every spec criterion on Compose. P=P3 runs one problem, LOAD=1 adds k6, KEEP=1 keeps the stack"
	@echo "make p1/p2/p3/p5 Start one M2 Tester Agent exam on the prepared cluster; RUN_ID=demo, DRY_RUN=1 previews"
	@echo "make p4           Export all SATELIT Audit pages for M2; does not start the satellite session"
	@echo "make demo-status  Show local M2 readiness without accessing the cluster"
	@echo "make demo-ports   Open the six M2 localhost port forwards (foreground)"
	@echo "make demo-capture Capture M2 logs for RUN_ID (foreground)"
	@echo "make satellite-start RECORDING_STARTED=1  Start the satellite exam once when recording begins"

env:
	./scripts/env.sh

$(GOLANGCI_LINT):
	curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$(dir $@)" $(GOLANGCI_LINT_VERSION)

# gofumpt only separates multiline declarations, so this also covers one-line ones.
lint: $(GOLANGCI_LINT)
	@git ls-files -co --exclude-standard '*.go' | xargs awk ' \
		FNR == 1 { prev = "" } \
		/^(func|type|var|const) / && prev != "" && prev !~ /^\/\// { \
			print FILENAME ":" FNR ": add a blank line above this top-level declaration"; bad = 1 \
		} \
		{ prev = $$0 } END { exit bad }'
	$(call in_modules,"$(GOLANGCI_LINT)" run ./...)

fmt: $(GOLANGCI_LINT)
	$(call in_modules,"$(GOLANGCI_LINT)" fmt ./...)

test:
	$(call in_modules,go test -race -count=1 -timeout=5m ./...)

build:
	$(call in_modules,go build ./...)

security:
	$(call in_modules, \
		go mod verify; \
		go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 -test ./...)

ci:
	@$(MAKE) lint
	@$(MAKE) test
	@$(MAKE) build
	@$(MAKE) security

dev:
	docker compose up --build

start:
	docker compose up -d --build

stop:
	docker compose down

logs:
	docker compose logs -f

integration:
	./scripts/integration.sh

.PHONY: deploy deploy-demo
deploy:
	python scripts/deploy-k8s.py --build $(DEPLOY_ARGS)

deploy-demo:
	python scripts/deploy-k8s.py --build --overlay demo --enable-ingestion --enable-telemetry $(DEPLOY_ARGS)

DEMO_RUN = TestDemo$(if $(P),/$(P))

demo:
	cd demo && DEMO_LOAD=$(LOAD) DEMO_KEEP=$(KEEP) go test -tags demo -count=1 -timeout 30m -v -run '$(DEMO_RUN)' .

.PHONY: data-integration
data-integration:
	./scripts/data-integration.sh

# M2 shortcuts call the staged operator CLI. They never start another problem
# or a satellite session as an implicit dependency.
DEMO_PYTHON ?= python
RUN_ID ?= demo
DRY_RUN ?= 0
DEMO_CONFIG ?=
ifneq ($(DRY_RUN),0)
ifneq ($(DRY_RUN),1)
$(error DRY_RUN must be 0 or 1)
endif
endif
DEMO_FLAGS = --run-id "$(RUN_ID)" $(if $(filter 1,$(DRY_RUN)),--dry-run) $(if $(DEMO_CONFIG),--config "$(DEMO_CONFIG)")
DEMO_COMMAND = $(DEMO_PYTHON) demo-assets/m2/demo.py $(DEMO_FLAGS)

M2_PROBLEM_GOALS := $(filter p1 p2 p3 p4 p5,$(MAKECMDGOALS))
ifneq ($(word 2,$(M2_PROBLEM_GOALS)),)
$(error Run one M2 problem per invocation; wait for its scenario and recovery before starting the next)
endif

.PHONY: p1 p2 p3 p4 p5 demo-status demo-ports demo-capture satellite-start
p1 p2 p3:
	$(DEMO_COMMAND) exam $@

p4:
	$(DEMO_COMMAND) export-satellite

p5:
	$(DEMO_COMMAND) exam p5 $(if $(SINCE),--since "$(SINCE)")

demo-status:
	$(DEMO_COMMAND) status

demo-ports:
	$(DEMO_COMMAND) ports

demo-capture:
	$(DEMO_COMMAND) capture

satellite-start:
	@test "$(RECORDING_STARTED)" = "1" || { echo "Start recording first, then run make satellite-start RECORDING_STARTED=1" >&2; exit 2; }
	$(DEMO_COMMAND) start-satellite --recording-started
