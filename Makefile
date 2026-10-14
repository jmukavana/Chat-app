SHELL := /bin/bash

# Load .env (if present) as make variables and export them to every recipe, so
# `run`, `run-chat`, `migrate` and `test-integration` all see the same config
# as docker compose. Values here are only fallbacks used when .env is absent.
-include .env
export

DATABASE_URL ?= postgres://chat:chat@localhost:5432/chat?sslmode=disable

# --project-directory . makes `docker compose` read the repo-root .env even
# though the compose file lives in deployments/.
COMPOSE := docker compose -f deployments/docker-compose.yml --project-directory .

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## Start the stack, wait for Postgres, apply the migration
	$(COMPOSE) up -d
	@echo "waiting for postgres ..."
	@for i in $$(seq 1 60); do \
		psql "$(DATABASE_URL)" -c 'select 1' >/dev/null 2>&1 && break; \
		sleep 1; \
		[ $$i = 60 ] && { echo "postgres did not come up"; exit 1; }; \
	done
	@$(MAKE) --no-print-directory migrate
	@echo "stack ready."

.PHONY: down
down: ## Stop the stack
	$(COMPOSE) down

.PHONY: reset
reset: ## Stop and wipe volumes
	$(COMPOSE) down -v

.PHONY: migrate
migrate: ## Apply db/migrations/0001_init.up.sql (idempotent)
	psql "$(DATABASE_URL)" -q -v ON_ERROR_STOP=1 -f db/migrations/0001_init.up.sql

.PHONY: migrate-down
migrate-down: ## Apply db/migrations/0001_init.down.sql
	psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f db/migrations/0001_init.down.sql

.PHONY: run
run: ## Run the gateway
	go run ./cmd/gateway

.PHONY: run-chat
run-chat: ## Run the chat service
	go run ./cmd/chat

.PHONY: build
build: ## Build all binaries into ./bin
	go build -o bin/ ./...

.PHONY: test
test: ## Run unit tests
	go test ./...

.PHONY: test-integration
test-integration: ## Run tests that need Postgres (make up first)
	go test -tags=integration ./...

.PHONY: sqlc
sqlc: ## Regenerate internal/chat/chatdb from db/queries
	sqlc generate

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: vet
vet: ## go vet
	go vet ./...
