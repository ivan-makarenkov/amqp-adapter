-include .env
export

.DEFAULT_GOAL := help

.PHONY: help
help: ## Available commands
	@clear
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[0;33m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""


##@ Targets

.PHONY: test
test: ## Run tests (short mode, skips integration)
	go test -v -short ./...

.PHONY: rabbitmq-up
rabbitmq-up: ## Start RabbitMQ test container and wait for AMQP port
	@docker rm -f mq-rabbitmq-test 2>/dev/null || true
	@docker run -d --name mq-rabbitmq-test -p 5672:5672 -p 15672:15672 rabbitmq:3-management >/dev/null
	@echo "Waiting for RabbitMQ AMQP readiness..."
	@go run ./scripts/wait-rabbitmq -url amqp://guest:guest@127.0.0.1:5672/ -timeout 60s -interval 1s

.PHONY: rabbitmq-down
rabbitmq-down: ## Stop RabbitMQ test container
	@docker stop mq-rabbitmq-test 2>/dev/null || true
	@docker rm mq-rabbitmq-test 2>/dev/null || true

.PHONY: test-functional
test-functional: ## Start RabbitMQ (docker compose) and run functional tests
	@set -e; \
	COMPOSE_STARTED=0; \
	if ! docker compose -f tests/docker-compose.yml ps --status running 2>/dev/null | grep -q rabbitmq; then \
		docker compose -f tests/docker-compose.yml up -d --wait; \
		COMPOSE_STARTED=1; \
	fi; \
	trap 'if [ "$$COMPOSE_STARTED" = "1" ]; then docker compose -f tests/docker-compose.yml down -v; fi' EXIT; \
	MQ_TEST_SKIP_COMPOSE=1 go test -C tests -v -count=1 -p 1 ./...

.PHONY: test-functional-down
test-functional-down: ## Stop RabbitMQ from tests/docker-compose.yml
	docker compose -f tests/docker-compose.yml down -v

.PHONY: test-integration
test-integration: test-functional ## alias: functional tests with RabbitMQ

.PHONY: lint-install
lint-install:
	@if ! [ -x "$$(command -v golangci-lint)" ]; then \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.64.2 ; \
	fi

.PHONY: lint
lint: lint-install ## Run linter (golangci-lint)
	golangci-lint run ./... --config .golangci.yml

.PHONY: format
format: ## Format code
	go install golang.org/x/tools/cmd/goimports@latest
	goimports -l -w .


##@ Aliases

t: ## Run tests
	@make test

.PHONY: l
l: lint ## Run linter (golangci-lint)

.PHONY: f
f: ## Format code
	@make format

