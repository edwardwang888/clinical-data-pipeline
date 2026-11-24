.PHONY: up down logs dlq test test-local lint

# Containers run as you, so files the listener writes to data/raw are yours, not root's.
export HOST_UID := $(shell id -u)
export HOST_GID := $(shell id -g)

## Build and start Redis, the listener, the consumer and the producer.
up:
	mkdir -p data/raw data/rejected
	docker compose up -d --build
	@echo "Listener on localhost:2575. Raw files appear under data/raw/ (rejected ones under data/rejected/). Follow logs: make logs"

## Stop everything (the Redis volume and data/raw are kept).
down:
	docker compose down

## Show the latest dead-letter entries: bad input the listener kept as evidence.
dlq:
	docker compose exec redis redis-cli XREVRANGE hl7.dlq + - COUNT 20

## Follow the logs of all services.
logs:
	docker compose logs -f

## Run the full test suite in a container, against the compose Redis.
test:
	docker compose --profile test build tests
	docker compose --profile test run --rm tests

## Run the tests on this machine with uv (Redis tests need redis-server installed).
test-local:
	uv run pytest

## Check the code with ruff (rules in pyproject.toml). Needs uv.
lint:
	uv run ruff check .
