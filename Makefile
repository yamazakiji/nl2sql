UV ?= uv
PYTHON ?= $(UV) run python

.PHONY: dev-up dev-down format lint test install

install:
	$(UV) sync

format:
	$(UV) run ruff check --select I --fix
	$(UV) run ruff check --fix
	$(UV) run black src tests

lint:
	$(UV) run ruff check
	$(UV) run mypy

test:
	$(UV) run pytest

dev-up:
	docker-compose up --build

dev-down:
	docker compose down
