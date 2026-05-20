.PHONY: up down test lint

up:
	docker compose -f infra/docker/docker-compose.yml up -d

down:
	docker compose -f infra/docker/docker-compose.yml down

test:
	source .venv/bin/activate && pytest tests/ -v

lint:
	source .venv/bin/activate && python -m py_compile ingestion/producers/*.py