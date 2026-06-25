.PHONY: up down logs test lint

up:
	cd docker && docker compose up -d --build

down:
	cd docker && docker compose down -v

logs:
	cd docker && docker compose logs -f

test:
	pytest -v

lint:
	ruff check .
