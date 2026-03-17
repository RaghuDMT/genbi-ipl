.PHONY: up down logs restart-python test etl etl-season lint

# Start all services
up:
	docker compose up --build -d

# Stop all services
down:
	docker compose down

# Tail logs from all services
logs:
	docker compose logs -f

# Restart only the Python service (faster iteration)
restart-python:
	docker compose up --build -d intelligence

# Run tests
test:
	docker compose exec intelligence pytest tests/ -v

# Run ETL pipeline (downloads data, loads DuckDB, generates corpus, embeds)
etl:
	python etl/run_etl.py

# Reload a specific season
etl-season:
	python etl/run_etl.py --season 

# Run evaluation suite
eval:
	python evaluation/run_eval.py

# Lint
lint:
	cd intelligence && python -m ruff check .
	cd gateway && go vet ./...
