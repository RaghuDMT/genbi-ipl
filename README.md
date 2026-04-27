# genbi-ipl

AI-powered generative BI for IPL and WPL cricket analytics.

Ask questions in plain English and get SQL, results, and a plain-language explanation grounded in ball-by-ball cricket data.

## What This Project Does

`genbi-ipl` is a text-to-SQL system for cricket analytics.

- Go gateway handles API concerns (validation, rate limiting, logging)
- Python intelligence service handles orchestration (semantic rewrite, retrieval, SQL generation, validation, execution)
- DuckDB stores cricket fact and dimension tables
- ChromaDB supports retrieval for few-shot and entity context

## Current Status

- Phase 1 ETL: complete
- Enrichment module (`etl/enrich.py`): available and runnable
- Core derived columns (`required_run_rate`, `pressure_index`, etc.): populated
- Optional weather table: removed from enrichment flow
- Venue dedupe step: currently skipped in orchestrator (`enrich.venues.dedupe_skipped` log)

## Data Snapshot (local DB)

From `data/db/genbi.duckdb` (queried on April 27, 2026):

- Matches: 1,326
- Deliveries: 306,056
- Players: 1,104
- Venues: 61
- Teams: 21
- Seasons: 23
- Match date range: 2008-04-18 to 2026-04-23
- Tournament split: IPL 1,227, WPL 99

## Architecture

```text
Browser UI
  -> Go Gateway (port 8080)
  -> Python Intelligence Service (port 8000)
     -> DuckDB (analytics data)
     -> ChromaDB (retrieval context)
```

## Prerequisites

- Docker Desktop
- Git
- Groq API key (set in `.env`)

Optional for local host runs:
- Python 3.12

## Quick Start

```bash
git clone https://github.com/RaghuS007/genbi-ipl.git
cd genbi-ipl

# Copy env file
cp .env.example .env        # macOS/Linux
# or
copy .env.example .env      # Windows PowerShell

# Edit .env and set GROQ_API_KEY

# Start services
docker compose up --build -d

# Health checks
curl http://localhost:8080/health
curl http://localhost:8000/health

# First-time data setup
docker compose exec intelligence python scripts/download_data.py
docker compose exec intelligence python -m etl.run_etl
docker compose exec intelligence python scripts/verify_etl.py
```

Then open `http://localhost:8080`.

## Makefile Shortcuts

If `make` is available:

```bash
make up
make health
make setup-data
make test
make verify
make down
```

On Windows, if `make` is not installed, run the equivalent `docker compose` commands directly.

## Enrichment Runbook

The enrichment module runs after ETL:

```bash
python -m etl.enrich
```

Useful flags:

```bash
# Skip network-backed enrichment
python -m etl.enrich --skip-network

# Limit player enrichment scope
python -m etl.enrich --top-n 100
```

What enrichment currently does:

- Adds/updates enrichment columns idempotently
- Enriches players from Cricsheet `people.csv`
- Enriches venues from curated metadata (and Wikipedia fallback when network mode is enabled)
- Loads static auction table data
- Computes derived `fact_ball` analytics columns
- Logs validation coverage at the end

## Useful Commands

```bash
# Logs
docker compose logs -f

# Restart Python service only
docker compose up --build -d intelligence

# Re-run ETL
docker compose exec intelligence python -m etl.run_etl

# Run ETL tests
docker compose exec intelligence pytest etl/tests/ -v
```

## Repository Structure

```text
genbi-ipl/
  gateway/              Go API gateway
  intelligence/         Python intelligence service
  etl/                  ETL + enrichment modules
  config/               semantic layer + templates
  scripts/              utility scripts
  frontend/static/      UI assets
  data/                 local DB/cache/raw data
  docker-compose.yaml
  Makefile
```

## Troubleshooting

- `make` not found on Windows:
  install `make` (Chocolatey/Scoop) or use `docker compose` commands directly.

- `ModuleNotFoundError` when running locally:
  use the same Python environment where dependencies are installed.

- DB locked (`genbi.duckdb` in use):
  stop stale Python processes and rerun.

## Data Attribution

Ball-by-ball data is sourced from Cricsheet and licensed under CC BY-SA 4.0.

## License

MIT. See `LICENSE`.
