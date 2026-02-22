# Payments Data Pipeline

A data ingestion pipeline and analytics layer for a payments platform. Processes merchant transaction files and customer data into a PostgreSQL database, with SQL analytics views for reporting.

## Architecture

```
data/landing/          → Raw files from vendors (CSV, JSON)
src/pipeline/          → Python ingestion pipeline
sql/init.sql           → Database schema + seed data
sql/analytics/         → Reporting views
```

### Pipeline Flow

1. Files land in `data/landing/` from external vendors
2. Pipeline discovers, validates, transforms, and loads files into Postgres
3. Analytics views aggregate data for merchant reporting

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (for PostgreSQL)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager) — `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Quick Start

```bash
# Start database and install dependencies
make setup

# Verify everything is working
make verify

# Run the ingestion pipeline
make run-pipeline

# Create/refresh analytics views
make run-analytics

# View the weekly merchant report
make query-report
```

## Available Commands

| Command | Description |
|---------|-------------|
| `make setup` | Start Postgres, install Python deps |
| `make verify` | Check database connectivity and data |
| `make run-pipeline` | Run the ingestion pipeline |
| `make run-analytics` | Refresh analytics views |
| `make query-report` | Display weekly merchant report |
| `make psql` | Open interactive psql shell |
| `make test` | Run the test suite |
| `make reset` | Reset database to initial seed state |
| `make teardown` | Stop and remove all containers |

## Database

PostgreSQL runs on **port 5433** (to avoid conflicts with local installations).

```
Host:     localhost
Port:     5433
Database: payments
User:     pipeline
Password: pipeline
```

Connect directly:
```bash
psql postgresql://pipeline:pipeline@localhost:5433/payments
```

### Schema

- **merchants** — Merchant accounts (20 seeded)
- **customers** — Customer records (300 seeded)
- **transactions** — Payment transactions (3000 seeded)
- **refunds** — Refund records (215 seeded)
- **analytics.\*** — Reporting views

## Pipeline

The ingestion pipeline (`src/pipeline/`) processes files from `data/landing/`:

- **Transaction CSVs** (`transactions_YYYYMMDD.csv`) — parsed, validated, transformed, loaded
- **Customer JSONs** (`customers_YYYYMMDD.json`) — parsed, validated, transformed, loaded

```bash
# Run with default settings
make run-pipeline

# Or directly
uv run python -m pipeline.main
```

## Analytics

SQL views in `sql/analytics/`:

- **daily_summary** — Daily transaction volumes and amounts
- **weekly_merchant_report** — Weekly merchant performance with refunds
- **merchant_performance** — Comprehensive merchant metrics and rankings

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run tests
make test
```

## Project Structure

```
├── data/
│   ├── landing/              ← Vendor file drop
│   └── archive/              ← Processed files
├── sql/
│   ├── init.sql              ← DDL + seed data
│   └── analytics/            ← Reporting views
├── src/pipeline/
│   ├── main.py               ← Entry point
│   ├── config.py             ← Settings
│   ├── ingestion.py          ← File parsing
│   ├── validation.py         ← Schema checks
│   ├── transforms.py         ← Data transforms
│   ├── loader.py             ← DB writes
│   ├── exceptions.py         ← Custom exceptions
│   └── utils.py              ← Helpers
├── tests/                    ← Test suite
├── scripts/                  ← Data generation
├── docker-compose.yml
├── Makefile
└── pyproject.toml
```
