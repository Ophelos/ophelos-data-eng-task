.PHONY: setup teardown run-pipeline run-analytics test verify reset query-report psql

# Use docker compose v2 plugin if available, fall back to docker-compose v1
DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

# One command to get everything running
setup:
	$(DOCKER_COMPOSE) up -d --wait
	uv sync
	@echo "✓ Database ready on localhost:5433"
	@echo "✓ Python dependencies installed"
	@echo "Run 'make verify' to confirm everything is working."

# Confirm environment is healthy
verify:
	@uv run python -c "import psycopg2; \
		conn = psycopg2.connect('postgresql://pipeline:pipeline@localhost:5433/payments'); \
		cur = conn.cursor(); \
		cur.execute('SELECT COUNT(*) FROM transactions'); \
		count = cur.fetchone()[0]; \
		print(f'✓ Database connected. {count} transactions loaded.'); \
		conn.close()"
	@echo "✓ Environment is ready."

# Run the ingestion pipeline against landing/ files
run-pipeline:
	uv run python -m pipeline.main

# Run analytics SQL and show results
run-analytics:
	@$(DOCKER_COMPOSE) exec -T db -- psql -U pipeline -d payments \
		-f /analytics/daily_summary.sql \
		-f /analytics/weekly_merchant_report.sql \
		-f /analytics/merchant_performance.sql
	@echo "✓ Analytics views refreshed. Query them with 'make query-report'."

# Quick way to see the weekly report
query-report:
	@$(DOCKER_COMPOSE) exec -T db -- psql -U pipeline -d payments \
		-c "SELECT * FROM analytics.weekly_merchant_report ORDER BY week DESC, merchant_name LIMIT 20;"

# Open an interactive psql shell
psql:
	$(DOCKER_COMPOSE) exec db -- psql -U pipeline -d payments

# Run test suite
test:
	uv run pytest tests/ -v

# Tear down everything
teardown:
	$(DOCKER_COMPOSE) down -v

# Reset database to clean state (re-sed)
reset:
	$(DOCKER_COMPOSE) down -v
	rm -f processed_files.log
	$(DOCKER_COMPOSE) up -d --wait
	@echo "✓ Database reset to initial state."
