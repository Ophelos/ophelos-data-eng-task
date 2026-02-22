.PHONY: setup teardown run-pipeline run-analytics test verify reset query-report psql

# One command to get everything running
setup:
	docker compose up -d --wait
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
	@docker compose exec -T db psql -U pipeline -d payments \
		-f /analytics/daily_summary.sql \
		-f /analytics/weekly_merchant_report.sql \
		-f /analytics/merchant_performance.sql
	@echo "✓ Analytics views refreshed. Query them with 'make query-report'."

# Quick way to see the weekly report
query-report:
	@docker compose exec -T db psql -U pipeline -d payments \
		-c "SELECT * FROM analytics.weekly_merchant_report ORDER BY week DESC, merchant_name LIMIT 20;"

# Open an interactive psql shell
psql:
	docker compose exec db psql -U pipeline -d payments

# Run test suite
test:
	uv run pytest tests/ -v

# Tear down everything
teardown:
	docker compose down -v

# Reset database to clean state (re-seed)
reset:
	docker compose down -v
	rm -f processed_files.log
	docker compose up -d --wait
	@echo "✓ Database reset to initial state."
