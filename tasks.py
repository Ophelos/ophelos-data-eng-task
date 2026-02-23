#!/usr/bin/env python3
"""Cross-platform task runner. Works on Windows, Mac, and Linux.

Usage: uv run tasks.py <command>
"""

import subprocess
import sys
import os


def run(cmd, check=True, shell=True):
    """Run a shell command, streaming output."""
    print(f"  → {cmd}")
    result = subprocess.run(cmd, shell=shell, check=check)
    return result.returncode == 0


def setup():
    """Start database and install Python dependencies."""
    run("docker compose up -d --wait")
    run("uv sync")
    print("\n✓ Database ready on localhost:5433")
    print("✓ Python dependencies installed")
    print("Run 'uv run tasks.py verify' to confirm everything is working.")


def verify():
    """Confirm environment is healthy."""
    run(
        'uv run python -c "'
        "import psycopg2; "
        "conn = psycopg2.connect('postgresql://pipeline:pipeline@localhost:5433/payments'); "
        "cur = conn.cursor(); "
        "cur.execute('SELECT COUNT(*) FROM transactions'); "
        "count = cur.fetchone()[0]; "
        "print(f'✓ Database connected. {count} transactions loaded.'); "
        'conn.close()"'
    )
    print("✓ Environment is ready.")


def run_pipeline():
    """Run the ingestion pipeline against landing/ files."""
    run("uv run python -m pipeline.main")


def run_analytics():
    """Run analytics SQL and show results."""
    run(
        "docker compose exec -T db psql -U pipeline -d payments"
        " -f /analytics/daily_summary.sql"
        " -f /analytics/weekly_merchant_report.sql"
        " -f /analytics/merchant_performance.sql"
    )
    print("✓ Analytics views refreshed. Query them with 'uv run tasks.py query-report'.")


def query_report():
    """Display the weekly merchant report."""
    run(
        'docker compose exec -T db psql -U pipeline -d payments'
        ' -c "SELECT * FROM analytics.weekly_merchant_report ORDER BY week DESC, merchant_name LIMIT 20;"'
    )


def psql():
    """Open an interactive psql shell."""
    run("docker compose exec db psql -U pipeline -d payments")


def test():
    """Run the test suite."""
    run("uv run pytest tests/ -v")


def teardown():
    """Stop and remove all containers."""
    run("docker compose down -v")


def reset():
    """Reset database to clean state (re-seed)."""
    run("docker compose down -v")
    log = "processed_files.log"
    if os.path.exists(log):
        os.remove(log)
    run("docker compose up -d --wait")
    print("✓ Database reset to initial state.")


COMMANDS = {
    "setup": setup,
    "verify": verify,
    "run-pipeline": run_pipeline,
    "run-analytics": run_analytics,
    "query-report": query_report,
    "psql": psql,
    "test": test,
    "teardown": teardown,
    "reset": reset,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: uv run tasks.py <command>\n")
        print("Commands:")
        for name, func in COMMANDS.items():
            print(f"  {name:20s} {func.__doc__}")
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd not in COMMANDS:
        print(f"Unknown command: {cmd}")
        print(f"Available: {', '.join(COMMANDS)}")
        sys.exit(1)

    COMMANDS[cmd]()


if __name__ == "__main__":
    main()
