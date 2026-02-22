"""Database loader module.

Handles writing transformed data into PostgreSQL. Manages connections,
batch inserts, and post-load tracking.
"""

import logging
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from pipeline.config import config
from pipeline.exceptions import LoadError

logger = logging.getLogger(__name__)

PROCESSED_LOG = Path("processed_files.log")


class DatabaseLoader:
    """Loads DataFrames into PostgreSQL tables.

    Handles connection management, batch inserts, and tracks
    which files have been successfully processed.
    """

    def __init__(self, database_url: str | None = None):
        self.database_url = database_url or config.database_url
        self._conn = None

    def connect(self):
        """Establish database connection."""
        try:
            self._conn = psycopg2.connect(self.database_url)
            logger.info("Connected to database")
        except psycopg2.Error as e:
            raise LoadError(f"Failed to connect to database: {e}") from e

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Database connection closed")

    def load_transactions(self, df: pd.DataFrame, source_file: str) -> int:
        """Load transaction data into the transactions table.

        Args:
            df: Transformed transaction DataFrame.
            source_file: Name of the source file (for tracking).

        Returns:
            Number of rows inserted.

        Raises:
            LoadError: If the database operation fails.
        """
        if self._conn is None:
            raise LoadError("Not connected to database")

        logger.info("Loading %d transactions from %s", len(df), source_file)

        columns = [
            "transaction_id", "merchant_id", "customer_id",
            "amount", "transaction_date", "status", "payment_method",
        ]

        try:
            with self._conn.cursor() as cur:
                values = [
                    tuple(row[col] for col in columns)
                    for _, row in df.iterrows()
                ]

                insert_sql = f"""
                    INSERT INTO transactions
                        ({', '.join(columns)})
                    VALUES %s
                """
                execute_values(cur, insert_sql, values, page_size=config.batch_size)

            self._conn.commit()
            self._log_processed(source_file)
            logger.info("Successfully loaded %d transactions", len(df))
            return len(df)

        except psycopg2.Error as e:
            self._conn.rollback()
            raise LoadError(
                f"Failed to load transactions: {e}",
                table="transactions",
            ) from e

    def load_customers(self, df: pd.DataFrame, source_file: str) -> int:
        """Load customer data into the customers table.

        Args:
            df: Transformed customer DataFrame.
            source_file: Name of the source file (for tracking).

        Returns:
            Number of rows inserted.

        Raises:
            LoadError: If the database operation fails.
        """
        if self._conn is None:
            raise LoadError("Not connected to database")

        logger.info("Loading %d customers from %s", len(df), source_file)

        columns = [
            "customer_id", "merchant_id", "email",
            "first_name", "last_name", "country", "created_at",
        ]

        try:
            with self._conn.cursor() as cur:
                values = [
                    tuple(row[col] for col in columns)
                    for _, row in df.iterrows()
                ]

                insert_sql = f"""
                    INSERT INTO customers
                        ({', '.join(columns)})
                    VALUES %s
                """
                execute_values(cur, insert_sql, values, page_size=config.batch_size)

            self._conn.commit()
            self._log_processed(source_file)
            logger.info("Successfully loaded %d customers", len(df))
            return len(df)

        except psycopg2.Error as e:
            self._conn.rollback()
            raise LoadError(
                f"Failed to load customers: {e}",
                table="customers",
            ) from e

    def _log_processed(self, filename: str) -> None:
        """Record a successfully processed file.

        Args:
            filename: Name of the processed file.
        """
        from datetime import datetime

        with open(PROCESSED_LOG, "a") as f:
            f.write(f"{datetime.now().isoformat()},{filename}\n")
        logger.debug("Logged processed file: %s", filename)
