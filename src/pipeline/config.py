"""Pipeline configuration and settings."""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for the ingestion pipeline.

    All settings can be overridden via environment variables.
    """

    # Database
    database_url: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL",
            "postgresql://pipeline:pipeline@localhost:5433/payments",
        )
    )

    # Paths
    landing_dir: Path = field(
        default_factory=lambda: Path(
            os.getenv("LANDING_DIR", "data/landing")
        )
    )
    archive_dir: Path = field(
        default_factory=lambda: Path(
            os.getenv("ARCHIVE_DIR", "data/archive")
        )
    )

    # Processing
    batch_size: int = 500
    timezone: str = "UTC"

    # File patterns
    transaction_pattern: str = "transactions_*.csv"
    customer_pattern: str = "customers_*.json"

    # Expected schemas
    transaction_columns: list[str] = field(default_factory=lambda: [
        "transaction_id",
        "merchant_id",
        "customer_id",
        "amount",
        "transaction_date",
        "status",
        "payment_method",
    ])
    customer_columns: list[str] = field(default_factory=lambda: [
        "customer_id",
        "merchant_id",
        "email",
        "first_name",
        "last_name",
        "country",
        "created_at",
    ])


# Global config instance
config = PipelineConfig()
