"""Pipeline entry point.

Orchestrates the full ingestion pipeline: discovery, validation,
transformation, and loading of data files.

Usage:
    python -m pipeline.main
"""

import logging
import sys

from pipeline.config import config
from pipeline.ingestion import FileIngestor
from pipeline.loader import DatabaseLoader
from pipeline.transforms import TransformPipeline
from pipeline.validation import SchemaValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """Execute the full ingestion pipeline.

    Discovers files in the landing directory, validates, transforms,
    and loads them into the database.
    """
    logger.info("Starting ingestion pipeline")
    logger.info("Landing directory: %s", config.landing_dir)

    ingestor = FileIngestor()
    validator = SchemaValidator()
    transformer = TransformPipeline()
    loader = DatabaseLoader()

    files = ingestor.discover_files()
    total_loaded = 0
    errors = 0

    try:
        loader.connect()

        # Process transaction files
        for filepath in files["transactions"]:
            try:
                df = ingestor.ingest_csv(filepath)

                if not validator.validate(df, "transactions"):
                    logger.warning("Skipping %s: validation failed", filepath.name)
                    continue

                transformed = transformer.transform_transactions(df)
                count = loader.load_transactions(transformed, filepath.name)
                total_loaded += count
                logger.info("✓ Loaded %s (%d rows)", filepath.name, count)

            except Exception:
                logger.error("Failed to process %s. Check file format.", filepath.name)
                errors += 1
                continue

        # Process customer files
        for filepath in files["customers"]:
            try:
                df = ingestor.ingest_json(filepath)

                if not validator.validate(df, "customers"):
                    logger.warning("Skipping %s: validation failed", filepath.name)
                    continue

                transformed = transformer.transform_customers(df)
                count = loader.load_customers(transformed, filepath.name)
                total_loaded += count
                logger.info("✓ Loaded %s (%d rows)", filepath.name, count)

            except Exception:
                logger.error("Failed to process %s. Check file format.", filepath.name)
                errors += 1
                continue

    finally:
        loader.close()

    logger.info(
        "Pipeline complete: %d rows loaded, %d errors",
        total_loaded,
        errors,
    )

    if errors > 0:
        logger.warning("Pipeline finished with %d error(s)", errors)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
