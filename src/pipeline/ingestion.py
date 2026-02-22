"""File ingestion module.

Handles reading and parsing of data files from the landing directory.
Supports CSV (transactions) and JSON (customers) formats.
"""

import json
import logging
from pathlib import Path

import pandas as pd

from pipeline.config import config
from pipeline.exceptions import IngestionError

logger = logging.getLogger(__name__)


class FileIngestor:
    """Reads and parses data files from the landing directory.

    Supports multiple file formats and handles common parsing issues
    like encoding and malformed records.
    """

    def __init__(self, landing_dir: Path | None = None):
        self.landing_dir = landing_dir or config.landing_dir

    def ingest_csv(self, filepath: Path) -> pd.DataFrame:
        """Read a CSV file into a DataFrame.

        Args:
            filepath: Path to the CSV file.

        Returns:
            Parsed DataFrame.

        Raises:
            IngestionError: If the file cannot be read or parsed.
        """
        logger.info("Ingesting CSV: %s", filepath.name)
        try:
            df = pd.read_csv(filepath)
            logger.info("Read %d rows from %s", len(df), filepath.name)
            return df
        except Exception as e:
            raise IngestionError(
                f"Failed to read CSV: {filepath.name}",
                file_path=str(filepath),
            ) from e

    def ingest_json(self, filepath: Path) -> pd.DataFrame:
        """Read a JSON array file into a DataFrame.

        Args:
            filepath: Path to the JSON file.

        Returns:
            Parsed DataFrame.

        Raises:
            IngestionError: If the file cannot be read or parsed.
        """
        logger.info("Ingesting JSON: %s", filepath.name)
        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            if not isinstance(data, list):
                raise IngestionError(
                    f"Expected JSON array, got {type(data).__name__}",
                    file_path=str(filepath),
                )

            df = pd.DataFrame(data)
            logger.info("Read %d rows from %s", len(df), filepath.name)
            return df
        except json.JSONDecodeError as e:
            raise IngestionError(
                f"Invalid JSON in {filepath.name}",
                file_path=str(filepath),
            ) from e

    def discover_files(self) -> dict[str, list[Path]]:
        """Find all data files in the landing directory.

        Returns:
            Dictionary mapping file type to list of file paths.
        """
        transaction_files = sorted(
            self.landing_dir.glob(config.transaction_pattern)
        )
        customer_files = sorted(
            self.landing_dir.glob(config.customer_pattern)
        )

        logger.info(
            "Discovered %d transaction files, %d customer files",
            len(transaction_files),
            len(customer_files),
        )

        return {
            "transactions": transaction_files,
            "customers": customer_files,
        }
