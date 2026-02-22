"""Schema validation for incoming data files.

Validates structure and basic quality checks before data enters
the transformation layer.
"""

import logging
from typing import Any

import pandas as pd

from pipeline.config import config

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates DataFrames against expected schemas.

    Performs structural checks to ensure incoming data matches
    the expected format before processing.
    """

    def __init__(self):
        self._schemas = {
            "transactions": {
                "columns": config.transaction_columns,
                "required_count": len(config.transaction_columns),
            },
            "customers": {
                "columns": config.customer_columns,
                "required_count": len(config.customer_columns),
            },
        }

    def validate(self, df: pd.DataFrame, schema_name: str) -> bool:
        """Validate a DataFrame against a named schema.

        Args:
            df: DataFrame to validate.
            schema_name: Name of the schema to validate against
                        ('transactions' or 'customers').

        Returns:
            True if validation passes.

        Raises:
            ValueError: If schema_name is not recognised.
        """
        if schema_name not in self._schemas:
            raise ValueError(f"Unknown schema: {schema_name}")

        schema = self._schemas[schema_name]
        logger.info("Validating '%s' schema (%d rows)", schema_name, len(df))

        # Check we have the right number of columns
        if len(df.columns) != schema["required_count"]:
            logger.error(
                "Column count mismatch for '%s': expected %d, got %d",
                schema_name,
                schema["required_count"],
                len(df.columns),
            )
            return False

        # Check for empty DataFrame
        if df.empty:
            logger.warning("DataFrame is empty for schema '%s'", schema_name)
            return False

        # Check for null primary keys
        pk_column = df.columns[0]
        null_count = df[pk_column].isna().sum()
        if null_count > 0:
            logger.warning(
                "Found %d null values in primary key column '%s'",
                null_count,
                pk_column,
            )
            return False

        logger.info("Validation passed for '%s'", schema_name)
        return True

    def get_schema_columns(self, schema_name: str) -> list[str]:
        """Return the expected column names for a schema.

        Args:
            schema_name: Name of the schema.

        Returns:
            List of expected column names.
        """
        if schema_name not in self._schemas:
            raise ValueError(f"Unknown schema: {schema_name}")
        return self._schemas[schema_name]["columns"]
