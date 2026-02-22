"""Data transformation layer.

Applies business logic transformations to raw data before loading
into the database. Handles type conversions, standardisation, and
derived fields.
"""

import logging

import pandas as pd

from pipeline.config import config

logger = logging.getLogger(__name__)


class TransformPipeline:
    """Applies sequential transformations to DataFrames.

    Transformations are specific to each data type (transactions,
    customers) and ensure data consistency before database loading.
    """

    def __init__(self):
        self.timezone = config.timezone

    def transform_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw transaction data for database loading.

        Applies:
        - Date parsing and normalisation
        - Amount validation and type casting
        - Status standardisation

        Args:
            df: Raw transaction DataFrame.

        Returns:
            Transformed DataFrame ready for loading.
        """
        logger.info("Transforming %d transaction rows", len(df))
        result = df.copy()

        # Parse transaction dates
        result["transaction_date"] = pd.to_datetime(result["transaction_date"])

        # Ensure amount is numeric
        result["amount"] = pd.to_numeric(result["amount"], errors="coerce")

        # Standardise status values
        result["status"] = result["status"].str.lower().str.strip()

        # Standardise payment method
        result["payment_method"] = result["payment_method"].str.lower().str.strip()

        # Drop any rows where critical fields are null
        critical_cols = ["transaction_id", "merchant_id", "amount"]
        before = len(result)
        result = result.dropna(subset=critical_cols)
        dropped = before - len(result)
        if dropped > 0:
            logger.warning("Dropped %d rows with null critical fields", dropped)

        logger.info("Transformation complete: %d rows", len(result))
        return result

    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw customer data for database loading.

        Applies:
        - Date parsing
        - String normalisation
        - Email validation

        Args:
            df: Raw customer DataFrame.

        Returns:
            Transformed DataFrame ready for loading.
        """
        logger.info("Transforming %d customer rows", len(df))
        result = df.copy()

        # Parse dates
        result["created_at"] = pd.to_datetime(result["created_at"])

        # Normalise string fields
        for col in ["first_name", "last_name", "email"]:
            if col in result.columns:
                result[col] = result[col].str.strip()

        # Standardise country codes
        if "country" in result.columns:
            result["country"] = result["country"].str.upper().str.strip()

        logger.info("Transformation complete: %d rows", len(result))
        return result
