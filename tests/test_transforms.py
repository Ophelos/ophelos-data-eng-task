"""Tests for the data transformation module."""

import pandas as pd
import pytest

from pipeline.transforms import TransformPipeline


@pytest.fixture
def transformer():
    return TransformPipeline()


class TestTransformTransactions:
    """Tests for transaction transformations."""

    def test_dates_parsed(self, transformer, sample_transactions_df):
        """Transaction dates should be parsed to datetime."""
        result = transformer.transform_transactions(sample_transactions_df)
        assert pd.api.types.is_datetime64_any_dtype(result["transaction_date"])

    def test_amount_numeric(self, transformer, sample_transactions_df):
        """Amounts should be numeric after transformation."""
        result = transformer.transform_transactions(sample_transactions_df)
        assert pd.api.types.is_numeric_dtype(result["amount"])

    def test_status_lowercased(self, transformer):
        """Status values should be normalised to lowercase."""
        df = pd.DataFrame({
            "transaction_id": ["txn_001"],
            "merchant_id": ["m_001"],
            "customer_id": ["c_001"],
            "amount": [50.00],
            "transaction_date": ["2024-01-15T10:00:00"],
            "status": ["  Completed  "],
            "payment_method": ["Card"],
        })
        result = transformer.transform_transactions(df)
        assert result["status"].iloc[0] == "completed"
        assert result["payment_method"].iloc[0] == "card"

    def test_null_amount_dropped(self, transformer):
        """Rows with null amounts should be dropped."""
        df = pd.DataFrame({
            "transaction_id": ["txn_001", "txn_002"],
            "merchant_id": ["m_001", "m_002"],
            "customer_id": ["c_001", "c_002"],
            "amount": [50.00, None],
            "transaction_date": ["2024-01-15T10:00:00", "2024-01-15T11:00:00"],
            "status": ["completed", "completed"],
            "payment_method": ["card", "card"],
        })
        result = transformer.transform_transactions(df)
        assert len(result) == 1


class TestTransformCustomers:
    """Tests for customer transformations."""

    def test_dates_parsed(self, transformer, sample_customers_df):
        """Customer dates should be parsed to datetime."""
        result = transformer.transform_customers(sample_customers_df)
        assert pd.api.types.is_datetime64_any_dtype(result["created_at"])

    def test_country_uppercased(self, transformer):
        """Country codes should be normalised to uppercase."""
        df = pd.DataFrame({
            "customer_id": ["c_001"],
            "merchant_id": ["m_001"],
            "email": ["test@example.com"],
            "first_name": ["Alice"],
            "last_name": ["Smith"],
            "country": ["  gb  "],
            "created_at": ["2024-01-01"],
        })
        result = transformer.transform_customers(df)
        assert result["country"].iloc[0] == "GB"
