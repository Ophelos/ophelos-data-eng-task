"""Tests for the schema validation module."""

import pandas as pd
import pytest

from pipeline.validation import SchemaValidator


@pytest.fixture
def validator():
    return SchemaValidator()


class TestSchemaValidator:
    """Tests for SchemaValidator."""

    def test_valid_transactions_pass(self, validator, sample_transactions_df):
        """Valid transaction data should pass validation."""
        assert validator.validate(sample_transactions_df, "transactions") is True

    def test_valid_customers_pass(self, validator, sample_customers_df):
        """Valid customer data should pass validation."""
        assert validator.validate(sample_customers_df, "customers") is True

    def test_empty_dataframe_fails(self, validator):
        """Empty DataFrames should fail validation."""
        df = pd.DataFrame(columns=[
            "transaction_id", "merchant_id", "customer_id",
            "amount", "transaction_date", "status", "payment_method",
        ])
        assert validator.validate(df, "transactions") is False

    def test_wrong_column_count_fails(self, validator):
        """DataFrames with wrong number of columns should fail."""
        df = pd.DataFrame({
            "transaction_id": ["txn_001"],
            "merchant_id": ["m_001"],
            "amount": [10.00],
        })
        assert validator.validate(df, "transactions") is False

    def test_unknown_schema_raises(self, validator, sample_transactions_df):
        """Requesting an unknown schema should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown schema"):
            validator.validate(sample_transactions_df, "orders")

    def test_null_primary_key_fails(self, validator):
        """Rows with null primary keys should fail validation."""
        df = pd.DataFrame({
            "transaction_id": [None, "txn_002"],
            "merchant_id": ["m_001", "m_002"],
            "customer_id": ["c_001", "c_002"],
            "amount": [10.00, 20.00],
            "transaction_date": ["2024-01-15", "2024-01-15"],
            "status": ["completed", "completed"],
            "payment_method": ["card", "card"],
        })
        assert validator.validate(df, "transactions") is False
