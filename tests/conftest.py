"""Shared test fixtures for the pipeline test suite."""

import pandas as pd
import pytest


@pytest.fixture
def sample_transactions_df():
    """A valid transaction DataFrame matching the expected schema."""
    return pd.DataFrame({
        "transaction_id": ["txn_001", "txn_002", "txn_003", "txn_004", "txn_005"],
        "merchant_id": ["m_001", "m_002", "m_001", "m_003", "m_002"],
        "customer_id": ["c_001", "c_002", "c_003", "c_004", "c_005"],
        "amount": [49.99, 150.00, 25.50, 399.99, 12.00],
        "transaction_date": [
            "2024-01-15T10:23:00",
            "2024-01-15T14:05:30",
            "2024-01-15T09:12:00",
            "2024-01-15T16:45:00",
            "2024-01-15T11:30:00",
        ],
        "status": ["completed", "completed", "pending", "completed", "failed"],
        "payment_method": ["card", "bank_transfer", "card", "wallet", "card"],
    })


@pytest.fixture
def sample_customers_df():
    """A valid customer DataFrame matching the expected schema."""
    return pd.DataFrame({
        "customer_id": ["c_001", "c_002", "c_003"],
        "merchant_id": ["m_001", "m_001", "m_002"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "first_name": ["Alice", "Bob", "Charlie"],
        "last_name": ["Smith", "Jones", "Brown"],
        "country": ["GB", "DE", "FR"],
        "created_at": ["2024-01-01", "2024-01-05", "2024-01-10"],
    })


@pytest.fixture
def empty_df():
    """An empty DataFrame."""
    return pd.DataFrame()
