import pytest
import pandas as pd
from src.load_data import generate_create_table_sql, generate_insert_sql

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_generate_create_table_sql():
    df = pd.DataFrame({
        "order_id": [1, 2],
        "customer_state": ["SP", "RJ"],
        "delivery_time_days": [1, 2.5],
        "created_at": pd.to_datetime(["2023-01-01", "2023-01-02"])
    })
    sql = generate_create_table_sql(df, "test_table")
    assert "CREATE TABLE test_table" in sql
    assert "order_id INT" in sql
    assert "customer_state TEXT" in sql
    assert "delivery_time_days FLOAT" in sql
    assert "created_at TIMESTAMP" in sql


def test_generate_insert_sql():
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    sql = generate_insert_sql(df, "test_table")
    assert "INSERT INTO test_table (col1, col2) VALUES ($1, $2)" == sql
