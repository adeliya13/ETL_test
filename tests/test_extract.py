import pandas as pd
import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from src.extract_data import get_data


def test_get_data_file_exists(tmp_path):
    test_file = tmp_path / "test_orders.csv"
    df = pd.DataFrame({"order_id": [1, 2], "customer_id": [101, 102]})
    df.to_csv(test_file, index=False)
    assert os.path.exists(test_file)

    result_df = get_data(str(test_file))

    assert not result_df.empty
    assert result_df.shape == (2, 2)


def test_get_data_file_not_exists():
    result_df = get_data("non_existing_file.csv")
    assert result_df.empty
