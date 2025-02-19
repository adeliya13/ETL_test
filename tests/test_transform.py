import pandas as pd
from src.transform_data import transform_data, aggregate_data

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def test_transform_data():
    orders = pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [100, 101],
        "order_purchase_timestamp": ["2023-01-01", "2023-01-03"],
        "order_delivered_customer_date": ["2023-01-02", "2023-01-05"],
        "order_estimated_delivery_date": ["2023-01-02", "2023-01-06"],
    })

    customers = pd.DataFrame({
        "customer_id": [100, 101],
        "customer_state": ["SP", "RJ"]
    })

    transformed = transform_data(orders, customers)

    assert not transformed.empty
    assert transformed.shape[0] == 2
    assert "delivery_time_days" in transformed.columns
    assert "delivered_on_time" in transformed.columns

    assert transformed["delivered_on_time"].all() == True


def test_aggregate_data():
    df = pd.DataFrame({
        "customer_state": ["SP", "SP", "RJ"],
        "order_id": [1, 2, 3],
        "delivered_on_time": [True, False, True],
        "delivery_time_days": [1, 3, 2]
    })

    agg = aggregate_data(df)
    assert agg.shape[0] == 2
    assert set(agg.columns) == {"customer_state", "total_orders", "on_time_delivery_rate", "avg_delivery_time"}

    sp_row = agg[agg["customer_state"] == "SP"].iloc[0]
    assert sp_row["total_orders"] == 2
    assert sp_row["on_time_delivery_rate"] == 50.0
    assert sp_row["avg_delivery_time"] == 2.0

    rj_row = agg[agg["customer_state"] == "RJ"].iloc[0]
    assert rj_row["total_orders"] == 1
    assert rj_row["on_time_delivery_rate"] == 100.0
    assert rj_row["avg_delivery_time"] == 2.0
