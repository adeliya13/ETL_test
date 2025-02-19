from src.transform_data import transform_data, aggregate_data
from src.load_data import load_to_database
from src.extract_data import get_data
from src.config import settings

import asyncio


def main():
    # EXTRACT
    orders_data = get_data('orders.csv')
    customers_data = get_data('customers.csv')

    # TRANSFORM
    merged_data = transform_data(orders_data, customers_data)
    aggregated_data = aggregate_data(merged_data)

    # LOAD
    db_url = (
        f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    )
    asyncio.run(load_to_database(aggregated_data, "orders_aggregated", db_url))


if __name__ == "__main__":
    main()
