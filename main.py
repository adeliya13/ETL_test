from src.extract_data import get_data
from src.transform_data import transform_data, aggregate_data


orders_data = get_data('orders.csv')
customers_data = get_data('customers.csv')

transformed = transform_data(orders_data, customers_data)
aggregates = aggregate_data(transformed)
print(aggregates.head())