import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# очищение и объединение данных
def transform_data(orders_data: pd.DataFrame, customers_data: pd.DataFrame) -> pd.DataFrame:
    logging.info(f'Начало процесса очистки и объединения данных.')

    merged_data = pd.merge(
        orders_data,
        customers_data,
        on='customer_id',
        how='inner'
    )
    logging.info(f"Размер объединённой таблицы: {merged_data.shape}")

    date_columns = [
        'order_purchase_timestamp',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for column in date_columns:
        if column in merged_data.columns:
            merged_data[column] = pd.to_datetime(
                merged_data[column],
                errors='coerce'
            )

    if 'order_id' in merged_data.columns:
        before_drop = merged_data.shape[0]
        merged_data.drop_duplicates(subset=['order_id'], inplace=True)
        after_drop = merged_data.shape[0]
        logging.info(f"Дубликаты по 'order_id' удалены: {before_drop - after_drop} строк.")

    # удаление записей с критичными пропусками (например если нет даты покупки или даты доставки)
    required_fields = ['order_purchase_timestamp', 'order_delivered_customer_date']
    before_na_drop = merged_data.shape[0]
    merged_data.dropna(subset=required_fields, inplace=True)
    after_na_drop = merged_data.shape[0]
    logging.info(f"Записей с пропусками в {required_fields} удалено: {before_na_drop - after_na_drop}.")

    # доставленная дата не должна быть раньше даты покупки
    if all(column in merged_data.columns for column in ['order_purchase_timestamp', 'order_delivered_customer_date']):
        condition = merged_data['order_delivered_customer_date'] >= merged_data['order_purchase_timestamp']
        invalid_dates_count = merged_data.shape[0] - merged_data[condition].shape[0]
        merged_data = merged_data[condition]
        logging.info(f"Строк с некорректными датами (доставка раньше покупки) удалено: {invalid_dates_count}.")

    # Количество дней доставки
    if all(col in merged_data.columns for col in ['order_purchase_timestamp', 'order_delivered_customer_date']):
        merged_data['delivery_time_days'] = (
                merged_data['order_delivered_customer_date'] - merged_data['order_purchase_timestamp']
        ).dt.days

    # колонна "доставлено вовремя" true/false
    merged_data['delivered_on_time'] = (
            merged_data['order_delivered_customer_date'] <= merged_data['order_estimated_delivery_date']
    )
    logging.info("трансформация завершена.")
    return merged_data


# агрегация по регионам (кол-во заказов, % заказов, время доставки в среднем)
def aggregate_data(transformed_df: pd.DataFrame) -> pd.DataFrame:
    if 'customer_state' not in transformed_df.columns:
        logging.warning(f'Колонка customer_state отсутствует в данных. Агрегация невозможна.')
        return pd.DataFrame()

    logging.info("Начало агрегации данных по столбцу customer_state.")
    grouped = transformed_df.groupby('customer_state').agg(
        total_orders=('order_id', 'count'),
        on_time_delivery_rate=('delivered_on_time', 'mean'),
        avg_delivery_time=('delivery_time_days', 'mean')
    ).reset_index()

    # процент доставленных вовремя
    grouped['on_time_delivery_rate'] = grouped['on_time_delivery_rate'] * 100
    grouped['on_time_delivery_rate'] = grouped['on_time_delivery_rate'].round(2)
    grouped['avg_delivery_time'] = grouped['avg_delivery_time'].round(2)

    logging.info("Агрегация завершена.")
    logging.info(f"Размер агрегированного dataframe: {grouped.shape}")
    return grouped
