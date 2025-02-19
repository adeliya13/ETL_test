import logging
import pandas as pd
import asyncpg
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


async def load_to_database(data: pd.DataFrame, table_name: str, db_url: str) -> None:
    if data.empty:
        logging.warning("DataFrame пустой, нечего загружать.")
        return

    try:
        parsed = urlparse(db_url)
        user = parsed.username
        password = parsed.password
        host = parsed.hostname
        port = parsed.port
        database = parsed.path.strip('/')

        connection = await asyncpg.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )

        async with connection.transaction():
            drop_sql = f"DROP TABLE IF EXISTS {table_name};"
            await connection.execute(drop_sql)

            create_sql = generate_create_table_sql(data, table_name)
            await connection.execute(create_sql)

        insert_sql = generate_insert_sql(data, table_name)

        rows_to_insert = list(data.itertuples(index=False, name=None))

        async with connection.transaction():
            await connection.executemany(insert_sql, rows_to_insert)

        logging.info(f"Данные успешно загружены в таблицу '{table_name}' (asyncpg).")
        await connection.close()

    except Exception as e:
        logging.error(f"Ошибка при загрузке данных в '{table_name}': {e}")


def generate_create_table_sql(df: pd.DataFrame, table_name: str) -> str:
    col_defs = []
    for col in df.columns:
        dtype = df[col].dtype

        if pd.api.types.is_integer_dtype(dtype):
            col_type = 'INT'
        elif pd.api.types.is_float_dtype(dtype):
            col_type = 'FLOAT'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_type = 'TIMESTAMP'
        else:
            col_type = 'TEXT'

        col_defs.append(f"{col} {col_type}")

    cols_sql = ", ".join(col_defs)
    create_sql = f"CREATE TABLE {table_name} ({cols_sql});"
    return create_sql


def generate_insert_sql(df: pd.DataFrame, table_name: str) -> str:
    columns = list(df.columns)
    col_str = ", ".join(columns)
    placeholders = ", ".join([f"${i + 1}" for i in range(len(columns))])
    insert_sql = f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})"
    return insert_sql
