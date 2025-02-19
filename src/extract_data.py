import pandas as pd
import logging
import os


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_data(file_name: str) -> pd.DataFrame:
    file_path = os.path.join('dataframes', file_name)

    if not os.path.exists(file_path):
        logging.warning(f'Файл {file_name} не найден в {file_path}.')
        return pd.DataFrame()

    data = pd.read_csv(file_path)
    logging.info(f'Файл {file_name} загружен. Размер: {data.shape}')
    return data




