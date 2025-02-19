# ETL Test

Данный репозиторий содержит решение **тестового задания** на позицию **Middle Data Engineer**. Основная цель – продемонстрировать навыки в следующих областях:

- Проектирование структуры ETL-процесса (Extract, Transform, Load).
- Работа с Python (pandas, asyncpg/sqlalchemy).
- Очистка и преобразование данных.
- Загрузка результатов в реляционную базу данных (PostgreSQL).
- Организация кода и хранение в GitHub.
- Dockerизация и автоматический запуск (через Docker Compose).
- Юнит-тесты с pytest и краткая документация.

---

## 1. Общий обзор

**Задача**:

1. Извлечь данные из CSV-файлов (`orders.csv` и `customers.csv`).
2. Преобразовать (очистить, объединить, рассчитать время доставки, флаг «доставлено вовремя»).
3. Сгруппировать данные по регионам и посчитать:
   - Количество заказов.
   - Процент заказов, доставленных вовремя.
   - Среднее время доставки.
4. Загрузить агрегированные результаты в PostgreSQL.
5. Выполнить всё по шагам, снабдить проект документацией, тестами и продемонстрировать умение использовать Docker.

---

## 2. Шаги выполнения задачи

1. **Проектирование структуры**  
   - Созданы отдельные модули под Extract, Transform, Load.  
   - Подготовлены файлы: `requirements.txt`, `.gitignore`, `README.md`.

2. **Реализация Extract**  
   - **`extract_data.py`** содержит функцию `get_data(...)`, которая читает CSV через pandas, логирует размер.

3. **Реализация Transform**  
   - **`transform_data.py`**:
     - Объединение таблиц по `customer_id`.
     - Преобразование столбцов в формат дат.
     - Удаление дубликатов и некорректных строк (например, если дата доставки раньше даты покупки).
     - Создание столбца `delivery_time_days` и флага `delivered_on_time`.
     - Сгруппированные результаты формируются в `aggregate_data(...)`.

4. **Реализация Load**  
   - **`load_data.py`** использует библиотеку `asyncpg`:
     - Генерирует таблицу (DDL) в PostgreSQL под типы столбцов DataFrame.
     - Вставляет данные (INSERT) из DataFrame построчно.
   - В `main.py` вызываем `load_to_database(...)` через `asyncio.run(...)`.

5. **Докеризация**  
   - **`Dockerfile`** на базе `python:3.11-slim`.
   - **`docker-compose.yml`** поднимает два контейнера:
     - `db` (образ `postgres:15`)  
     - `etl_app` (наш Python-код).
   - Переменные окружения (логин, пароль, хост) берутся из `.env`.

6. **Тесты (pytest)**  
   - В **`tests/`**: проверяем корректность чтения данных, трансформаций (особенно работу с датами и флагом доставки).
   - Запуск:
     ```bash
     pytest
     ```
     или
     ```bash
     docker-compose run etl_app pytest
     ```

7. **Загрузка в GitHub**  
   - Создан репозиторий, добавлены все файлы, оформлен README.

---


## 3. Структура проекта

```etl_project/ 
├── .gitignore
├── .env               
├── Dockerfile
├── docker-compose.yml
├── main.py
├── README.md
├── requirements.txt
├── src/
│   ├── config.py       # Pydantic settings для считывания логина/пароля из .env
│   ├── extract_data.py # Модуль для извлечения данных (Extract)
│   ├── transform_data.py    # Модуль для очистки и преобразования (Transform)
│   ├── load_data.py         # Модуль для асинхронной загрузки (Load) 
└── tests/ # Тесты на функции ETL
    ├── test_extract.py
    ├── test_transform.py
```

## 4. Установка и запуск (локально)

### 4.1 Клонирование

```bash
git clone https://github.com/<YourGitHubName>/etl_test_task.git
cd etl_test_task
```
### 4.2 Установка зависимостей
- (Опционально) создать виртуальное окружение:
``` bash
python -m venv venv
source venv/bin/activate
venv\Scripts\activate     
```
- Установить библиотеки:
```bash
pip install -r requirements.txt
```
### 4.3 Запуск локально

- Настроить подключение к базе (либо запустить свою PostgreSQL).
- Запустить:
```bash
python main.py
```

## 5. Запуск через Docker
### 5.1 Подготовка
Убедитесь, что Docker и Docker Compose установлены.
В корне проекта лежит .env, например:
```
DB_USER=postgres
DB_PASSWORD=db_password
DB_HOST=db
DB_PORT=5432
DB_NAME=etl_db
```
### 5.2 Сборка и запуск
```bash
docker-compose build
docker-compose up
```

## 5. Тесты
- Локально:
``` bash
pytest
```
- Через Docker:
``` bash
docker-compose run etl_app pytest
```


