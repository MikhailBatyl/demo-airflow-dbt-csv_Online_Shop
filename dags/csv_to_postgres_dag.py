from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import types as sqltypes, text  # <<< добавил text

CSV_FOLDER = "/opt/airflow/plugins"  # Путь к CSV файлам

def map_dtype_to_sqlalchemy(dtype):
    """Преобразует pandas dtype в SQLAlchemy тип."""
    if pd.api.types.is_integer_dtype(dtype):
        return sqltypes.INTEGER()
    elif pd.api.types.is_float_dtype(dtype):
        return sqltypes.FLOAT()
    elif pd.api.types.is_bool_dtype(dtype):
        return sqltypes.BOOLEAN()
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return sqltypes.DATE()  # <<< меняем TIMESTAMP на DATE
    else:
        return sqltypes.TEXT()

def sync_table_schema(engine, table_name, df):
    """Добавляет недостающие колонки в таблицу Postgres на основе DataFrame."""
    with engine.begin() as conn:
        existing_cols = pd.read_sql(
            f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            """,
            conn
        )["column_name"].tolist()

        for col in df.columns:
            if col not in existing_cols:
                col_type = map_dtype_to_sqlalchemy(df[col].dtype)
                conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {col_type}'))  # <<<
                print(f"➕ Добавлен столбец {col} в {table_name}")

def load_csv_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    engine = hook.get_sqlalchemy_engine()

    for file_name in os.listdir(CSV_FOLDER):
        if not file_name.endswith(".csv"):
            continue

        table_name = file_name.replace(".csv", "").lower()
        file_path = os.path.join(CSV_FOLDER, file_name)

        if not os.path.exists(file_path):
            print(f"⚠ Файл {file_name} не найден, пропускаем")
            continue

        df = pd.read_csv(file_path)

        # Синхронизируем схему
        sync_table_schema(engine, table_name, df)

        # Определяем ID колонку
        id_col = next((col for col in df.columns if col.endswith("_id")), None)

        if id_col:
            existing_ids = pd.read_sql(f'SELECT "{id_col}" FROM "{table_name}"', engine)
            df = df[~df[id_col].isin(existing_ids[id_col])]

        if df.empty:
            print(f"ℹ Нет новых строк для {table_name}")
            continue

        dtype_mapping = {col: map_dtype_to_sqlalchemy(dtype) for col, dtype in df.dtypes.items()}

        df.to_sql(table_name, engine, if_exists="append", index=False, dtype=dtype_mapping)
        print(f"✅ Добавлено {len(df)} новых строк в таблицу {table_name}")

with DAG(
    dag_id="import_csv_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */8 * * *",  # раз в 8 часов
    catchup=False,
    tags=["csv", "postgres", "import"]
) as dag:

    # wait_for_generation = ExternalTaskSensor(
    #     task_id="wait_for_generation",
    #     external_dag_id="daily_generate_orders",
    #     external_task_id=None,
    #     allowed_states=["success"],
    #     failed_states=["failed"],
    #     mode="reschedule",
    #     poke_interval=60,
    #     timeout=600,
    # )
        
    import_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres
    )
    
    # wait_for_generation >> 
    import_task





# Вар 1

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.hooks.postgres_hook import PostgresHook
# from datetime import datetime
# import os
# import pandas as pd
# import sqlalchemy
# from sqlalchemy import types as sqltypes

# CSV_FOLDER = "/opt/airflow/plugins"  # Путь к CSV файлам

# def map_dtype_to_sqlalchemy(dtype):
#     """Преобразует pandas dtype в SQLAlchemy тип."""
#     if pd.api.types.is_integer_dtype(dtype):
#         return sqltypes.INTEGER()
#     elif pd.api.types.is_float_dtype(dtype):
#         return sqltypes.FLOAT()
#     elif pd.api.types.is_bool_dtype(dtype):
#         return sqltypes.BOOLEAN()
#     elif pd.api.types.is_datetime64_any_dtype(dtype):
#         return sqltypes.TIMESTAMP()
#     else:
#         return sqltypes.TEXT()

# def sync_table_schema(engine, table_name, df):
#     """Добавляет недостающие колонки в таблицу Postgres на основе DataFrame."""
#     with engine.begin() as conn:
#         existing_cols = pd.read_sql(
#             f"""
#             SELECT column_name 
#             FROM information_schema.columns 
#             WHERE table_name = '{table_name}'
#             """,
#             conn
#         )["column_name"].tolist()

#         for col in df.columns:
#             if col not in existing_cols:
#                 # Определяем SQL тип по dtype
#                 col_type = map_dtype_to_sqlalchemy(df[col].dtype)
#                 conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {col_type}')
#                 print(f"➕ Добавлен столбец {col} в {table_name}")


# def load_csv_to_postgres():
#     hook = PostgresHook(postgres_conn_id="postgres_conn")
#     engine = hook.get_sqlalchemy_engine()

#     for file_name in os.listdir(CSV_FOLDER):
#         if not file_name.endswith(".csv"):
#             continue

#         table_name = file_name.replace(".csv", "").lower()
#         file_path = os.path.join(CSV_FOLDER, file_name)

#         if not os.path.exists(file_path):
#             print(f"⚠ Файл {file_name} не найден, пропускаем")
#             continue

#         df = pd.read_csv(file_path)

#         # Синхронизируем схему таблицы
#         sync_table_schema(engine, table_name, df)

#         # Определяем ключевой столбец для проверки дубликатов
#         id_col = None
#         for col in df.columns:
#             if col.endswith("_id"):
#                 id_col = col
#                 break

#         # Если есть ключевой столбец — исключаем существующие ID
#         if id_col:
#             existing_ids = pd.read_sql(f'SELECT "{id_col}" FROM "{table_name}"', engine)
#             df = df[~df[id_col].isin(existing_ids[id_col])]

#         if df.empty:
#             print(f"ℹ Нет новых строк для {table_name}")
#             continue

#         # Создаем словарь типов для to_sql
#         dtype_mapping = {col: map_dtype_to_sqlalchemy(dtype) for col, dtype in df.dtypes.items()}

#         # Загружаем новые строки
#         df.to_sql(table_name, engine, if_exists="append", index=False, dtype=dtype_mapping)
#         print(f"✅ Добавлено {len(df)} новых строк в таблицу {table_name}")

# with DAG(
#     dag_id="import_csv_to_postgres",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="0 */6 * * *",  # раз в 6 часов
#     catchup=False,
#     tags=["csv", "postgres", "import"]
# ) as dag:
    
#     # Сенсор для ожидания последнего успешного запуска daily_generate_orders
#     wait_for_generation = ExternalTaskSensor(
#         task_id="wait_for_generation",
#         external_dag_id="daily_generate_orders",  # DAG, который генерирует CSV
#         external_task_id=None,                    # ждём завершения всего DAG
#         allowed_states=["success"],               # ждём только успешный DAG-run
#         failed_states=["failed"],                 # если DAG упал — прекращаем ожидание
#         # execution_date_fn=lambda dt: None,        # не жёстко по execution_date, а по последнему run
#         mode="reschedule",                        # лучше reschedule, чтобы worker не висел
#         poke_interval=60,                         # проверка каждые 60 секунд
#         timeout=600,                              # максимум 10 минут ожидания
#     )
        
#     import_task = PythonOperator(
#         task_id="load_csv_to_postgres",
#         python_callable=load_csv_to_postgres
#     )
    
#     wait_for_generation >> import_task