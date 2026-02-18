from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import types as sqltypes, text  # <<< добавил text

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,   # требует настройки SMTP в [smtp]
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

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

@task
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
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",  # раз в 6 часов (совпадает с daily_generate_orders)
    catchup=False,
    tags=["csv", "postgres", "import"]
) as dag:

    wait_for_generation = ExternalTaskSensor(
        task_id="wait_for_generation",
        external_dag_id="daily_generate_orders",
        external_task_id="generate_new_data",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=600,
    )

    import_task = load_csv_to_postgres()

    wait_for_generation >> import_task
