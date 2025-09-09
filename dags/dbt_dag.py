from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Основные настройки DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_run_models',
    default_args=default_args,
    description='DAG для запуска dbt моделей',
    schedule_interval="@hourly",  # каждые час
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'analytics'],
) as dag:

    # Команда для запуска dbt
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command="""
        cd /usr/app && \
        dbt run --profiles-dir /usr/app
        """
    )

    # Можно добавить тесты dbt после запуска моделей
    test_dbt = BashOperator(
        task_id='test_dbt_models',
        bash_command="""
        cd /usr/app && \
        dbt test --profiles-dir /usr/app
        """
    )

    run_dbt >> test_dbt