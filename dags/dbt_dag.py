from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

# Основные настройки DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,   # требует настройки SMTP в [smtp]
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_run_models',
    default_args=default_args,
    description='DAG для запуска dbt моделей',
    schedule_interval="0 */6 * * *",  # раз в 6 часов (после import_csv_to_postgres)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'analytics'],
) as dag:

    wait_for_import = ExternalTaskSensor(
        task_id="wait_for_import",
        external_dag_id="import_csv_to_postgres",
        external_task_id="load_csv_to_postgres",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=600,
    )

    # Команда для запуска dbt
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command="""
        cd /usr/app && \
        dbt run --profiles-dir /usr/app
        """
    )

    # Тесты dbt после запуска моделей
    test_dbt = BashOperator(
        task_id='test_dbt_models',
        bash_command="""
        cd /usr/app && \
        dbt test --profiles-dir /usr/app
        """
    )

    # Data Lineage — генерация документации (граф зависимостей, описание моделей)
    docs_dbt = BashOperator(
        task_id='docs_generate',
        bash_command="""
        cd /usr/app && \
        dbt docs generate --profiles-dir /usr/app
        """
    )

    wait_for_import >> run_dbt >> test_dbt >> docs_dbt