from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import logging

from pathlib import Path

default_args = {
    'owner': 'creator',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="Log_Local_Path",
    default_args=default_args,
    schedule_interval=None,
    description="Выводит локальный путь в логи",
    catchup=False,
    tags=['technical', 'logging']
)

def log_local_path(**kwargs):
    # Пример: путь к текущей директории
    path = str(Path(__file__).parent.joinpath('sql_scripts'))
    logging.info(f"Локальный путь: {path}")

log_local_path_task = PythonOperator(
    task_id="log_local_path",
    python_callable=log_local_path,
    dag=dag,
)

# Если нужно добавить другие задачи — можно расширить