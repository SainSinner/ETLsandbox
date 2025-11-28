import datetime
import pendulum
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
# from airflow.providers.apache.impala.hooks.impala import ImpalaHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

DESCRIPTION = 'Расчет витрины "Рекомендованная численность персонала ТТ на дату"'
DOC_MD = ''

STEPS = ['01_temp_order_events_gs', '02_v_temp_order_events_gs', '03_v_v_temp_order_events_gs']


def run_sql(file_name, **kwargs):
    sql_query = get_sql_file_content(file_name)
    # TODO: вернуть хук для импалы
    # hook = ImpalaHook(impala_conn_id='impala_mgl')
    hook = PostgresHook(postgres_conn_id="backend_db")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query)


def get_sql_file_content(file_name):
    # TODO: вернуть import get_current_env
    # from magn.custorm_dags.common.utils import get_current_env
    # kontur = get_current_env()
    path = str(Path(__file__).parent.joinpath('sql_scripts'))
    sql_file_path = Path(path, f'{file_name}.sql')
    with open(sql_file_path, 'r') as file:
        # query = file.read().format(Kontur=kontur)
        query = file.read()
    return query


with DAG(
        dag_id='HR_STAFF_PSTN_FTE_CALC',
        schedule_interval=None,
        start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
        description=DESCRIPTION,
        doc_md=DOC_MD,
        catchup=False,
        dagrun_timeout=datetime.timedelta(hours=2),
        tags=['HR', 'SEMS'],
) as dag:
    lbl_start = EmptyOperator(
        task_id='Start',
        retries=3,
    )

    lbl_end = EmptyOperator(
        task_id='End',
        retries=3,
    )

    previous_task = lbl_start

    for step_name in STEPS:
        run_scripts_tsk = PythonOperator(
            task_id=f"run_{step_name.upper()}_tsk",
            python_callable=run_sql,
            # retries=3,
            op_kwargs={
                'file_name': step_name
            },
        )
        previous_task >> run_scripts_tsk
        previous_task = run_scripts_tsk

    previous_task >> lbl_end
