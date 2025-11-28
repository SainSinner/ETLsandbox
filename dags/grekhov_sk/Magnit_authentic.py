import datetime
import pendulum
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.impala.hooks.impala import ImpalaHook

DESCRIPTION = 'Расчет витрины "Рекомендованная численность персонала ТТ на дату"'
DOC_MD = ''

STEPS = ['01_temp_work_strg_list', '02_temp_strg_indicators', '03_temp_strg_sku_sale', '04_temp_write_off_oprt',
         '05_temp_strg_site', '06_temp_pos_txn', '07_temp_ecom_txn', '08_temp_income_sku', '09_temp_pos_sbp_corr',
         '10_temp_banana_box', '11_temp_oprt_am', '12_temp_print', '13_temp_site_income', '14_temp_forecast',
         '15_temp_result_step_1', '16_temp_result_step_2', '17_merge_data']


def run_sql(file_name, **kwargs):
    sql_query = get_sql_file_content(file_name)
    # TODO
    imp_hook = ImpalaHook(impala_conn_id='impala_mgl')
    with imp_hook.get_conn() as imp_conn:
        with imp_conn.cursor() as imp_cur:
            imp_cur.execute(sql_query)


def get_sql_file_content(file_name):
    from magn.custorm_dags.common.utils import get_current_env
    kontur = get_current_env()
    path = str(Path(__file__).parent.joinpath('sql_scripts'))
    sql_file_path = Path(path, f'{file_name}.sql')
    with open(sql_file_path, 'r') as file:
        query = file.read().format(Kontur=kontur)
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

    for step_name in STEPS:
        run_scripts_tsk = PythonOperator(
            task_id=f"run_{step_name.upper()}_tsk",
            python_callable=run_sql,
            # retries=3,
            op_kwargs={
                'file_name': step_name
            },
        )

        lbl_start >> run_scripts_tsk
