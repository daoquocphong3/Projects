import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.models import DagRun
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator


dag_vars = Variable.get("bigquery_github_variables", deserialize_json = True)
GCP_CNN = dag_vars['conn']
GCP_PJ = dag_vars['project']
GCP_DATASET = dag_vars['dataset']


default_args = {
    'owner': 'Phong',
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023,1, 1),
    'end_date': datetime(2023, 1, 1),
    'schedule_interval': "00 05 1 * *",
}


with DAG(
    dag_id='BigQuery_Github_monthly_check',
    default_args=default_args,
) as dag:

    t1 = BigQueryCheckOperator(
        task_id="check_first_day_of_month",
        sql="""
            #StandardSQL
            SELECT 
                EXTRACT(DAY FROM DATE('{{ ds }}')) = 1
        """,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CNN,
    )


    def print_result(**kwargs):
        execute_date = kwargs['ds']
        date = datetime.strptime(str(execute_date), "%Y-%m-%d").date()

        dag_runs = DagRun.find(dag_id='BigQuery_Github_daily_v1')
        dag_runs = [dag_run for dag_run in dag_runs if dag_run.execution_date.date() <= date and dag_run.execution_date.date() > (date - relativedelta(months = 1))]

        yesterday = date - timedelta(days=1)

        dates = set(dag.execution_date.date() for dag in dag_runs)
        if len(dates) < yesterday.day:
            missing_dates = []
            start_date_ = (date - relativedelta(months = 1) + timedelta(days=1))
            for i in range(date.day):
                temp_date = start_date_ + timedelta(days=i)
                if temp_date not in dates:
                    missing_dates.append(temp_date)
            print(f'Month {yesterday.month} year {yesterday.year} github check failed!\nThese days missing: {missing_dates}')
            return

        dag_runs.sort(key=lambda x: x.execution_date)

        failed_dates = []
        for i in range(len(dag_runs)):
            if i < len(dag_runs)-1 and (dag_runs[i].execution_date.date() == dag_runs[i+1].execution_date.date()):
                    pass
            else:
                if dag_runs[i].state != 'success':
                    failed_dates.append(datetime.strftime(dag_runs[i].execution_date.date(), "%Y-%m-%d"))

        if not len(failed_dates):
            print(f'Month {yesterday.month} year {yesterday.year} Github check succesfully!')
        else:
            print(f'Month {yesterday.month} year {yesterday.year} Github check failed!\nThese days failed to run github: {failed_dates}')

    t8 = PythonOperator(
        task_id='Print_Github_monthly_check',
        provide_context=True,
        python_callable=print_result,
        trigger_rule='all_done'
    )

    t1 >> t8


