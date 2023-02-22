import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator


dag_vars = Variable.get("bigquery_github_variables", deserialize_json = True)
GCP_CNN = dag_vars['conn']
GCP_PJ = dag_vars['project']
GCP_DATASET = dag_vars['dataset']


default_args = {
    'owner': 'Phong',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 2),
    'end_date': datetime(2023, 1, 1),
    'schedule': "00 03 * * *",
    'email': ['test@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}


with DAG(
    dag_id='BigQuery_Github_daily_v1',
    default_args=default_args,
) as dag:

    t2 = BigQueryCheckOperator(
        task_id="check_githubarchive_day",
        sql="""
            #standardSQL
            Select table_name
            From `githubarchive.day.INFORMATION_SCHEMA.TABLES`
            Where table_name = '{{ yesterday_ds_nodash }}' 
        """,
        use_legacy_sql=False,
        trigger_rule='all_done',
        gcp_conn_id=GCP_CNN,
    )
                # DATE("{'{{ yesterday_ds }}'}") as date,

    t3 = BigQueryOperator(
        task_id="write_to_github_daily_events2",
        sql=f"""
            #standardSQL
            SELECT
                repo.id as repo_id,
                ANY_VALUE(repo.name HAVING MAX created_at)
                 as repo_name,
                COUNTIF(type = 'WatchEvent') as stars,
                COUNTIF(type = 'ForkEvent') as forks,
                COUNTIF(type = 'PushEvent') as pushes,
            FROM `githubarchive.day.{'{{ yesterday_ds_nodash }}'}`
            GROUP BY 
                repo.id
        """,
        destination_dataset_table=f"{GCP_PJ}.{GCP_DATASET}.github_daily_events2${'{{ yesterday_ds_nodash }}'}",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_NEVER",
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CNN,
    )

    t4 = BigQueryCheckOperator(
        task_id='check_after_write_to_github_daily_events2',
        sql=f"""
            #standardSQL
            SELECT *
            FROM 
                `{GCP_PJ}.{GCP_DATASET}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE 
                table_name = 'github_daily_events2'
                AND partition_id = "{'{{ yesterday_ds_nodash }}'}"
        """,
        use_legacy_sql=False,
        trigger_rule='all_success',
        gcp_conn_id=GCP_CNN,
    )

    t2 >> t3 >> t4 