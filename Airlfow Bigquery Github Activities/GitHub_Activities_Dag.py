import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator


default_args = {
    'owner': 'Phong',
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 2),
    'end_date': datetime(2023, 2, 1),
    'schedule': "00 03 * * *",
}

GCP_CNN = "Google_cloud_connection"
GCP_PJ = 'your_project'
GCP_DATASET = 'your_dataset'


with DAG(
    dag_id='BigQuery_pratice_dag_v46',
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

    t3 = BigQueryOperator(
        task_id="write_to_github_daily_events",
        sql=f"""
            #standardSQL
            SELECT
                DATE("{'{{ yesterday_ds }}'}") as date,
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
        destination_dataset_table=f"{GCP_PJ}.{GCP_DATASET}.github_daily_events${'{{ yesterday_ds_nodash }}'}",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_NEVER",
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CNN,
    )

    t4 = BigQueryCheckOperator(
        task_id='check_after_write_to_github_daily_events',
        sql=f"""
            #standardSQL
            SELECT *
            FROM 
                `{GCP_PJ}.{GCP_DATASET}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE 
                table_name = 'github_daily_events'
                AND partition_id = "{'{{ yesterday_ds_nodash }}'}"
        """,
        use_legacy_sql=False,
        trigger_rule='all_success',
        gcp_conn_id=GCP_CNN,
    )

    t5 = BigQueryCheckOperator(
        task_id='dummy_branch_task',
        sql="""
        #standardSQL
        select 1 as col
        """,
        use_legacy_sql=False,
        trigger_rule='all_success',
        gcp_conn_id=GCP_CNN,
    )

    t6 = BigQueryOperator(
        task_id="write_to_github_monthly_report",
        sql=f"""
            #standardSQL
            CREATE TABLE `{GCP_PJ}.{GCP_DATASET}.github_monthly_report_{'{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y%m") }}'}` as 
            WITH
                github_agg AS(
                    SELECT
                        repo_id,
                        ANY_VALUE(repo_name
                            HAVING
                            max date) AS repo_name,
                        SUM(stars) AS stars_this_month,
                        SUM(forks) AS forks_this_month,
                        SUM(pushes) AS pushes_this_month,
                    FROM
                        `{GCP_PJ}.{GCP_DATASET}.github_daily_events`
                    WHERE
                        date >= "{'{{ macros.ds_add(ds, - macros.datetime.strptime(yesterday_ds, "%Y-%m-%d").day) }}'}"
                    GROUP BY
                        repo_id),

                hn_agg AS (
                    SELECT
                        REGEXP_EXTRACT(url, 'https?://github.com/([^/]+/[^/#?]+)') AS url,
                        ANY_VALUE(struct(id, title)
                            HAVING
                            MAX score) AS story,
                        MAX(score) AS score
                    FROM
                        `bigquery-public-data.hacker_news.full`
                    WHERE
                        type = 'story'
                        AND url LIKE '%://github.com/%'
                        AND url NOT LIKE '%://github.com/blog/%'
                    GROUP BY
                        url)

            SELECT
                repo_id,
                repo_name,
                hn.story.id AS hn_id,
                hn.story.title AS hn_title,
                score AS hn_score,
                stars_this_month,
                forks_this_month,
                pushes_this_month
            FROM
                github_agg gh
            LEFT JOIN
                hn_agg hn
            ON
                gh.repo_name = hn.url
        """,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CNN,
    )

    t7 = BigQueryCheckOperator(
        task_id='check_github_monthly_report',
        sql=f"""
            #standardSQL
            SELECT table_name
            FROM
                `{GCP_PJ}.{GCP_DATASET}.INFORMATION_SCHEMA.TABLES`
            WHERE
                table_name = "github_monthly_report_{'{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%m%Y") }}'}"
        """,
        use_legacy_sql=False,
        trigger_rule='all_success',
        gcp_conn_id=GCP_CNN,
    )

    # Print message of ELT result
    # the content of message depend of the tasks's state
    # Print "Write yesterday parition into github events!" for everday except

    # Print "Write yesterday parition into github events and previous month github report table!"
    # for the first day of each month

    # send "ETL failed to run for {today}" if something went wrong.

    def print_result(**kwargs):
        # state is 1 if success, 0 for others
        succes_states1 = [0, 1, 1, 1, 0, 0, 0]
        message1 = 'Write yesterday parition into github events succesfully!'

        succes_states2 = [1, 1, 1, 1, 1, 1, 1]
        message2 = '''Write yesterday parition into github events succesfully!
                    Previous month github report table write succesfully!'''

        real_states = []

        date = kwargs['execution_date']
        task_operators = [t1, t2, t3, t4, t5, t6, t7]

        for task_op in task_operators:
            ti = TaskInstance(task_op, date)
            state = ti.current_state()
            state_code = 1 if state == 'success' else 0
            real_states.append(state_code)

        if real_states == succes_states1:
            print('----------------------------------------')
            print(message1)
        elif real_states == succes_states2:
            print('----------------------------------------')
            print(message2)
        else:
            print('----------------------------------------')
            print(f'ETL failed to run for {datetime.date(datetime.now())}')

    t8 = PythonOperator(
        task_id='Print_result_of_github_ETL',
        provide_context=True,
        python_callable=print_result,
        trigger_rule='all_done'
    )

    t1 >> t2 >> t3 >> t4 >> t6
    t1 >> t5 >> t6 >> t7 >> t8
    t4 >> t8
