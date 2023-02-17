# Airflow Pineline BigQuery Github Acticvities (Monthly report)
## Visualization: 
Here is Google Data Stuido DashBoard for the results table: [GitHub Activities Jan 2023](https://lookerstudio.google.com/s/nNe2JozhK88)<br>
- Star and Hacker News Score are for the popularity of Repo.
- Forks and Push are for the developing status of repo.

## Dataset:
In this project, I use 2 public datasets of google Bigquery: Github Archive and Hacker News
### Github Archive:
You can look at the dataset here: [GithubArchive](https://console.cloud.google.com/bigquery?project=githubarchive&page=project) - [More_Information](https://www.gharchive.org/)

- GitHub Archive is a project of captures every activity of all public projects in GitHub. <br>
- It has 3 datasets: Day, Month, Year <br>
- In this project, I use dataset Day. <br>
- Each table in the Day dataset is a day and captures all activity of that day. Like below:<br>
![image](https://user-images.githubusercontent.com/55779400/218960697-6b4bde98-6f49-4533-8c9d-50ebb42c25a6.png)

- This is the schema of each day table: <br>

<!-- ![image](https://user-images.githubusercontent.com/55779400/218961000-62704aab-1df4-42d2-b8b4-ca0ea2d90ad0.png) -->
![image](https://user-images.githubusercontent.com/55779400/219074652-dfad8bc3-03d6-4e8f-bc3d-e67f4918aef5.png)

### Hacker News:
You can look at the dataset here: [HackerNews](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=hacker_news&page=dataset&project=apply-ds-test-371316) - [More_Information](https://github.com/HackerNews/API)

- Hacker News is a social news website focusing on computer science and entrepreneurship.
- This dataset contains a randomized sample of roughly one-quarter of all stories and comments from Hacker News from its launch in 2006. 
- In this dataset, I use table Full.
- Schema of the Full table: <br>

<!-- ![image](https://user-images.githubusercontent.com/55779400/218963958-297ecf3e-5b83-4b90-b7c2-49fd854d0118.png) -->
<!-- ![image](https://user-images.githubusercontent.com/55779400/219014949-def95133-9f0a-4d39-a177-aa421a4e9193.png) -->
Field | Description
------|------------
**id** | The item's unique id.
deleted | `true` if the item is deleted.
type | The type of item. One of "job", "story", "comment", "poll", or "pollopt".
by | The username of the item's author.
time | Creation date of the item, in [Unix Time](http://en.wikipedia.org/wiki/Unix_time).
text | The comment, story or poll text. HTML.
dead | `true` if the item is dead.
parent | The comment's parent: either another comment or the relevant story.
poll | The pollopt's associated poll.
kids | The ids of the item's comments, in ranked display order.
url | The URL of the story.
score | The story's score, or the votes for a pollopt.
title | The title of the story, poll or job. HTML.
parts | A list of related pollopts, in display order.
descendants | In the case of stories or polls, the total comment count.

## Tasks:
### Task 1: Check first day of month
- Use simple DML of Bigquery to check if today if a first day of month.

``` python
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
```
<!-- ![image](https://user-images.githubusercontent.com/55779400/219014222-bbf6d8cb-f98f-49a6-a49b-94a625445c00.png) -->


### Task 2: Check if there is a yesterday table in GithubArchive Day dataset
- Use INFORMATION_SCHEMA.TABLES filter table_name to only yesterday <br>
- If yesterday table is available, the query result is table_name (yesterday_ds_nodash like:  20230101)
- If there is no yesterday table, the query does not return anything.

More about INFORMATION_SCHEMA: [INFORMATION_SCHEMA](https://cloud.google.com/bigquery/docs/information-schema-intro)<br>

<!-- ![image](https://user-images.githubusercontent.com/55779400/219014148-3a555058-0b50-41ad-ac09-a7fbd67b86df.png) -->
```python
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
```


### Task 3: Write data to Github_Daily_Events table:
- Github_daily_events table stores data of every GitHub repo.
- Each line is a date, repo_id, repo_name and it's stars, forks, pushes. Example of rows in the table: <br>

![image](https://user-images.githubusercontent.com/55779400/218974771-59eae5a6-8c20-45e5-b9de-4db194dd62d1.png)



- The table is big, with about 25 million rows for 31 days in January 2023. So I partitioned it by date to reduce the cost of querying on the table.
- Ingestion time partitioning reduces the stored size of the table by putting the date column to \_PARTITIONTIME(a pseudocolumn). 
- But when I plug the table into Google Data Studio, the \_PARTITIONTIME didn't appear so I choose Time-unit column partitioning instead.
- By the way, [PyPI package downloads](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=pypi&page=dataset&project=apply-ds-test-371316) table is Time-unit column partitioning too, each partition contains on average 700 million rows.
- More info about [BigQuery Partitioned Table](https://cloud.google.com/bigquery/docs/partitioned-tables?_ga=2.103336576.-1647680310.1670343964)
- Query create the Github_daily_events table: <br>

<!-- ![image](https://user-images.githubusercontent.com/55779400/218955332-b0a72d8f-edf2-47f9-865f-d607b102c04e.png) -->
``` bigquery
  CREATE TABLE `github_daily_events`
  (
    date DATE,
    repo_id INT64,
    repo_name STRING,
    stars INT64,
    forks INT64,
    pushes INT64
  )
  PARTITION BY date
  OPTIONS(
    partition_expiration_days=Null,
    require_partition_filter=true
  );
```



#### Task 3 code:

<!-- ![image](https://user-images.githubusercontent.com/55779400/219014043-d6a85190-345c-4f15-a7e2-e7df9798eace.png) -->
```python
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
```

- Query GithubArchive day yesterday table, group by repo.id, using countif to get stars, forks, and pushes of the repos that day.
- Even repo.name may be unique and act as an identifier or not, there is no guarantee, but repo.name can be changed, there are many repo.names of the same repo.id. For that reason, group by must be done on repo.id not repo.name.
- For repo.name, I only chose the one appeare last (max create_at) using [Aggregate functions of BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions) - ANY_VALUE.
- Write directly into the yesterday partition of the table, and 'WRITE_TRUNCATE' in case they already have data, truncate it, then write, there will be no duplicate rows in the partition.
- 'CREATE_NEVER' for create_disposition.

### Task 4: Check if the yesterday parition is Github_Daily_Events Table:
<!-- ![image](https://user-images.githubusercontent.com/55779400/219014359-b4b45e10-8467-4358-97a3-f7a73941b82f.png) -->

```python
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
  ```
  
### Task 5: Branch Dummy task for show:
``` python
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
```

<!-- ![image](https://user-images.githubusercontent.com/55779400/219014428-c3317c54-8adc-4b25-aa2e-6c4ac2fc850b.png) -->

### Task 6: Write Github_Montly_Report table:
- The goal of this task is to create a table with a summary of monthly GitHub activities.
- I use CTEs so there is no need to create more tables.

#### First I need to combile all the data of that month in github_daily_events table.

<!-- ![image](https://user-images.githubusercontent.com/55779400/219014481-f914d96e-79e7-4d75-b7a1-2ba19f9754dc.png) -->
```bigquery
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
```

- Use group by on repo_id column because it is an identifier.
- Get repo_id, the sum of stars, forks, and pushes for every day.
- Repo_name can be changed in that month, so I only get the repo_name on the last day.
- There is an easier way to get the first day of previous month in BigQuery like: `date_add(current_date(), INTERVAL -1 month)`. But if on `2023-02-02` I need to run task for testing on date `2023-02-01`, the `current_day()` will be changed to `'2023-02-02'`, not `'2023-02-01'`. Therefore, the `Where` statement would be `date >= '2023-01-02'`. This is wrong. Use `ds` instead of `current_day()` like <br>`date_add({'{{ ds }}'}, INTERVAL -1 month)`.
- It is better to use [airflow macros](https://airflow.apache.org/docs/apache-airflow/1.10.3/macros.html) to handle dates, not only in this query but also all dates in other queries. 

#### Next job is to get all story in Hacker News dataset with url is a github repo.

<!-- ![image](https://user-images.githubusercontent.com/55779400/219014534-12990de6-f57c-4edd-af51-7b86a688db9f.png) -->
```bigquery
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
```

- Get the url point to the repo_name of Github, title, and the score of these stories.
- There are many stories pointing to the same url. I only chose the one with the most score.
- Each story only appears in one row and the score is updated in that row (I guess, there is no more clarification on the score matter) so I ignore creation date of the event.
- Use REGEXP_EXTRACT only the the repo_name (cut off "htpps://" )
- Another way to get hn.id and hn.title is using two ANY_VALUE functions, but the result of ANY_VALUE function is nondeterministic not randomly(state in documentary). If there are two rows with max score, there will be no guarantee to get the id and title from the same row. Therefore, I use struct to make sure they are from the same row.

#### Join those two table together and create the monthly report table:

<!-- ![image](https://user-images.githubusercontent.com/55779400/219014587-bf2d8f59-bbb0-4f61-a942-0b3bd04d91d5.png) -->
``` bigquery
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
```
- Use LEFT JOIN cause I wanted to preserve all rows in github_agg. If repos don't have any Hacker News stories, the title and score will be null.
- Some repos are changed name so they don't match the url that Hacker News point to. The simplest way to solve this problem is to store repo_name as an array and `LEFT JOIN ON hn.url in gh.repo_name`.
- Since most of the repos that changed name are small ones,and they are not likely to have a story; for this project I leave it that way.

#### Whole task 6 code: 
```python
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
```

### Task 7: Check if the GIthub_Montly_Report_ is added to dataset:
<!-- ![image](https://user-images.githubusercontent.com/55779400/219014627-d0f4d59d-8166-4db1-902b-81b89f890cc8.png) -->
```python
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
```

### Task 8: Print result message:
<!-- ![image](https://user-images.githubusercontent.com/55779400/219014680-fa906919-4859-4503-9f4b-e99101377e09.png) -->
```python
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
```

- Use PythonOperator to check every state of other tasks.
- Depending on the state of each task print out the message, like in the code: 
### Task flow: 
```
    t1 >> t2 >> t3 >> t4 >> t6
    t1 >> t5 >> t6 >> t7 >> t8
    t4 >> t8
 ```
 
## Result: 
### Graph: Airflow task flow diagram:
![image](https://user-images.githubusercontent.com/55779400/218958413-aed328f0-0ac0-47b6-9db1-4dcc5f6ac187.png)

- I control the task flow by the trigger_rule, `"all_done"` for task2 - `"check_githubarchive_day"` in case task1 failed, and task8 - `"Print_result_of_github_ETL"`, `"all_success"` by default for other tasks.
- The reason of task4 - `"check_after_write_to_github_daily_events"` is upstream of task8 - `"Print_result_of_github_ETL"` is to wait for task4 to complete. If not task8 may run before task4 is completed and print an error message result.
- If task5 `"dummy_branch_task"` is a real task and took a long time to complete, there will be a need for task5 upstream of task8, in case task4 failed as explained above.

### Execution grid from 2023-01-02 to 2023-02-01 (2 Jan to 1 Feb):
<!-- ![image](https://user-images.githubusercontent.com/55779400/219010478-6b9a5566-c0cd-4807-928e-2766527bf766.png) -->
![image](https://user-images.githubusercontent.com/55779400/219011214-55c4ec20-9787-4d1e-bd17-877ebbd770c8.png)

- As expected, task1 only run on 01/02/2023, and on that day all tasks are completed successfully.
- Other days, only 3 tasks were completed successfully.

## Lessons learn after this project: 

- The task flow diagram and the execution grid are awkward.
- It is better to create two separated dags:
    - One for retrieving daily GitHub activities (run daily).
    - And the other is for creating monthly reports (run on the first day of each month)
- That way the successful run would be all task run successfully. Then we can receive emails on retries or failures.
- Cut the customed result message task, cause that is unnecessary. Now each dag has a single duty, there is only a need to receive successful dag_run emails.






## Reference: 
- This project is inspired by [Tuan Vu](https://www.youtube.com/@tuan-vu), who make this [airflow tutorial](https://www.youtube.com/watch?v=wAyu5BN3VpY&list=PLYizQ5FvN6pvIOcOd6dFZu3lQqc6zBGp2&index=6) on Youtube.
- The code of him is very old(4 years ago), so most of the code above is writed be me with some research through documentaries and code helping site like StackOverFlow. 
- There are some problems within the tutorial that I already handle as mention above, like:
    - He group by GitHub Archive with repo.name, leading to there are multiple rows of the same repo.ids. Which I handle above with repo name changing problem.
    - He retrieve data from Hacker News daily and filter by creation date, and get a sum score for an story. Which is not right for me. Cause when I check, each story contain in one rows, there is no need to sum. And if the story increase the score after that day, the score the collected can not be updated.
    - He did not use trigger_rule as well as check for state of the task, which I did(for this project, that not really shine but for others project when I need to check for state of some task of another dag, that would help a lots).
    - He did not mention anything about paritioned table, he just created one by default of airflow and write into each. Which is not possible now, BigQuery force user to create a paritioned table before write data into it (I can set airflow to create partitioned table if needed by specific the time-unit column, but that way there are some options I can not set, so I chose create it beforehand).
