# Airflow Pineline BigQuery Github Acticvities (Monthly report)

## Dataset:
In this project I use 2 public dataset of google Bigquery: Github Archive and Hacker News
### Github Archive:
You can look at the dataset here: [GithubArchive](https://console.cloud.google.com/bigquery?project=githubarchive&page=project) - [More_Information](https://www.gharchive.org/)

- Github Archive is a project of capture every activity of all public projects in github. <br>
- It's have 3 dataset: Day, Month, Year <br>
- In this project I use dataset Day. <br>
- Each table in the Day dataset is a day and capture all activity of that day. Like below:<br>
![image](https://user-images.githubusercontent.com/55779400/218960697-6b4bde98-6f49-4533-8c9d-50ebb42c25a6.png)

- This is the schema of each table: <br>

![image](https://user-images.githubusercontent.com/55779400/218961000-62704aab-1df4-42d2-b8b4-ca0ea2d90ad0.png)

### Hacker News:
You can look at the dataset here: [HackerNews](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=hacker_news&page=dataset&project=apply-ds-test-371316) - [More_Information](https://github.com/HackerNews/API)

- Hacker News is a social news website focusing on computer science and entrepreneurship.
- This dataset contains a randomized sample of roughly one quarter of all stories and comments from Hacker News from its launch in 2006. 
- In this dataset I use table Full.
- Schema of the Full table: <br>![image](https://user-images.githubusercontent.com/55779400/218963958-297ecf3e-5b83-4b90-b7c2-49fd854d0118.png)

## Tasks:
### Task 1: Check first day of month
- Use simple DML of Bigquery to check if today if a first day of month.

![image](https://user-images.githubusercontent.com/55779400/218970128-6e64a5b5-daf0-4cf5-9b5d-4200c73372ef.png)

<!-- ![image](https://user-images.githubusercontent.com/55779400/218970013-25f7d59d-6347-461e-94a1-da6b4d678e35.png) -->

<!-- ![image](https://user-images.githubusercontent.com/55779400/218966517-39ee5bd4-1b2e-4bab-af6d-4900302cf32b.png)<br> -->

### Task 2: Check if there is a yesterday table in GithubArchive Day dataset
- Use INFORMATION_SCHEMA.TABLES filter table_name to only yesterday <br>
- If yesterday table is available, the query result is table_name (yesterday_ds_nodash like: 20230101)
- If there no yesterday table, the query not return anything.

More about INFORMATION_SCHEMA: [INFORMATION_SCHEMA](https://cloud.google.com/bigquery/docs/information-schema-intro)<br>

![image](https://user-images.githubusercontent.com/55779400/218970243-516fb562-81c0-44e2-b6e7-36ce87dedab7.png)

### Task 3: Write data to Github_Daily_Events table:
- Github_daily_events table stores data of every github repo.
- Each line is a date, repo_id, repo_name and it's stars, forks, pushes. Example of rows in table: <br>

![image](https://user-images.githubusercontent.com/55779400/218974771-59eae5a6-8c20-45e5-b9de-4db194dd62d1.png)

- The table is big for data 25.016.299 rows for 31 days in January 2023. So I partitioned it by date to reduce the cost of query on table.
- Ingestion time partitioning reduces stored size of table by putting date column to \_PARTITIONTIME(a pseudocolumn). 
- But when I plug table into Google Data Studio, the \_PARTITIONTIME didn't appear so I choose Time-unit column partitioning instead.
- Btw [PyPI package downloads](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=pypi&page=dataset&project=apply-ds-test-371316) table is Time-unit column paritioning too, each partition contains on average 700 mil rows.
- More info about [BigQuery Partitioned Table](https://cloud.google.com/bigquery/docs/partitioned-tables?_ga=2.103336576.-1647680310.1670343964)
- Query create table: <br>

![image](https://user-images.githubusercontent.com/55779400/218955332-b0a72d8f-edf2-47f9-865f-d607b102c04e.png)

- **Task 3 code:**

![image](https://user-images.githubusercontent.com/55779400/218998819-fb441eca-2dc8-4606-b89e-188421bbf740.png)

- Query GithubArchive day yesterday table, group by repo.id, using countif get stars, forks, and pushes of the repos that day.
- Repo.name can be changed, so there are many repo.names of the same repo.id. Even repo.name maybe unique and act as identifier or not, there is no guarantee, so group by must be done on repo.id not repo.name.
- For repo.name, I only chose the one appear last (max create_at) using [Aggregate functions of BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions) - ANY_VALUE.
- Write directly into the yesterday partition of table, and 'WRITE_TRUNCATE' in case they already have data so there is no duplicate rows.
- 'CREATE_NEVER' for create_disposition.

### Task 4: Check if the yesterday parition is Github_Daily_Events Table:
![image](https://user-images.githubusercontent.com/55779400/219002078-38f0a9b1-d77e-4efd-9adb-0a2fc4414640.png)

### Task 5: Branch Dummy task for show:
![image](https://user-images.githubusercontent.com/55779400/219002150-2e1f1206-d0b6-41f9-bbcb-ccf7b0bd755a.png)

### Task 6: Write Github_Montly_Report table:
- The goal of this task is to create a table with summary monthly github activities.
- I use CTEs so there is no need to create more tables.

#### First I need to combile all the data of that month in github_daily_events table.

![image](https://user-images.githubusercontent.com/55779400/219004561-9369cecf-888f-4d9b-b287-8563001bb297.png)

- Use group by on repo_id column because it an identifier.
- Get repo_id, sum of stars, forks, pushes for everday.
- Repo_name like before can be changed in that month, so I only get the repo_name on the last day.

#### Next job is to get all story in Hacker News dataset with url is a github repo.

![image](https://user-images.githubusercontent.com/55779400/219006303-df1620e7-b7e6-41ff-84d8-1275b392842e.png)

- Get the url point to the repo_name of Github, title, and the score of these stories.
- There are many stories point to the same url. I only chose the one with most score.
- Each story only appears in one row and the score is update in that row (I guess cause there no more clarificaiton on the score matter) so I ignore creation date of the event.
- Use REGEXP_EXTRACT only the the repo_name (cut of "htpps://" )

#### Join those two table together and create the monthly report table:

![image](https://user-images.githubusercontent.com/55779400/219008149-ac4dca3e-2d0c-47b3-8f6b-b77287c911fb.png)

### Task 7: Check if the GIthub_Montly_Report_ is added to dataset:
![image](https://user-images.githubusercontent.com/55779400/219008883-d5f5e8a5-b497-4815-89c1-971374802f56.png)

### Task 8: Print result message:
- Use PythonOperator to check every state of others task.
- Depend on state of each task print out message, like in the code: 

![image](https://user-images.githubusercontent.com/55779400/219009931-086a6745-7c4f-4d4f-9769-be4b5cb2b888.png)

## Result: 
### Graph: Airflow tasks diagram:
![image](https://user-images.githubusercontent.com/55779400/218958413-aed328f0-0ac0-47b6-9db1-4dcc5f6ac187.png)

### Exercution: 
<!-- ![image](https://user-images.githubusercontent.com/55779400/219010478-6b9a5566-c0cd-4807-928e-2766527bf766.png) -->
![image](https://user-images.githubusercontent.com/55779400/219011214-55c4ec20-9787-4d1e-bd17-877ebbd770c8.png)


## Lessons learn in project:

- The tasks diagram and the executoin are awkward.
- It is better to create two separated dags: One for retrive daily github activies(run daily). And the other for create monthly report(run at first day of each month)
- That way the successful run would be all task run successfully. Then we can recieve email on retries or failures.
- Cut the customed result message, cause that task is unecessary. Cause now each dags have a single purpose, there is only need to recieve successful dag run email.







## Reference: 
- This project is inspired by [Tuan Vu](https://www.youtube.com/@tuan-vu), who make this [airflow tutorial](https://www.youtube.com/watch?v=wAyu5BN3VpY&list=PLYizQ5FvN6pvIOcOd6dFZu3lQqc6zBGp2&index=6) on Youtube. 
- Big thank for him to help me learn Airflow and help me develop this project.
- The datasets used in this project are the same but the idea of this project is little different from the tutorial, and there are some errors in the tutorial that I will point out later.
- The code of tutorial is kind of old (3 years ago) so I need to read documentaries of BigQuery and some others to write most of the code.

