## Reference State check airlfow:
https://stackoverflow.com/questions/43732642/status-of-airflow-task-within-the-dag

I am doing something similar. I need to check for one task if the previous 10 runs of another task were successful. taky2 sent me on the right path. It is actually fairly easy:
```
from airflow.models import TaskInstance
ti = TaskInstance(*your_task*, execution_date)
state = ti.current_state()
```

As I want to check that within the dag, it is not neccessary to specify the dag. I simply created a function to loop through the past n_days and check the status.
```

def check_status(**kwargs):
    last_n_days = 10
    for n in range(0,last_n_days):
        date = kwargs['execution_date']- timedelta(n)
        ti = TaskInstance(*my_task*, date) #my_task is the task you defined within the DAG rather than the task_id (as in the example below: check_success_task rather than 'check_success_days_before') 
        state = ti.current_state()
        if state != 'success':
            raise ValueError('Not all previous tasks successfully completed.')
```

When you call the function make sure to set provide_context.

```
check_success_task = PythonOperator(
    task_id='check_success_days_before',
    python_callable= check_status,
    provide_context=True,
    dag=dag
)
```

UPDATE: When you want to call a task from another dag, you need to call it like this:

```
from airflow import configuration as conf
from airflow.models import DagBag, TaskInstance

dag_folder = conf.get('core','DAGS_FOLDER')
dagbag = DagBag(dag_folder)
check_dag = dagbag.dags[*my_dag_id*]
my_task = check_dag.get_task(*my_task_id*)
ti = TaskInstance(my_task, date)
```

Apparently there is also an api-call by now doing the same thing:

```
from airflow.api.common.experimental.get_task_instance import get_task_instance
ti = get_task_instance(*my_dag_id*, *my_task_id*, date)
```

