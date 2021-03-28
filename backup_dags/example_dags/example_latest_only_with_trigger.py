"""
Example LatestOnlyOperator and TriggerRule interactions
"""

# [START example]
import datetime as dt

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id='example_latest_only_with_trigger',
    schedule_interval=dt.timedelta(hours=4),
    start_date=days_ago(2),
    tags=['example']
)

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)
task1 = DummyOperator(task_id='task1', dag=dag)
task2 = DummyOperator(task_id='task2', dag=dag)
task3 = DummyOperator(task_id='task3', dag=dag)
task4 = DummyOperator(task_id='task4', dag=dag, trigger_rule=TriggerRule.ALL_DONE) 
# task will run only if all the previous tasks are done
# i.e. task1, task2 and latest_only

latest_only >> task1 >> [task3, task4]
task2 >> [task3, task4]
# [END example]
