"""
Example of the LatestOnlyOperator
"""
import datetime as dt

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id='example_latest_only',
    schedule_interval=dt.timedelta(hours=4),
    start_date=days_ago(2),
    tags=['example']
)

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)
task1 = DummyOperator(task_id='task1', dag=dag)

latest_only >> task1
# when triggered, only the most reqcent schedule of task_1 will run
# but latest_only will always run 
