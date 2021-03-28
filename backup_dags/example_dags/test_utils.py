"""Used for unit tests"""
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

dag = DAG(dag_id='test_utils', schedule_interval=None, tags=['example'])

task = BashOperator(
    task_id='sleeps_forever',
    dag=dag,
    bash_command="sleep 10000000000",
    start_date=days_ago(2),
    owner='airflow',
)
