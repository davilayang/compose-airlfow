"""Example DAG demonstrating the usage of the PigOperator."""

from airflow.models import DAG
from airflow.operators.pig_operator import PigOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_pig_operator',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)

run_this = PigOperator(
    task_id="run_example_pig_script",
    pig="ls /;",
    pig_opts="-x local",
    dag=dag,
)

run_this
