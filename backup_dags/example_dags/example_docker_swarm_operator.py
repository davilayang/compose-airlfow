
"""
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.contrib.operators.docker_swarm_operator import DockerSwarmOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'example_docker_swarm_operator',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False
)

with dag as dag:
    t1 = DockerSwarmOperator(
        api_version='auto',
        docker_url='tcp://localhost:2375', # Set your docker URL
        command='/bin/sleep 10',
        image='centos:latest',
        auto_remove=True,
        task_id='sleep_with_swarm',
    )
"""
