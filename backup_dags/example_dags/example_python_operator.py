from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_python_operator',
    default_args=args,
    schedule_interval=None, # only triggered manually
    tags=['example']
)


def print_context(ds, **kwargs): # or (ds, **context)
    # see Task log for printed context 
    pprint(kwargs) 
    print(ds)
    return 'Whatever you return gets printed in the logs'


print_the_context = PythonOperator(
    task_id='print_the_context',
    provide_context=True, # making context available to callable
    python_callable=print_context,
    dag=dag,
)


def my_sleeping_function(random_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)


# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
for i in range(5):
    task = PythonOperator(
        task_id='sleep_for_' + str(i),
        python_callable=my_sleeping_function,
        op_kwargs={'random_base': float(i) / 10}, # task "Rendered Template"
        dag=dag,
    )

    print_the_context >> task
