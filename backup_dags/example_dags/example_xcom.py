from __future__ import print_function

import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'provide_context': True,
}

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}


# https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
# ti as task_instance
def push(**kwargs): 
    """Pushes an XCom without a specific target"""
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)
    # push to context's task_instance
    # the xcom in the form of key-value pair


def push_by_returning(**kwargs):
    """Pushes an XCom without a specific target, just by returning it"""
    return value_2
    # also pushes to xcom
    # but returning it will also do, defult to key as "return_value"

def puller(**kwargs):
    # print to log
    logging.debug(kwargs)
    ti = kwargs['ti']

    # get value_1
    v1 = ti.xcom_pull(key=None, task_ids='push') # the key is actually 'value from pusher 1'
    assert v1 == value_1

    # get value_2
    v2 = ti.xcom_pull(task_ids='push_by_returning') # the key is actually 'return_value'
    assert v2 == value_2

    # get both value_1 and value_2
    v1, v2 = ti.xcom_pull(key=None, task_ids=['push', 'push_by_returning'])
    assert (v1, v2) == (value_1, value_2)


with DAG(
    'example_xcom', 
    schedule_interval=None, 
    default_args=args, 
    tags=['example']
) as dag:

    push1 = PythonOperator(
        task_id='push',
        python_callable=push,
    ) # see Task => Task Instance Details => Xcom

    push2 = PythonOperator(
        task_id='push_by_returning',
        python_callable=push_by_returning,
    ) # see Task => Task instance Details => None

    pull = PythonOperator(
        task_id='puller',
        python_callable=puller,
    )

    [push1, push2] >> pull
