from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    "example_passing_params_via_test_command",
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
    },
    schedule_interval='*/1 * * * *',
    dagrun_timeout=timedelta(minutes=4),
    tags=['example']
)

# see macros: https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
# ds is the shorthand for execution_date
def my_py_command(ds, **kwargs):
    # Print out the "foo" param passed in via
    # `airflow test example_passing_params_via_test_command run_this <date>
    # -tp '{"foo":"bar"}'`
    if kwargs["test_mode"]: # or context["test_mode"]
        print(" 'foo' was passed in via test={} command : kwargs[params][foo] \
               = {}".format(kwargs["test_mode"], kwargs["params"]["foo"]))

    # Print out the value of "miff", passed in below via the Python Operator
    print(" 'miff' was passed in via task params = {}".format(kwargs["params"]["miff"]))
    # see logs of task

    return 1


# test_mode=True when it's called by command line
# 1. exec into worker
## docker exec -it docker-airflow_airflow-webserver_1 /bin/bash
# 2. run tasks test
## airflow tasks test -t '{"foo": "something"}' example_passing_params_via_test_command run_this 2021-02-28
# or, do this when triggering in webserver

my_templated_command = """
    echo " 'foo was passed in via Airflow CLI Test command with value {{ params.foo }} "
    echo " 'miff was passed in via BashOperator with value {{ params.miff }} "
"""

run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=my_py_command,
    params={"miff": "agg"}, # the params passing down by context to the callable
    dag=dag,
)

also_run_this = BashOperator(
    task_id='also_run_this',
    bash_command=my_templated_command,
    params={"miff": "agg"},
    dag=dag,
)

run_this >> also_run_this
