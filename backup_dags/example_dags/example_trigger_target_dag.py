import pprint

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

pp = pprint.PrettyPrinter(indent=4)

# This example illustrates the use of the TriggerDagRunOperator. There are 2
# entities at work in this scenario:
# 1. The Controller DAG - the DAG that conditionally executes the trigger
#    (in example_trigger_controller.py)
# 2. The Target DAG - DAG being triggered
#
# This example illustrates the following features :
# 1. A TriggerDagRunOperator that takes:
#   a. A python callable that decides whether or not to trigger the Target DAG
#   b. An optional params dict passed to the python callable to help in
#      evaluating whether or not to trigger the Target DAG
#   c. The id (name) of the Target DAG
#   d. The python callable can add contextual info to the DagRun created by
#      way of adding a Pickleable payload (e.g. dictionary of primitives). This
#      state is then made available to the TargetDag
# 2. A Target DAG : c.f. example_trigger_target_dag.py

dag = DAG(
    dag_id="example_trigger_target_dag",
    default_args={"start_date": days_ago(2), "owner": "Airflow"},
    schedule_interval=None,
    tags=['example']
)


def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag,
)

# You can also access the DagRun object in templates
bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Here is the message: $message"',
    env={'message': '{{ dag_run.conf["message"] if dag_run else "" }}'}, # templating
    dag=dag,
)
