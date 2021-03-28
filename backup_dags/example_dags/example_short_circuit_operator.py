import airflow.utils.helpers
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator 
# the operator only returns True or False
# True, downstream tasks are run 
# False, downstream tasks are skipped

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(dag_id='example_short_circuit_operator', default_args=args, tags=['example'])

cond_true = ShortCircuitOperator(
    task_id='condition_is_True',
    python_callable=lambda: True, # always True
    dag=dag,
)

cond_false = ShortCircuitOperator(
    task_id='condition_is_False',
    python_callable=lambda: False, # always False
    dag=dag,
)

ds_true = [DummyOperator(task_id='true_' + str(i), dag=dag) for i in [1, 2]]
ds_false = [DummyOperator(task_id='false_' + str(i), dag=dag) for i in [1, 2]]

# using utils to chain the tasks
# same as cond_true >> ds_true[0] >> ds_true[1]
airflow.utils.helpers.chain(cond_true, *ds_true)

airflow.utils.helpers.chain(cond_false, *ds_false)
