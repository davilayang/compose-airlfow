"""Example DAG demonstrating the DummyOperator and a custom DummySkipOperator which skips by default."""

from airflow.exceptions import AirflowSkipException
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}


# Create some placeholder operators
class DummySkipOperator(DummyOperator):
    ui_color = '#e8b7e4'

    def execute(self, context):
        raise AirflowSkipException


# defined as a function
# not need to set return values
def create_test_pipeline(suffix, trigger_rule, dag):
    skip_operator = DummySkipOperator(task_id='skip_operator_{}'.format(suffix), dag=dag)
    always_true = DummyOperator(task_id='always_true_{}'.format(suffix), dag=dag)
    join = DummyOperator(task_id=trigger_rule, dag=dag, trigger_rule=trigger_rule)
    final = DummyOperator(task_id='final_{}'.format(suffix), dag=dag)

    skip_operator >> join
    always_true >> join
    join >> final


dag = DAG(dag_id='example_skip_dag', default_args=args, tags=['example'])
create_test_pipeline('1', 'all_success', dag)
create_test_pipeline('2', 'one_success', dag)
