"""Example DAG demonstrating the usage of the TaskGroup."""
# https://airflow.apache.org/docs/apache-airflow/stable/_modules/airflow/example_dags/example_task_group.html
# https://github.com/apache/airflow/tree/master/airflow/utils
# https://github.com/apache/airflow/blob/master/airflow/utils/task_group.py

from airflow.models.dag import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# [START howto_task_group]
with DAG(dag_id="example_task_group", start_date=days_ago(2), tags=["example"]) as dag:
    start = DummyOperator(task_id="start")

    # [START howto_task_group_section_1]
    with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
        task_1 = DummyOperator(task_id="task_1")
        task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
        task_3 = DummyOperator(task_id="task_3")

        task_1 >> [task_2, task_3]
    # [END howto_task_group_section_1]

    # [START howto_task_group_section_2]
    with TaskGroup("section_2", tooltip="Tasks for section_2") as section_2:
        task_1 = DummyOperator(task_id="task_1")

        # [START howto_task_group_inner_section_2]
        with TaskGroup("inner_section_2", tooltip="Tasks for inner_section2") as inner_section_2:
            task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
            task_3 = DummyOperator(task_id="task_3")
            task_4 = DummyOperator(task_id="task_4")

            [task_2, task_3] >> task_4
        # [END howto_task_group_inner_section_2]

    # [END howto_task_group_section_2]

    end = DummyOperator(task_id='end')

    start >> section_1 >> section_2 >> end
# [END howto_task_group]