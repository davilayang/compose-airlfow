"""
Example DAG demonstrating setting up inter-DAG dependencies using ExternalTaskSensor and
ExternalTaskMarker

In this example, child_task1 in example_external_task_marker_child depends on parent_task in
example_external_task_marker_parent. When parent_task is cleared with "Recursive" selected,
the presence of ExternalTaskMarker tells Airflow to clear child_task1 and its
downstream tasks.
"""

import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskMarker, ExternalTaskSensor

start_date = datetime.datetime(2015, 1, 1)

# in the webserver, these two DAGs are listed as: 
# example_external_task_marker_parent and example_external_task_marker_child

# the parent DAG
with DAG("example_external_task_marker_parent",
         start_date=start_date,
         schedule_interval=None) as parent_dag:
    # point to the child DAG with its DAG is and task id
    parent_task = ExternalTaskMarker(task_id="parent_task",
                                     external_dag_id="example_external_task_marker_child",
                                     external_task_id="child_task1")

# the child DAG
with DAG("example_external_task_marker_child", start_date=start_date, schedule_interval=None) as child_dag:
    # sensor to wait from another DAG
    child_task1 = ExternalTaskSensor(task_id="child_task1",
                                     external_dag_id=parent_dag.dag_id, # retrieve from the object
                                     external_task_id=parent_task.task_id,
                                     mode="reschedule")
                                     # when sensor's mode is set to reschedule
                                     # the sensor task frees the worker slot when the criteria is not met 
    
    child_task2 = DummyOperator(task_id="child_task2")
    child_task1 >> child_task2
