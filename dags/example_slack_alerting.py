""" example_slack_alerting.py

An example of using the Slack Alert airflow function with a simple single-task DAG.
It uses the PythonOperator to simulate a coin flip. If the coin flips "tails" it 
raises an exception, forcing a failed task Slack alert. If the coin flips "heads"
it passes and calls the succeess task Slack alert.

"""

import random
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# Connection ID for Slack callback (Webserver => Admin => Connections)
CONN_ID_SLACK = "slack_data_alerts"


def slack_alerting(task_id: str, message: str, context):

    """
    slack_alerting

    Execute the Slack Webhook Operator that sends a message to
    #data-alerts channel under user name "Airflow Alerting"

    INPUTS:
        task_id - str, id of the task, should be unique in the DAG

        message - str, message to send to Slack

        context - context of DAG's Task Instance

    """

    alerting = SlackWebhookOperator(
        task_id=task_id,
        http_conn_id=CONN_ID_SLACK,
        message=message,
        username="Airflow Alerting", # not necessary, specified by the webhook
        channel="data-alerts", # # not necessary, specified by the webhook
    )
    alerting.execute(context=context)


def slack_on_failure(context):

    """
    slack_on_failure

    Return a callback function which sends message to Slack channel when
    any of the task is failed

    INPUTS:
        context - context of DAG's Task Instance

    OUTPUTS:
        alerting - callback function

    """

    task_id = "slack_alerting_failure"

    ti = context.get("task_instance")
    message = (
        ":fearful: Task Failed! \n"
        f"- *Task* `{ti.task_id}` of *DAG* `{ti.dag_id}` \n"
        f"- *DAG Execution Time*: {context.get('execution_date')} \n"
        f"- <{ti.log_url}|*See Task Detail Logs*> \n"
    )

    alerting = slack_alerting(task_id, message, context)

    return alerting


def coin_flip():
    """
    This is the simple coin flip code that raises an exception "half the time"

    Args:
        None

    Returns:
        True only if coin flips "heads"

    Raises:
        ValueError: If coin flips "tails"

    """
    # flip = random.random() > 0.5
    flip = random.random() > 1.0
    if not flip:
        raise ValueError("Coin flipped tails. We lose!")
    print("Coin flipped heads. We win!")
    return True


# Default DAG arguments. Note the "onl_failure_callback"
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["foo@bar.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
    # Here we show an example of setting a failure callback applicable to all
    # tasks in the DAG
    "on_failure_callback": slack_on_failure,
}

# Create the DAG with the parameters and schedule
dag = DAG(
    "example_slack_alerting",
    start_date=days_ago(1),
    end_date=None,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    # on_success_callback=slack_on_dag_completion,
)

"""
Create a DAG with a single operator, "coin_flip" It's a simply python script 
that "flips a coin" and raises an error if it is "tails". That way the coin 
flip DAG sometimes succeeds and sometimes fails, which triggers either the 
success or fail slack callback functions
"""
with dag:

    t1 = DummyOperator(
        task_id="starting",
    )
    t2 = PythonOperator(
        task_id="coin_flip",
        python_callable=coin_flip,
    )

    t1 >> t2



