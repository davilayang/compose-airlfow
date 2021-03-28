"""
Example Airflow DAG that performs query in a Cloud SQL instance.

This DAG relies on the following OS environment variables

* GCP_PROJECT_ID - Google Cloud project for the Cloud SQL instance
* GCP_REGION - Google Cloud region where the database is created
*
* GCSQL_POSTGRES_INSTANCE_NAME - Name of the postgres Cloud SQL instance
* GCSQL_POSTGRES_USER - Name of the postgres database user
* GCSQL_POSTGRES_PASSWORD - Password of the postgres database user
* GCSQL_POSTGRES_PUBLIC_IP - Public IP of the Postgres database
* GCSQL_POSTGRES_PUBLIC_PORT - Port of the postgres database
*
"""

import google.auth
import os

from airflow import models

# from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExecuteQueryOperator
from operators.cloud_sql import CloudSQLXcomExecuteQueryOperator
from airflow.utils.dates import days_ago

credentials, project_id = google.auth.default()

SQL = [
    "CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)",
    # 'CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)',  # shows warnings logged
    "INSERT INTO TABLE_TEST VALUES (0)",
    # 'CREATE TABLE IF NOT EXISTS TABLE_TEST2 (I INTEGER)',
    "DROP TABLE TABLE_TEST",
    # 'DROP TABLE TABLE_TEST2',
    # "REASSIGN OWNED BY 'airflow-import' TO 'approval-api'"
]

# postgres_airflow_user

# [START howto_operator_cloudsql_query_connections]

postgres_kwargs = dict(
    user="airflow-import",
    password="TSfJMB0p4is6",  # how to give this? in connection
    public_port="5432",  # hard coded
    public_ip="35.240.83.217",  #  ipAddresses[0] => ipAddress, can retreive in first step
    project_id=project_id,
    location="europe-west1",  # "region"  can retreive in first step
    instance="apolitical-postgres-beta",  # "name"  can retreive in first step
    database="ab-testing",  # given as variable
)

# The connections below are created using one of the standard approaches - via environment
# variables named AIRFLOW_CONN_* . The connections can also be created in the database
# of AIRFLOW (using command line or UI).

# Postgres: connect via proxy over TCP
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_use_tcp=True".format(**postgres_kwargs)
)

# Postgres: connect via proxy over UNIX socket (specific proxy version)
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_SOCKET"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_version=v1.13&"
    "sql_proxy_use_tcp=False".format(**postgres_kwargs)
)

# [START howto_operator_cloudsql_query_operators]

connection_names = [
    "proxy_postgres_tcp",
    # "proxy_postgres_socket",
]

connection_name = connection_names[0]

with models.DAG(
    dag_id="example_gcp_sql_query",
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:
    task = CloudSQLXcomExecuteQueryOperator(
        task_id="example_gcp_sql_task_" + connection_name,
        sql=SQL,
        user="airflow-import",
        user_conn_id="postgres_airflow_user",
        public_port="5432",  # hard coded
        public_ip="35.240.83.217",  #  ipAddresses[0] => ipAddress, can retreive in first step
        project_id=project_id,
        location="europe-west1",  # "region"  can retreive in first step
        instance="apolitical-postgres-beta",  # "name"  can retreive in first step
        database="ab-testing",  # given as variable
    )

# [END howto_operator_cloudsql_query_operators]
