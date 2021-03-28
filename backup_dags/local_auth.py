# local_auth.py
# Set up for running the GCP operators on local machine

import os
import google

from airflow import models
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook

from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLBaseOperator,
    CloudSQLExportInstanceOperator,
    CloudSQLDeleteInstanceDatabaseOperator,
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLImportInstanceOperator,
)

from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSObjectCreateAclEntryOperator,
)

# typings
from pendulum import Pendulum
from typing import Dict, Optional, Union, Sequence

credentials, project_id = google.auth.default()

# GCP Project ID
# PROJECT_ID = os.environ["PROJECT_ID"] # KeyError if not set
GCP_PROJECT_ID = project_id

# GCS Bucket
GCS_BUCKET = os.environ["GCS_BUCKET"]
# GCS_BUCKET = "apol-postgres-backup"

# Cloud SQL instances & its service account
LIVE_INSTANCE_NAME = os.environ["LIVE_INSTANCE_NAME"]
RC_INSTANCE_NAME = os.environ["RC_INSTANCE_NAME"]
BETA_INSTANCE_NAME = os.environ["BETA_INSTANCE_NAME"]

# databases on Live to be exported & imported on Beta & RC
databases = ["ab-testing", "bucketeer-training"]

### Utility functions ###


def get_dump_uri(ts: Pendulum, db: str, bucket: str = GCS_BUCKET) -> str:

    """
    get_dump_uri

    Return URI of a SQL database dump on the GCS Bucket

    INPUTS:
        ts - Pendulum, timestamp for next DAG execution date

        db - str, database on Cloud SQL Live instance

    KEYWORDS:
        bucket = GCS_BUCKET - GCS bucket that stores the exported dumps
                              of Live Cloud SQL instance

    OUTPUTS:
        dump_uri - str, URI to a exported SQL dump on GCS bucket

    """

    year = ts.strftime("%Y")
    month = ts.strftime("%m")
    file = ts.strftime(f"%Y-%m-%dT%H:%M:%S%z_{db}.sql")

    dump_uri = f"gs://{bucket}/{year}/{month}/{file}"

    return dump_uri


def get_export_body(db: str) -> Dict[str, dict]:

    """
    get_export_body

    Return the request body for exporting a database SQL dump to GCS Bucket

    INPUTS:
        db - str, name of database on Live instance to be exported

    OUTPUTS:
        export_body - dict, request body for SQL Admin API to export
                      database dump

    """

    export_body = {
        "exportContext": {
            "uri": f"{{{{ task_instance.xcom_pull('prepare_{db}_dump_uri', key='sql_dump_uri') }}}}",
            "databases": [db],  # only one allowed
            "fileType": "SQL",
            "sqlExportOptions": {"schemaOnly": False},
        }
    }
    return export_body


def get_import_body(db: str, db_user: str = "airflow-import") -> Dict[str, dict]:

    """
    get_import_body

    Return the request body for importing a database with a SQL dump from
    GCS Bucket

    N.B. "importUser" is the user to execute the commands in a SQL dump,
    default is "cloudsqlsuperuser". The user needs to be granted
    membership of other roles to run "ALTER DEFAULT ..." clauses

    INPUTS:
        db - str, name of database to be imported

    KEYWORDS:
        db_user = "airflow-import" - the special database user on RC and
                                     BETA created for importing SQL dumps.
                                     It has membership of all other
                                     database roles

    OUTPUTS:
        import_body - dict, request body for SQL Admin API to import
                      database dump

    """

    import_body = {
        "importContext": {
            "uri": f"{{{{ task_instance.xcom_pull('prepare_{db}_dump_uri', key='sql_dump_uri') }}}}",
            "database": db,
            "fileType": "SQL",
            "importUser": db_user,
        }
    }

    return import_body


def get_create_db_body(cloud_sql_instance: str, db: str) -> Dict[str, str]:

    """
    get_create_db_body

    Return the request body for creating a database in a Cloud SQL instance

    INPUTS:
        cloud_sql_instance - str, instance ID on Cloud SQL

        db - str, name of database to be created

    OUTPUTS:
        create_body - dict, request body for SQL Admin API to create
                      a database

    """

    create_body = {
        "instance": cloud_sql_instance,
        "name": db,
        "project": GCP_PROJECT_ID,
    }

    return create_body


def prepare_sql_dump_uri(db, **context):

    """
    prepare_sql_dump_uri

    Push xcom messages of SQL dump URI and its file name to message
    broker of Airflow pipeline

    INPUTS:
        db - str, name of database

        **context - context of a Airflow task instance

    """

    # TODO: change to next_execution_date in deployment
    sql_dump_uri = get_dump_uri(context.get("execution_date"), db)
    sql_dump = sql_dump_uri.split("/", 3)[-1]  # without gs://bucket/

    context["ti"].xcom_push(key="sql_dump_uri", value=sql_dump_uri)
    context["ti"].xcom_push(key="sql_dump", value=sql_dump)


# https://github.com/apache/airflow/blob/master/airflow/providers/google/cloud/operators/cloud_sql.py#L265
class CloudSQLInstanceServiceAccountOperator(CloudSQLBaseOperator):

    @apply_defaults
    def __init__(
        self,
        instance: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v1beta4',
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            project_id=project_id,
            instance=instance,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def execute(self, context) -> None:
        hook = CloudSQLHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        instance_resource = hook.get_instance(project_id=self.project_id, instance=self.instance)
        service_account_email = instance_resource["serviceAccountEmailAddress"]
        task_instance = context['task_instance']
        task_instance.xcom_push(key="service_account_email", value=service_account_email)


### DAG ###

with models.DAG(
    "local_auth_to_GCP",
    schedule_interval=None,  # TODO: change in deployment
    start_date=days_ago(1),
    tags=["database", "postgres", "backup", "restore"],
) as dag:

    get_live_service_account = CloudSQLInstanceServiceAccountOperator(
        task_id="get_live_service_account",
        instance=LIVE_INSTANCE_NAME,
    )

    get_beta_service_account = CloudSQLInstanceServiceAccountOperator(
        task_id="get_beta_service_account",
        instance=BETA_INSTANCE_NAME,
    )
    get_rc_service_account = CloudSQLInstanceServiceAccountOperator(
        task_id="get_rc_service_account",
        instance=RC_INSTANCE_NAME,
    )

    # authorize Cloud SQL service account access to bucket
    grant_live_access_to_bucket = GCSBucketCreateAclEntryOperator(
        task_id="grant_live_access_to_bucket",
        entity="user-{{ ti.xcom_pull('get_live_service_account', key='service_account_email') }}",
        role="WRITER",
        bucket=GCS_BUCKET,
    )
    grant_beta_access_to_bucket = GCSBucketCreateAclEntryOperator(
        task_id="grant_beta_access_to_bucket",
        entity="user-{{ ti.xcom_pull('get_beta_service_account', key='service_account_email') }}",
        role="WRITER",
        bucket=GCS_BUCKET,
    )
    grant_rc_access_to_bucket = GCSBucketCreateAclEntryOperator(
        task_id="grant_rc_access_to_bucket",
        entity="user-{{ ti.xcom_pull('get_rc_service_account', key='service_account_email') }}",
        role="WRITER",
        bucket=GCS_BUCKET,
        retries=3, 
    )

    ready_for_db_workflow = DummyOperator(task_id="ready_for_db_workflow")

    (
        [
            get_live_service_account >> grant_live_access_to_bucket,
            get_rc_service_account >> grant_rc_access_to_bucket,
            get_beta_service_account >> grant_beta_access_to_bucket,
        ]
        >> ready_for_db_workflow
    )

    # for db in databases:

    #     # prepare the dump URI
    #     prepare_db_dump_uri = PythonOperator(
    #         task_id=f"prepare_{db}_dump_uri",
    #         python_callable=prepare_sql_dump_uri,
    #         op_args=[db],
    #         provide_context=True,
    #     )

    #     # export databases (from live)
    #     export_live_db = CloudSQLExportInstanceOperator(
    #         task_id=f"export_live_db_{db}",
    #         body=get_export_body(db),
    #         instance=LIVE_INSTANCE_NAME,
    #     )

    #     # authorize beta/rc service account to dump object
    #     grant_beta_access_to_dump_object = GCSObjectCreateAclEntryOperator(
    #         task_id=f"grant_beta_access_to_{db}_dump",
    #         entity=f"user-{BETA_SERVICE_ACCOUNT}",
    #         role="READER",
    #         bucket=GCS_BUCKET,
    #         object_name=f"{{{{ task_instance.xcom_pull('prepare_{db}_dump_uri', key='sql_dump') }}}}",
    #     )
    #     grant_rc_access_to_dump_object = GCSObjectCreateAclEntryOperator(
    #         task_id=f"grant_rc_access_to_{db}_dump",
    #         entity=f"user-{RC_SERVICE_ACCOUNT}",
    #         role="READER",
    #         bucket=GCS_BUCKET,
    #         object_name=f"{{{{ task_instance.xcom_pull('prepare_{db}_dump_uri', key='sql_dump') }}}}",
    #     )

    #     # delete databases (beta and rc)
    #     delete_beta_db = CloudSQLDeleteInstanceDatabaseOperator(
    #         task_id=f"delete_beta_db_{db}",
    #         database=db,
    #         instance=BETA_INSTANCE_NAME,
    #     )
    #     delete_rc_db = CloudSQLDeleteInstanceDatabaseOperator(
    #         task_id=f"delete_rc_db_{db}",
    #         database=db,
    #         instance=RC_INSTANCE_NAME,
    #     )

    #     # create databases (beta and rc)
    #     create_beta_db = CloudSQLCreateInstanceDatabaseOperator(
    #         task_id=f"create_beta_db_{db}",
    #         body=get_create_db_body(BETA_INSTANCE_NAME, db),
    #         instance=BETA_INSTANCE_NAME,
    #     )
    #     create_rc_db = CloudSQLCreateInstanceDatabaseOperator(
    #         task_id=f"create_rc_db_{db}",
    #         body=get_create_db_body(RC_INSTANCE_NAME, db),
    #         instance=RC_INSTANCE_NAME,
    #     )

    #     # import from dump
    #     import_beta_db = CloudSQLImportInstanceOperator(
    #         task_id=f"import_beta_{db}",
    #         body=get_import_body(db),
    #         instance=BETA_INSTANCE_NAME,
    #     )
    #     import_rc_db = CloudSQLImportInstanceOperator(
    #         task_id=f"import_rc_{db}",
    #         body=get_import_body(db),
    #         instance=RC_INSTANCE_NAME,
    #     )

    #     # preparations
    #     (ready_for_db_workflow >> prepare_db_dump_uri)
    #     # beta workflow tasks dependencies
    #     (
    #         prepare_db_dump_uri
    #         >> export_live_db
    #         >> grant_beta_access_to_dump_object
    #         >> delete_beta_db
    #         >> create_beta_db
    #         >> import_beta_db
    #     )
    #     # rc workflow tasks dependencies
    #     (
    #         prepare_db_dump_uri
    #         >> export_live_db
    #         >> grant_rc_access_to_dump_object
    #         >> delete_rc_db
    #         >> create_rc_db
    #         >> import_rc_db
    #     )
