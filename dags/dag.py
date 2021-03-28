

from utils import utils
from airflow import models
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExportInstanceOperator,
    CloudSQLDeleteInstanceDatabaseOperator,
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLImportInstanceOperator,
    CloudSQLExecuteQueryOperator,
)

from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSObjectCreateAclEntryOperator,
)

# source instance
LIVE_INSTANCE_NAME = "apolitical-postgres-live"

# target instances
RC_INSTANCE_NAME = "apolitical-postgres-rc"
BETA_INSTANCE_NAME = "apolitical-postgres-beta"


databases = ["ab-testing", "article-recommendations", "bucketeer-training"]
import_users=["ab-testing"]

with models.DAG(
    "database_backup_restore",
    schedule_interval="@daily",  # Override to match your needs
    start_date=days_ago(1),
    tags=["database", "postgres"],
) as dag:


    # need an operator to push the service_account to xcom
    # or load from environment variable
    sa_beta = "p760321716721-opbrfg@gcp-sa-cloud-sql.iam.gserviceaccount.com"
    sa_rc = "p760321716721-wxasm5@gcp-sa-cloud-sql.iam.gserviceaccount.com"

    # authorize bucket access to beta/rc service account
    access_bucket_beta = GCSBucketCreateAclEntryOperator(
        entity=f"user-{sa_beta}",
        role="WRITER",
        bucket="apol-postgres-backup",
        task_id='authorize_beta_gcs_bucket_permission',
    )
    access_bucket_rc = GCSBucketCreateAclEntryOperator(
        entity=f"user-{sa_rc}",
        role="WRITER",
        bucket="apol-postgres-backup",
        task_id='authorize_rc_gcs_bucket_permission',
    )

    for db in databases: 
        sql_dump_uri = utils.get_dump_uri("{{ next_execution_date }}", db)
        sql_dump = sql_dump_uri.split("/", 2)[2]

        # export databases (live)
        export = CloudSQLExportInstanceOperator(
            task_id=f"export_live_{db}",
            body=utils.get_export_body(db, sql_dump_uri),
            instance=LIVE_INSTANCE_NAME,
        )

        # authorize access to dump object (beta and rc)
        access_object_beta = GCSObjectCreateAclEntryOperator(
            task_id='authorize_beta_gcs_object_permission',
            entity=f"user-{sa_beta}",
            role="READER",
            bucket="apol-postgres-backup", 
            object_name=sql_dump,
        )
        access_object_rc = GCSObjectCreateAclEntryOperator(
            task_id='authorize_rc_gcs_object_permission',
            entity=f"user-{sa_rc}",
            role="READER",
            bucket="apol-postgres-backup", 
            object_name=sql_dump,
        )

        # delete databases (beta and rc)
        delete_beta = CloudSQLDeleteInstanceDatabaseOperator(
            task_id=f"delete_beta_{db}", 
            database=db,
            instance=BETA_INSTANCE_NAME,
        )
        delete_rc = CloudSQLDeleteInstanceDatabaseOperator(
            task_id=f"delete_rc_{db}", 
            database=db,
            instance=RC_INSTANCE_NAME,
        )

        # create databases (beta and rc)
        create_beta = CloudSQLCreateInstanceDatabaseOperator(
            task_id=f"create_beta_{db}", 
            body=utils.get_create_db_body(BETA_INSTANCE_NAME, db),
            instance=BETA_INSTANCE_NAME,
        )
        create_rc =  CloudSQLCreateInstanceDatabaseOperator(
            task_id=f"create_rc_{db}", 
            body=utils.get_create_db_body(RC_INSTANCE_NAME, db),
            instance=RC_INSTANCE_NAME,
        )

        # import from dump
        import_beta = CloudSQLImportInstanceOperator(
            task_id=f"import_beta_{db}",
            body=utils.get_import_body(db, "data-analysis", sql_dump_uri),
            instance=BETA_INSTANCE_NAME,
        )
        import_rc = CloudSQLImportInstanceOperator(
            task_id=f"import_rc_{db}",
            body=utils.get_import_body(db, "data-analysis", sql_dump_uri),
            instance=RC_INSTANCE_NAME,
        )

        # beta workflow tasks dependencies
        (access_bucket_beta >> delete_beta)
        (
            export 
            >> access_object_beta
            >> delete_beta
            >> create_beta
            >> import_beta
        )
        # rc workflow tasks dependencies
        (access_bucket_rc >> delete_rc)
        (
            export 
            >> access_object_rc
            >> delete_rc
            >> create_rc
            >> import_rc
        )


