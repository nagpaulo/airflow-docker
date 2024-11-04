"""
gcloud compute firewall-rules create allow-internal-ingress \
    --network=default \
    --source-ranges=10.128.0.0/20 \
    --destination-ranges=10.128.0.0/20 \
    --direction=ingress \
    --action=allow \
    --rules=all

gcloud compute networks subnets update default \
    --region=us-east1 \
    --enable-private-ip-google-access
"""

import uuid
from airflow.decorators import dag, task_group
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain

# TODO GCP configurations
PROJECT_ID = "silver-charmer-243611"
REGION = "us-east1"
GCS_BUCKET = "owshq-airbyte-ingestion"
PAYMENTS_PATH = "mongodb-atlas/payments/*"
PROCESSED_PATH = "processed/metrics/"
USERS_PATH = "mongodb-atlas/users/*"
BQ_DATASET = "Analytics"
BQ_TABLE = "metrics"
SERVERLESS_JOB_NAME = "serverless-batch-etl-metrics"
PYTHON_FILE_LOCATION = f"gs://{GCS_BUCKET}/scripts/users_payments_analytics.py"


@dag(
    dag_id='gcp-gcs-pyspark-bq-analysis',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['gcp', 'payments', 'users', 'pyspark', 'analytics'],
    default_args={
        'owner': 'airflow',
        'retries': 1
    }
)
def pyspark_analytics_serverless():
    @task_group(group_id='Ingestion')
    def ingest_data():
        """Ingest payment and user data from GCS"""

        check_payment_files = GCSObjectsWithPrefixExistenceSensor(
            task_id='check_payment_files',
            bucket=GCS_BUCKET,
            prefix=PAYMENTS_PATH[:-2],
            deferrable=True,
            poke_interval=600
        )

        check_user_files = GCSObjectsWithPrefixExistenceSensor(
            task_id='check_user_files',
            bucket=GCS_BUCKET,
            prefix=USERS_PATH[:-2],
            deferrable=True,
            poke_interval=600
        )

        return [check_payment_files, check_user_files]

    @task_group(group_id='Transform')
    def process_data():
        """Process payment and user data using Dataproc Serverless Batch"""

        create_batch = DataprocCreateBatchOperator(
            task_id="create_spark_serverless_batch",
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": PYTHON_FILE_LOCATION,
                    "args": [
                        f"--payment_files=gs://{GCS_BUCKET}/{PAYMENTS_PATH}",
                        f"--user_files=gs://{GCS_BUCKET}/{USERS_PATH}",
                        f"--output_path=gs://{GCS_BUCKET}/{PROCESSED_PATH}"
                    ]
                },
                "environment_config": {
                    "execution_config": {
                        "subnetwork_uri": "default"
                    }
                }
            },
            region=REGION,
            project_id=PROJECT_ID,
            batch_id=f"{SERVERLESS_JOB_NAME}-{uuid.uuid4().hex[-2:]}"
        )

        return create_batch

    @task_group(group_id='Load')
    def load_to_bigquery():
        """Load processed data to BigQuery"""

        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_dataset',
            dataset_id=BQ_DATASET,
            project_id=PROJECT_ID,
            location=REGION,
            exists_ok=True
        )

        create_bq_table = BigQueryCreateEmptyTableOperator(
            task_id='create_bq_table',
            project_id=PROJECT_ID,
            dataset_id=BQ_DATASET,
            table_id=BQ_TABLE,
            schema_fields=[
                {'name': 'payment_date', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                {'name': 'country', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'payment_method', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'currency', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
                {'name': 'total_users', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                {'name': 'success_rate', 'type': 'FLOAT', 'mode': 'REQUIRED'},
                {'name': 'processed_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
            ]
        )

        return chain(create_dataset, create_bq_table)

    ingest_data_task = ingest_data()
    processed_data_task = process_data()
    loaded_data_task = load_to_bigquery()

    chain(ingest_data_task, processed_data_task, loaded_data_task)


dag = pyspark_analytics_serverless()
