from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator

# TODO Connections & Variables
GCS_CONN_ID = "google_cloud_default"
SOURCE_BUCKET = "storage-datalake-injection"
FOLDER_ORG_USERS = "bronze"
USERS_PARQUET = "users.parquet"
FOLDER_ORG_PAYMENTS = "bronze"
PAYMENTS_PARQUET = "payments.parquet"
DST_PAYMENTS=f"prata/{PAYMENTS_PARQUET}"
DST_USERS=f"prata/{USERS_PARQUET}"


# TODO Default Arguments
default_args = {
    "owner": "Paulo Roberto Mesquita da Silva",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="analyze_parquet_duckdb",
    schedule="@daily",
    start_date=datetime(2021,12,1),
    catchup=False,
    tags=["Engenharia de Dados"],
    default_args=default_args,
)
def main():
    
    transform_file = GCSFileTransformOperator(
        task_id='transform_file',
        gcp_conn_id=GCS_CONN_ID,
        source_bucket=SOURCE_BUCKET,
        source_object=f"{FOLDER_ORG_USERS}/{USERS_PARQUET}",
        destination_bucket=SOURCE_BUCKET,
        destination_object=f"{DST_USERS}",
        transform_script=[
            'spark-submit',
            '--packages', 'org.apache.spark:spark-sql_2.12:3.1.2',  # Or your Spark version
            'dags/Engenharia_dados/functions/transformation.py']
    )
    
    transform_file
        
dag = main()