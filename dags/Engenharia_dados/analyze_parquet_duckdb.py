from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow.operators.python import PythonOperator

# TODO Connections & Variables
GCS_CONN_ID = "google_cloud_default"
SOURCE_BUCKET = "storage-datalake-injection"
FOLDER_ORG_USERS = "bronze"
USERS_PARQUET = "users.parquet"
FOLDER_ORG_PAYMENTS = "bronze"
PAYMENTS_PARQUET = "payments.parquet"
DST_PAYMENTS=f"silver/{PAYMENTS_PARQUET}"
DST_USERS=f"silver/{USERS_PARQUET}"


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
    def show_variable():
        print(f"{FOLDER_ORG_USERS}/{USERS_PARQUET}")
        print(SOURCE_BUCKET)
    
    #TODO Task 1
    show = PythonOperator(
        task_id="show_variable",
        python_callable=show_variable
    )
    
    transform_task = GCSFileTransformOperator(
        task_id="transform_parquet",
        gcp_conn_id=GCS_CONN_ID,
        source_bucket=SOURCE_BUCKET,
        source_object=f"{FOLDER_ORG_USERS}/{USERS_PARQUET}",
        transform_script=[
            "python",
            "dags/Engenharia_dados/functions/transformation.py",
        ],
    )
    
    show >> transform_task
        
dag = main()