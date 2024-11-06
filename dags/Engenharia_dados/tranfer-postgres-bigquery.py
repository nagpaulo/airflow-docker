import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from contextlib import closing
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


load_dotenv()

default_args = {
    'owner': 'Paulo Roberto Mesquita da Silva',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

GCS_CONN_ID = "google_cloud_default"
DATASET_NAME = "datalake"
TABLE_NAME = "drivers"
SOURCE_BUCKET = "storage-datalake-injection"

@dag(
    dag_id='tranfer-postgres-bigquery',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 10, 20),
    catchup=False,
    tags=['hook', 'postgres', 'extract', 'Engenharia de Dados']
)
def tranfer_postgres_extract_to_bigquery():
    @task()
    def extract_drivers():
        try:
            hook = PostgresHook(postgres_conn_id='postgres_default')

            sql = """
                SELECT 
                    driver_id,
                    uuid,
                    first_name,
                    last_name,
                    date_birth::text,
                    city,
                    country,
                    phone_number,
                    license_number,
                    vehicle_type,
                    vehicle_make,
                    vehicle_model,
                    vehicle_year,
                    vehicle_license_plate,
                    cpf,
                    dt_current_timestamp::text
                FROM public.drivers
                LIMIT 10;
            """

            with closing(hook.get_conn()) as conn:
                df_drivers = pd.read_sql_query(sql, conn)

                print(f"Successfully extracted {len(df_drivers)} records from drivers table")
                if not df_drivers.empty:
                    print("Columns:", df_drivers.columns.tolist())
                    print("First record sample:", df_drivers.iloc[0].to_dict())

                return df_drivers

        except Exception as e:
            print(f"Error extracting drivers data: {str(e)}")
            raise
        
    create_test_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME
    )
    
    load_paquet = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_example',
        bucket=SOURCE_BUCKET,
        gcp_conn_id=GCS_CONN_ID,
        create_disposition="CREATE_IF_NEEDED",
        source_format="PARQUET",
        source_objects=['bronze/drivers.parquet'],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition='WRITE_TRUNCATE'
    )
    
        
    extract_drivers() >> create_test_dataset >> load_paquet
        
dados = tranfer_postgres_extract_to_bigquery()