""""""
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from bson import json_util 
from dotenv import load_dotenv
from datetime import datetime, timedelta
from contextlib import closing
from airflow.decorators import dag, task_group, task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

load_dotenv()

GCS_CONN_ID = "google_cloud_default"
SOURCE_BUCKET = "storage-datalake-injection"
DESTINATION_OBJECT = "file"
DATASET_NAME = "datalake"
DRIVERS_PARQUET = "drivers.parquet"
RESTAURANTS_PARQUET = "restaurants.parquet"
TABLE_NAME_RESTAURANTS = "restaurants"
DST_RESTAURANTS=f"bronze/{RESTAURANTS_PARQUET}"
TABLE_NAME_DRIVERS = "drivers"
DST_DRIVERS=f"bronze/{DRIVERS_PARQUET}"                               
TABLE_DRIVERS = f"{DATASET_NAME}.{TABLE_NAME_DRIVERS}"
TABLE_RESTAURANTS = f"{DATASET_NAME}.{TABLE_NAME_RESTAURANTS}"

default_args = {
    'owner': 'Paulo Roberto Mesquita da Silva',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="get-data-postgres-upload-paquet",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["transform", "google", "bucket", "Engenharia de Dados"],
)
def get_data_postgres_create_paquet():
    
    @task(task_id="extract_driver")
    def extract_driver():
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
                FROM public.drivers;
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
        
    @task(task_id="extract_restaurants")
    def extract_restaurants():
        try:
            hook = PostgresHook(postgres_conn_id='postgres_default')

            sql = """
                SELECT 
                    restaurant_id, 
                    uuid, 
                    name, 
                    address, 
                    city, 
                    country, 
                    phone_number, 
                    cuisine_type, 
                    opening_time, 
                    closing_time, 
                    average_rating, 
                    num_reviews, 
                    cnpj, 
                    dt_current_timestamp
                FROM public.restaurants;
            """

            with closing(hook.get_conn()) as conn:
                df_restaurants = pd.read_sql_query(sql, conn)

                print(f"Successfully extracted {len(df_restaurants)} records from restaurants table")
                if not df_restaurants.empty:
                    print("Columns:", df_restaurants.columns.tolist())
                    print("First record sample:", df_restaurants.iloc[0].to_dict())

                return df_restaurants
            
        except Exception as e:
            print(f"Error extracting restaurants data: {str(e)}")
            raise
    
    def gerar_paquet(id, df, file):    
        @task(task_id=f"gerar_paquet_{id}", retries=2)
        def converter_df_paquet(df, file):
            # Convertendo o DataFrame para um Table do PyArrow
            table = pa.Table.from_pandas(df)

            # Escrevendo o Table como um arquivo Parquet
            return pq.write_table(table, file)
        
        return converter_df_paquet(df, file)
    
    
    @task_group(group_id='Ingestion')
    def injection_datalake(drivers, restaurants):
        file_paquet_drivers = gerar_paquet("drivers", drivers, f"{DRIVERS_PARQUET}")
        file_paquet_restaurants = gerar_paquet("restaurants", restaurants, f"{RESTAURANTS_PARQUET}")
    
    @task_group(group_id='Upload')
    def upload():
        upload_file_drivers = LocalFilesystemToGCSOperator(
            task_id="upload_file_drivers",
            src=f"{DRIVERS_PARQUET}",
            dst=f"bronze/{DRIVERS_PARQUET}",
            bucket=SOURCE_BUCKET,
            gcp_conn_id=GCS_CONN_ID,
        )
        upload_file_restaurants = LocalFilesystemToGCSOperator(
            task_id="upload_file_restaurants",
            src=f"{RESTAURANTS_PARQUET}",
            dst=f"bronze/{RESTAURANTS_PARQUET}",
            bucket=SOURCE_BUCKET,
            gcp_conn_id=GCS_CONN_ID,
        )
    
    @task_group(group_id='BigQueryCreate')
    def create_table_bigquery():    
        create_test_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id=f"create_airflow_dataset", dataset_id=DATASET_NAME
        )
        
        load_paquet_drivers = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_drivers',
            bucket=SOURCE_BUCKET,
            gcp_conn_id=GCS_CONN_ID,
            create_disposition="CREATE_IF_NEEDED",
            source_format="PARQUET",
            source_objects=[DST_DRIVERS],
            destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME_DRIVERS}",
            write_disposition='WRITE_TRUNCATE'
        )
        
        load_paquet_restaurants = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_restaurants',
            bucket=SOURCE_BUCKET,
            gcp_conn_id=GCS_CONN_ID,
            create_disposition="CREATE_IF_NEEDED",
            source_format="PARQUET",
            source_objects=[DST_RESTAURANTS],
            destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME_RESTAURANTS}",
            write_disposition='WRITE_TRUNCATE'
        )
        
    driver_data = extract_driver()
    restaurants_data = extract_restaurants()
    
    injection_datalake(driver_data, restaurants_data) >> upload() >> create_table_bigquery()
        

dados = get_data_postgres_create_paquet()