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
from airflow.decorators import dag, task_group, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

load_dotenv()

MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'owshq')
GCS_CONN_ID = "google_cloud_default"
SOURCE_BUCKET = "storage-datalake-injection"
DESTINATION_OBJECT = "file"
USERS_PARQUET = "users.parquet"
PAYMENTS_PARQUET = "payments.parquet"
DATASET_NAME = "datalake"
TABLE_NAME_PAYMENTS = "payments"
DST_PAYMENTS=f"bronze/{PAYMENTS_PARQUET}"
TABLE_NAME_USERS = "users"
DST_USERS=f"bronze/{USERS_PARQUET}"                               
TABLE_USERS = f"{DATASET_NAME}.{TABLE_NAME_USERS}"
TABLE_PAYMENTS = f"{DATASET_NAME}.{TABLE_NAME_PAYMENTS}"

default_args = {
    'owner': 'Paulo Roberto Mesquita da Silva',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="get-data-mongo-upload-paquet",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["transform", "google", "bucket", "Engenharia de Dados"],
)
def get_data_mongo_create_paquet():
    
    @task(task_id="extract_users")
    def extract_users():
        try:
            hook = MongoHook(conn_id='mongodb_default')
            client = hook.get_conn()
            db = client.owshq

            try:
                users_cursor = db.users.find({})
                users_list = list(users_cursor)
                users_json = json.loads(json_util.dumps(users_list))
                df_users = pd.DataFrame(users_json)

                print(f"Successfully extracted {len(df_users)} records from Users collection")
                if not df_users.empty:
                    print("Sample columns:", df_users.columns.tolist()[:5])
                    print("First record sample:", df_users.iloc[0].to_dict())
                else:
                    print("Warning: No records found in Users collection")

                # Converter data em String
                df_users['last_access_time'] = str(df_users['last_access_time'])
                # Removendo a coluna '_id'
                df_users = df_users.drop('last_access_time', axis=1)
                df_users = df_users.drop('_id', axis=1)
                return df_users

            except Exception as e:
                print(f"Error during Users collection processing: {str(e)}")
                raise
            finally:
                client.close()

        except Exception as e:
            print(f"Error connecting to MongoDB: {str(e)}")
            raise
        
    @task(task_id="extract_payments")
    def extract_payments():
        try:
            hook = MongoHook(conn_id='mongodb_default')
            client = hook.get_conn()
            db = client.owshq

            try:
                payments_cursor = db.payments.find({})
                payments_list = list(payments_cursor)
                payments_json = json.loads(json_util.dumps(payments_list))
                df_payments = pd.DataFrame(payments_json)

                print(f"Successfully extracted {len(df_payments)} records from Payments collection")
                if not df_payments.empty:
                    print("Sample columns:", df_payments.columns.tolist()[:5])
                    print("First record sample:", df_payments.iloc[0].to_dict())
                else:
                    print("Warning: No records found in Payments collection")

                # Removendo a coluna '_id'
                df_payments = df_payments.drop('_id', axis=1)
                return df_payments

            except Exception as e:
                print(f"Error during Payments collection processing: {str(e)}")
                raise
            finally:
                client.close()

        except Exception as e:
            print(f"Error connecting to MongoDB: {str(e)}")
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
    def injection_datalake(users, payments):
        file_paquet_users = gerar_paquet("users", users, f"{USERS_PARQUET}")
        file_paquet_payments = gerar_paquet("payments", payments, f"{PAYMENTS_PARQUET}")
    
    @task_group(group_id='Upload')
    def upload():
        upload_file_user = LocalFilesystemToGCSOperator(
            task_id="upload_file_user",
            src=f"{USERS_PARQUET}",
            dst=f"bronze/{USERS_PARQUET}",
            bucket=SOURCE_BUCKET,
            gcp_conn_id=GCS_CONN_ID,
        )
        upload_file_payments = LocalFilesystemToGCSOperator(
            task_id="upload_file_payments",
            src=f"{PAYMENTS_PARQUET}",
            dst=f"bronze/{PAYMENTS_PARQUET}",
            bucket=SOURCE_BUCKET,
            gcp_conn_id=GCS_CONN_ID,
        )
        
    @task_group(group_id='BigQueryCreate')
    def create_table_bigquery():    
        create_test_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id=f"create_airflow_dataset", dataset_id=DATASET_NAME
        )
        
        load_paquet_users = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_users',
            bucket=SOURCE_BUCKET,
            gcp_conn_id=GCS_CONN_ID,
            create_disposition="CREATE_IF_NEEDED",
            source_format="PARQUET",
            source_objects=[DST_USERS],
            destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME_USERS}",
            write_disposition='WRITE_TRUNCATE'
        )
        
        load_paquet_payments = GCSToBigQueryOperator(
            task_id='gcs_to_bigquery_payments',
            bucket=SOURCE_BUCKET,
            gcp_conn_id=GCS_CONN_ID,
            create_disposition="CREATE_IF_NEEDED",
            source_format="PARQUET",
            source_objects=[DST_PAYMENTS],
            destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME_PAYMENTS}",
            write_disposition='WRITE_TRUNCATE'
        )
        
    users_data = extract_users()
    payments_data = extract_payments()
    
    injection_datalake(users_data, payments_data) >> upload() >> create_table_bigquery()
        

dados = get_data_mongo_create_paquet()