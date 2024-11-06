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

load_dotenv()

MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'owshq')
GCS_CONN_ID = "google_cloud_default"
SOURCE_BUCKET = "storage-datalake-injection"
DESTINATION_OBJECT = "file"
USER_PARQUET = "users.parquet"
PAYMENTS_PARQUET = "payments.parquet"

default_args = {
    'owner': 'Paulo Roberto Mesquita da Silva',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="get-dados-create-paquet",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["transform", "google", "bucket"],
)
def get_dados_create_paquet():
    
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
        file_paquet_user = gerar_paquet("users", users, f"{USER_PARQUET}")
        file_paquet_payments = gerar_paquet("payments", payments, f"{PAYMENTS_PARQUET}")
    
    @task_group(group_id='Upload')
    def upload():
        upload_file_user = LocalFilesystemToGCSOperator(
            task_id="upload_file_user",
            src=f"{USER_PARQUET}",
            dst=f"bronze/{USER_PARQUET}",
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
        
    users_data = extract_users()
    payments_data = extract_payments()
    
    injection_datalake(users_data, payments_data) >> upload()
        

dados = get_dados_create_paquet()