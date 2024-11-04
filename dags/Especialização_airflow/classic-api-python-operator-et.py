# TODO import required libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# TODO declare DAG structure
with DAG(
    dag_id='classic-api-python-operator-et',
    start_date=datetime(2024, 10, 10),
    catchup=False,
) as dag:

    # TODO declare functions
    def extract():
        return {"data": "extract"}

    def transform(ti):
        data = ti.xcom_pull(task_ids='extract')
        return {"data": "transform"}

    # TODO declare tasks
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    # TODO declare dependencies
    extract_data >> transform_data
