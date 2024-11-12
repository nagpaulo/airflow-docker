import duckdb
import pandas as pd
import pyarrow as pa

import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator

@dag(
    dag_id="gerar-data-duckdb",
    schedule="@daily",
    start_date=datetime(2021,12,1),
    catchup=False
)
def main():
    def create_data():
        # create a connection to a file called 'file.db'
        con = duckdb.connect("file.db")
        # create a table and load data into it
        con.sql("CREATE OR REPLACE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        # query the table
        con.table("test").show()
        # explicitly close the connection
        con.close()
        # Note: connections also closed implicitly when they go out of scope
    
    def select_test():
        arrow_table = pa.Table.from_pydict({"a": [42]})
        result = duckdb.sql("SELECT * FROM arrow_table")      
        print(result)

    create_data =  PythonOperator(
        task_id="create_data",
        python_callable=create_data
    )
    
    select = PythonOperator(
        task_id="dataframe",
        python_callable=select_test
    )    
    
    create_data >> select

main()

