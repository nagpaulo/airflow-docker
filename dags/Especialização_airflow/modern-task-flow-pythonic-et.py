# TODO required libraries
from airflow.decorators import dag, task
from datetime import datetime


# TODO DAG structure using decorators
@dag(
    dag_id="modern-task-flow-pythonic-et",
    start_date=datetime(2024, 10, 20),
    catchup=False
)
def init():

    # TODO declare tasks using decorators
    @task()
    def extract():
        return {"data": "extract"}

    @task()
    def transform(data: dict):
        return {"data": "transform"}

    # TODO declare dependencies
    transform(extract())


dag = init()
