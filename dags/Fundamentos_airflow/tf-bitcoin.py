import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

@dag(
    dag_id="tf-bitcoin",
    schedule="@daily",
    start_date=datetime(2021,12,1),
    catchup=False
)
def main():
    
    @task(task_id="extract", retries=2)
    def extract_bitcoin():
        API = Variable.get("api-bitcoin")
        return requests.get(API).json()["bitcoin"]
    
    @task(task_id="transform")
    def process_bitcoin(response):
        return {"usd": response["usd"], "change": response["usd_24h_change"]}
    
    @task(task_id="store")
    def store_bitcoin(data):
        logging.info(f"Bitcoin price: {data["usd"]}, change: {data['change']}")
        
    store_bitcoin(process_bitcoin(extract_bitcoin()))

main()