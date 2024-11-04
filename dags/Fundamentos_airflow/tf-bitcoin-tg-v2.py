import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

@dag(
    dag_id="tf-bitcoin-tg-v2",
    schedule="@daily",
    start_date=datetime(2021,12,1),
    catchup=False
)
def main():
    
    # TODO TaskGroup
    with TaskGroup("transformers") as transformers:
        
        # TODO Task 1
        @task(task_id="extract", retries=2)
        def extract_bitcoin():
            return requests.get(API).json()["bitcoin"]
        
        # TODO Task 2
        @task(task_id="transform")
        def process_bitcoin(response):
            return {"usd": response["usd"], "change": response["usd_24h_change"]}
    
        # TODO Dependencies
        processed_data = process_bitcoin(extract_bitcoin())
    
    with TaskGroup("store") as stories:   
        @task(task_id="store")
        def store_bitcoin(data):
            logging.info(f"Bitcoin price: {data["usd"]}, change: {data['change']}")
        
        store_bitcoin(processed_data)

main()