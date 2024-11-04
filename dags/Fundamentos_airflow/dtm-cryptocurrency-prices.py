import requests
import logging

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# TODO list of cryptocurrencies to fetch (input for dynamic task mapping) 
cryptocurrencies = ["ethereum", "dogecoin", "bitcoin"]

@dag(
    dag_id="dtm-cryptocurrency-prices",
    schedule_interval="@daily",
    start_date=datetime(2024,10,18),
    catchup=False
)
def crypto_prices():
    
    # TODO task to dynamically map over the list of cryptocurrencies with custom index name
    @task(map_index_template="{{ crypto }}")
    def fetch_price(crypto: str):
        """ Fecth the price of a given cryptocurrency from the API """
        
        # TODO use the cryptocurrency name in the task name
        context = get_current_context()
        context["crypto"] = crypto
        
        # TODO API Call to fetch the price of the cryptocurrency
        api_url = Variable.get("api_call_template_crypto").format(crypto=crypto)
        response = requests.get(api_url).json()
        price = response[crypto]['usd']
        logging.info(f"the price of {crypto} is ${price}")
        
        return price
    
    # TODO dymamically map the fetch_price task over the list of cryptocurrencies
    prices = fetch_price.partial().expand(crypto=cryptocurrencies)
    
    prices

# TODO instantiate the DAG
crypto_prices()