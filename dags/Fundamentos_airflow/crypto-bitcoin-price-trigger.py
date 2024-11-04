import requests
import logging

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

high_price_threshold = 72000
medium_price_threshold = 40000

@dag(
    dag_id="crypto-bitcoin-price-trigger",
    schedule_interval="@daily",
    start_date=datetime(2024,10,18),
    catchup=False
)
def bitcoin_trigger_rule():
    
    start_task = EmptyOperator(task_id="start")
    
    # TODO task to dynamically map over the list of cryptocurrencies with custom index name
    @task(task_id="fetch_bitcoin_price")
    def fetch_bitcoin_price():
                
        # TODO API Call to fetch the price of the cryptocurrency
        api_url = Variable.get("api_call_template_crypto")
        response = requests.get(api_url).json()
        price = response["bitcoin"]['usd']
        logging.info(f"The current price of Bitcoin is ${price}.")
        return price
    
    # TODO branching logic to decide whether to process or skip based on price
    @task.branch(task_id="branch_decision")
    def branch_based_on_price(price: float):
        """Branching task to decide based on Bitcoin prive"""
        if price > high_price_threshold:
            logging.info(f"Bitcoin price (${price}) is in the high range.")
            return "high_price_processing"
        elif price > medium_price_threshold:
            logging.info(f"Bitcoin price (${price}) is in the medium range.")
            return "medium_price_processing"
        else:
            logging.info(f"Bitcoin price (${price}) is in the low range.")
            return "low_price_processing"
        
    @task(task_id="high_price_processing")
    def process_high_price():
        logging.info("Processing high price: Taking action for high Bitcoin prices.")
        return "High Price processed"
    
    @task(task_id="medium_price_processing")
    def process_medium_price():
        logging.info("Processing medium price: Taking action for medium Bitcoin prices.")
        return "Medium Price processed"
    
    @task(task_id="low_price_processing")
    def process_low_price():
        logging.info("Processing low price: Taking action for low Bitcoin prices.")
        return "Low Price processed"
    
    # TODO task for skipping the processing (if below the threshort) 
    skip_processing = EmptyOperator(task_id="skip_processing")
    
    # TODO join task to synchronize the branch
    join_task = EmptyOperator(task_id="join_task", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    final_task = EmptyOperator(task_id="final_task", trigger_rule=TriggerRule.ALL_DONE)
    
    # TODO task dependencies
    price = fetch_bitcoin_price() # TODO Execute task and capture its return value
    decision = branch_based_on_price(price) # TODO Use the returned value in the branching task
    
    start_task >> price >> decision # TODO Define task dependencies
    decision >> process_high_price() >> join_task # TODO Branch to process_price
    decision >> process_medium_price() >> join_task # TODO Branch to process_price
    decision >> process_low_price() >> join_task # TODO Branch to process_price
    decision >> skip_processing >> join_task # TODO Branch to skip_processing
    
    join_task >> final_task
    
bitcoin_trigger_rule()