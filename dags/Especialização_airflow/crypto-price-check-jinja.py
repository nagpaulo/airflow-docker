import requests

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id="crypto-price-check-jinja",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["crypto", "TaskFlowAPI"],
)
def crypto_price_check_dag():
    """
    A DAG to monitor the price of a cryptocurrency (e.g., Bitcoin) and make decisions based on the price threshold.
    It demonstrates the use of Jinja templating for dynamic log messages and variable passing.
    """

    @task
    def fetch_crypto_data(symbol: str = "bitcoin") -> dict:
        """
        Fetches the latest cryptocurrency data from the CoinGecko API.

        Jinja Template Variables:
        - `execution_date`: The execution date of the current DAG run.
        - `dag_run`: The DAG run object, which includes useful metadata.

        Returns:
            dict: A dictionary containing cryptocurrency price information.
        """
        context = get_current_context()  # TODO get the context for Jinja templating
        execution_date = context["execution_date"]

        print(
            f"{{{{ execution_date }}}} - Fetching {symbol} data as of {{{{ execution_date.strftime('%Y-%m-%d') }}}}"
        )

        response = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=usd")

        if response.status_code != 200:
            raise ValueError(f"Failed to fetch data: {response.status_code}, {response.text}")

        data = response.json()
        print(f"API Response: {data}")

        if symbol not in data:
            raise KeyError(f"Symbol '{symbol}' not found in the API response.")

        price = data[symbol]["usd"]
        return {"symbol": symbol, "price": price, "date": execution_date}

    @task
    def check_price_threshold(crypto_data: dict, threshold: float = 30000) -> bool:
        """
        Checks if the current price of the cryptocurrency is above a given threshold.

        Jinja Template Variables:
        - `dag_run.logical_date`: Date and time when the DAG run logically starts.

        Args:
            crypto_data (dict): Dictionary with crypto price and symbol.
            threshold (float): Price threshold for decision-making.

        Returns:
            bool: `True` if price is above the threshold; otherwise, `False`.
        """

        symbol = crypto_data["symbol"]
        price = crypto_data["price"]
        date = crypto_data["date"]

        print(
            f"{{{{ dag_run.logical_date }}}} - Checking if {symbol} price of ${{{{ price }}}} exceeds threshold ${threshold}"
        )

        if price > threshold:
            print(f"Price of {symbol} (${price}) is above the threshold (${threshold}).")
            return True
        else:
            print(f"Price of {symbol} (${price}) is below the threshold (${threshold}).")
            return False

    @task
    def take_action(should_alert: bool, crypto_data: dict):
        """
        Takes action based on the threshold check result.

        Jinja Template Variables:
        - `ds`: Execution date as YYYY-MM-DD string.

        Args:
            should_alert (bool): Result of the threshold check.
            crypto_data (dict): Dictionary with crypto price and symbol.
        """

        symbol = crypto_data["symbol"]
        price = crypto_data["price"]

        if should_alert:
            print(
                f"{{{{ ds }}}} - Alert: {symbol} price of ${{{{ price }}}} has exceeded the threshold! Action is required."
            )
        else:
            print(f"{{{{ ds }}}} - No action required as {symbol} price is within acceptable range.")

    # TODO define TaskFlow
    crypto_data = fetch_crypto_data()
    should_alert = check_price_threshold(crypto_data)
    take_action(should_alert, crypto_data)


# TODO instantiate the DAG
dag = crypto_price_check_dag()
