from airflow.decorators import task


@task
def fetch_price_data(crypto: str):
    price_data = f"Price data for {crypto}"
    return price_data
