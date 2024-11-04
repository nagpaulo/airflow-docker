from airflow.decorators import task


@task
def fetch_volume_data(crypto: str):
    volume_data = f"Volume data for {crypto}"
    return volume_data
