from airflow.decorators import task
from airflow import Dataset


@task(outlets=[Dataset("crypto/cleaned_data")])
def clean_data(price_data, volume_data):
    cleaned_data = f"Cleaned data from {price_data} and {volume_data}"
    return cleaned_data
