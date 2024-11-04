from airflow.decorators import task
from airflow import Dataset


@task(outlets=[Dataset("crypto/aggregate_data")])
def aggregate_metrics(cleaned_data):
    aggregated_data = f"Aggregated metrics from {cleaned_data}"
    return aggregated_data
