from airflow.decorators import task


@task
def generate_trends(aggregated_data):
    trends = f"Trends generated from {aggregated_data}"
    return trends
