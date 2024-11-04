from airflow.decorators import task


@task
def check_volatility(trends):
    if "high volatility" in trends:
        alert = "Triggering volatility alert"
    else:
        alert = "No alert"
    return alert
