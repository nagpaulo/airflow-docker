import pandas as pd
import logging
import tempfile
import os

from airflow.decorators import dag, task
from airflow.providers.openai.hooks.openai import OpenAIHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

logger = logging.getLogger("airflow.task")
default_args = {"retries": 2, "retry_delay": 30}


@dag(
    dag_id="gpt-analyze-products-review",
    schedule_interval=None,
    start_date=datetime(2024, 10, 31),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args
)
def analyze_product_reviews():
    """
    DAG to analyze product reviews, summarize sentiment and key highlights using GenAI.
    """

    @task
    def fetch_reviews() -> pd.DataFrame:
        """
        Generate a static DataFrame of sample reviews.
        """

        data = [
            {"review_id": 1, "content": "Amazing product! Highly recommend.", "product_id": 101},
            {"review_id": 2, "content": "Not worth the price, poor quality.", "product_id": 102},
            {"review_id": 3, "content": "Satisfied with the purchase.", "product_id": 103}
        ]
        return pd.DataFrame(data)

    @task
    def analyze_review_content(reviews: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze reviews using GenAI to extract sentiment and highlights.
        :param reviews: DataFrame of reviews
        :return: DataFrame with sentiment and highlights added
        """

        hook = OpenAIHook(conn_id="openai_default")
        openai_client = hook.get_conn()

        reviews["sentiment"] = None
        reviews["highlights"] = None

        for idx, review in reviews.iterrows():
            response = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Analyze the following review text for sentiment and highlights."},
                    {"role": "user", "content": review["content"]}
                ],
                temperature=0.3,
                max_tokens=150
            )

            analysis = response.choices[0].message.content
            sentiment, highlights = analysis.split("Highlights:")
            reviews.at[idx, "sentiment"] = sentiment.strip()
            reviews.at[idx, "highlights"] = highlights.strip()

        return reviews

    @task
    def save_analysis_results(analyzed_reviews: pd.DataFrame):
        """
        Save the analysis results to Google Cloud Storage.
        :param analyzed_reviews: DataFrame of analyzed reviews
        """

        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False, mode='w') as temp_file:
            analyzed_reviews.to_csv(temp_file.name, index=False)
            temp_file_path = temp_file.name

        try:
            gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
            bucket_name = 'owshq-airbyte-ingestion'
            object_name = f'processed/openai/reviews_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=temp_file_path
            )

            logger.info(f"Successfully uploaded analyzed reviews to GCS: gs://{bucket_name}/{object_name}")

        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    reviews = fetch_reviews()
    analyzed_reviews = analyze_review_content(reviews)
    save_analysis_results(analyzed_reviews)


dag = analyze_product_reviews()
