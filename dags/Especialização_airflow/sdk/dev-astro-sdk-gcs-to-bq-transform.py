"""
"""

# TODO Imports
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# TODO Datasets
users_parquet_dataset = Dataset("bigquery://OwsHQ.users")
payments_parquet_dataset = Dataset("bigquery://OwsHQ.payments")
events_parquet_dataset = Dataset("bigquery://OwsHQ.events")
analysis_parquet_dataset = Dataset("bigquery://OwsHQ.analysis")
analysis_postgres_dataset = Dataset("postgres://OwsHQ.analysis")

# TODO Connections & Variables
landing_zone_path = "gs://owshq-airbyte-ingestion/"
source_gcs_conn_id = "google_cloud_default"
bq_conn_id = "google_cloud_default"
postgres_conn_id = "postgres_default"
schema = "Analytics"

# TODO Default Arguments
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# TODO DAG Definition
@dag(
    dag_id="dev-astro-sdk-gcs-to-bq-transform",
    start_date=datetime(2024, 9, 26),
    max_active_runs=1,
    schedule_interval=timedelta(hours=8),
    default_args=default_args,
    catchup=False,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['development', 'ingestion', 'airbyte', 'postgres', 'mongodb', 'gcs']
)
def init():

    # TODO Tasks Declaration
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    users_parquet = aql.load_file(
        task_id="users_parquet",
        input_file=File(path=landing_zone_path + "mongodb-atlas/users/", filetype=FileType.PARQUET, conn_id=source_gcs_conn_id),
        output_table=Table(name="users", metadata=Metadata(schema=schema), conn_id=bq_conn_id),
        if_exists="replace",
        use_native_support=True,
        outlets=[users_parquet_dataset]
    )

    payments_parquet = aql.load_file(
        task_id="payments_parquet",
        input_file=File(path=landing_zone_path + "mongodb-atlas/payments/", filetype=FileType.PARQUET, conn_id=source_gcs_conn_id),
        output_table=Table(name="payments", metadata=Metadata(schema=schema), conn_id=bq_conn_id),
        if_exists="replace",
        use_native_support=True,
        outlets=[payments_parquet_dataset]
    )

    # TODO @aql.transform [sql]
    @aql.transform
    def join_tables(users: Table, payments: Table):
        return """
        SELECT users.cpf,
           users.city,
           users.country,
           users.first_name,
           users.last_name,
           users.date_birth,
           payments.order_id,
           payments.amount,
           payments.status
        FROM {{ users }} AS users
        INNER JOIN {{ payments }} AS payments
        ON users.cpf = payments.cpf
        """

    join_datasets = join_tables(
        users=users_parquet,
        payments=payments_parquet,
        output_table=Table(name="events", conn_id=bq_conn_id, metadata=Metadata(schema=schema))
    )

    events_to_parquet = aql.export_to_file(
        task_id="events_to_parquet",
        input_data=join_datasets,
        output_file=File(path="gs://owshq-airbyte-ingestion/outputs/{{ ds }}/events.parquet"),
        if_exists="replace",
        outlets=[events_parquet_dataset]
    )

    pd_ds_events = aql.load_file(
        task_id="pd_ds_events",
        input_file=File(
            path="gs://owshq-airbyte-ingestion/outputs/{{ ds }}/events.parquet",
            filetype=FileType.PARQUET,
            conn_id=source_gcs_conn_id
        )
    )

    # TODO @aql.dataframe [pandas]
    @aql.dataframe(columns_names_capitalization="original")
    def users_events_analysis(events_df: pd.DataFrame) -> pd.DataFrame:
        if 'city' in events_df.columns and 'country' in events_df.columns and 'amount' in events_df.columns and 'order_id' in events_df.columns:

            orders_per_city = (
                events_df.groupby('city')['order_id']
                .count()
                .reset_index()
                .rename(columns={'order_id': 'total_orders'})
            )

            avg_order_amount = (
                events_df.groupby('country')['amount']
                .mean()
                .reset_index()
                .rename(columns={'amount': 'avg_order_amount'})
            )

            if 'city' in orders_per_city.columns and 'city' in avg_order_amount.columns:
                summary = pd.merge(orders_per_city, avg_order_amount, how='outer', on='city')
            else:
                summary = pd.merge(orders_per_city, avg_order_amount, how='outer', left_on='city', right_on='country')
            return summary
        else:
            raise

    events_analysis = users_events_analysis(events_df=pd_ds_events)

    analysis_to_parquet = aql.export_to_file(
        task_id="analysis_to_parquet",
        input_data=events_analysis,
        output_file=File(path="gs://owshq-airbyte-ingestion/analysis/{{ ds }}/analysis.parquet"),
        if_exists="replace",
        outlets=[analysis_parquet_dataset]
    )

    # TODO output to postgres database
    postgres_analysis_output = aql.load_file(
        task_id="postgres_analysis_output",
        input_file=File(
            path="gs://owshq-airbyte-ingestion/analysis/{{ ds }}/analysis.parquet",
            filetype=FileType.PARQUET,
            conn_id=source_gcs_conn_id
        ),
        output_table=Table(
            name="analysis",
            metadata=Metadata(schema='public'),
            conn_id=postgres_conn_id
        ),
        if_exists="replace",
        use_native_support=True,
        outlets=[analysis_postgres_dataset]
    )

    # TODO Task Dependencies
    chain(
        start,
        [users_parquet, payments_parquet],
        join_datasets,
        events_to_parquet,
        pd_ds_events,
        events_analysis,
        analysis_to_parquet,
        postgres_analysis_output,
        end
    )


# TODO DAG Instantiation
dag = init()
