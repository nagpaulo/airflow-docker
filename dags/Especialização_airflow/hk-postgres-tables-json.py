import os
import pandas as pd
from dotenv import load_dotenv
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from contextlib import closing

load_dotenv()

default_args = {
    'owner': 'luan moreno m. maciel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


@dag(
    dag_id='hk-postgres-tables-json',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 10, 20),
    catchup=False,
    tags=['hook', 'postgres', 'extract']
)
def postgres_data_extract_json():
    @task()
    def extract_drivers():
        try:
            hook = PostgresHook(postgres_conn_id='postgres_default')

            sql = """
                SELECT 
                    driver_id,
                    uuid,
                    first_name,
                    last_name,
                    date_birth::text,
                    city,
                    country,
                    phone_number,
                    license_number,
                    vehicle_type,
                    vehicle_make,
                    vehicle_model,
                    vehicle_year,
                    vehicle_license_plate,
                    cpf,
                    dt_current_timestamp::text
                FROM public.drivers
                LIMIT 10;
            """

            with closing(hook.get_conn()) as conn:
                df_drivers = pd.read_sql_query(sql, conn)

                print(f"Successfully extracted {len(df_drivers)} records from drivers table")
                if not df_drivers.empty:
                    print("Columns:", df_drivers.columns.tolist())
                    print("First record sample:", df_drivers.iloc[0].to_dict())

                return df_drivers.to_dict('records')

        except Exception as e:
            print(f"Error extracting drivers data: {str(e)}")
            raise

    @task()
    def extract_restaurants():
        try:
            hook = PostgresHook(postgres_conn_id='postgres_default')

            sql = """
                SELECT 
                    restaurant_id,
                    uuid,
                    name,
                    address,
                    city,
                    country,
                    phone_number,
                    cuisine_type,
                    to_char(opening_time, 'HH24:MI') as opening_time,
                    to_char(closing_time, 'HH24:MI') as closing_time,
                    CAST(average_rating AS FLOAT) as average_rating,
                    num_reviews,
                    cnpj,
                    dt_current_timestamp::text
                FROM public.restaurants
                LIMIT 10;
            """

            with closing(hook.get_conn()) as conn:
                df_restaurants = pd.read_sql_query(sql, conn)

                print(f"Successfully extracted {len(df_restaurants)} records from restaurants table")
                if not df_restaurants.empty:
                    print("Columns:", df_restaurants.columns.tolist())
                    print("First record sample:", df_restaurants.iloc[0].to_dict())

                return df_restaurants.to_dict('records')

        except Exception as e:
            print(f"Error extracting restaurants data: {str(e)}")
            raise

    @task()
    def process_data(drivers_data, restaurants_data):
        try:
            df_drivers = pd.DataFrame(drivers_data) if drivers_data else pd.DataFrame()
            df_restaurants = pd.DataFrame(restaurants_data) if restaurants_data else pd.DataFrame()

            summary = {
                "drivers_count": len(df_drivers),
                "restaurants_count": len(df_restaurants),
                "extraction_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "drivers_columns": df_drivers.columns.tolist() if not df_drivers.empty else [],
                "restaurants_columns": df_restaurants.columns.tolist() if not df_restaurants.empty else []
            }

            print("\nDetailed Data Summary:")
            print(f"Drivers Table:")
            print(f"- Total records: {summary['drivers_count']}")
            print(f"- Columns: {', '.join(summary['drivers_columns']) if summary['drivers_columns'] else 'No columns'}")
            print(f"\nRestaurants Table:")
            print(f"- Total records: {summary['restaurants_count']}")
            print(f"- Columns: {', '.join(summary['restaurants_columns']) if summary['restaurants_columns'] else 'No columns'}")
            print(f"\nExtraction Timestamp: {summary['extraction_timestamp']}")

            return summary

        except Exception as e:
            print(f"Error processing extracted data: {str(e)}")
            raise

    drivers_data = extract_drivers()
    restaurants_data = extract_restaurants()
    process_data(drivers_data, restaurants_data)


postgres_dag = postgres_data_extract_json()
