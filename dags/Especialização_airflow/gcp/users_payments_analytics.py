import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, current_timestamp, to_timestamp, count, sum, when, round, avg)
from typing import List


def create_spark_session() -> SparkSession:
    """Create Spark session"""
    return (SparkSession.builder
            .appName("Metrics")
            .getOrCreate())


def process_payment_data(spark: SparkSession, payment_files: List[str], user_files: List[str], output_path: str) -> None:
    """Process payment and user data to generate analytics"""

    # TODO Unpack the file lists with * to pass multiple paths to spark.read.parquet
    payments_df = spark.read.parquet(*payment_files)
    users_df = spark.read.parquet(*user_files)

    # TODO Select and transform payment columns
    payments_df = payments_df.select(
        to_timestamp('payment_date').alias('payment_date'),
        'payment_method',
        'status',
        'currency',
        col('amount').cast('float'),
        'uuid'
    )

    # TODO Select user columns
    users_df = users_df.select(
        'uuid',
        'country',
        'job'
    )

    # TODO Join payments and users on 'uuid'
    combined_df = payments_df.join(users_df, 'uuid', 'left')

    # TODO Calculate metrics
    metrics_df = (combined_df.groupBy(
        'payment_date',
        'country',
        'payment_method',
        'status',
        'currency'
    ).agg(
        round(avg('amount'), 2).alias('amount'),
        count('uuid').alias('total_users'),
        round((sum(when(col('status') == 'Completed', 1).otherwise(0)) / count('*') * 100), 2).alias('success_rate')
    ))

    # TODO Add processed timestamp
    metrics_df = metrics_df.withColumn('processed_at', current_timestamp())

    # TODO Write metrics to Parquet
    metrics_df.write.mode('overwrite').parquet(output_path)

    logging.info(f"Successfully processed {len(payment_files)} payment files and {len(user_files)} user files")


def main():
    parser = argparse.ArgumentParser(description='Process payment data')
    parser.add_argument('--payment_files', type=str, required=True)
    parser.add_argument('--user_files', type=str, required=True)
    parser.add_argument('--output_path', type=str, required=True)

    args = parser.parse_args()
    payment_files = args.payment_files.split(',')
    user_files = args.user_files.split(',')

    spark = create_spark_session()

    try:
        process_payment_data(
            spark=spark,
            payment_files=payment_files,
            user_files=user_files,
            output_path=args.output_path
        )
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
