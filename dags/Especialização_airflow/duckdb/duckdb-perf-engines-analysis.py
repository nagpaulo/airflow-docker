import duckdb
import pandas as pd
import numpy as np
import sqlite3
import time
import logging
import json
from pathlib import Path
from typing import Dict

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}


@dag(
    dag_id="duckdb-perf-engines-analysis",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['benchmark', 'duckdb', 'performance'],
)
def init_db_perf_init():

    @task
    def generate_sample_dataset(rows: int = 20_000_000) -> Dict[str, str]:
        """Generate a large dataset for benchmarking"""
        logging.info(f"Generating sample dataset with {rows} rows")

        # TODO Generate more complex data for better benchmarking
        np.random.seed(42)
        df = pd.DataFrame({
            'id': range(rows),
            'timestamp': pd.date_range('2024-01-01', periods=rows, freq='1min'),
            'category': np.random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'], rows),
            'value1': np.random.normal(100, 15, rows),
            'value2': np.random.uniform(0, 1000, rows),
            'status': np.random.choice(['active', 'inactive', 'pending', 'suspended', 'archived'], rows),
            'metadata': [f'meta_{i % 1000}' for i in range(rows)],
            'score': np.random.random(rows),
            'flag': np.random.choice([True, False], rows),
            'priority': np.random.choice([1, 2, 3, 4, 5], rows)
        })

        logging.info(f"Generated DataFrame size: {df.memory_usage().sum() / 1024 / 1024:.2f} MB")

        # TODO Save in different formats
        base_path = '/tmp/benchmark_data'
        Path(base_path).mkdir(exist_ok=True)

        csv_path = f'{base_path}/data.csv'
        parquet_path = f'{base_path}/data.parquet'
        sqlite_path = f'{base_path}/data.db'
        duckdb_path = f'{base_path}/data.duckdb'

        # TODO Save files with compression
        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False, compression='snappy')

        # TODO SQLite with optimized settings
        sqlite_conn = sqlite3.connect(sqlite_path)
        df.to_sql('benchmark_data', sqlite_conn, if_exists='replace', index=False)
        sqlite_conn.execute('CREATE INDEX idx_category ON benchmark_data(category)')
        sqlite_conn.close()

        # TODO DuckDB with optimized settings
        conn = duckdb.connect(duckdb_path)
        conn.execute("CREATE TABLE IF NOT EXISTS benchmark_data AS SELECT * FROM df")
        conn.execute("CREATE INDEX idx_category ON benchmark_data(category)")
        conn.close()

        return {
            'csv_path': csv_path,
            'parquet_path': parquet_path,
            'sqlite_path': sqlite_path,
            'duckdb_path': duckdb_path,
            'rows': rows,
            'size_mb': df.memory_usage().sum() / 1024 / 1024
        }

    @task
    def benchmark_read_performance(file_paths: Dict[str, str]) -> Dict[str, float]:
        """Benchmark reading data performance"""
        results = {}

        # TODO 1. Pandas CSV Read
        start_time = time.time()
        pd.read_csv(file_paths['csv_path'])
        results['pandas_csv_read'] = time.time() - start_time

        # TODO 2. Pandas Parquet Read
        start_time = time.time()
        pd.read_parquet(file_paths['parquet_path'])
        results['pandas_parquet_read'] = time.time() - start_time

        # TODO 3. DuckDB CSV Read
        start_time = time.time()
        conn = duckdb.connect(':memory:')
        conn.execute("SELECT * FROM read_csv_auto(?)", [file_paths['csv_path']])
        conn.close()
        results['duckdb_csv_read'] = time.time() - start_time

        # TODO 4. DuckDB Parquet Read
        start_time = time.time()
        conn = duckdb.connect(':memory:')
        conn.execute("SELECT * FROM read_parquet(?)", [file_paths['parquet_path']])
        conn.close()
        results['duckdb_parquet_read'] = time.time() - start_time

        # TODO 5. SQLite Read
        start_time = time.time()
        conn = sqlite3.connect(file_paths['sqlite_path'])
        pd.read_sql("SELECT * FROM benchmark_data", conn)
        conn.close()
        results['sqlite_read'] = time.time() - start_time

        return results

    @task
    def benchmark_aggregation(file_paths: Dict[str, str]) -> Dict[str, float]:
        """Benchmark aggregation operations"""
        results = {}

        agg_query = """
            WITH source_data AS (
                SELECT * FROM read_parquet(?)
            )
            SELECT 
                category,
                status,
                COUNT(*) as count,
                AVG(value1) as avg_value1,
                SUM(value2) as total_value2,
                MIN(value1) as min_value1,
                MAX(value1) as max_value1,
                APPROX_QUANTILE(value2, 0.5) as median_value2
            FROM source_data
            GROUP BY category, status
            ORDER BY category, status
        """

        # TODO 1. Pandas Aggregation
        start_time = time.time()
        df = pd.read_parquet(file_paths['parquet_path'])
        pandas_result = df.groupby(['category', 'status']).agg({
            'value1': ['count', 'mean', 'min', 'max'],
            'value2': ['sum', 'median']
        })
        results['pandas_aggregation'] = time.time() - start_time

        # TODO 2. DuckDB Aggregation
        start_time = time.time()
        conn = duckdb.connect(':memory:')
        conn.execute(agg_query, [file_paths['parquet_path']])
        conn.close()
        results['duckdb_aggregation'] = time.time() - start_time

        # TODO 3. SQLite Aggregation
        start_time = time.time()
        conn = sqlite3.connect(file_paths['sqlite_path'])
        pd.read_sql("""
            SELECT 
                category,
                status,
                COUNT(*) as count,
                AVG(value1) as avg_value1,
                SUM(value2) as total_value2,
                MIN(value1) as min_value1,
                MAX(value1) as max_value1
            FROM benchmark_data
            GROUP BY category, status
            ORDER BY category, status
        """, conn)
        conn.close()
        results['sqlite_aggregation'] = time.time() - start_time

        return results

    @task
    def benchmark_joins(file_paths: Dict[str, str]) -> Dict[str, float]:
        """Benchmark join operations"""
        results = {}

        dim_df = pd.DataFrame({
            'category': ['A', 'B', 'C', 'D', 'E'],
            'category_name': ['Alpha', 'Beta', 'Charlie', 'Delta', 'Echo'],
            'department': ['Dept1', 'Dept2', 'Dept1', 'Dept3', 'Dept2']
        })

        dim_path = '/tmp/benchmark_data/dimensions.parquet'
        dim_df.to_parquet(dim_path)

        # TODO 1. Pandas Join
        start_time = time.time()
        main_df = pd.read_parquet(file_paths['parquet_path'])
        _ = main_df.merge(dim_df, on='category', how='left')
        results['pandas_join'] = time.time() - start_time

        # TODO 2. DuckDB Join
        start_time = time.time()
        conn = duckdb.connect(':memory:')
        conn.execute("""
            SELECT 
                m.*,
                d.category_name,
                d.department
            FROM read_parquet(?) m
            LEFT JOIN read_parquet(?) d ON m.category = d.category
        """, [file_paths['parquet_path'], dim_path])
        conn.close()
        results['duckdb_join'] = time.time() - start_time

        return results

    @task
    def benchmark_window_functions(file_paths: Dict[str, str]) -> Dict[str, float]:
        """Benchmark window function performance"""
        results = {}

        window_query = """
            WITH source_data AS (
                SELECT * FROM read_parquet(?)
            )
            SELECT 
                *,
                AVG(value1) OVER (
                    PARTITION BY category 
                    ORDER BY timestamp 
                    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
                ) as moving_avg,
                ROW_NUMBER() OVER (
                    PARTITION BY category 
                    ORDER BY value2 DESC
                ) as value_rank,
                value2 - LAG(value2) OVER (
                    PARTITION BY category 
                    ORDER BY timestamp
                ) as value_change
            FROM source_data
        """

        # TODO 1. Pandas Window Functions
        start_time = time.time()
        df = pd.read_parquet(file_paths['parquet_path'])
        _ = df.assign(
            moving_avg=df.groupby('category')['value1']
            .rolling(window=100, min_periods=1)
            .mean()
            .reset_index(0, drop=True),
            value_rank=df.groupby('category')['value2']
            .rank(method='first', ascending=False),
            value_change=df.groupby('category')['value2']
            .diff()
        )
        results['pandas_window'] = time.time() - start_time

        # TODO 2. DuckDB Window Functions
        start_time = time.time()
        conn = duckdb.connect(':memory:')
        conn.execute(window_query, [file_paths['parquet_path']])
        conn.close()
        results['duckdb_window'] = time.time() - start_time

        return results

    @task
    def perf_report(read_results: Dict[str, float], agg_results: Dict[str, float], join_results: Dict[str, float], window_results: Dict[str, float]) -> Dict:
        """Generate a comprehensive performance report and store in XCom"""

        def calc_percentage_slower(duckdb_time: float, other_time: float) -> float:
            return ((other_time - duckdb_time) / duckdb_time) * 100

        detailed_metrics = {
            'read_performance': {
                **read_results,
                'relative_performance': {
                    'duckdb_vs_pandas_csv': f"{(read_results['pandas_csv_read'] / read_results['duckdb_csv_read']):.1f}x faster",
                    'duckdb_vs_pandas_parquet': f"{(read_results['pandas_parquet_read'] / read_results['duckdb_parquet_read']):.1f}x faster",
                    'duckdb_vs_sqlite': f"{(read_results['sqlite_read'] / read_results['duckdb_csv_read']):.1f}x faster"
                }
            },
            'computation_performance': {
                'aggregation': {
                    'times': agg_results,
                    'duckdb_advantage': f"{(agg_results['pandas_aggregation'] / agg_results['duckdb_aggregation']):.1f}x faster than Pandas"
                },
                'joins': {
                    'times': join_results,
                    'duckdb_advantage': f"{(join_results['pandas_join'] / join_results['duckdb_join']):.1f}x faster than Pandas"
                },
                'windows': {
                    'times': window_results,
                    'duckdb_advantage': f"{(window_results['pandas_window'] / window_results['duckdb_window']):.1f}x faster than Pandas"
                }
            },
            'summary': {
                'duckdb_advantages': {
                    'read_speedup': f"{(read_results['pandas_csv_read'] / read_results['duckdb_csv_read']):.1f}x",
                    'aggregation_speedup': f"{(agg_results['pandas_aggregation'] / agg_results['duckdb_aggregation']):.1f}x",
                    'join_speedup': f"{(join_results['pandas_join'] / join_results['duckdb_join']):.1f}x",
                    'window_speedup': f"{(window_results['pandas_window'] / window_results['duckdb_window']):.1f}x"
                }
            }
        }


        xcom_report = f"""
            # DuckDB Performance Benchmark Results
        
            ## Summary of DuckDB Advantages
            - Data Reading: {detailed_metrics['summary']['duckdb_advantages']['read_speedup']} faster
            - Aggregations: {detailed_metrics['summary']['duckdb_advantages']['aggregation_speedup']} faster
            - Joins: {detailed_metrics['summary']['duckdb_advantages']['join_speedup']} faster
            - Window Functions: {detailed_metrics['summary']['duckdb_advantages']['window_speedup']} faster
        
            ## Detailed Performance Results
        
            ### Read Operations
            - DuckDB CSV: {read_results['duckdb_csv_read']:.3f}s
            - DuckDB Parquet: {read_results['duckdb_parquet_read']:.3f}s
            - Pandas CSV: {read_results['pandas_csv_read']:.3f}s ({calc_percentage_slower(read_results['duckdb_csv_read'], read_results['pandas_csv_read']):.1f}% slower)
            - Pandas Parquet: {read_results['pandas_parquet_read']:.3f}s ({calc_percentage_slower(read_results['duckdb_parquet_read'], read_results['pandas_parquet_read']):.1f}% slower)
            - SQLite: {read_results['sqlite_read']:.3f}s ({calc_percentage_slower(read_results['duckdb_csv_read'], read_results['sqlite_read']):.1f}% slower)
        
            ### Computation Operations
            - Aggregations:
              * DuckDB: {agg_results['duckdb_aggregation']:.3f}s
              * Pandas: {agg_results['pandas_aggregation']:.3f}s ({calc_percentage_slower(agg_results['duckdb_aggregation'], agg_results['pandas_aggregation']):.1f}% slower)
              * SQLite: {agg_results['sqlite_aggregation']:.3f}s ({calc_percentage_slower(agg_results['duckdb_aggregation'], agg_results['sqlite_aggregation']):.1f}% slower)
        
            - Joins:
              * DuckDB: {join_results['duckdb_join']:.3f}s
              * Pandas: {join_results['pandas_join']:.3f}s ({calc_percentage_slower(join_results['duckdb_join'], join_results['pandas_join']):.1f}% slower)
        
            - Window Functions:
              * DuckDB: {window_results['duckdb_window']:.3f}s
              * Pandas: {window_results['pandas_window']:.3f}s ({calc_percentage_slower(window_results['duckdb_window'], window_results['pandas_window']):.1f}% slower)
        """

        return {
            'detailed_metrics': detailed_metrics,
            'markdown_report': xcom_report
        }

    data_paths = generate_sample_dataset()
    read_results = benchmark_read_performance(data_paths)
    agg_results = benchmark_aggregation(data_paths)
    join_results = benchmark_joins(data_paths)
    window_results = benchmark_window_functions(data_paths)
    report = perf_report(read_results, agg_results, join_results, window_results)


dag_instance = init_db_perf_init()
