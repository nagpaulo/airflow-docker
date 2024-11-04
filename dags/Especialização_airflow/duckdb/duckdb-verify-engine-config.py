from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import duckdb


@dag(
    dag_id="duckdb-verify-engine-config",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['duckdb', 'olap', 'engine']
)
def duckdb_init():

    @task
    def verify_duckdb():
        """Verify DuckDB installation and basic functionality"""
        try:
            conn = duckdb.connect(':memory:')
            version = conn.execute("SELECT version()").fetchone()[0]

            conn.execute("""
                CREATE TABLE test (id INTEGER, name VARCHAR);
                INSERT INTO test VALUES (1, 'Test Entry');
            """)

            result = conn.execute("SELECT * FROM test").fetchall()

            conn.close()

            return {
                "status": "success",
                "duckdb_version": version,
                "test_query_result": result
            }

        except Exception as e:
            return {
                "status": "error",
                "error_message": str(e)
            }

    verify_duckdb()


dag_instance = duckdb_init()
