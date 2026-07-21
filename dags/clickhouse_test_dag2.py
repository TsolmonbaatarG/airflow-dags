from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import clickhouse_connect
from datetime import datetime

def query_clickhouse():
    conn = BaseHook.get_connection("clickhouse_default")
    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password
    )
    result = client.query("SELECT now()")
    print("ClickHouse result:", result.result_rows)

with DAG(
    dag_id="clickhouse_test_dag2",
    start_date=datetime(2024, 1, 1),  # ← FIXED
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="query_clickhouse",
        python_callable=query_clickhouse
    )