from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# Default args for the DAG
default_args = {
    'owner': 'airflow',
}

# Define the DAG
dag = DAG(
    'snowflake_query',
    default_args=default_args,
    description='Run a simple query in Snowflake for testing',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)

# Snowflake query
query = """
SELECT CURRENT_VERSION();
"""

# SnowflakeOperator task
run_query = SnowflakeOperator(
    task_id='run_query',
    sql=query,
    snowflake_conn_id='snowflake_conn_test',
    dag=dag,
)

run_query