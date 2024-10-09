from airflow.decorators import (
    dag,
    task,
)  # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from pendulum import datetime, duration
import pandas as pd
import duckdb
import os
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "galaxy_data")
_DUCKDB_TABLE_URI = f"duckdb://{_DUCKDB_INSTANCE_NAME}/{_DUCKDB_TABLE_NAME}"

@dag(
    start_date=datetime(2024, 7, 1),  # Date after which the DAG can be scheduled
    schedule="@daily",  # See: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # See: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # Allow only one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # Add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=30),  # tasks wait 30s in between retries
    },  # default_args are applied to all tasks in a DAG
    tags=["example", "ETL"],  # Add tags in the UI
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow into production, use a
    # cloud-based database and, based on concurrency capabilities, adjust the two parameters below.
    concurrency=1, # allow only a single task execution at a time, prevents parallel DuckDB calls
    is_paused_upon_creation=False, # start running the DAG as soon as it's created
)
def example_etl_galaxies_load():  # By default, the dag_id is the name of the decorated function

    @task
    def extract_galaxy_data_duckdb(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ):
        cursor = duckdb.connect(duckdb_instance_name)
        galaxy_data_df = cursor.sql(f"SELECT * FROM {table_name};").df()
        
        return galaxy_data_df

    @task
    def create_sql_query(df):
        sql_text = []
        for index, row in df.iterrows():       
            sql_text.append(
                    f'INSERT INTO {_DUCKDB_TABLE_NAME} ('+ str(', '.join(df.columns))+ ') VALUES '+ str(tuple(row.values))
                )        
        return sql_text

    create_galaxy_table_postgres = SQLExecuteQueryOperator(
        task_id="create_galaxy_table_postgres",
        conn_id="postgres_default",
        sql=f"""
            DROP TABLE IF EXISTS {_DUCKDB_TABLE_NAME};
            CREATE TABLE {_DUCKDB_TABLE_NAME} (
                name VARCHAR PRIMARY KEY,
                distance_from_milkyway INT,
                distance_from_solarsystem INT,
                type_of_galaxy VARCHAR,
                characteristics VARCHAR
            );
            """,
        retries=0,
        )

    create_sql_query_obj = create_sql_query(extract_galaxy_data_duckdb())

    load_galaxy_data_postgres = SQLExecuteQueryOperator(
        task_id = "load_galaxy_data_postgres",
        conn_id = "postgres_default",
        sql = create_sql_query_obj,
        retries=0,
    )

    create_galaxy_table_postgres >> load_galaxy_data_postgres

example_etl_galaxies_load()
