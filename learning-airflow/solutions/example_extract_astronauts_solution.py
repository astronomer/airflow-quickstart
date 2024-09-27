from airflow import Dataset
from airflow.decorators import (
    dag,
    task,
)  # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import requests
import duckdb
import os

# Define variables used in a DAG as environment variables in .env for your whole Airflow instance
# to standardize your DAGs.
_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "astronaut_data")
_DUCKDB_TABLE_URI = f"duckdb://{_DUCKDB_INSTANCE_NAME}/{_DUCKDB_TABLE_NAME}"

# -------------- #
# DAG Definition #
# -------------- #

# Instantiate a DAG with the @dag decorator and set DAG parameters 
# (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters).

@dag(
    start_date=datetime(2024, 1, 1),  # date after which the DAG can be scheduled
    schedule=[Dataset("current_astronauts")],  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=5),  # tasks wait 30s in between retries
    },  # default_args are applied to all tasks in a DAG
    tags=["example", "space"],  # add tags in the UI
    is_paused_upon_creation=False, # start running the DAG as soon as its created
)
def example_extract_astronauts():

    @task(retries=2)
    def get_astronauts_from_table() -> None:
          
        cursor = duckdb.connect(_DUCKDB_INSTANCE_NAME)
        cursor.execute(
            f"SELECT (num_astros) FROM {_DUCKDB_TABLE_NAME};"
        )
        num_astros = cursor.fetchone()
        print(f"The number of astronauts is {num_astros[0]}.")

    get_astronauts_from_table()

example_extract_astronauts()
