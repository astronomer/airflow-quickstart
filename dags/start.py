"""
Create Airflow pool example DAG

This example kicks off the pipeline by creating a pool and producing a dataset.
"""

from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime
import logging

# modularize code by importing functions from the include folder
from include.global_variables import airflow_conf_variables as gv

# use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# ------------------- #
# Dataset Definition #
# ------------------- #

start_dataset = Dataset("start")

# -------------- #
# DAG Definition #
# -------------- #

# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2023, 1, 1), # date after which the DAG can be scheduled
    schedule="@once", # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False, # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=5, # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args=gv.default_args,
    description="Run this DAG to start the pipeline!",
    tags=["start", "setup"], # add tags in the UI
)
def start(): # by default the dag_id is the name of the decorated function

    # --------------- #
    # Task Definition #
    # --------------- #
    # this task uses a traditional Airflow operator, which you can mix and match with `@task`-decorated functions.
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    # this task uses the BashOperator to run a bash command creating an Airflow
    # pool called 'duckdb' containing one worker slot. All tasks running
    # queries against DuckDB will be assigned to this pool, preventing parallel
    # requests to DuckDB.

    t_log.info("Creating a pool for all tasks running queries against the in-memory database.")

    create_duckdb_pool = BashOperator(
        task_id="bash_task",
        bash_command="airflow pools list | grep -q 'duckdb' || airflow pools set duckdb 1 'Pool for duckdb'",
        outlets=[start_dataset],
    )

# instantiate the DAG
start()
