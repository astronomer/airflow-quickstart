"""DAG that kicks off the pipeline by producing to the start dataset."""

# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv


# -------- #
# Datasets #
# -------- #

start_dataset = Dataset("start")

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # after being unpaused this DAG will run once, afterwards it can be run
    # manually with the play button in the Airflow UI
    schedule="@once",
    catchup=False,
    default_args=gv.default_args,
    description="Run this DAG to start the pipeline!",
    tags=["start", "setup"],
)
def start():

    # this task uses the BashOperator to run a bash command creating an Airflow
    # pool called 'duckdb' which contains one worker slot. All tasks running
    # queries against DuckDB will be assigned to this pool, preventing parallel
    # requests to DuckDB.
    create_duckdb_pool = BashOperator(
        task_id="bash_task",
        bash_command="airflow pools list | grep -q 'duckdb' || airflow pools set duckdb 1 'Pool for duckdb'",
        outlets=[start_dataset],
    )


# when using the @dag decorator, the decorated function needs to be
# called after the function definition
start_dag = start()

