"""
## Galaxies ETL example DAG

This example demonstrates an ETL pipeline using Airflow.
The pipeline mocks data extraction for data about galaxies using a modularized
function, filters the data based on the distance from the Milky Way, and loads the
filtered data into a DuckDB database.
"""

from airflow.decorators import (
    dag,
    task,
)  # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from pendulum import datetime, duration
from tabulate import tabulate
import pandas as pd
import duckdb
import logging
import os

# Modularize code by importing functions from the include folder
from include.custom_functions.galaxy_functions import get_galaxy_data

# Use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# ---------------------------------------------------------------------------- #
# Exercise 1: Make an Airflow param customizable using an environment variable #
# ---------------------------------------------------------------------------- #
# Add an environment variable to allow a user to set a different value for the
# `_CLOSENESS_THRESHOLD_LY_PARAMETER` while supplying a default value.
# Hint 1: don't forget to replace the hard-coded value of 500000 currently being
# passed to the `Param()` function in `params`. See the DAG definition below.
# Hint 2: you can use the same format for the environment variable as the one
# used to define `_NUM_GALAXIES_TOTAL`.

# Define variables used in a DAG as environment variables in .env for your whole Airflow instance
# to standardize your DAGs
_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "galaxy_data")
_DUCKDB_TABLE_URI = f"duckdb://{_DUCKDB_INSTANCE_NAME}/{_DUCKDB_TABLE_NAME}"
_CLOSENESS_THRESHOLD_LY_DEFAULT = os.getenv("CLOSENESS_THRESHOLD_LY_DEFAULT", 500000)
_CLOSENESS_THRESHOLD_LY_PARAMETER_NAME = "closeness_threshold_light_years"
_NUM_GALAXIES_TOTAL = os.getenv("NUM_GALAXIES_TOTAL", 20)

# -------------- #
# DAG Definition #
# -------------- #


# Instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
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
    params={  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
        _CLOSENESS_THRESHOLD_LY_PARAMETER_NAME: Param(
            500000,
            type="number",
            title="Galaxy Closeness Threshold",
            description="Set how close galaxies need ot be to the milkyway in order to be loaded to DuckDB.",
        )
    },
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow into production, use a
    # cloud-based database and, based on concurrency capabilities, adjust the two parameters below.
    concurrency=1, # allow only a single task execution at a time, prevents parallel DuckDB calls
    is_paused_upon_creation=False, # start running the DAG as soon as it's created
)
def example_etl_galaxies():  # By default, the dag_id is the name of the decorated function

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # The @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag-decorated
    # function is automatically added to the DAG.
    # If one exists for your use case, you can still use traditional Airflow operators
    # and mix them with @task decorators. Check out registry.astronomer.io for available operators.
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    @task(retries=2)  # You can override default_args at the task level
    def create_galaxy_table_in_duckdb(  # By default, the name of the decorated function is the task_id
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ) -> None:
        """
        Create a table in DuckDB to store galaxy data.
        This task simulates a setup step in an ETL pipeline.
        Args:
            duckdb_instance_name: The name of the DuckDB instance.
            table_name: The name of the table to be created.
        """

        t_log.info("Creating galaxy table in DuckDB.")

        cursor = duckdb.connect(duckdb_instance_name)

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                name STRING PRIMARY KEY,
                distance_from_milkyway INT,
                distance_from_solarsystem INT,
                type_of_galaxy STRING,
                characteristics STRING
            )"""
        )
        cursor.close()

        t_log.info(f"Table {table_name} created in DuckDB.")

    @task
    def extract_galaxy_data(num_galaxies: int = _NUM_GALAXIES_TOTAL) -> pd.DataFrame:
        """
        Retrieve data about galaxies.
        This task simulates an extraction step in an ETL pipeline.
        Args:
            num_galaxies (int): The number of galaxies for which data should be returned.
            Default is 20. Maximum is 20.
        Returns:
            pd.DataFrame: A DataFrame containing data about galaxies.
        """

        galaxy_df = get_galaxy_data(num_galaxies)

        return galaxy_df

    @task
    def transform_galaxy_data(galaxy_df: pd.DataFrame, **context):
        """
        Filter the galaxy data based on the distance from the Milky Way.
        This task simulates a transformation step in an ETL pipeline.
        Args:
            closeness_threshold_light_years (int): The threshold for filtering
            galaxies based on distance.
            Default is 500,000 light years.
        Returns:
            pd.DataFrame: A DataFrame containing filtered galaxy data.
        """

        # retrieve param values from the context
        closeness_threshold_light_years = context["params"][
            _CLOSENESS_THRESHOLD_LY_PARAMETER_NAME
        ]

        t_log.info(
            f"Filtering for galaxies closer than {closeness_threshold_light_years} light years."
        )

        filtered_galaxy_df = galaxy_df[
            galaxy_df["distance_from_milkyway"] < closeness_threshold_light_years
        ]

        return filtered_galaxy_df

    @task(
        outlets=[Dataset(_DUCKDB_TABLE_URI)]
    )  # Define that this task produces updates to an Airflow Dataset.
    # Downstream DAGs can be scheduled based on combinations of Dataset updates
    # coming from tasks in the same Airflow instance or calls to the Airflow API.
    # See: https://www.astronomer.io/docs/learn/airflow-datasets
    def load_galaxy_data(
        filtered_galaxy_df: pd.DataFrame,
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ):
        """
        Load the filtered galaxy data into a DuckDB database.
        This task simulates a loading step in an ETL pipeline.
        Args:
            filtered_galaxy_df (pd.DataFrame): The filtered galaxy data to be loaded.
            duckdb_instance_name (str): The name of the DuckDB instance.
            table_name (str): The name of the table to load the data into.
        """

        t_log.info("Loading galaxy data into DuckDB.")
        cursor = duckdb.connect(duckdb_instance_name)
        cursor.sql(
            f"INSERT OR IGNORE INTO {table_name} BY NAME SELECT * FROM filtered_galaxy_df;"
        )
        t_log.info("Galaxy data loaded into DuckDB.")

    @task
    def print_loaded_galaxies(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ):
        """
        Get the galaxies stored in the DuckDB database that were filtered
        based on closeness to the Milky Way and print them to the logs.
        Args:
            duck_db_conn_id (str): The connection ID for the duckdb database
            where the table is stored.
        Returns:
            pd.DataFrame: A DataFrame containing the galaxies closer than
            500,000 light years from the Milky Way.
        """

        cursor = duckdb.connect(duckdb_instance_name)
        near_galaxies_df = cursor.sql(f"SELECT * FROM {table_name};").df()
        near_galaxies_df = near_galaxies_df.sort_values(
            by="distance_from_milkyway", ascending=True
        )
        t_log.info(tabulate(near_galaxies_df, headers="keys", tablefmt="pretty"))

    # ------------- #
    # Calling tasks #
    # ------------- #

    # Each call of a @task-decorated function creates one task in the Airflow UI.
    # Passing the return value of one @task-decorated function to another one
    # automatically creates a task dependency.
    create_galaxy_table_in_duckdb_obj = create_galaxy_table_in_duckdb()
    extract_galaxy_data_obj = extract_galaxy_data()
    transform_galaxy_data_obj = transform_galaxy_data(extract_galaxy_data_obj)
    load_galaxy_data_obj = load_galaxy_data(transform_galaxy_data_obj)


    # -------------------------------- #
    # Exercise 2: Setting dependencies #
    # -------------------------------- #
    # You can set explicit dependencies using the chain function or bit-shift operators.
    # Replace the bit-shift approach taken here with a chain function.
    # See: https://www.astronomer.io/docs/learn/managing-dependencies

    create_galaxy_table_in_duckdb_obj >> load_galaxy_data_obj >> print_loaded_galaxies()


# Instantiate the DAG
example_etl_galaxies()
