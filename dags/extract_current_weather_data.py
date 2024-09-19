"""
## Current weather example DAG 

This example retrieves current weather information and loads it into a DuckDB database.
"""

from airflow import Dataset
from airflow.decorators import (
    dag,
    task,
) # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from pendulum import datetime
import pandas as pd
import logging

# modularize code by importing functions from the include folder
from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c
from include.global_variables import user_input_variables as uv
from include.meterology_utils import (
    get_lat_long_for_cityname,
    get_current_weather_from_city_coordinates,
)

# use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# ------------------ #
# Dataset Definition #
# ------------------ #

start_dataset = Dataset("start")

# -------------- #
# DAG Definition #
# -------------- #

# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2023, 1, 1), # date after which the DAG can be scheduled
    # this DAG runs as soon as the "DS_START" Dataset has been produced
    schedule=[start_dataset], # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False, # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=5, # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args=gv.default_args, # default_args are applied to all tasks in a DAG
    description="DAG that retrieves weather information and loads it into a DuckDB database.",
    tags=["part_1"], # add tags in the UI
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow in production, use a 
    # cloud-based database and based on concurrency capabilities adjust the parameter below.
    concurrency=1, # only allow a single task execution at a time, prevents parallel DuckDB calls
)
def extract_current_weather_data(): # by default the dag_id is the name of the decorated function

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can still use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    @task(retries=2) # you can override default_args at the task level
    def get_lat_long_for_city(city) -> dict: # by default the name of the decorated function is the task_id
        """
        Use the `get_lat_long_for_cityname` function from the local
        `metereology_utils` module to retrieve the coordinates of a city from an API.
        """

        t_log.info("Retrieving latitudinal and longitudinal coordinates for a specified city from an API.")

        city_coordinates = get_lat_long_for_cityname(city)
        
        return city_coordinates

    @task(retries=2)
    def get_current_weather(coordinates, **context) -> list:
        """
        Use the `get_current_weather_from_city_coordinates` function from the local
        `metereology_utils` module to retrieve the current weather in a city
        from the open-meteo API.
        """

        t_log.info("Getting the current weather from an API.")

        # retrieved here from the Airflow context, the logical_date is the date for which the DAG run is scheduled
        logical_date = context["logical_date"]

        city_weather_and_coordinates = get_current_weather_from_city_coordinates(
            coordinates, logical_date
        )

        return city_weather_and_coordinates

    @task(retries=2)
    def turn_json_into_table(
        duckdb_conn_id: str, current_weather_table_name: str, current_weather: list
    ) -> None:
        """
        Convert the JSON input with info about the current weather into a Pandas
        DataFrame and load it into a DuckDB database.
        Args:
            duckdb_conn_id (str): The connection ID for the DuckDB connection.
            current_weather_table_name (str): The name of the table to be created in DuckDB.
            current_weather (list): The JSON input to be loaded into DuckDB.
        """
        
        t_log.info("Converting the current weather info into a Pandas DataFrame.")

        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        current_weather_df = pd.DataFrame(current_weather)

        t_log.info("Creating a table in the DuckDB database.")

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()
        cursor.sql(
            f"CREATE TABLE IF NOT EXISTS {current_weather_table_name} AS SELECT * FROM current_weather_df"
        )

        t_log.info("Loading the weather info into the table.")

        cursor.sql(
            f"INSERT INTO {current_weather_table_name} SELECT * FROM current_weather_df"
        )
        cursor.close()

    # each call of a @task decorated function creates one task in the Airflow UI
    # passing the return value of one @task decorated function to another one
    # automatically creates a task dependency
    turn_json_into_table(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        current_weather_table_name=c.IN_CURRENT_WEATHER_TABLE_NAME,
        current_weather=get_current_weather(get_lat_long_for_city(city=uv.MY_CITY)),
    )

# instantiate the DAG
extract_current_weather_data()
