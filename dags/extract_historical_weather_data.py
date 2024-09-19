"""
Historical weather example DAG

This example retrieves historical weather data from an API loads it into a DuckDB database.
"""

from airflow.decorators import (
    dag,
    task,
) # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.datasets import Dataset
from pendulum import datetime
import pandas as pd
from airflow.io.path import ObjectStoragePath
import logging

# modularize code by importing functions from the include folder
from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c
from include.meterology_utils import (
    get_lat_long_for_cityname,
    get_historical_weather_from_city_coordinates,
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

# ---------- #
# Exercise 1 #
# ---------- #
# Schedule this DAG to run as soon as the 'start' DAG has finished running.
# Tip: Look at how the 'extract_current_weather_data' DAG is scheduled.

# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2023, 1, 1), # date after which the DAG can be scheduled
    schedule=None, # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False, # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=5, # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args=gv.default_args, # default_args are applied to all tasks in a DAG
    description="DAG that retrieves weather information and saves it to a local JSON.",
    tags=["part_2"], # add tags in the UI
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow in production, use a 
    # cloud-based database and based on concurrency capabilities adjust the parameter below.
    concurrency=1, # only allow a single task execution at a time, prevents parallel DuckDB calls
)
def extract_historical_weather_data(): # by default the dag_id is the name of the decorated function

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
        `metereology_utils` module to retrieve the coordinates of a city.
        """

        t_log.info("Retrieving latitudinal and longitudinal coordinates for a specified city from an API.")

        city_coordinates = get_lat_long_for_cityname(city)

        return city_coordinates

    @task(retries=2)
    def get_historical_weather(coordinates) -> dict:
        """
        Use the `get_historical_weather_from_city_coordinates` function from the local
        `metereology_utils` module to retrieve the historical weather in a city
        from the open-meteo API.
        """

        t_log.info("Retrieving historical weather data for the city from an API.")

        historical_weather_and_coordinates = (
            get_historical_weather_from_city_coordinates(coordinates)
        )

        return historical_weather_and_coordinates.to_dict()

    # ---------- #
    # Exercise 2 #
    # ---------- #
    # Modify the following code so that both the `get_lat_long_for_city` task
    # and the `get_historical_weather` task run on a whole list of cities. Choose 3-5 cities
    # to retrieve historical weather data for.
    # Tip: this task can be accomplished by using Dynamic Task Mapping and you only need to 
    # modify two lines of code.

    coordinates = get_lat_long_for_city(city="Bern")
    historical_weather = get_historical_weather(coordinates=coordinates)

    @task(
        outlets=[Dataset("duckdb://include/dwh/historical_weather_data")],
    )
    def turn_json_into_table(
        duckdb_conn_id: str,
        historical_weather_table_name: str,
        historical_weather: dict,
    ) -> None:
        """
        Convert the JSON input with info about historical weather into a pandas
        DataFrame and load it into DuckDB.
        Args:
            duckdb_conn_id (str): The connection ID for the DuckDB connection.
            historical_weather_table_name (str): The name of the table to store the historical weather data.
            historical_weather (list): The historical weather data to load into DuckDB.
        """
        
        t_log.info("Converting the current weather info into a Pandas DataFrame.")

        from duckdb_provider.hooks.duckdb_hook import DuckDBHook
        from airflow.models.xcom import LazyXComAccess

        if type(historical_weather) == LazyXComAccess:
            list_of_df = []

            for item in historical_weather:
                df = pd.DataFrame(item)
                list_of_df.append(df)

            historical_weather_df = pd.concat(list_of_df, ignore_index=True)
        else:
            historical_weather_df = pd.DataFrame(historical_weather)

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()

        t_log.info("Creating a table in the DuckDB database.")

        cursor.sql(
            f"CREATE TABLE IF NOT EXISTS {historical_weather_table_name} AS SELECT * FROM historical_weather_df;"
        )

        t_log.info("Loading the weather info into the table.")

        cursor.sql(
            f"INSERT INTO {historical_weather_table_name} SELECT * FROM historical_weather_df;"
        )
        cursor.close()

    turn_json_into_table(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        historical_weather_table_name=c.IN_HISTORICAL_WEATHER_TABLE_NAME,
        historical_weather=historical_weather,
    )

# instantiate the DAG
extract_historical_weather_data()
