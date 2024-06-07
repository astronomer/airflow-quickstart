"""DAG that retrieves weather information and saves it to duckdb."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import pandas as pd

# import tools from the Astro SDK
from astro import sql as aql

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c
from include.meterology_utils import (
    get_lat_long_for_cityname,
    get_historical_weather_from_city_coordinates,
)

# --- #
# DAG #
# --- #


@aql.dataframe(pool="duckdb")
def turn_json_into_table(in_json):
    """Converts the list of JSON input into one pandas dataframe."""
    if type(in_json) == dict:
        df = pd.DataFrame(in_json)
    else:
        df = pd.concat([pd.DataFrame(d) for d in in_json], ignore_index=True)
    return df


# ---------- #
# Exercise 1 #
# ---------- #
# Schedule this DAG to run as soon as the 'start' DAG has finished running.
# Tip: Look at how the 'extract_current_weather_data' DAG is scheduled.


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves weather information and saves it to a local JSON.",
    tags=["part_2"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def extract_historical_weather_data():
    @task
    def get_lat_long_for_city(city):
        """Use the 'get_lat_long_for_cityname' function from the local
        'metereology_utils' module to retrieve the coordinates of a city."""

        city_coordinates = get_lat_long_for_cityname(city)
        return city_coordinates

    @task
    def get_historical_weather(coordinates):
        """Use the 'get_historical_weather_from_city_coordinates' function from the local
        'metereology_utils' module to retrieve the historical weather in a city
        from the open-meteo API."""

        historical_weather_and_coordinates = (
            get_historical_weather_from_city_coordinates(coordinates)
        )

        return historical_weather_and_coordinates.to_dict()

    # ---------- #
    # Exercise 2 #
    # ---------- #
    # Modify the following two lines of code so that both the 'get_lat_long_for_city' task
    # and the 'get_historical_weather' run on a whole list of cities. Choose 3-5 cities
    # to retrieve historical weather data for.
    # Tip: This task can be accomplished by using Dynamic Task Mapping and you only need to modify two lines of code.

    coordinates = get_lat_long_for_city(city="Bern")
    historical_weather = get_historical_weather(coordinates=coordinates)

    @task(
        outlets=[Dataset("duckdb://include/dwh/historical_weather_data")],
    )
    def turn_json_into_table(
        duckdb_conn_id: str,
        historical_weather_table_name: str,
        historical_weather: dict,
    ):
        """
        Convert the JSON input with info about historical weather into a pandas
        DataFrame and load it into DuckDB.
        Args:
            duckdb_conn_id (str): The connection ID for the DuckDB connection.
            historical_weather_table_name (str): The name of the table to store the historical weather data.
            historical_weather (list): The historical weather data to load into DuckDB.
        """
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        if type(historical_weather) == list:
            list_of_df = []

            for item in historical_weather:
                df = pd.DataFrame(item)
                list_of_df.append(df)

            historical_weather_df = pd.concat(list_of_df, ignore_index=True)
        else:
            historical_weather_df = pd.DataFrame(historical_weather)

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()
        cursor.sql(
            f"CREATE TABLE IF NOT EXISTS {historical_weather_table_name} AS SELECT * FROM historical_weather_df"
        )
        cursor.sql(
            f"INSERT INTO {historical_weather_table_name} SELECT * FROM historical_weather_df"
        )
        cursor.close()

    turn_json_into_table(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        historical_weather_table_name=c.IN_HISTORICAL_WEATHER_TABLE_NAME,
        historical_weather=historical_weather,
    )


extract_historical_weather_data()
