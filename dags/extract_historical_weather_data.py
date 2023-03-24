"""DAG that retrieves weather information and saves it to a local JSON."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

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

    # use the @aql.dataframe decorated function to write the (list of) JSON(s) returned from
    # the get_current_weather task as a permanent table to DuckDB
    turn_json_into_table(
        in_json=historical_weather,
        output_table=Table(
            name=c.IN_HISTORICAL_WEATHER_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )


extract_historical_weather_data()
