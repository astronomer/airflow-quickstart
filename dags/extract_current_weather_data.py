"""DAG that retrieves current weather information and loads it into DuckDB."""

# --------------- #
# Package imports #
# --------------- #

from airflow import Dataset
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
from include.global_variables import user_input_variables as uv
from include.meterology_utils import (
    get_lat_long_for_cityname,
    get_current_weather_from_city_coordinates,
)

# -------- #
# Datasets #
# -------- #

start_dataset = Dataset("start")

# --- #
# DAG #
# --- #

# a dataframe decorated function turns a returned Pandas dataframe into
# a Astro SDK Table object
@aql.dataframe(pool="duckdb")
def turn_json_into_table(in_json):
    return pd.DataFrame(in_json)


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "DS_START" Dataset has been produced to
    schedule=[start_dataset],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves weather information and saves it to a local JSON.",
    tags=["part_1"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def extract_current_weather_data():
    @task
    def get_lat_long_for_city(city):
        """Use the 'get_lat_long_for_cityname' function from the local
        'metereology_utils' module to retrieve the coordinates of of a city."""

        city_coordinates = get_lat_long_for_cityname(city)
        return city_coordinates

    # use the open weather API to get the current weather at the provided coordinates
    @task
    def get_current_weather(coordinates, **context):
        """Use the 'get_current_weather_from_city_coordinates' function from the local
        'metereology_utils' module to retrieve the current weather in a city
        from the open-meteo API."""

        # the logical_date is the date for which the DAG run is scheduled it
        # is retrieved here from the Airflow context
        logical_date = context["logical_date"]

        city_weather_and_coordinates = get_current_weather_from_city_coordinates(
            coordinates, logical_date
        )

        return city_weather_and_coordinates

    # set dependencies to get current weather
    current_weather = get_current_weather(get_lat_long_for_city(city=uv.MY_CITY))

    # use the @aql.dataframe decorated function to write the JSON returned from
    # the get_current_weather task as a permanent table to DuckDB
    turn_json_into_table(
        current_weather,
        output_table=Table(
            name=c.IN_CURRENT_WEATHER_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )


extract_current_weather_data()
