"""DAG that retrieves weather information and saves it to a local JSON."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv
from include.meterology_utils import (
    get_lat_long_for_cityname,
    get_current_weather_from_city_coordinates,
    get_historical_weather_from_city_coordinates,
)

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "DS_START" Dataset has been produced to
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves weather information and saves it to a local JSON.",
    tags=["extract", "weather"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def extract_weather():

    # use a python package to get lat/long for a city from its name
    @task
    def get_lat_long_for_city(city):
        city_coordinates = get_lat_long_for_cityname(city)
        return city_coordinates

    # use the open weather API to get the current weather at the provided coordinates
    @task(outlets=[gv.DS_WEATHER_JSON])
    def get_current_weather(coordinates, **context):
        # the logical_date is the date for which the DAG run is scheduled it
        # is retrieved here from the Airflow context
        logical_date = context["logical_date"]
        timestamp_no_dash = context["ts_nodash"]

        city_weather_and_coordinates = get_current_weather_from_city_coordinates(
            coordinates, logical_date
        )

        with open(
            f"{gv.CURRENT_WEATHER_DATA_FOLDER_PATH}{timestamp_no_dash}_{coordinates['city']}_weather.json",
            "w",
        ) as f:
            f.write(json.dumps(city_weather_and_coordinates))

        return city_weather_and_coordinates

    @task()
    def get_historical_weather(coordinates):
        historical_weather_and_coordinates = (
            get_historical_weather_from_city_coordinates(coordinates)
        )

        with open(
            f"{gv.HISTORICAL_WEATHER_DATA_FOLDER_PATH}{coordinates['city']}_historical_weather.json",
            "w",
        ) as f:
            f.write(json.dumps(historical_weather_and_coordinates.to_dict()))

        return historical_weather_and_coordinates.to_dict()

    coordinates = get_lat_long_for_city.expand(city=[uv.MY_CITY])
    get_current_weather.expand(coordinates=coordinates)
    get_historical_weather.expand(coordinates=coordinates)


extract_weather()
