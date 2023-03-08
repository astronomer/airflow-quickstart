"""DAG that queries and ingests local weather data from an API to MinIO."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from pendulum import datetime
from geopy.geocoders import Nominatim
from geopy.adapters import AdapterHTTPError
import requests
import io
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "start" Dataset has been produced to
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Queries and ingests local weather data from an API to MinIO.",
    tags=["ingestion", "minio"],
    render_template_as_native_obj=True
)
def in_local_weather():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_weather_bucket",
        bucket_name=gv.WEATHER_BUCKET_NAME
    )

    @task
    def get_lat_long_for_city(city):
        """Converts a string of a city name provided into
        lat/long coordinates."""

        geolocator = Nominatim(user_agent="MyApp")

        try:
            location = geolocator.geocode(city)
            lat = location.latitude
            long = location.longitude

            # log the coordinates retrieved
            gv.task_log.info(
                f"Coordinates for {city}: {lat}/{long}"
            )

        # if the coordinates cannot be retrieved log a warning
        except (AttributeError, KeyError, ValueError, AdapterHTTPError) as err:
            gv.task_log.warn(
                f"""Coordinates for {city}: could not be retrieved.
                Error: {err}"""
            )
            lat = "NA"
            long = "NA"

        city_coordinates = {"city": city, "lat": lat, "long": long}

        # save the coordinates in the include folder to be access by the streamlit app
        with open("include/coordinates.json", 'w') as f:
            f.write(json.dumps(city_coordinates))

        return city_coordinates

    @task(
        templates_dict={"logical_date": "{{ ds }}"}
    )
    def get_current_weather(coordinates, **kwargs):
        """Queries an open weather API for the current weather at the
        coordinates provided."""

        lat = coordinates["lat"]
        long = coordinates["long"]
        city = coordinates["city"]
        logical_date = kwargs["templates_dict"]["logical_date"]

        r = requests.get(
            f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}&current_weather=true"
        )

        # if the API call is successful log the current temp
        if r.status_code == 200:
            current_weather = r.json()["current_weather"]

            gv.task_log.info(
                "The current temperature in {0} is {1}°C".format(
                    city,
                    current_weather["temperature"]
                )
            )

        # if the API call is not successful, log a warning
        else:
            current_weather = {
                "temperature": "NULL",
                "windspeed": "NULL",
                "winddirection": "NULL",
                "weathercode": "NULL",
                "time": f"{logical_date}"
            }

            gv.task_log.warn(
                f"""
                    Could not retrieve current temperature for {city} at
                    {lat}/{long} from https://api.open/meteo.com.
                    Request returned {r.status_code}.
                """
            )

        return {
            "city": city,
            "current_weather": current_weather,
            "API_response": r.status_code
        }
    
    write_current_weather_to_minio = LocalFilesystemToMinIOOperator(
        task_id="write_current_weather_to_minio",
        minio_ip=gv.MINIO_IP,
        bucket_name=gv.WEATHER_BUCKET_NAME,
        object_name="test.json",
        json_serializeable_information="{{ti.xcom_pull(task_ids='get_current_weather')}}"
    )


    # set dependencies
    coordinates = get_lat_long_for_city(uv.MY_CITY)
    current_weather = get_current_weather(coordinates)
    create_bucket_tg >> current_weather >>  write_current_weather_to_minio


in_local_weather()
