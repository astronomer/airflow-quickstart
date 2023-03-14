# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
import logging
import os
from minio import Minio
from pendulum import duration
import json

# ----------------------- #
# Configuration variables #
# ----------------------- #

# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
WEATHER_BUCKET_NAME = "weather"
CLIMATE_BUCKET_NAME = "climate"
ARCHIVE_BUCKET_NAME = "archive"
WEBSITE_FOOTPRINT_BUCKET_NAME = "websitecarbon"

# Source files climate data
CLIMATE_DATA_FOLDER_PATH = f"{os.environ['AIRFLOW_HOME']}/include/climate_data/"
TEMP_COUNTRY_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_country.csv"
)
TEMP_GLOBAL_PATH = f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_global.csv"

# Storage paths weather data
CURRENT_WEATHER_DATA_FOLDER_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/current_weather_data/"
)
HISTORICAL_WEATHER_DATA_FOLDER_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/historical_weather_data/"
)

# Storage paths footprint data
FOOTPRINT_DATA_FOLDER_PATH = f"{os.environ['AIRFLOW_HOME']}/include/footprint_data/"

# Datasets
DS_DUCKDB_IN_CURRENT_WEATHER = Dataset("duckdb://in_current_weather")
DS_DUCKDB_IN_HISTORICAL_WEATHER = Dataset("duckdb://in_historical_weather")
DS_DUCKDB_IN_CLIMATE = Dataset("duckdb://in_climate")
DS_DUCKDB_IN_FOOTPRINT = Dataset("duckdb://in_footprint")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")
DS_WEATHER_JSON = Dataset("local://include/weather_data")
DS_FOOTPRINT_JSON = Dataset("local://include/footprint_data")
DS_START = Dataset("start")

# DuckDB config
CONN_ID_DUCKDB = "duckdb_default"
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
IN_CURRENT_WEATHER_TABLE_NAME = "in_current_weather"
IN_HISTORICAL_WEATHER_TABLE_NAME = "in_historical_weather"
IN_FOOTPRINT_TABLE_NAME = "in_footprint"
REPORTING_GLOBAL_CLIMATE_TABLE_NAME = "report_global_climate"
REPORTING_COUNTRY_CLIMATE_TABLE_NAME = "report_country_climate"
REPORTING_TABLE_HISTORICAL_WEATHER = "report_historical_weather"

# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}

# default coordinates
default_coordinates = {"city": "No city provided", "lat": 0, "long": 0}


# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client


# command to run streamlit app within codespaces/docker
# modifications are necessary to support double-port-forwarding
STREAMLIT_COMMAND = "streamlit run weather_v_climate_app.py --server.enableWebsocketCompression=false --server.enableCORS=false"
