"""DAG that runs a transformation on data in DuckDB using the Astro SDK"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv

# ----------------- #
# Astro SDK Queries #
# ----------------- #


@aql.transform(pool="duckdb")
def create_global_climate_reporting_table(
    temp_global: Table,
):
    return """
        SELECT CAST(dt AS DATE) AS date, 
        AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))/10*10) AS decade_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))) AS year_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY MONTH(CAST(dt AS DATE))) AS month_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY CAST(dt AS DATE)) AS day_average_temp,
        FROM {{ temp_global }}
    """


@aql.transform(pool="duckdb")
def create_historical_weather_reporting_table(
    in_historical_weather: Table, city: str, hot_day_celsius: float
):
    return """
        SELECT time, city, temperature_2m_max AS day_max_temperature,
        AVG(temperature_2m_max) OVER(PARTITION BY YEAR(CAST(time AS DATE))/10*10) AS decade_average_day_max_temp,
        AVG(temperature_2m_max) OVER(PARTITION BY YEAR(CAST(time AS DATE))) AS year_average_day_max_temp,
        AVG(temperature_2m_max) OVER(PARTITION BY MONTH(CAST(time AS DATE))) AS month_average_day_max_temp,
        SUM(
            CASE
            WHEN CAST(temperature_2m_max AS FLOAT) >= {{ hot_day_celsius }} THEN 1
            ELSE 0
            END
        ) OVER(PARTITION BY YEAR(CAST(time AS DATE))) AS heat_days_per_year
        FROM {{ in_historical_weather }}
        WHERE city = {{city}}
    """


# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=[
        gv.DS_DUCKDB_IN_HISTORICAL_WEATHER,
        gv.DS_DUCKDB_IN_CURRENT_WEATHER,
        gv.DS_DUCKDB_IN_CLIMATE,
        gv.DS_DUCKDB_IN_FOOTPRINT,
    ],
    catchup=False,
    default_args=gv.default_args,
    description="Runs transformations on data in DuckDB using the Astro SDK.",
    tags=["transform", "duckdb"],
)
def transform():

    create_global_climate_reporting_table(
        Table(name="temp_global", conn_id=gv.CONN_ID_DUCKDB),
        output_table=Table(
            name=gv.REPORTING_GLOBAL_CLIMATE_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )

    create_historical_weather_reporting_table(
        Table(name="in_historical_weather", conn_id=gv.CONN_ID_DUCKDB),
        uv.MY_CITY,
        hot_day_celsius=30,
        output_table=Table(
            name=gv.REPORTING_TABLE_HISTORICAL_WEATHER, conn_id=gv.CONN_ID_DUCKDB
        ),
    )

    # clean up temporary tables created
    aql.cleanup()


transform()
