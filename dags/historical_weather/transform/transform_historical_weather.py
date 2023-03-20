"""DAG that runs a transformation on data in DuckDB using the Astro SDK"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import pandas as pd

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv
from include.global_variables import constants as c

# ----------------- #
# Astro SDK Queries #
# ----------------- #


# Create a reporting table that counts heat days per year for each city location
@aql.transform(pool="duckdb")
def create_historical_weather_reporting_table(in_table: Table, hot_day_celsius: float):
    return """
        SELECT time, city, temperature_2m_max AS day_max_temperature,
        SUM(
            CASE
            WHEN CAST(temperature_2m_max AS FLOAT) >= {{ hot_day_celsius }} THEN 1
            ELSE 0
            END
        ) OVER(PARTITION BY city, YEAR(CAST(time AS DATE))) AS heat_days_per_year
        FROM {{ in_table }}
    """


# ---------- #
# Exercise 3 #
# ---------- #
# Use pandas to transform the 'historical_weather_reporting_table' into a table
# showing the hottest day in your year of birth (or the closest year, if your year
# of birth is not available for your city). Make sure the function returns a pandas dataframe
# Tip: the returned dataframe will be shown in your streamlit App.

# import a Astro SDK Table as a pandas dataframe by typing the import as pd.DataFrame
@aql.dataframe(pool="duckdb")
def find_hottest_day_birthyear(in_table: pd.DataFrame, birthyear: int):
    # print ingested df to the logs
    gv.task_log.info(in_table)

    output_df = in_table

    ####### YOUR TRANSFORMATION ##########

    # print result table to the logs
    gv.task_log.info(output_df)

    return output_df


# --- #
# DAG #
# --- #

# ---------- #
# Exercise 1 #
# ---------- #
# Schedule this DAG to run as soon as the 'extract_historical_weather_data' DAG has finished running.
# Tip: You can either add your own Dataset as an outlet in the last task of the previous DAG or
# use the a Astro Python SDK Table based Dataset as seen in the 'transform_climate_data' DAG.


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=None,
    catchup=False,
    default_args=gv.default_args,
    description="Runs transformations on climate and current weather data in DuckDB.",
    tags=["part_2"],
)
def transform_historical_weather():

    create_historical_weather_reporting_table(
        in_table=Table(
            name=c.IN_HISTORICAL_WEATHER_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
        hot_day_celsius=uv.HOT_DAY,
        output_table=Table(
            name=c.REPORT_HISTORICAL_WEATHER_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )

    find_hottest_day_birthyear(
        in_table=Table(
            name=c.IN_HISTORICAL_WEATHER_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
        birthyear=uv.BIRTH_YEAR,
        output_table=Table(
            name=c.REPORT_HOT_DAYS_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
    )


transform_historical_weather()
