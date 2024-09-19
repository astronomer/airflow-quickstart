"""DAG that runs a transformation on data in DuckDB using the Astro SDK"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import pandas as pd

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv
from include.global_variables import constants as c

# --- #
# DAG #
# --- #

# ---------- #
# Exercise 1 #
# ---------- #
# Schedule this DAG to run as soon as the 'extract_historical_weather_data' DAG has finished running.
# Tip: You will need to use the dataset feature.


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

    @task(
        outlets=[Dataset("duckdb://include/dwh/historical_weather_data")],
    )
    def create_historical_weather_reporting_table(duckdb_conn_id: str, in_table: str, hot_day_celsius: float):
        
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()
        cursor.sql(
            f"SELECT time, city, temperature_2m_max AS day_max_temperature, SUM(CASE WHEN CAST(temperature_2m_max AS FLOAT) >= {hot_day_celsius} THEN 1 ELSE 0 END) OVER(PARTITION BY city, YEAR(CAST(time AS DATE))) AS heat_days_per_year FROM {in_table};"
        )
        cursor.close()

    create_historical_weather_reporting_table(
        duckdb_conn_id=gv.CONN_ID_DUCKDB, 
        in_table=c.IN_HISTORICAL_WEATHER_TABLE_NAME, 
        hot_day_celsius=uv.HOT_DAY
    )

    # ---------- #
    # Exercise 3 #
    # ---------- #
    # Use Pandas to transform the 'historical_weather_reporting_table' into a table
    # showing the hottest day in your year of birth (or the closest year, if your year
    # of birth is not available for your city).
    # Tip: the saved dataframe will be shown in your Streamlit app.

    @task(
        pool="duckdb",
    )
    def find_hottest_day_birthyear(
        duckdb_conn_id: str,
        input_table_name: pd.DataFrame,
        birthyear: int,
        output_table_name: str,
    ):

        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()
        input_df = cursor.sql(
            f"""
            SELECT * FROM {input_table_name}
            """
        ).df()

        ####### YOUR TRANSFORMATION ##########

        output_df = input_df

        # saving the output_df to a new table
        cursor.sql(
            f"CREATE TABLE IF NOT EXISTS {output_table_name} AS SELECT * FROM output_df"
        )
        cursor.sql(f"INSERT INTO {output_table_name} SELECT * FROM output_df")
        cursor.close()

    find_hottest_day_birthyear(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        input_table_name=c.IN_HISTORICAL_WEATHER_TABLE_NAME,
        birthyear=uv.BIRTH_YEAR,
        output_table_name=c.REPORT_HOT_DAYS_TABLE_NAME,
    )


transform_historical_weather()
