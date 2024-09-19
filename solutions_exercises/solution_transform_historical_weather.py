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
# Tip: You will need to use the Dataset feature.


@dag(
    start_date=datetime(2023, 1, 1),
    # SOLUTION: Run this DAG as soon as the historical weather data table is updated
    schedule=[Dataset("duckdb://include/dwh/historical_weather_data")],
    catchup=False,
    default_args=gv.default_args,
    description="Runs transformations on climate and current weather data in DuckDB.",
    tags=["part_2", "solution"],
)
def solution_transform_historical_weather():

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
    # One possible solution of using Pandas to find the hottest day of the year
    # for a given birth year.

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

        df = input_df

        # select the data from one year
        try:
            df_birthyear = df[df["time"].str.startswith(str(birthyear))]
        except:
            # if my birthyear is not available, use the year 2022
            df_birthyear = df[df["time"].str.startswith(str("2022"))]

        # group the data by city, use an apply function to find the row with the highest temperature
        output_df = (
            df_birthyear.groupby("city")
            .apply(lambda x: x.loc[x["temperature_2m_max"].idxmax()])
            .reset_index(drop=True)
        )

        # rename columns
        output_df.columns = [
            "Date hottest day",
            "°C hottest day",
            "City",
            "lat",
            "long",
        ]

        # select columns shown in output table
        output_df = output_df[["Date hottest day", "City", "°C hottest day"]]

        # saving the output_df to a new table
        cursor.sql(
            f"CREATE OR REPLACE TABLE {output_table_name} AS SELECT * FROM output_df"
        )
        cursor.sql(f"INSERT INTO {output_table_name} SELECT * FROM output_df")
        cursor.close()

    find_hottest_day_birthyear(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        input_table_name=c.IN_HISTORICAL_WEATHER_TABLE_NAME,
        birthyear=uv.BIRTH_YEAR,
        output_table_name=c.REPORT_HOT_DAYS_TABLE_NAME,
    )


solution_transform_historical_weather()
