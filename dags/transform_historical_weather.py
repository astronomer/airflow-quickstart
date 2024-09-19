"""
Historical weather data transformation example DAG

This example runs a transformation on data in a DuckDB database.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import pandas as pd
import logging

# modularize code by importing functions from the include folder
from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv
from include.global_variables import constants as c

# use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# -------------- #
# DAG Definition #
# -------------- #

# ---------- #
# Exercise 1 #
# ---------- #
# Schedule this DAG to run as soon as the 'extract_historical_weather_data' DAG has finished running.
# Tip: You will need to use the dataset feature.

# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2023, 1, 1), # date after which the DAG can be scheduled
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=None, # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,
    max_consecutive_failed_dag_runs=5, # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args=gv.default_args,
    description="Runs transformations on climate and current weather data in DuckDB.",
    tags=["part_2"], # add tags in the UI,
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow in production, use a 
    # cloud-based database and based on concurrency capabilities adjust the parameter below.
    concurrency=1, # only allow a single task execution at a time, prevents parallel DuckDB calls
)
def transform_historical_weather(): # by default the dag_id is the name of the decorated function

    # --------------- #
    # Task Definition #
    # --------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can still use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    @task(
        outlets=[Dataset("duckdb://include/dwh/historical_weather_data")],
    )
    # by default the name of the decorated function is the task_id
    def create_historical_weather_reporting_table(duckdb_conn_id: str, in_table: str, hot_day_celsius: float) -> None:
        
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()

        t_log.info("Creating a table for the weather data in the database.")

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
    ) -> None:

        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()

        t_log.info("Extracting historical weather data from the database.")

        input_df = cursor.sql(
            f"""
            SELECT * FROM {input_table_name}
            """
        ).df()

        ####### YOUR TRANSFORMATION ##########

        output_df = input_df

        t_log.info("Loading the extracted data into a new table.")

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

# instantiate the DAG
transform_historical_weather()
