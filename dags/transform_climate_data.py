"""DAG that runs a transformation on data in DuckDB"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=[Dataset("duckdb://include/dwh/in_climate")],
    catchup=False,
    default_args=gv.default_args,
    description="Runs a transformation on climate data in DuckDB.",
    tags=["part_1"],
)
def transform_climate_data():

    @task(
        outlets=[Dataset("duckdb://include/dwh/report_climate")]
    )
    def create_global_climate_reporting_table(in_climate: str, output_table: str, duckdb_conn_id: str):
        """Run a SQL transformation on the 'in_climate' table in order to create averages over different time periods"""
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()
        cursor.sql(
            f"CREATE TABLE IF NOT EXISTS {output_table} AS SELECT * FROM {in_climate};"
        )
        cursor.sql(
            f"INSERT INTO {output_table} SELECT CAST(dt AS DATE) AS date, AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))/10*10) AS decade_average_temp, AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))) AS year_average_temp, AVG(LandAverageTemperature) OVER(PARTITION BY MONTH(CAST(dt AS DATE))) AS month_average_temp, AVG(LandAverageTemperature) OVER(PARTITION BY CAST(dt AS DATE)) AS day_average_temp, FROM {in_climate};"
        )
        cursor.close()

    create_global_climate_reporting_table(
        in_climate=c.IN_CLIMATE_TABLE_NAME, 
        output_table=c.REPORT_CLIMATE_TABLE_NAME, 
        duckdb_conn_id=gv.CONN_ID_DUCKDB
    )

transform_climate_data()
