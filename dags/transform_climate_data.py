"""
Climate data transformation example DAG

This example runs a transformation on data in a DuckDB database.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
import logging

# modularize code by importing functions from the include folder
from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c

# use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# -------------- #
# DAG Definition #
# -------------- #

# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2023, 1, 1), # date after which the DAG can be scheduled
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=[Dataset("duckdb://include/dwh/in_climate")], # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,
    max_consecutive_failed_dag_runs=5, # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args=gv.default_args,
    description="Runs a transformation on climate data in DuckDB.",
    tags=["part_1"], # add tags in the UI
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow in production, use a 
    # cloud-based database and based on concurrency capabilities adjust the parameter below.
    concurrency=1, # only allow a single task execution at a time, prevents parallel DuckDB calls
)
def transform_climate_data(): # by default the dag_id is the name of the decorated function

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
        outlets=[Dataset("duckdb://include/dwh/report_climate")]
    )
    # by default the name of the decorated function is the task_id
    def create_global_climate_reporting_table(in_climate: str, output_table: str, duckdb_conn_id: str) -> None:
        """Run a SQL transformation on the 'in_climate' table in order to create averages over different time periods"""
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()

        t_log.info("Creating a table for the climate data in the database.")

        cursor.sql(
            f"CREATE TABLE IF NOT EXISTS {output_table} AS SELECT * FROM {in_climate};"
        )

        t_log.info("Performing a transformation on the data.")

        cursor.sql(
            f"INSERT INTO {output_table} SELECT CAST(dt AS DATE) AS date, AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))/10*10) AS decade_average_temp, AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))) AS year_average_temp, AVG(LandAverageTemperature) OVER(PARTITION BY MONTH(CAST(dt AS DATE))) AS month_average_temp, AVG(LandAverageTemperature) OVER(PARTITION BY CAST(dt AS DATE)) AS day_average_temp, FROM {in_climate};"
        )
        cursor.close()

    create_global_climate_reporting_table(
        in_climate=c.IN_CLIMATE_TABLE_NAME, 
        output_table=c.REPORT_CLIMATE_TABLE_NAME, 
        duckdb_conn_id=gv.CONN_ID_DUCKDB
    )

# instantiate the DAG
transform_climate_data()
