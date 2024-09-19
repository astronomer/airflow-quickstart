"""
Historical climate data example DAG 

This example loads historical climate data from local storage into a DuckDB database.
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import logging

# modularize code by importing functions from the include folder
from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c

# use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# ------------------- #
# Dataset Definition #
# ------------------- #

start_dataset = Dataset("start")

# -------------- #
# DAG Definition #
# -------------- #

# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2023, 1, 1), # date after which the DAG can be scheduled
    # this DAG runs as soon as the start Dataset has been updated
    schedule=[start_dataset], # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False, # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=5, # auto-pauses the DAG after 5 consecutive failed runs, experimental
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args=gv.default_args, # default_args are applied to all tasks in a DAG
    description="Loads historic climate data form local storage to DuckDB.",
    tags=["part_1"], # add tags in the UI
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow in production, use a 
    # cloud-based database and based on concurrency capabilities adjust the parameter below.
    concurrency=1, # only allow a single task execution at a time, prevents parallel DuckDB calls
)
def in_climate_data(): # by default the dag_id is the name of the decorated function

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can still use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    # by default the name of the decorated function is the task_id
    @task(pool="duckdb", outlets=[Dataset("duckdb://include/dwh/in_climate")])
    def import_climate_data(
        duckdb_conn_id: str, climate_table_name: str, csv_file_path: str
    ):
        """
        Load historical climate data from a CSV file to the DuckDB database.
        Args:
            duckdb_conn_id (str): The connection ID for the DuckDB connection.
            csv_file_path (str): The path to the CSV file containing the historic climate data.
        """
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook


        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()

        t_log.info("Creating a table in the DuckDB database for the weather data.")

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {climate_table_name} (
                dt DATE,
                LandAverageTemperature DOUBLE,
                LandAverageTemperatureUncertainty DOUBLE,
                LandMaxTemperature DOUBLE,
                LandMaxTemperatureUncertainty DOUBLE,
                LandMinTemperature DOUBLE,
                LandMinTemperatureUncertainty DOUBLE,
                LandAndOceanAverageTemperature DOUBLE,
                LandAndOceanAverageTemperatureUncertainty DOUBLE
            );
            """
        )

        t_log.info("Loading data from a CSV file to the DuckDB database.")

        cursor.execute(
            """
            INSERT INTO CLIMATE_TABLE_NAME
            SELECT * FROM read_csv('CSV_FILE_PATH',
            delim = ',',
            header = true,
            columns = {
                'dt': 'DATE',
                'LandAverageTemperature': 'DOUBLE',
                'LandAverageTemperatureUncertainty': 'DOUBLE',
                'LandMaxTemperature': 'DOUBLE',
                'LandMaxTemperatureUncertainty': 'DOUBLE',
                'LandMinTemperature': 'DOUBLE',
                'LandMinTemperatureUncertainty': 'DOUBLE',
                'LandAndOceanAverageTemperature': 'DOUBLE',
                'LandAndOceanAverageTemperatureUncertainty': 'DOUBLE'
            });
            """.replace(
                "CSV_FILE_PATH", csv_file_path
            ).replace(
                "CLIMATE_TABLE_NAME", climate_table_name
            )
        )

    import_climate_data(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        climate_table_name=c.IN_CLIMATE_TABLE_NAME,
        csv_file_path=gv.CLIMATE_DATA_PATH,
    )

# instantiate the DAG
in_climate_data()
