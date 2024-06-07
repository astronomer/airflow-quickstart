"""Loads historic climate data form local storage to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c

# -------- #
# Datasets #
# -------- #

start_dataset = Dataset("start")

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the start Dataset has been updated
    schedule=[start_dataset],
    catchup=False,
    default_args=gv.default_args,
    description="Loads historic climate data form local storage to DuckDB.",
    tags=["part_1"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def in_climate_data():

    @task(pool="duckdb", outlets=[Dataset("duckdb://include/dwh/in_climate")])
    def import_climate_data(
        duckdb_conn_id: str, climate_table_name: str, csv_file_path: str
    ):
        """
        Load historic climate data from a CSV file to DuckDB.
        Args:
            duckdb_conn_id (str): The connection ID for the DuckDB connection.
            csv_file_path (str): The path to the CSV file containing the historic climate data.
        """
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook

        duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
        cursor = duckdb_conn.cursor()
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


in_climate_data_dag = in_climate_data()
