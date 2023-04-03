"""Loads historic climate data form local storage to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
from airflow.decorators import dag
from pendulum import datetime

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table
from astro.files import File

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

    # use the Astro SDK load_file function to load the climate data from
    # the local CSV file to a table in DuckDB
    aql.load_file(
        task_id="import_climate_data",
        input_file=File(gv.CLIMATE_DATA_PATH, conn_id="local_file_default"),
        output_table=Table(c.IN_CLIMATE_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
        if_exists="replace",
        pool="duckdb",
    )


in_climate_data_dag = in_climate_data()
