"""Loads historic climate data form local storage to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime
import os

# import tools from the Astro SDK
from astro.sql.table import Table
from astro.files import get_file_list
from astro.sql.operators.load_file import LoadFileOperator

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the start Dataset has been updated
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Loads historic climate data form local storage to DuckDB.",
    tags=["load", "DuckDB", "climate"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def in_climate_data():
    @task
    def list_file_names():
        return os.listdir(gv.CLIMATE_DATA_FOLDER_PATH)

    @task
    def get_kwargs(file_names, path_names):
        list_of_dicts = []
        for file_name, path_name in zip(file_names, path_names):
            table_name = file_name.split(".")[-2]
            kwarg_dict = {
                "output_table": Table(conn_id=gv.CONN_ID_DUCKDB, name=table_name),
                "input_file": path_name,
            }
            list_of_dicts.append(kwarg_dict)
        return list_of_dicts

    import_climate_data = LoadFileOperator.partial(
        task_id="import_climate_data",
        if_exists="replace",
        max_active_tis_per_dag=1,
        pool="duckdb",
        outlets=[gv.DS_DUCKDB_IN_CLIMATE]
    ).expand_kwargs(
        get_kwargs(
            list_file_names(),
            get_file_list(
                path=gv.CLIMATE_DATA_FOLDER_PATH, conn_id="local_file_default"
            ),
        )
    )


in_climate_data_dag = in_climate_data()
