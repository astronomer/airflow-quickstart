"""DAG that loads weather information from local JSON to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv

# import tools from the Astro SDK
from astro.sql.table import Table
from astro.files import get_file_list
from astro.sql.operators.load_file import LoadFileOperator

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "DS_WEATHER_JSON" Dataset has been produced to
    schedule=[gv.DS_WEATHER_JSON],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that loads weather information from local JSON to DuckDB.",
    tags=["load", "DuckDB", "weather"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def in_historical_weather():
    @task
    def get_kwargs(path_names):
        list_of_dicts = []
        for path_name in path_names:
            kwarg_dict = {
                "output_table": Table(
                    conn_id=gv.CONN_ID_DUCKDB, name=gv.IN_HISTORICAL_WEATHER_TABLE_NAME
                ),
                "input_file": path_name,
            }
            list_of_dicts.append(kwarg_dict)
        return list_of_dicts

    # set dependencies

    list_of_path_names = get_file_list(
        path=gv.HISTORICAL_WEATHER_DATA_FOLDER_PATH, conn_id="local_file_default"
    )

    kwargs = get_kwargs(list_of_path_names)

    # import data from local JSON(s) to duckdb

    import_weather_data = LoadFileOperator.partial(
        task_id="import_weather_data",
        if_exists="append",
        max_active_tis_per_dag=1,
        pool="duckdb",
        outlets=[gv.DS_DUCKDB_IN_HISTORICAL_WEATHER]
    ).expand_kwargs(kwargs)


in_historical_weather()
