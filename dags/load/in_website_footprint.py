"""DAG that loads carbon footprint information from local JSON to DuckDB."""

from airflow.decorators import dag, task
from pendulum import datetime

from include.global_variables import airflow_conf_variables as gv

# import tools from the Astro SDK
from astro.sql.table import Table
from astro.files import get_file_list
from astro.sql.operators.load_file import LoadFileOperator

# ---------------- #
# Exercise Part 1  #
# ---------------- #

# TASK: provide a list of URLs you want to get carbon footprint information on.
# please only test public URLs, a public report will be generated, learn
# more at https://www.websitecarbon.com/
list_of_websites = ["https://www.astronomer.io/", "https://duckdb.org/"]


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_FOOTPRINT_JSON],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that loads carbon footprint information from local JSON to DuckDB.",
    tags=["load", "DuckDB", "footprint"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def in_website_footprint():

    @task
    def get_kwargs(path_names):
        list_of_dicts = []
        for path_name in path_names:
            kwarg_dict = {
                "output_table": Table(
                    conn_id=gv.CONN_ID_DUCKDB, name=gv.IN_FOOTPRINT_TABLE_NAME
                ),
                "input_file": path_name,
            }
            list_of_dicts.append(kwarg_dict)
        return list_of_dicts

    # set dependencies

    list_of_path_names = get_file_list(
        path=gv.FOOTPRINT_DATA_FOLDER_PATH, conn_id="local_file_default"
    )

    kwargs = get_kwargs(list_of_path_names)

    # import data from local JSON(s) to duckdb

    import_footprint_data = LoadFileOperator.partial(
        task_id="import_footprint_data",
        if_exists="append",
        max_active_tis_per_dag=1,
         pool="duckdb",
        outlets=[gv.DS_DUCKDB_IN_FOOTPRINT]
    ).expand_kwargs(kwargs)

in_website_footprint()
