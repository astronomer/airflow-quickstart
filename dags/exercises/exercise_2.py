"""DAG that loads carbon footprint information from MinIO to duckdb."""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator

# ---------------- #
# Exercise Part 1  #
# ---------------- #
# TASK: this DAG should always run after the 'exercise_1' DAG. Use Datasets to accomplish this.
# Hint: you can find Dataset syntax examples here: https://docs.astronomer.io/learn/airflow-datasets

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=gv.default_args,
    description="",
    tags=["exercise", "duckdb", "minio"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def exercise_2():
    pass


exercise_2()