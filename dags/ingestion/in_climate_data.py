"""DAG that loads climate ingests from local csv files into MinIO."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalCSVToMinIOOperator

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "start" Dataset has been produced to
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Ingests climate data from provided csv files to MinIO.",
    tags=["ingestion", "minio"],
)
def in_climate_data():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_climate_bucket", bucket_name=gv.CLIMATE_BUCKET_NAME
    )

    # dynamically map over the custom LocalCSVToMinIOOperator to read the contents
    # of 2 local csv files to MinIO
    testing_minio_op = LocalCSVToMinIOOperator.partial(
        task_id="ingest_climate_data",
        minio_ip=gv.MINIO_IP,
        bucket_name=gv.CLIMATE_BUCKET_NAME,
    ).expand_kwargs(
        [
            {
                "csv_path": gv.TEMP_COUNTRY_PATH,
                "object_name": gv.TEMP_COUNTRY_PATH.split("/")[-1],
            },
            {
                "csv_path": gv.TEMP_GLOBAL_PATH,
                "object_name": gv.TEMP_GLOBAL_PATH.split("/")[-1],
            },
        ]
    )

    # set dependencies
    create_bucket_tg >> testing_minio_op


in_climate_data()
