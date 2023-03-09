"""DAG that calculates the carbon footprint of websites."""

from airflow.decorators import dag, task
from pendulum import datetime
import requests
import uuid

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator

# ---------------- #
# Exercise Part 1  #
# ---------------- #

# TASK: provide a list of URLs you want to get carbon footprint information on.
# please only test public URLs, a public report will be generated, learn
# more at https://www.websitecarbon.com/
list_of_websites = ["https://www.astronomer.io/", "https://min.io/"]


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=gv.default_args,
    description="",
    tags=["exercise", "ingestion", "minio"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def exercise_1():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_weather_bucket", bucket_name=gv.WEBSITE_FOOTPRINT_BUCKET_NAME
    )

    @task
    def get_website_footprint(website):
        r = requests.get(f"https://api.websitecarbon.com/site?url={website}")
        payload = r.json()
        return payload
    
    # ---------------- #
    # Exercise Part 2  #
    # ---------------- #
    # TASK: Dynamically map the 'get_website_footprint' task to retrieve information for all
    # websites in 'list_of_websites'
    # Hint: you will need to use `.expand()`

    website_footprint_info = get_website_footprint(website=list_of_websites[0])


    # ---------------- #
    # Exercise Part 3  #
    # ---------------- #
    # TASK: After completing part 2 of the exercise, dynamically map the 'write_to_minio' task
    # to write the information from each of the tested websites to MinIO
    # Hint: you will need to add both `.partial` and `.expand` in the right places

    write_to_minio = LocalFilesystemToMinIOOperator(
        task_id="write_to_minio",
        minio_ip=gv.MINIO_IP,
        bucket_name=gv.WEBSITE_FOOTPRINT_BUCKET_NAME,
        object_name=f"website_footprint_{uuid.uuid4()}.json",
        json_serializeable_information=website_footprint_info,
    )

    create_bucket_tg >> website_footprint_info >> write_to_minio


exercise_1()
