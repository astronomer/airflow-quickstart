"""DAG that retrieves carbon footprint information of websites."""

from airflow.decorators import dag, task
from pendulum import datetime
import requests
import json

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import user_input_variables as uv

# ---------------- #
# Exercise Part 1  #
# ---------------- #

# TASK: provide a list of URLs you want to get carbon footprint information on.
# please only test public URLs, a public report will be generated, learn
# more at https://www.websitecarbon.com/
list_of_websites = ["https://www.astronomer.io/", "https://min.io/"]


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves carbon footprint information of websites.",
    tags=["extract", "footprint"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def extract_website_footprint():
    @task(
        outlets=[gv.DS_FOOTPRINT_JSON]
    )
    def get_website_footprint(website, **context):
        r = requests.get(f"https://api.websitecarbon.com/site?url={website}")
        payload = r.json()

        timestamp_no_dash = context["ts_nodash"]

        with open(
            f"{gv.FOOTPRINT_DATA_FOLDER_PATH}{timestamp_no_dash}.json",
            "w",
        ) as f:
            f.write(json.dumps(payload))

        return payload

    # ---------------- #
    # Exercise Part 2  #
    # ---------------- #
    # TASK: Dynamically map the 'get_website_footprint' task to retrieve information for all
    # websites in 'list_of_websites'
    # Hint: you will need to use `.expand()`

    get_website_footprint(website=list_of_websites[0])


extract_website_footprint()
