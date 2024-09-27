"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import (
    dag,
    task,
)  # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import requests
import duckdb
import os

_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "astronaut_data")
_DUCKDB_TABLE_URI = f"duckdb://{_DUCKDB_INSTANCE_NAME}/{_DUCKDB_TABLE_NAME}"


# -------------- #
# DAG Definition #
# -------------- #


# Instantiate a DAG with the @dag decorator and set DAG parameters 
# (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters).
@dag(
    start_date=datetime(2024, 1, 1),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=5),  # tasks wait 30s in between retries
    },  # default_args are applied to all tasks in a DAG
    tags=["example", "space"],  # add tags in the UI
    is_paused_upon_creation=False, # start running the DAG as soon as its created
)
def example_astronauts():

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # The @task decorator turns any Python function into an Airflow task.
    # Any @task-decorated function that is called inside the @dag-decorated
    # function is automatically added to the DAG.
    # 
    # If one exists for your use case, you can still use traditional Airflow operators
    # and mix them with @task decorators. Check out registry.astronomer.io for available operators.
    # 
    # See: https://www.astronomer.io/docs/learn/airflow-decorators for information about the @task
    # decorator.
    # See: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional 
    # operators.

    # -------------------------------------------------- #
    # Exercise 1: Define this task as a Dataset producer #
    # -------------------------------------------------- #
    # With Datasets, DAGs that access the same data can have explicit, visible 
    # relationships, and DAGs can be scheduled based on updates to these datasets. 
    # This feature helps make Airflow data-aware and expands Airflow scheduling 
    # capabilities beyond time-based methods such as cron. Downstream DAGs can be 
    # scheduled based on combinations of Dataset updates coming from tasks in the 
    # same Airflow instance or calls to the Airflow API.
    # 
    # To define this task as a producer of a Dataset, pass a Dataset object,
    # encapsulated in a list, as an argument to the task's `outlets` parameter.
    # For more guidance, see: https://www.astronomer.io/docs/learn/airflow-datasets
    
    @task
    def get_astronaut_names(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        from airflow.models import Variable

        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            list_of_people_in_space = r.json()["people"]
        except:
            print("API currently not available, using hardcoded data instead.")
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Marco Alain Sieber"},
                {"craft": "ISS", "name": "Claude Nicollier"},
            ]

        return list_of_people_in_space

    @task
    def get_astronaut_numbers(**context) -> int:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        from airflow.models import Variable

        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            number_of_people_in_space = r.json()["number"]
        except:
            print("API currently not available, using hardcoded data instead.")
            number_of_people_in_space = 12

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )

        Variable.set(key="number_of_people_in_space", value=number_of_people_in_space)

        return number_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is in space flying on the {craft}! {greeting}")

    @task
    def print_astronauts(**context) -> None:

        # ------------------------------------------- #
        # Exercise 2: Manually pull a value from XCom #
        # ------------------------------------------- #
        # The TaskFlow API uses Airflow XComs to allow you to pass small amounts 
        # of data between tasks automatically by passing the return value of a 
        # @task-decorated function to another one when you set dependencies. 
        # For an example, see:
        # https://www.astronomer.io/docs/learn/airflow-passing-data-between-tasks?tab=traditional#example-dag 
        # 
        # This exercise will help you understand XComs and give you a peek 
        # under the hood of the TaskFlow API.
        # 
        # Modify this task to print, instead of the hard-coded value currently 
        # being used to define `number_of_people_in_space`, the value you pushed 
        # to XCom in the `get_astronauts` upstream task. To do this, replace the 
        # hard-coded value with an `xcom_pull` function using the task instance 
        # context.
        # 
        # Hint: the `xcom_pull` code should be almost identical to the `xcom_push` 
        # function call, except for the fact that you need to pass a `task_ids` 
        # parameter and argument to the function instead of a `value`.
        # 
        # For more guidance, see: 
        # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
        
        number_of_people_in_space = 12
        print(f"{number_of_people_in_space} people are in space!")

    @task(retries=2)  # You can override default_args at the task level
    def create_astronauts_table_in_duckdb(  # By default, the name of the decorated function is the task_id
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ) -> None:
        """
        Create a table in DuckDB to store data about astronauts.
        This task simulates a setup step in an ETL pipeline.
        Args:
            duckdb_instance_name: The name of the DuckDB instance.
            table_name: The name of the table to be created.
        """
        cursor = duckdb.connect(duckdb_instance_name)

        cursor.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} (
                num_astros INT,
            )"""
        )
        cursor.close()

    @task(retries=2)
    def load_astronauts_in_duckdb(
        num_astros: int,
    ) -> None:
          
        cursor = duckdb.connect(_DUCKDB_INSTANCE_NAME)
        cursor.sql(
            f"INSERT INTO {_DUCKDB_TABLE_NAME} (num_astros) VALUES ({num_astros});"
        )

    # ------------------------------------ #
    # Calling tasks + setting dependencies #
    # ------------------------------------ #

    # Each call of a @task-decorated function creates one task in the Airflow UI.
    # Passing the return value of one @task-decorated function to another one
    # automatically creates a task dependency.

    # This task uses dynamic task mapping to create a variable number of copies
    # of the `print_astronaut_craft` task at runtime in parallel.
    # See: https://www.astronomer.io/docs/learn/dynamic-tasks
    chain(
        print_astronaut_craft.partial(greeting="Hello! :)").expand(
            person_in_space=get_astronaut_names()
        ),
        print_astronauts(),
        create_astronauts_table_in_duckdb(),
        load_astronauts_in_duckdb(get_astronaut_numbers())
    )

# Instantiate the DAG
example_astronauts()
