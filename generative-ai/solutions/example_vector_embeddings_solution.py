"""
## Compute and compare vector embeddings of words

This DAG demonstrates how to compute vector embeddings of words using
the SentenceTransformers library and compare the embeddings of a word of
interest to a list of words to find the semantically closest match.
"""

from airflow.decorators import (
    dag,
    task,
)  # This DAG uses the TaskFlow API. See: https://www.astronomer.io/docs/learn/airflow-decorators
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from pendulum import datetime, duration
from tabulate import tabulate
import duckdb
import logging
import os

# modularize code by importing functions from the include folder
from include.custom_functions.embedding_func import get_embeddings_one_word

# use the Airflow task logger to log information to the task logs (or use print())
t_log = logging.getLogger("airflow.task")

# define variables used in a DAG as environment variables in .env for your whole Airflow instance
# to standardize your DAGs
_DUCKDB_INSTANCE_NAME = os.getenv("DUCKDB_INSTANCE_NAME", "include/astronomy.db")
_DUCKDB_TABLE_NAME = os.getenv("DUCKDB_TABLE_NAME", "embeddings_table")
_WORD_OF_INTEREST_PARAMETER_NAME = os.getenv(
    "WORD_OF_INTEREST_PARAMETER_NAME", "my_word_of_interest"
)
_WORD_OF_INTEREST_DEFAULT = os.getenv("WORD_OF_INTEREST_DEFAULT", "star")
_LIST_OF_WORDS_PARAMETER_NAME = os.getenv(
    "LIST_OF_WORDS_PARAMETER_NAME", "my_list_of_words"
)
_LIST_OF_WORDS_DEFAULT = ["sun", "rocket", "planet", "light", "happiness"]
# -------------- #
# DAG Definition #
# -------------- #


# instantiate a DAG with the @dag decorator and set DAG parameters (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters)
@dag(
    start_date=datetime(2024, 5, 1),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after 5 consecutive failed runs, experimental
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=30),  # tasks wait 30s in between retries
    },  # default_args are applied to all tasks in a DAG
    tags=["example", "GenAI"],  # add tags in the UI
    params={  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
        _WORD_OF_INTEREST_PARAMETER_NAME: Param(
            _WORD_OF_INTEREST_DEFAULT,
            type="string",
            title="The word you want to search a close match for.",
            minLength=1,
            maxLength=50,
        ),
        _LIST_OF_WORDS_PARAMETER_NAME: Param(
            _LIST_OF_WORDS_DEFAULT,
            type="array",
            title="A list of words to compare to the word of interest.",
        ),
    },
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow in production, use a
    # cloud-based database and based on concurrency capabilities adjust the two parameters below.
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    concurrency=1, # only allow a single task execution at a time, prevents parallel DuckDB calls
    is_paused_upon_creation=False, # start running the DAG as soon as its created
)
def example_vector_embeddings():  # by default the dag_id is the name of the decorated function

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can still use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    @task(retries=2)  # you can override default_args at the task level
    def get_words(
        **context,
    ) -> list:  # by default the name of the decorated function is the task_id
        """
        Get the list of words to embed from the context.
        Returns:
            list: A list of words to embed.
        """

        # retrieve param values from the context
        words = context["params"][_LIST_OF_WORDS_PARAMETER_NAME]

        return words

    @task
    def create_embeddings(list_of_words: list) -> list:
        """
        Create embeddings for a list of words.
        Args:
            list_of_words (list): A list of words to embed.
        Returns:
            list: A list of dictionaries with the words as keys and the embeddings as values.
        """

        list_of_words_and_embeddings = []

        for word in list_of_words:
            word_and_embeddings = get_embeddings_one_word(
                word
            )  # using the modularized function in the include folder
            list_of_words_and_embeddings.append(word_and_embeddings)

        return list_of_words_and_embeddings

    @task
    def create_vector_table(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
    ) -> None:
        """
        Create a table in DuckDB to store the embeddings.
        Args:
            duckdb_instance_name (str): The name of the DuckDB instance.
            table_name (str): The name of the table to create.
        """

        cursor = duckdb.connect(duckdb_instance_name)

        # setting up DuckDB to store vectors
        cursor.execute("INSTALL vss;")
        cursor.execute("LOAD vss;")
        cursor.execute("SET hnsw_enable_experimental_persistence = true;")

        table_name = "embeddings_table"

        cursor.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} (
                text STRING,
                vec FLOAT[384]
            );

            -- Create an HNSW index on the embedding vector
            CREATE INDEX my_hnsw_index ON {table_name} USING HNSW (vec);
            """
        )
        cursor.close()

    @task
    def insert_words_into_db(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
        list_of_words_and_embeddings: list = None,
    ) -> None:
        """
        Insert words and their embeddings into the DuckDB table.
        Args:
            duckdb_instance_name (str): The name of the DuckDB instance.
            table_name (str): The name of the table to insert into.
            list_of_words_and_embeddings (list): A list of dictionaries with words as keys and embeddings as values.
        """

        cursor = duckdb.connect(duckdb_instance_name)
        cursor.execute("LOAD vss;")

        for i in list_of_words_and_embeddings:
            word = list(i.keys())[0]
            vec = i[word]
            cursor.execute(
                f"""
                INSERT INTO {table_name} (text, vec)
                VALUES (?, ?);
                """,
                (word, vec),
            )

        cursor.close()

    @task
    def embed_word(**context):
        """
        Embed a single word and return the embeddings.
        Returns:
            dict: A dictionary with the word as key and the embeddings as value.
        """

        my_word_of_interest = context["params"][_WORD_OF_INTEREST_PARAMETER_NAME]
        embeddings = get_embeddings_one_word(my_word_of_interest)

        embeddings = embeddings[my_word_of_interest]

        return {my_word_of_interest: embeddings}

    @task
    def find_closest_word_match(
        duckdb_instance_name: str = _DUCKDB_INSTANCE_NAME,
        table_name: str = _DUCKDB_TABLE_NAME,
        word_of_interest_embedding: dict = None,
    ):
        """
        Find the closest word match to the word of interest in the DuckDB table.
        Args:
            duckdb_instance_name (str): The name of the DuckDB instance.
            table_name (str): The name of the table to query.
            word_of_interest_embedding (dict): A dictionary with the word as key and the embeddings as value.
        Returns:
            list: A list of the top 3 closest words to the word of interest.
        """

        cursor = duckdb.connect(duckdb_instance_name)
        cursor.execute("LOAD vss;")

        word = list(word_of_interest_embedding.keys())[0]
        vec = word_of_interest_embedding[word]

        top_3 = cursor.execute(
            f"""
            SELECT text FROM {table_name}
            ORDER BY array_distance(vec, {vec}::FLOAT[384])
            LIMIT 3;
            """
        )

        top_3 = top_3.fetchall()

        t_log.info(f"Top 3 closest words to '{word}':")
        t_log.info(tabulate(top_3, headers=["Word"], tablefmt="pretty"))

        return top_3

    # ------------------------------------ #
    # Calling tasks + Setting dependencies #
    # ------------------------------------ #

    # each call of a @task decorated function creates one task in the Airflow UI
    # passing the return value of one @task decorated function to another one
    # automatically creates a task dependency
    create_embeddings_obj = create_embeddings(list_of_words=get_words())
    embed_word_obj = embed_word()

    # you can set explicit dependencies using the chain function (or bit-shift operators)
    # See: https://www.astronomer.io/docs/learn/managing-dependencies
    chain(
        create_vector_table(),
        insert_words_into_db(list_of_words_and_embeddings=create_embeddings_obj),
        find_closest_word_match(word_of_interest_embedding=embed_word_obj),
    )


example_vector_embeddings()
