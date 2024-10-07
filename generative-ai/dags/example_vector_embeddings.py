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
from airflow.models.connection import Connection
from pendulum import datetime, duration
from tabulate import tabulate
import duckdb
import os

# DuckDB connection for data quality checks (local development)
conn = Connection(
    conn_id="duckdb_conn",
    conn_type="duckdb",
    host="include/astronomy.db"
    )
env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
os.environ[env_key] = conn_uri

# Imports needed for data quality checks
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator





# Define variables used in a DAG as environment variables in .env for your whole Airflow instance
# to standardize your DAGs.
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

# ------------------------------------------- #
# Exercise 1: Experiment with alternative LMs #
# ------------------------------------------- #
# In include/embedding_func.py, pass alternative LMs to the `SentenceTransformer`
# function. For possible LMs, see: https://sbert.net/docs/sentence_transformer/pretrained_models.html.
# Here, you must also define the dimensions of the LM for the `vec` column of the database.
# It must correspond to the LM passed to `SentenceTransformer` in include/embedding_func.py.
# The default of 384 corresponds to "all-MiniLM-L6-v2".
_LM_DIMENSIONS = os.getenv("LM_DIMS", "384")

# ------------------------------- #
# Exercise 3: Modularize this DAG #
# ------------------------------- #
# Keeping code that isn't part of your DAG or operator instantiations
# in a separate file makes your DAG easier to read, maintain, and update.
# Follow best practices and modularize this DAG by converting 
# `get_embeddings_one_word` from top-level code to an imported module.
# Hint: all you need to do in this case is replace the top-level function 
# with an import statement.
# For more guidance, see: 
# https://www.astronomer.io/docs/learn/dag-best-practices#treat-your-dag-file-like-a-config-file
def get_embeddings_one_word(word):
    """
    Embeds a single word using the SentenceTransformers library.
    Args:
        word (str): The word to embed.
    Returns:
        dict: A dictionary with the word as key and the embeddings as value.
    """
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("all-MiniLM-L6-v2")

    embeddings = model.encode(word)
    embeddings = embeddings.tolist()

    return {word: embeddings}

# -------------- #
# DAG Definition #
# -------------- #


# Instantiate a DAG with the @dag decorator and set DAG parameters 
# (see: https://www.astronomer.io/docs/learn/airflow-dag-parameters).
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
    # The @task decorator turns any Python function into an Airflow task.
    # Any @task-decorated function that is called inside the @dag-decorated
    # function is automatically added to the DAG.
    # 
    # If one exists for your use case, you can still use traditional Airflow operators
    # and mix them with @task decorators. Check out registry.astronomer.io for available operators.
    #
    # See: https://www.astronomer.io/docs/learn/airflow-decorators for information about the @task decorator.
    # See: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators.

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
        lm_dims: str = _LM_DIMENSIONS,
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
                vec FLOAT[{lm_dims}]
            );
            """
        )

        cursor.execute(
            f"""
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

    # ----------------------------------- #
    # Exercise 2: Add data quality checks #
    # ----------------------------------- #
    # Follow best practices and add data quality checks 
    # to ensure that the row count and uniqueness of the 
    # data in the table are consistent with expectations. 
    # Astronomer recommends using operators available from 
    # the Airflow SQL provider to implement data quality checks 
    # for most use cases, but Airflow also supports tools such 
    # as Great Expectations and Soda. Add tasks using the 
    # `SQLColumnCheckOperator` and `SQLTableCheckOperator` 
    # to perform row count and uniqueness checks, respectively.
    # Don't forget to add the tasks to the chain function
    # below. The operators have already been imported for you.

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
        lm_dims: str = _LM_DIMENSIONS,
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
            ORDER BY array_distance(vec, {vec}::FLOAT[{lm_dims}])
            LIMIT 3;
            """
        )

        top_3 = top_3.fetchall()

        # ----------------------- #
        # Exercise 4: Add logging #
        # ----------------------- #
        # Add an Airflow task logger to log information to the task logs using 
        # the default logger for tasks. Use `t_log` as a top-level variable for 
        # instantiating an `airflow.task` logger object and uncomment the logging 
        # statements in the `find_closest_word_match` task.
        # Hint 1: don't forget to import the Python logging framework!
        # Hint 2: for more info about setting up logging and example code, see: 
        # https://www.astronomer.io/docs/learn/logging#add-custom-task-logs-from-a-dag
        # Use the Airflow task logger to log information to the task logs (or use print()).
        # t_log.info(f"Top 3 closest words to '{word}':")
        # t_log.info(tabulate(top_3, headers=["Word"], tablefmt="pretty"))

        return top_3

    # ------------------------------------ #
    # Calling tasks + setting dependencies #
    # ------------------------------------ #

    # Each call of a @task-decorated function creates one task in the Airflow UI.
    # Passing the return value of one @task-decorated function to another one
    # automatically creates a task dependency.
    create_embeddings_obj = create_embeddings(list_of_words=get_words())
    embed_word_obj = embed_word()

    # You can set explicit dependencies using the chain function (or bit-shift operators).
    # See: https://www.astronomer.io/docs/learn/managing-dependencies
    chain(
        create_vector_table(),
        insert_words_into_db(list_of_words_and_embeddings=create_embeddings_obj),
        find_closest_word_match(word_of_interest_embedding=embed_word_obj),
    )


example_vector_embeddings()
