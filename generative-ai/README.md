# Project Overview

Apache Airflow is one of the most common orchestration engines for AI/Machine Learning jobs, especially for retrieval-augmented generation (RAG). This template shows an simple example of building vector embeddings for text and then performing a semantic search on the embeddings.

Astronomer is the best place to host Apache Airflow -- try it out with a free trial at [astronomer.io](https://www.astronomer.io/).

# Learning Paths

To learn more about data engineering with Apache Airflow, make a few changes to this project! For example, try one of the following:

1. Add additional words to the dictionary and use a different embedding model
2. Take a look at the architecture of [Ask Astro](https://github.com/astronomer/ask-astro), Astronomer's reference architecture for LLM applications

# Project Contents

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
  - `example_vector_embeddings.py`: This DAG demonstrates how to compute vector embeddings of words using the SentenceTransformers library and
    compare the embeddings of a word of interest to a list of words to find the semantically closest match.get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

# Deploying to Production

### ❗Warning❗

This template used DuckDB, an in-memory database, for running dbt transformations. While this is great to learn Airflow, your data is not guaranteed to persist between executions! For production applications, use a _persistent database_ instead (consider DuckDB's hosted option MotherDuck or another database like Postgres, MySQL, or Snowflake).
