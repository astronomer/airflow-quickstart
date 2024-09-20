Overview
========

Welcome to this hands-on repository to get started with [Apache Airflow®](https://airflow.apache.org/)! :rocket:

Building Extract, Transform, and Load (ETL) workloads is a common pattern in Apache Airflow. This template shows an example pattern for defining an ETL workload using DuckDB as the data warehouse of choice.

Astronomer is the best place to host Apache Airflow -- try it out with a free trial at [astronomer.io](https://www.astronomer.io/).

# Learning Paths

To learn more about data engineering with Apache Airflow, make a few changes to this project! For example, try one of the following:

1. Changing the data warehouse to a different provider (Snowflake, AWS Redshift, etc.)
2. Adding a different data source and integrating it with this ETL project
3. Adding the [EmailOperator](https://registry.astronomer.io/providers/apache-airflow/versions/2.8.1/modules/EmailOperator) to the project and notifying a user on job completion

# Project Contents

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes two example DAGs:
  - `example_etl_galaxies`: This example demonstrates an ETL pipeline using Airflow. The pipeline extracts data about galaxies, filters the data based on the distance from the Milky Way, and loads the filtered data into a DuckDB database.
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

# Deploying to Production

### ❗Warning❗

This template uses DuckDB, an in-memory database. While this is great for learning Airflow, your data is not guaranteed to persist between executions! For production applications, use a _persistent database_ instead (consider DuckDB's hosted option MotherDuck or another database like Postgres, MySQL, or Snowflake).
