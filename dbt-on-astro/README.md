# Project Overview

Apache Airflow is one of the most widely-used engines for orchestrating Extract, Transform, and Load (ETL) jobs, especially for transformations using [dbt](https://www.getdbt.com). dbt is a framework to create reliable transformations to produce high-quality data for businesses, usually in analytical databases like Snowflake and BigQuery.

This project showcases using dbt and Airflow together with [Cosmos](https://github.com/astronomer/astronomer-cosmos), allowing users to deploy dbt in production with Airflow best-practices.

Astronomer is the best place to host Apache Airflow -- try it out with a free trial at [astronomer.io](https://www.astronomer.io/).

# Learning Paths

To learn more about data engineering with Apache Airflow, dbt, and Cosmos, make a few changes to this project! For example, try one of the following:

1. Use Postgres, MySQL, Snowflake, or another production-ready database instead of DuckDB
2. Change the Cosmos DbtDag to Cosmos DbtTaskGroups! For extra help, check out the [Cosmos examples of DbtTaskGroups](https://github.com/astronomer/astronomer-cosmos/blob/main/dev/dags/basic_cosmos_task_group.py)

# Project Contents

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. This project includes one example DAG:
  - `dbt_cosmos_dag.py`: This DAG sets up [Cosmos](https://github.com/astronomer/astronomer-cosmos), allowing files in the /dbt directory to transform into Airflow tasks and taskgroups.
- dbt/jaffle_shop: This folder contains the dbt project [jaffle_shop](https://github.com/dbt-labs/jaffle_shop_duckdb), a fictional ecommerce store. Use this as a starting point to learn how dbt and Airflow work together!
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. In this example, constants.py includes configuration for your Cosmos project.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. In this project, we pin the Cosmos version.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

# Deploying to Production

### ❗Warning❗

This template used DuckDB, an in-memory database, for running dbt transformations. While this is great to learn Airflow, your data is not guaranteed to persist between executions! For production applications, use a _persistent database_ instead (consider DuckDB's hosted option MotherDuck or another database like Postgres, MySQL, or Snowflake).
