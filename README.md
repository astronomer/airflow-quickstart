# [Apache Airflow®](https://airflow.apache.org/) Quickstart

Welcome to the Astronomer's Apache Airflow Quickstart repository!

Launch your journey with Airflow by signing up for a trial at astronomer.io! If you've already created a trial and have cloned this repository, check out your chosen quickstart in the table of contents.

## Table of Contents

This repository contains 3 projects for demonstrating the capabilities and use cases of Airflow.

- [ETL](etl/README.md) A project for learning the basics of ETL with Airflow
- [Generative AI](generative-ai/README.md) A project for learning how to use Airflow to train a generative AI model
- [Learning Airflow](learning-airflow/README.md) A project for learning the basics of Airflow

## Purpose of This Repository

These templates are designed to help new users learn and try Airflow quickly. 

Exercises in each project will expose you to ways to enhance and customize Airflow's functionality.

As you move your workloads into business-critical applications or adopt advanced Airflow features, reach out to [Astronomer's Airflow experts](https://www.astronomer.io/contact/) to get help deploying to production.

## Run a Template Locally

1. Navigate to one of the projects above by `cd`ing into its corresponding directory.

2. Start Airflow on your local machine by running `astro dev start`.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

3. Verify that all 4 Docker containers were created by running `docker ps`.

Note: Running `astro dev start` will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

4. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with `admin` for both your Username and Password.

You should also be able to access your Postgres Database at `localhost:5432/postgres`.

5. Learn about enhancing and customizing Airflow's functionality by working through the exercise(s) in the DAG.
