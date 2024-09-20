# Airflow DAG for running dbt on the jaffle_shop project using the duckdb adapter.
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig

from include.constants import jaffle_shop_path, venv_execution_config

dbt_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(jaffle_shop_path),
    profile_config=ProfileConfig(
        # these map to dbt/jaffle_shop/profiles.yml
        profile_name="duckdb_profile",
        target_name="dev",
        profiles_yml_filepath=jaffle_shop_path / "profiles.yml",
    ),
    execution_config=venv_execution_config,
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_cosmos_dag",
    # Warning - in-memory DuckDB is not a persistent database between workers. To move this workflow in production, use a
    # cloud-based database and based on concurrency capabilities adjust the two parameters below.
    max_active_runs=1,  # only allow one concurrent run of this DAG, prevents parallel DuckDB calls
    concurrency=1, # only allow a single task execution at a time, prevents parallel DuckDB calls
    is_paused_upon_creation=False, # start running the DAG as soon as its created
)
