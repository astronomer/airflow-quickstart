"Contains constants used in the DAGs"

from pathlib import Path
from cosmos import ExecutionConfig

jaffle_shop_path = Path("/usr/local/airflow/dbt/jaffle_shop")
dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)
