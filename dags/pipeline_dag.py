from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"
DBT_DIR = "/opt/project/dbt_project"
PYTHON = "/opt/project_venv/bin/python"
DBT = "/opt/project_venv/bin/dbt"

with DAG(
    dag_id="idempotent_multi_source_pipeline",
    description="Ingest four sources into BigQuery, then build dbt staging and marts",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline"],
) as dag:

    # ---- CSV orders ----
    ingest_csv = BashOperator(
        task_id="ingest_csv",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} ingestion/ingest_csv.py",
    )
    dbt_csv = BashOperator(
        task_id="dbt_build_csv_orders",
        bash_command=(
            f"cd {DBT_DIR} && {DBT} build -s tag:csv_orders --target prod "
            f"--target-path target/airflow_csv --log-path logs/airflow_csv"
        ),
    )

    # ---- API order status events ----
    ingest_api = BashOperator(
        task_id="ingest_api",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} ingestion/ingest_api.py",
    )
    dbt_api = BashOperator(
        task_id="dbt_build_api_events",
        bash_command=(
            f"cd {DBT_DIR} && {DBT} build -s tag:api_events --target prod "
            f"--target-path target/airflow_api --log-path logs/airflow_api"
        ),
    )

    # ---- Customer databases ----
    ingest_db = BashOperator(
        task_id="ingest_db",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} ingestion/ingest_db.py",
    )
    dbt_db = BashOperator(
        task_id="dbt_build_db_customers",
        bash_command=(
            f"cd {DBT_DIR} && {DBT} build -s tag:db_customers --target prod "
            f"--target-path target/airflow_db --log-path logs/airflow_db"
        ),
    )

    # ---- Order: each source's dbt build waits for its own ingestion only ----
    ingest_csv >> dbt_csv
    ingest_api >> dbt_api
    ingest_db >> dbt_db