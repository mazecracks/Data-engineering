# dags/imf_sdmx_to_s3.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from pipeline.run_pipeline import run_pipeline
from pipeline.loader_tasks import (
    discover_pending_transformed_files,
    load_one_pending_file,
)

default_args = {
    "owner": "maze",

    # Retry behaviour
    "retries": 3,
    "retry_delay": timedelta(minutes=5),

    # Smarter retries
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),

    # Optional but good
    "depends_on_past": False,
}

with DAG(
    dag_id="imf_sdmx_to_s3",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sdmx", "s3", "loader"],
) as dag:

    # 1) Ingest raw IMF -> S3 (existing)
    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        op_kwargs={"selected_flows": ["imts"]},  # change to None for all
    )

    # 2) Fail-fast poke: wait for ANY transformed csv
    wait_for_transformed = S3KeySensor(
        task_id="wait_for_transformed",
        bucket_name="{{ var.value.IMF_BUCKET}}",
        bucket_key="imf/transformed/*/*.csv",
        wildcard_match=True,
        poke_interval=30,
        timeout=300,         # 5 mins fail-fast
        mode="reschedule",
    )

    # 3) Discover pending transformed files (returns list of {"item": {...}} dicts)
    discover = PythonOperator(
        task_id="discover_pending_transformed_files",
        python_callable=discover_pending_transformed_files,
    )

    # 4) Load each file (dynamic mapping)
    load = PythonOperator.partial(
        task_id="load_one_file",
        python_callable=load_one_pending_file,
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=15),
    ).expand(op_kwargs=discover.output)

    run >> wait_for_transformed >> discover >> load
