# dags/imf_sdmx_to_s3.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.run_pipeline import run_pipeline  

with DAG(
    dag_id="imf_sdmx_to_s3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sdmx", "s3"],
) as dag:

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,          
        op_kwargs={"selected_flows": ["bop"]}, # change to None for all
    )
