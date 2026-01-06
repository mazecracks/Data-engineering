# dags/imf_sdmx_to_s3.py
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.run_pipeline import run_pipeline  


default_args = {
    'owner':'maze',
     
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
    default_args = default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sdmx", "s3"],
) as dag:

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,          
        op_kwargs={"selected_flows": ["imts"]}, # change to None for all
    )
