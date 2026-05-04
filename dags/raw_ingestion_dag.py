from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

import sys
sys.path.insert(0, "/opt/airflow/plugins")
from dag_instrumentation import on_dag_success, on_dag_failure, on_task_success, on_task_failure

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_ingestion_dag",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["finance", "raw", "ingestion"],
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    run_raw_ingestion = BashOperator(
        task_id="run_raw_ingestion",
        bash_command="python /opt/airflow/scripts/raw_ingestion.py",
        env={
            "AIRFLOW_RUN_ID":  "{{ run_id }}",
            "AIRFLOW_DAG_ID":  "{{ dag.dag_id }}",
            "AIRFLOW_TASK_ID": "{{ task.task_id }}",
        },
        on_success_callback=on_task_success,
        on_failure_callback=on_task_failure,
    )