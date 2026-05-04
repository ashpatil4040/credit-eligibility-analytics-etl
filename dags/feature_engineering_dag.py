from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

import sys
sys.path.insert(0, "/opt/airflow/plugins")
from dag_instrumentation import on_dag_success, on_dag_failure, on_task_success, on_task_failure

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="feature_engineering_dag",
    start_date=datetime(2026, 1, 1),
    schedule="15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["finance", "features", "etl"],
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    wait_for_raw_ingestion = ExternalTaskSensor(
        task_id="wait_for_raw_ingestion",
        external_dag_id="raw_ingestion_dag",
        external_task_id=None,
        execution_delta=timedelta(minutes=15),
        allowed_states=["success"],
        failed_states=["failed"],
        timeout=900,
        poke_interval=30,
        mode="reschedule",
        on_success_callback=on_task_success,
        on_failure_callback=on_task_failure,
    )

    run_feature_engineering = BashOperator(
        task_id="run_feature_engineering",
        bash_command="python /opt/airflow/scripts/feature_engineering.py",
        env={
            "AIRFLOW_RUN_ID":  "{{ run_id }}",
            "AIRFLOW_DAG_ID":  "{{ dag.dag_id }}",
            "AIRFLOW_TASK_ID": "{{ task.task_id }}",
        },
        on_success_callback=on_task_success,
        on_failure_callback=on_task_failure,
    )

    wait_for_raw_ingestion >> run_feature_engineering