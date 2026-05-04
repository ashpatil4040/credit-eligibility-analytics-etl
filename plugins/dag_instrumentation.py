import sys
import os

# scripts/ is mounted at /opt/airflow/scripts — make it importable
sys.path.insert(0, "/opt/airflow/scripts")

from otel_setup import setup_telemetry, get_tracer, get_logger, ATTR_DAG_ID, ATTR_TASK_ID, ATTR_RUN_ID, ATTR_STAGE
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

setup_telemetry("airflow-dags")
_tracer = get_tracer("airflow.dag")
_logger = get_logger("airflow.dag")


def _common_attrs(context: dict) -> dict:
    """Extract standard Airflow context fields into span attribute dict."""
    dag_run = context.get("dag_run")
    return {
        ATTR_DAG_ID:  context.get("dag").dag_id if context.get("dag") else "unknown",
        ATTR_TASK_ID: context.get("task_instance").task_id if context.get("task_instance") else "unknown",
        ATTR_RUN_ID:  dag_run.run_id if dag_run else "unknown",
        "airflow.execution_date": str(context.get("execution_date", "")),
        "airflow.try_number": str(context.get("task_instance").try_number if context.get("task_instance") else 0),
    }


def on_dag_success(context: dict) -> None:
    """
    DAG-level success callback.
    Creates a single span capturing the full DAG run outcome.
    Attach to: on_success_callback in every DAG.
    """
    attrs = _common_attrs(context)
    dag_id = attrs[ATTR_DAG_ID]

    with _tracer.start_as_current_span(f"{dag_id}.dag_run", kind=SpanKind.INTERNAL) as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
        span.set_attribute(ATTR_STAGE, "dag_run")
        span.set_attribute("dag_run.outcome", "success")
        span.set_status(Status(StatusCode.OK))
        _logger.info("DAG run succeeded: dag_id=%s run_id=%s", dag_id, attrs[ATTR_RUN_ID])


def on_dag_failure(context: dict) -> None:
    """
    DAG-level failure callback.
    Records the exception (if any) and marks the span as error.
    Attach to: on_failure_callback in every DAG.
    """
    attrs = _common_attrs(context)
    dag_id = attrs[ATTR_DAG_ID]
    exception = context.get("exception")

    with _tracer.start_as_current_span(f"{dag_id}.dag_run", kind=SpanKind.INTERNAL) as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
        span.set_attribute(ATTR_STAGE, "dag_run")
        span.set_attribute("dag_run.outcome", "failure")
        if exception:
            span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception) if exception else "DAG failed"))
        _logger.error("DAG run failed: dag_id=%s run_id=%s error=%s", dag_id, attrs[ATTR_RUN_ID], exception)


def on_task_success(context: dict) -> None:
    """
    Task-level success callback — attach to BashOperator on_success_callback.
    Creates a task span as a child of the dag_run span context.
    """
    attrs = _common_attrs(context)
    task_id = attrs[ATTR_TASK_ID]
    dag_id = attrs[ATTR_DAG_ID]

    with _tracer.start_as_current_span(f"{dag_id}.{task_id}", kind=SpanKind.INTERNAL) as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
        span.set_attribute(ATTR_STAGE, "task_execution")
        span.set_attribute("task.outcome", "success")
        span.set_status(Status(StatusCode.OK))
        _logger.info("Task succeeded: dag_id=%s task_id=%s run_id=%s", dag_id, task_id, attrs[ATTR_RUN_ID])


def on_task_failure(context: dict) -> None:
    """
    Task-level failure callback — attach to BashOperator on_failure_callback.
    """
    attrs = _common_attrs(context)
    task_id = attrs[ATTR_TASK_ID]
    dag_id = attrs[ATTR_DAG_ID]
    exception = context.get("exception")

    with _tracer.start_as_current_span(f"{dag_id}.{task_id}", kind=SpanKind.INTERNAL) as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
        span.set_attribute(ATTR_STAGE, "task_execution")
        span.set_attribute("task.outcome", "failure")
        if exception:
            span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception) if exception else "Task failed"))
        _logger.error("Task failed: dag_id=%s task_id=%s error=%s", dag_id, task_id, exception)