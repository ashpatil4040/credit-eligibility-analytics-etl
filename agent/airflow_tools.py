"""
airflow_tools.py — Action tools for controlling the Airflow pipeline.

PHASE 5 — SAFETY PATTERN:
  These tools are NOT passed to AgentExecutor yet.
  They exist as tested Python functions but the agent cannot call them
  until Phase 6 (risk classifier) adds the safety gate.

  Phase 5: tools exist, agent cannot call them
  Phase 6: risk classifier added as safety gate
  Phase 7: tools passed to AgentExecutor with classifier in the loop

  Why: An agent that can trigger/clear/retry DAGs without a safety gate
  could cause data loss or reprocess customer records incorrectly.
"""

import os
import json
import logging
import requests
from langchain.tools import tool

logger = logging.getLogger(__name__)

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_API_BASE_URL", "http://airflow-apiserver:8080")
AIRFLOW_USER     = os.getenv("AIRFLOW_API_USERNAME", "airflow")
AIRFLOW_PASS     = os.getenv("AIRFLOW_API_PASSWORD", "change_me_local_only")

DAG_IDS = ["raw_ingestion_dag", "feature_engineering_dag", "eligibility_analytics_dag"]


def _airflow_get(path: str) -> dict:
    url = f"{AIRFLOW_BASE_URL}/api/v1{path}"
    resp = requests.get(url, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=10)
    resp.raise_for_status()
    return resp.json()


def _airflow_post(path: str, payload: dict) -> dict:
    url = f"{AIRFLOW_BASE_URL}/api/v1{path}"
    resp = requests.post(
        url,
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def _airflow_patch(path: str, payload: dict) -> dict:
    url = f"{AIRFLOW_BASE_URL}/api/v1{path}"
    resp = requests.patch(
        url,
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def _validate_dag_id(dag_id: str) -> str | None:
    dag_id = dag_id.strip().strip("'\"")
    if dag_id not in DAG_IDS:
        return f"Invalid dag_id '{dag_id}'. Must be one of: {DAG_IDS}"
    return None


@tool
def trigger_dag_run(dag_id: str) -> str:
    """
    Triggers a new DAG run for the given dag_id.
    Valid dag_ids: raw_ingestion_dag, feature_engineering_dag, eligibility_analytics_dag
    IDEMPOTENCY: Aborts if a run is already active to prevent duplicates.
    """
    dag_id = dag_id.strip().strip("'\"")
    err = _validate_dag_id(dag_id)
    if err:
        return err
    try:
        data = _airflow_get(f"/dags/{dag_id}/dagRuns?state=running&limit=1")
        if data.get("dag_runs"):
            run_id = data["dag_runs"][0]["dag_run_id"]
            return f"Aborted: {dag_id} already has an active run ({run_id}). Will not trigger a duplicate."
        result = _airflow_post(f"/dags/{dag_id}/dagRuns", {"conf": {}})
        return f"Triggered {dag_id}. run_id={result['dag_run_id']} state={result['state']}"
    except Exception as e:
        return f"Error triggering {dag_id}: {e}"


@tool
def retry_task(input: str) -> str:
    """
    Retries a failed task by setting its state to 'up_for_retry'.
    Input format: dag_id,run_id,task_id
    Example: eligibility_analytics_dag,scheduled__2026-05-06T15:30:00+00:00,run_eligibility_analytics
    IDEMPOTENCY: Aborts if task is not in a failed state.
    """
    input = input.strip().strip("'\"")
    parts = [p.strip() for p in input.split(",", 2)]
    if len(parts) != 3:
        return "Input must be 'dag_id,run_id,task_id'"
    dag_id, run_id, task_id = parts
    err = _validate_dag_id(dag_id)
    if err:
        return err
    try:
        task_data = _airflow_get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}")
        current_state = task_data.get("state")
        if current_state not in ("failed", "upstream_failed"):
            return f"Aborted: task '{task_id}' is in state '{current_state}', not failed. Will not retry."
        _airflow_patch(
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}",
            {"new_state": "up_for_retry"},
        )
        return f"Task '{task_id}' in {dag_id}/{run_id} set to 'up_for_retry'."
    except Exception as e:
        return f"Error retrying task: {e}"


@tool
def unpause_dag(dag_id: str) -> str:
    """
    Unpauses a DAG so it resumes its schedule.
    Valid dag_ids: raw_ingestion_dag, feature_engineering_dag, eligibility_analytics_dag
    IDEMPOTENCY: Safe to call even if DAG is already unpaused.
    """
    dag_id = dag_id.strip().strip("'\"")
    err = _validate_dag_id(dag_id)
    if err:
        return err
    try:
        data = _airflow_get(f"/dags/{dag_id}")
        if not data.get("is_paused"):
            return f"{dag_id} is already unpaused. No action taken."
        _airflow_patch(f"/dags/{dag_id}", {"is_paused": False})
        return f"{dag_id} successfully unpaused."
    except Exception as e:
        return f"Error unpausing {dag_id}: {e}"


@tool
def clear_dag_run(input: str) -> str:
    """
    Clears all task instances in a DAG run so they can be re-run.
    Input format: dag_id,run_id
    Example: eligibility_analytics_dag,scheduled__2026-05-06T15:30:00+00:00
    WARNING: Resets ALL tasks in the run. Use only on failed runs.
    IDEMPOTENCY: Aborts if run is not in 'failed' state.
    """
    input = input.strip().strip("'\"")
    parts = [p.strip() for p in input.split(",", 1)]
    if len(parts) != 2:
        return "Input must be 'dag_id,run_id'"
    dag_id, run_id = parts
    err = _validate_dag_id(dag_id)
    if err:
        return err
    try:
        data = _airflow_get(f"/dags/{dag_id}/dagRuns/{run_id}")
        current_state = data.get("state")
        if current_state != "failed":
            return f"Aborted: run '{run_id}' is in state '{current_state}', not failed. Will not clear."
        result = _airflow_post(f"/dags/{dag_id}/dagRuns/{run_id}/clear", {"dry_run": False})
        cleared = len(result.get("task_instances", []))
        return f"Cleared {cleared} task instances in {dag_id}/{run_id}. They will be re-run."
    except Exception as e:
        return f"Error clearing dag run: {e}"


# ── Exported for Phase 7 use ──────────────────────────────────────────────────
# Add to agent_core.py ONLY after Phase 6 safety gate is wired in:
#   from airflow_tools import ACTION_TOOLS
#   TOOLS = READ_TOOLS + ACTION_TOOLS   # Phase 7 only
ACTION_TOOLS = [trigger_dag_run, retry_task, unpause_dag, clear_dag_run]