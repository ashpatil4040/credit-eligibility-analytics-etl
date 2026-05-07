"""
tools.py — Real LangChain tools that give the agent live system access.

Phase 4: These tools replace hallucination with actual data from:
  - Airflow REST API (dag runs, task statuses)
  - Local CSV output (eligibility analytics results)
"""

import os
import json
import logging
import pandas as pd
import requests
from langchain.tools import tool

logger = logging.getLogger(__name__)

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_API_BASE_URL", "http://airflow-apiserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_API_USERNAME", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_API_PASSWORD", "change_me_local_only")
ELIGIBILITY_CSV = os.getenv("ELIGIBILITY_CSV_PATH", "/opt/airflow/data/eligibility_programs.csv")

DAG_IDS = ["raw_ingestion_dag", "feature_engineering_dag", "eligibility_analytics_dag"]


def _airflow_get(path: str) -> dict:
    url = f"{AIRFLOW_BASE_URL}/api/v1{path}"  # v1 not v2
    resp = requests.get(url, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=10)
    resp.raise_for_status()
    return resp.json()


@tool
def list_dag_runs(dag_id: str) -> str:
    """
    Returns the last 5 DAG runs for a given dag_id with their state and execution date.
    Valid dag_ids: raw_ingestion_dag, feature_engineering_dag, eligibility_analytics_dag
    Input must be exactly one of those dag_ids, nothing else.
    """
    dag_id = dag_id.strip().strip("'\"").split()[0].rstrip(",")
    if dag_id not in DAG_IDS:
        return f"Unknown dag_id '{dag_id}'. Valid options: {DAG_IDS}"
    try:
        data = _airflow_get(f"/dags/{dag_id}/dagRuns?limit=5&order_by=-execution_date")
        runs = data.get("dag_runs", [])
        if not runs:
            return f"No runs found for {dag_id}."
        lines = [f"Last {len(runs)} runs for {dag_id}:"]
        for r in runs:
            lines.append(f"  run_id={r['dag_run_id']}  state={r['state']}  executed={r['execution_date']}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching dag runs: {e}"


@tool
def get_task_status(input: str) -> str:
    """
    Returns task instance states for a specific DAG run.
    Input format must be exactly: dag_id,dag_run_id
    Example: eligibility_analytics_dag,scheduled__2024-01-01T00:30:00+00:00
    Do NOT wrap values in quotes.
    """
    input = input.strip().strip("'\"").split("\n")[0]
    try:
        parts = [p.strip().strip("'\"") for p in input.split(",", 1)]
        if len(parts) != 2:
            return "Input must be 'dag_id,dag_run_id'"
        dag_id, run_id = parts
        data = _airflow_get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        tasks = data.get("task_instances", [])
        if not tasks:
            return f"No task instances found for run {run_id}."
        lines = [f"Tasks for {dag_id} / {run_id}:"]
        for t in tasks:
            lines.append(f"  task={t['task_id']}  state={t['state']}  duration={t.get('duration', 'N/A')}s")
        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching task status: {e}"


@tool
def read_eligibility_output(input: str = "") -> str:
    """
    Reads the latest eligibility analytics CSV output and returns summary statistics:
    total customers, counts per risk level, counts per program assigned.
    No PII is returned.
    """
    try:
        if not os.path.exists(ELIGIBILITY_CSV):
            return f"Eligibility CSV not found at {ELIGIBILITY_CSV}. Has eligibility_analytics_dag run yet?"
        df = pd.read_csv(ELIGIBILITY_CSV)
        total = len(df)
        risk_counts = df["risk_level"].value_counts().to_dict() if "risk_level" in df.columns else {}
        program_counts = df["recommended_program"].value_counts().to_dict() if "recommended_program" in df.columns else {}
        lines = [
            f"Eligibility output summary ({total} customers total):",
            "Risk level breakdown:",
        ]
        for level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            lines.append(f"  {level}: {risk_counts.get(level, 0)}")
        lines.append("Program breakdown:")
        for prog, cnt in program_counts.items():
            lines.append(f"  {prog}: {cnt}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error reading eligibility CSV: {e}"


@tool
def get_high_risk_customers(input: str = "") -> str:
    """
    Returns HIGH and CRITICAL risk customers from the latest eligibility output.
    Returns customer_id, risk_level, and recommended_program only (no PII).
    """
    try:
        if not os.path.exists(ELIGIBILITY_CSV):
            return f"Eligibility CSV not found at {ELIGIBILITY_CSV}. Has eligibility_analytics_dag run yet?"
        df = pd.read_csv(ELIGIBILITY_CSV)
        if "risk_level" not in df.columns:
            return "Column 'risk_level' not found in eligibility output."
        high_risk = df[df["risk_level"].isin(["HIGH", "CRITICAL"])]
        if high_risk.empty:
            return "No HIGH or CRITICAL risk customers in the latest run."
        cols = [c for c in ["customer_id", "risk_level", "recommended_program"] if c in df.columns]
        lines = [f"HIGH/CRITICAL risk customers ({len(high_risk)} total):"]
        for _, row in high_risk[cols].iterrows():
            lines.append("  " + "  ".join(f"{c}={row[c]}" for c in cols))
        return "\n".join(lines)
    except Exception as e:
        return f"Error reading high risk customers: {e}"