"""
event_loop.py — Autonomous event loop for the credit-risk agent.

Phase 9: Runs as a Docker service that polls all three pipeline DAGs every
AGENT_POLL_INTERVAL seconds. When a DAG run is detected as failed, the agent
is automatically invoked to diagnose it and recommend (or, with approval,
execute) remediation.

Cooldown design:
  seen_failures maps dag_id -> run_id of the run we already analysed.
  A new failure on the same run_id is ignored (prevents infinite re-alerting).
  When the DAG next succeeds or a new (different) run_id fails, the cooldown
  clears and the agent is triggered again.

LEARNING NOTE:
  The event loop deliberately does NOT pass the agent's output to any
  auto-execute path. The agent may RECOMMEND action, but all high-risk
  tool calls still go through the approval gate (Phase 7). The loop is
  purely a trigger — it converts pipeline failures into agent prompts.
"""

import os
import sys
import time
import logging
import requests

# Allow imports from sibling agent modules and from scripts/ (for OTel)
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("event_loop")

POLL_INTERVAL    = int(os.getenv("AGENT_POLL_INTERVAL", "60"))
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_API_BASE_URL", "http://airflow-apiserver:8080")
AIRFLOW_USER     = os.getenv("AIRFLOW_API_USERNAME", "airflow")
AIRFLOW_PASS     = os.getenv("AIRFLOW_API_PASSWORD", "change_me_local_only")

DAGS = [
    "raw_ingestion_dag",
    "feature_engineering_dag",
    "eligibility_analytics_dag",
]

# Cooldown: dag_id -> run_id we already alerted on.
# Cleared when the DAG produces a successful run or a new failed run_id appears.
seen_failures: dict[str, str] = {}


def _airflow_get(path: str) -> dict:
    url = f"{AIRFLOW_BASE_URL}/api/v1{path}"
    resp = requests.get(url, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=10)
    resp.raise_for_status()
    return resp.json()


def _check_airflow_health() -> bool:
    """Return True if the Airflow API is reachable and the scheduler is healthy."""
    try:
        data = _airflow_get("/health")
        scheduler_status = data.get("scheduler", {}).get("status", "unknown")
        logger.info("Airflow health check: scheduler=%s", scheduler_status)
        return scheduler_status == "healthy"
    except Exception as exc:
        logger.warning("Airflow health check failed: %s", exc)
        return False


def _get_latest_run(dag_id: str) -> dict | None:
    """Return the most recent DAG run dict for dag_id, or None on error."""
    try:
        data = _airflow_get(f"/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date")
        runs = data.get("dag_runs", [])
        return runs[0] if runs else None
    except Exception as exc:
        logger.warning("Could not fetch runs for %s: %s", dag_id, exc)
        return None


def _invoke_agent(dag_id: str, run_id: str, state: str) -> str:
    """
    Run the agent with a failure-alert prompt and return its response.

    Lazy-imports agent_core so the LLM is only initialised on first failure,
    not at container startup.
    """
    from agent_core import run  # noqa: PLC0415

    prompt = (
        f"Pipeline alert: DAG '{dag_id}' run '{run_id}' has state '{state}'. "
        f"Please diagnose the failure by checking task statuses and any available "
        f"output data, then recommend the best remediation action."
    )
    logger.info("Invoking agent for %s / %s ...", dag_id, run_id)
    try:
        response = run(prompt)
        return response
    except Exception as exc:
        return f"[Agent invocation error: {exc}]"


def poll_once(cycle: int) -> None:
    """Check all three DAGs once. Trigger the agent on new failures."""
    logger.info("--- Poll cycle %d ---", cycle)
    for dag_id in DAGS:
        latest = _get_latest_run(dag_id)
        if latest is None:
            continue

        run_id = latest.get("dag_run_id", "unknown")
        state  = latest.get("state", "unknown")

        if state == "success":
            # Clear cooldown so a future failure on this DAG will alert again
            if dag_id in seen_failures:
                logger.info("%s recovered (success). Clearing failure cooldown.", dag_id)
                del seen_failures[dag_id]
            else:
                logger.info("%s: latest run %s = %s", dag_id, run_id, state)

        elif state in ("failed", "upstream_failed"):
            if seen_failures.get(dag_id) == run_id:
                logger.info(
                    "%s: run %s already analysed (cooldown active), skipping.", dag_id, run_id
                )
            else:
                logger.warning(
                    "FAILURE DETECTED: %s / %s (state=%s) — invoking agent", dag_id, run_id, state
                )
                seen_failures[dag_id] = run_id
                response = _invoke_agent(dag_id, run_id, state)
                logger.info("Agent response for %s:\n%s", dag_id, response)

        else:
            # running, queued, restarting, etc.
            logger.info("%s: latest run %s = %s", dag_id, run_id, state)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Credit-Risk Agent  |  Phase 9 — Autonomous Event Loop")
    logger.info("Poll interval : %ds", POLL_INTERVAL)
    logger.info("Monitored DAGs: %s", DAGS)
    logger.info("Airflow API   : %s", AIRFLOW_BASE_URL)
    logger.info("=" * 60)

    # Block until Airflow is ready before entering the poll loop
    logger.info("Waiting for Airflow API to become healthy...")
    while not _check_airflow_health():
        logger.info("Airflow not ready, retrying in 15s...")
        time.sleep(15)
    logger.info("Airflow is healthy. Entering poll loop.")

    cycle = 0
    while True:
        cycle += 1
        try:
            poll_once(cycle)
        except Exception as exc:
            logger.error("Unhandled error in poll cycle %d: %s", cycle, exc)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
