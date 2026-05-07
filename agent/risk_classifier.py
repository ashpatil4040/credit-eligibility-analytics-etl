"""
risk_classifier.py — Deterministic risk classification for agent tool calls.

LEARNING NOTE — Phase 6:
  Risk classification must be rule-based, NOT LLM-based.
  LLMs are probabilistic — the same input can produce different risk decisions
  on different runs. A risk gate must be deterministic and auditable.

  This module is a pure dict lookup: O(1), no network, no LLM, no randomness.
  Every classification decision is reproducible and can be unit-tested.
"""

from typing import Literal

RiskLevel = Literal["none", "low", "high"]

TOOL_RISK_MAP: dict[str, RiskLevel] = {
    # Read-only — zero risk, always execute
    "list_dag_runs":            "none",
    "get_task_status":          "none",
    "read_eligibility_output":  "none",
    "get_high_risk_customers":  "none",

    # Low risk — mutates state but reversible, auto-execute
    "trigger_dag_run":          "low",   # starts a new run
    "retry_task":               "low",   # retries single failed task
    "unpause_dag":              "low",   # resumes schedule

    # High risk — destructive or hard to reverse, require human approval
    "clear_dag_run":            "high",  # resets entire run state
    "patch_script":             "high",  # modifies pipeline code
    "delete_data":              "high",  # data loss risk
    "modify_schedule":          "high",  # changes DAG timing
}


def classify(tool_name: str) -> RiskLevel:
    """
    Return the risk level for a given tool name.

    Returns:
        "none"  — read-only, execute immediately
        "low"   — mutating but reversible, execute immediately
        "high"  — destructive or irreversible, require human approval

    Unknown tools default to "high" (fail-safe: unknown = assume dangerous).
    """
    return TOOL_RISK_MAP.get(tool_name, "high")