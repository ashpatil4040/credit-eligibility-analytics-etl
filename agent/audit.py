"""
audit.py — OTel audit trail for agent decisions and tool executions.

Phase 8: Every tool call emits a span to Jaeger so every agent decision
is auditable, searchable, and reproducible.

Span types emitted:
  agent.decision       — fired on every tool call (risk gate outcome)
  agent.tool_execution — fired after a tool completes (result preview)

LEARNING NOTE:
  This module reuses scripts/otel_setup.py directly via sys.path rather
  than duplicating the OTel bootstrapping code. This means the agent
  service appears in Jaeger as "credit-risk-agent" alongside the pipeline
  services, and all spans share the same Jaeger backend.

  Both functions are wrapped in try/except so a tracing failure can
  never crash the agent. Observability is a side-effect, not a dependency.
"""

import sys
import os
import logging

# Reuse the shared OTel bootstrap from the pipeline scripts directory.
# Must come before any opentelemetry imports so setup_telemetry() runs first.
_scripts_path = os.path.join(os.path.dirname(__file__), "..", "scripts")
sys.path.insert(0, os.path.abspath(_scripts_path))

logger = logging.getLogger(__name__)

_otel_available = False
_tracer = None


def _init_otel() -> None:
    global _otel_available, _tracer
    if _otel_available:
        return
    try:
        from otel_setup import setup_telemetry, get_tracer  # noqa: PLC0415
        setup_telemetry("credit-risk-agent")
        _tracer = get_tracer(__name__)
        _otel_available = True
        logger.info("audit.py: OTel tracer initialised (service=credit-risk-agent)")
    except Exception as exc:
        logger.warning("audit.py: OTel unavailable, spans will be skipped. reason=%s", exc)


# Initialise eagerly at import time so the first span isn't delayed.
_init_otel()


def record_agent_decision(
    tool_name: str,
    args: str,
    risk_level: str,
    decision: str,
    channel: str,
) -> None:
    """
    Emit an OTel span recording the risk-gate decision for a tool call.

    Parameters
    ----------
    tool_name  : name of the tool the agent wanted to call
    args       : raw input string passed to the tool (first 200 chars stored)
    risk_level : "none" | "low" | "high"
    decision   : "AUTO" | "APPROVED" | "REJECTED" | "TIMED_OUT"
    channel    : "auto" | "gmail" | "cli"
    """
    if not _otel_available or _tracer is None:
        return
    try:
        from opentelemetry.trace import Status, StatusCode  # noqa: PLC0415

        with _tracer.start_as_current_span("agent.decision") as span:
            span.set_attribute("agent.tool", tool_name)
            span.set_attribute("agent.risk_level", risk_level)
            span.set_attribute("agent.decision", decision)
            span.set_attribute("agent.channel", channel)
            span.set_attribute("agent.args_preview", args[:200])

            if decision in ("REJECTED", "TIMED_OUT"):
                span.set_status(Status(StatusCode.ERROR, f"{decision} by {channel}"))
            else:
                span.set_status(Status(StatusCode.OK))

    except Exception as exc:
        logger.debug("audit.record_agent_decision failed silently: %s", exc)


def record_tool_execution(tool_name: str, result_preview: str) -> None:
    """
    Emit an OTel span recording the outcome of a tool that was allowed to run.

    Parameters
    ----------
    tool_name      : name of the tool that executed
    result_preview : first 200 chars of the tool's return value
    """
    if not _otel_available or _tracer is None:
        return
    try:
        from opentelemetry.trace import Status, StatusCode  # noqa: PLC0415

        with _tracer.start_as_current_span("agent.tool_execution") as span:
            span.set_attribute("agent.tool", tool_name)
            span.set_attribute("agent.result_preview", result_preview[:200])
            span.set_status(Status(StatusCode.OK))

    except Exception as exc:
        logger.debug("audit.record_tool_execution failed silently: %s", exc)
