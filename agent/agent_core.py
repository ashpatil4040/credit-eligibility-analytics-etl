"""
agent_core.py — LangChain AgentExecutor with risk-classifier safety gate and OTel audit trail.

Phase 8: Every tool call is intercepted by classify() before execution.
  none/low  -> AUTO decision span emitted, tool executes immediately
  high      -> approval gate blocks until human responds (Phase 7);
               APPROVED/REJECTED/TIMED_OUT span emitted after decision
  on_tool_end -> tool_execution span emitted with result preview
  Action tools (trigger/retry/unpause/clear) now armed alongside read tools.
"""

import logging
import sys
import os
import time
from typing import Any, Union

from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.prompts import PromptTemplate
from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_core.agents import AgentAction, AgentFinish

sys.path.insert(0, os.path.dirname(__file__))
from llm_adapter import get_llm

# Tunable via .env — override without rebuilding the image
AGENT_MAX_ITERATIONS     = int(os.getenv("AGENT_MAX_ITERATIONS", "20"))
AGENT_MAX_EXECUTION_TIME = int(os.getenv("AGENT_MAX_EXECUTION_TIME", "180"))
from tools import list_dag_runs, get_task_status, read_eligibility_output, get_high_risk_customers
from airflow_tools import ACTION_TOOLS
from risk_classifier import classify
from approval_gate import request_approval
from audit import record_agent_decision, record_tool_execution

logger = logging.getLogger(__name__)

READ_TOOLS = [list_dag_runs, get_task_status, read_eligibility_output, get_high_risk_customers]
ALL_TOOLS  = READ_TOOLS + ACTION_TOOLS

SYSTEM_PROMPT = """You are an AI agent that monitors a credit-risk ETL pipeline built on Apache Airflow.

The pipeline runs three DAGs sequentially every hour:
  1. raw_ingestion_dag         (at :00) -- copies raw CSV files to a staging area
  2. feature_engineering_dag   (at :15) -- builds customer financial profiles from staged data
  3. eligibility_analytics_dag (at :30) -- assigns risk levels and program eligibility to customers

Risk levels: LOW, MEDIUM, HIGH, CRITICAL
Programs: SETTLEMENT_PLAN, HARDSHIP_PROGRAM, PAYMENT_EXTENSION, COLLECTIONS_REVIEW, STANDARD_MONITORING

Safety rules:
  - Never recommend executing, deleting, or modifying pipeline data without explicit human approval.
  - Any action involving HIGH or CRITICAL risk customers must be flagged for human review first.
  - If uncertain about live state, use a tool to check -- do not guess or fabricate data.
  - You may explain, summarise, and recommend. You may NOT autonomously act on the pipeline."""

_REACT_TEMPLATE = SYSTEM_PROMPT + """

You have access to the following tools:
{tools}

Use the following format STRICTLY. You MUST always end with Final Answer:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

IMPORTANT: After receiving Observations, you MUST write a Final Answer. Never stop after an Observation.

Begin!

Question: {input}
Thought:{agent_scratchpad}"""


class RiskGateCallbackHandler(BaseCallbackHandler):
    """
    Intercepts every tool call and checks its risk level before execution.

    LEARNING NOTE — Phase 8:
      none/low  -> record AUTO decision span, execute immediately
      high      -> send approval request, block until human responds;
                   record APPROVED/REJECTED/TIMED_OUT span;
                   if rejected: raise PermissionError to stop the tool
      on_tool_end -> record tool_execution span with result preview
    """

    def __init__(self) -> None:
        super().__init__()
        self._current_tool: str = "unknown"  # set in on_tool_start, read in on_tool_end

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        **kwargs: Any,
    ) -> None:
        tool_name = serialized.get("name", "unknown")
        self._current_tool = tool_name
        risk = classify(tool_name)
        logger.info("RISK GATE: tool=%s risk=%s", tool_name, risk)

        if risk == "high":
            approved = request_approval(
                tool_name=tool_name,
                args=input_str,
                reason="Agent requested this action during pipeline analysis.",
            )
            if not approved:
                # approval_gate already emitted the audit span.
                # Raise RuntimeError (not PermissionError) so LangChain's
                # handle_parsing_errors=True does NOT swallow it silently.
                raise RuntimeError(
                    f"[RISK GATE BLOCKED] Tool '{tool_name}' was rejected by human approver."
                )
            logger.info("RISK GATE: tool=%s approved by human", tool_name)
        else:
            # none / low — auto-allowed, emit audit span immediately
            record_agent_decision(tool_name, input_str, risk, "AUTO", "auto")

    def on_tool_end(
        self,
        output: Any,
        **kwargs: Any,
    ) -> None:
        record_tool_execution(self._current_tool, str(output)[:200])


_executor: AgentExecutor | None = None


def _build_executor() -> AgentExecutor:
    llm = get_llm()
    if llm is None:
        raise RuntimeError(
            "LLM is unavailable. Check LLM_PROVIDER and related env vars."
        )
    prompt = PromptTemplate.from_template(_REACT_TEMPLATE)
    agent = create_react_agent(llm=llm, tools=ALL_TOOLS, prompt=prompt)
    return AgentExecutor(
        agent=agent,
        tools=ALL_TOOLS,
        verbose=True,
        handle_parsing_errors=True,
        max_iterations=AGENT_MAX_ITERATIONS,
        max_execution_time=AGENT_MAX_EXECUTION_TIME,
        callbacks=[RiskGateCallbackHandler()],
    )


def _get_executor() -> AgentExecutor:
    global _executor
    if _executor is None:
        logger.info("Building AgentExecutor with audit trail (Phase 8)...")
        _executor = _build_executor()
    return _executor


def run(user_message: str) -> str:
    """Send a message to the agent and return its text response."""
    for attempt in range(3):
        try:
            executor = _get_executor()
            result = executor.invoke({"input": user_message})
            return result["output"]
        except Exception as exc:
            if "429" in str(exc) and attempt < 2:
                wait = 10 * (attempt + 1)
                logger.warning("Rate limit hit, waiting %ds before retry %d/3...", wait, attempt + 2)
                time.sleep(wait)
                continue
            logger.warning("Agent execution failed: %s", exc)
            return f"[Agent error -- {exc}]"