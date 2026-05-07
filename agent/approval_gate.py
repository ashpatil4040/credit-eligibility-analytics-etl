"""
approval_gate.py — Human-in-the-loop approval for high-risk agent actions.

Phase 7: Sends a Gmail approval request and waits for APPROVE/REJECT reply.
Falls back to a CLI prompt if Gmail is not configured.

LEARNING NOTE — Why blocking approval?
  High-risk tools (clear_dag_run, delete_data, etc.) are irreversible.
  The agent blocks until a human responds. If no response arrives within
  APPROVAL_TIMEOUT_SECONDS, the action is automatically rejected.
  This ensures the agent can never silently destroy data.
"""

import os
import time
import uuid
import logging

from notifier import send_gmail, poll_gmail_reply, send_cli_prompt
from audit import record_agent_decision

logger = logging.getLogger(__name__)

APPROVAL_TIMEOUT_SECONDS = int(os.getenv("APPROVAL_TIMEOUT_SECONDS", "300"))
APPROVAL_POLL_INTERVAL   = int(os.getenv("APPROVAL_POLL_INTERVAL", "10"))
GMAIL_ADDRESS            = os.getenv("GMAIL_ADDRESS", "")
# Set AGENT_NON_INTERACTIVE=true when running as a headless service (e.g. event_loop).
# Without a terminal AND without Gmail, high-risk tools must be auto-rejected to prevent
# the approval gate from being silently bypassed.
AGENT_NON_INTERACTIVE    = os.getenv("AGENT_NON_INTERACTIVE", "false").lower() == "true"


def request_approval(tool_name: str, args: str, reason: str) -> bool:
    """
    Request human approval for a high-risk tool call.

    Flow:
      1. Generate unique token for this request
      2. Send Gmail with APPROVE/REJECT instructions
      3. Poll IMAP for reply (blocking, up to APPROVAL_TIMEOUT_SECONDS)
      4. Log the decision with timestamp and channel
      5. Return True if approved, False if rejected or timed out

    Falls back to CLI prompt if Gmail credentials are not set.
    """
    token = str(uuid.uuid4())[:8].upper()
    subject = f"[APPROVAL-{token}] Agent wants to run: {tool_name}"

    body = f"""Credit-Risk Agent — Approval Request
======================================

The AI agent wants to execute a HIGH-RISK action and requires your approval.

  Tool:   {tool_name}
  Input:  {args}
  Reason: {reason}
  Risk:   HIGH

TO APPROVE: Reply to this email with the word APPROVE anywhere in the body.
TO REJECT:  Reply to this email with the word REJECT anywhere in the body.

You have {APPROVAL_TIMEOUT_SECONDS // 60} minutes to respond.
If no response is received, the action will be automatically REJECTED.

Token:     {token}
Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}
""".strip()

    gmail_configured = bool(GMAIL_ADDRESS and os.getenv("GMAIL_APP_PASSWORD"))

    # Non-interactive guard: in headless service mode, Gmail is the only valid channel.
    # If Gmail is not configured, auto-reject to prevent silent bypass.
    if AGENT_NON_INTERACTIVE and not gmail_configured:
        decision = "REJECTED"
        _log_decision(tool_name, args, token, decision, "auto-reject")
        record_agent_decision(tool_name, args, "high", decision, "auto-reject")
        logger.warning(
            "APPROVAL AUTO-REJECTED (non-interactive mode, Gmail not configured): tool=%s",
            tool_name,
        )
        return False

    if gmail_configured:
        try:
            send_gmail(subject, body)
            logger.info("Approval request sent via Gmail. token=%s tool=%s", token, tool_name)
            print(f"\n[APPROVAL REQUIRED] Email sent to {GMAIL_ADDRESS}")
            print(f"  Tool:   {tool_name}")
            print(f"  Input:  {args}")
            print(f"  Token:  {token}")
            print(f"  Waiting up to {APPROVAL_TIMEOUT_SECONDS}s for reply...\n")

            decision = poll_gmail_reply(token, APPROVAL_TIMEOUT_SECONDS, APPROVAL_POLL_INTERVAL)
            approved = decision == "APPROVED"
            _log_decision(tool_name, args, token, decision, "gmail")
            record_agent_decision(tool_name, args, "high", decision, "gmail")
            return approved

        except Exception as exc:
            logger.warning("Gmail approval failed (%s), falling back to CLI.", exc)

    # CLI fallback
    cli_message = (
        f"Agent wants to execute: {tool_name}\n"
        f"  Input:  {args}\n"
        f"  Reason: {reason}\n"
        f"  Risk:   HIGH"
    )
    approved = send_cli_prompt(cli_message)
    decision = "APPROVED" if approved else "REJECTED"
    _log_decision(tool_name, args, token, decision, "cli")
    record_agent_decision(tool_name, args, "high", decision, "cli")
    return approved


def _log_decision(
    tool_name: str, args: str, token: str, decision: str, channel: str
) -> None:
    logger.info(
        "APPROVAL DECISION: tool=%s token=%s decision=%s channel=%s",
        tool_name, token, decision, channel,
    )
    print(f"\n[APPROVAL {decision}] tool={tool_name} token={token} channel={channel}\n")