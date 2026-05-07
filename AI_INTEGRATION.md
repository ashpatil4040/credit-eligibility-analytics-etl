# AI Agent Integration Reference

This document covers the autonomous AI agent layer built on top of the
credit-risk Airflow ETL pipeline (Phases 3–9).

---

## Overview

The agent layer adds a self-healing loop to the pipeline:

```
Pipeline failure detected
        │
        ▼
Event loop (event_loop.py)
        │  polls every AGENT_POLL_INTERVAL seconds
        ▼
AgentExecutor (agent_core.py)
        │  ReAct loop: Thought → Action → Observation
        ▼
Risk classifier (risk_classifier.py)
        │  none/low → auto-execute
        │  high     → approval gate blocks
        ▼
Approval gate (approval_gate.py)
        │  Gmail: send email → poll IMAP for reply
        │  CLI:   interactive prompt (non-service mode only)
        ▼
OTel audit trail (audit.py)
        │  every decision and tool execution → Jaeger span
        ▼
Tool executed (tools.py / airflow_tools.py)
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Docker Compose Stack                            │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                   credit-risk-agent service                      │   │
│  │                                                                  │   │
│  │  event_loop.py ──► agent_core.py ──► tools.py                   │   │
│  │       │                  │         airflow_tools.py             │   │
│  │       │          risk_classifier.py                             │   │
│  │       │          approval_gate.py ──► notifier.py (Gmail)       │   │
│  │       │          audit.py ──────────► Jaeger (OTel spans)       │   │
│  │       │          llm_adapter.py ───► Groq API / Ollama          │   │
│  │       │                                                          │   │
│  │       └── Airflow REST API (http://airflow-apiserver:8080)       │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Module Reference

### `agent/event_loop.py` — Autonomous event loop (Phase 9)

Runs as the `credit-risk-agent` Docker service. Polls all three pipeline DAGs
on a fixed interval and triggers the agent when a failure is detected.

**Key behaviour:**

| Event | Action |
|---|---|
| DAG run `failed` / `upstream_failed` (new run_id) | Invoke agent with diagnostic prompt |
| Same run_id failure seen again | Skip (cooldown — prevents infinite re-alerting) |
| DAG run `success` | Clear cooldown for that DAG |

**Env vars consumed:**

| Variable | Default | Purpose |
|---|---|---|
| `AGENT_POLL_INTERVAL` | `60` | Seconds between poll cycles |
| `AIRFLOW_API_BASE_URL` | `http://airflow-apiserver:8080` | Airflow REST API base |
| `AIRFLOW_API_USERNAME` | `airflow` | Basic auth username |
| `AIRFLOW_API_PASSWORD` | `change_me_local_only` | Basic auth password |

---

### `agent/agent_core.py` — AgentExecutor with risk gate (Phases 3, 6, 8)

Builds a LangChain `AgentExecutor` using the ReAct pattern. Every tool call
passes through `RiskGateCallbackHandler` before execution.

**Tool inventory (8 tools):**

| Tool | Risk | Module |
|---|---|---|
| `list_dag_runs` | none | tools.py |
| `get_task_status` | none | tools.py |
| `read_eligibility_output` | none | tools.py |
| `get_high_risk_customers` | none | tools.py |
| `trigger_dag_run` | low | airflow_tools.py |
| `retry_task` | low | airflow_tools.py |
| `unpause_dag` | low | airflow_tools.py |
| `clear_dag_run` | **high** | airflow_tools.py |

**Env vars consumed:**

| Variable | Default | Purpose |
|---|---|---|
| `AGENT_MAX_ITERATIONS` | `20` | ReAct loop iteration cap |
| `AGENT_MAX_EXECUTION_TIME` | `180` | Wall-clock timeout (seconds) |

---

### `agent/risk_classifier.py` — Deterministic risk classification (Phase 6)

Pure dict lookup — O(1), no LLM, no network, deterministic and unit-testable.
Unknown tool names default to `"high"` (fail-safe: unknown = assume dangerous).

**Risk levels:**

| Level | Meaning | Action |
|---|---|---|
| `none` | Read-only | Auto-execute, emit `AUTO` audit span |
| `low` | Mutating but reversible | Auto-execute, emit `AUTO` audit span |
| `high` | Destructive or irreversible | Block on approval gate |

---

### `agent/approval_gate.py` — Human-in-the-loop gate (Phase 7)

Blocks execution of `high`-risk tools until a human responds.

**Approval flow:**

1. Generate a unique 8-char `TOKEN`
2. Send Gmail: `[APPROVAL-TOKEN] Agent wants to run: <tool_name>`
3. Poll IMAP inbox for a reply containing `APPROVE` or `REJECT`
4. Auto-reject after `APPROVAL_TIMEOUT_SECONDS` with no reply
5. CLI fallback if Gmail is not configured (interactive mode only)
6. **Non-interactive guard**: if `AGENT_NON_INTERACTIVE=true` and Gmail is not
   configured, immediately auto-reject without attempting a CLI prompt

**Env vars consumed:**

| Variable | Default | Purpose |
|---|---|---|
| `GMAIL_ADDRESS` | — | Gmail account for send + receive |
| `GMAIL_APP_PASSWORD` | — | Google App Password (not real password) |
| `GMAIL_SMTP_HOST` | `smtp.gmail.com` | SMTP server |
| `GMAIL_SMTP_PORT` | `587` | SMTP port (STARTTLS) |
| `GMAIL_IMAP_HOST` | `imap.gmail.com` | IMAP server |
| `GMAIL_IMAP_PORT` | `993` | IMAP port (SSL) |
| `APPROVAL_TIMEOUT_SECONDS` | `300` | Seconds before auto-reject |
| `APPROVAL_POLL_INTERVAL` | `10` | IMAP poll interval (seconds) |
| `AGENT_NON_INTERACTIVE` | `false` | Set `true` in Docker service mode |

**Security note:** `clear_dag_run` resets all task instances in a run — it is
the only armed tool currently classified as `high`. It will never execute
silently: either Gmail approval is required, or the action is auto-rejected.

---

### `agent/audit.py` — OTel audit trail (Phase 8)

Emits OpenTelemetry spans to Jaeger for every agent decision and tool execution.

**Spans emitted:**

| Span name | Attributes | When |
|---|---|---|
| `agent.decision` | `agent.tool`, `agent.risk_level`, `agent.decision`, `agent.channel`, `agent.args_preview` | On every `on_tool_start` |
| `agent.tool_execution` | `agent.tool`, `agent.result_preview` | On every `on_tool_end` |

**Decision values:** `AUTO` · `APPROVED` · `REJECTED` · `TIMED_OUT`  
**Channel values:** `auto` · `gmail` · `cli` · `auto-reject`

Spans with decision `REJECTED` or `TIMED_OUT` are marked `ERROR` status in Jaeger.

---

### `agent/llm_adapter.py` — Multi-provider LLM abstraction (Phase 2)

Single entry point `get_llm()` — switch providers by changing `LLM_PROVIDER` in `.env`
without touching any other agent code.

**Env vars consumed:**

| Variable | Default | Purpose |
|---|---|---|
| `LLM_PROVIDER` | `ollama` | `groq` or `ollama` |
| `LLM_TEMPERATURE` | `0.0` | Sampling temperature (0 = deterministic) |
| `GROQ_API_KEY` | — | Required when `LLM_PROVIDER=groq` |
| `GROQ_MODEL` | `llama3-8b-8192` | Groq model name |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `mistral` | Ollama model name |
| `OLLAMA_NUM_CTX` | `2048` | Context window tokens |
| `OLLAMA_NUM_PREDICT` | `512` | Max tokens to generate |

**Active model (as of May 2026):** `llama-3.3-70b-versatile` via Groq free tier.

> **Note:** `gemma2-9b-it` was decommissioned by Groq on May 6 2026. If you see
> a 404 or model-not-found error, check `GROQ_MODEL` in `.env`.

---

### `agent/tools.py` — Read-only Airflow tools (Phase 4)

| Tool | Input | Returns |
|---|---|---|
| `list_dag_runs` | `dag_id` | Last 5 runs with state + execution date |
| `get_task_status` | `dag_id,dag_run_id` | All task instance states for the run |
| `read_eligibility_output` | _(none)_ | Last 10 rows of `eligibility_programs.csv` |
| `get_high_risk_customers` | `n` (optional) | Top-N customers by risk score |

---

### `agent/airflow_tools.py` — Action tools (Phase 5)

| Tool | What it does | Risk |
|---|---|---|
| `trigger_dag_run` | POST `/dags/{id}/dagRuns` | low |
| `retry_task` | POST `/dags/{id}/dagRuns/{run}/taskInstances/{task}/clear` | low |
| `unpause_dag` | PATCH `/dags/{id}` `is_paused=false` | low |
| `clear_dag_run` | POST `/dags/{id}/dagRuns/{run}/clear` | **high** |

All tools have idempotency guards: they check current state before acting and
return a descriptive message instead of re-doing completed work.

---

### `agent/notifier.py` — Gmail transport (Phase 7)

| Function | Purpose |
|---|---|
| `send_gmail(subject, body)` | Send via SMTP STARTTLS |
| `poll_gmail_reply(token, timeout_s, interval_s)` | Poll IMAP for `APPROVE`/`REJECT` reply |
| `send_cli_prompt(message)` | Interactive terminal prompt (interactive mode only) |

---

## Running the Agent

### Interactive CLI (manual queries)

```bash
docker compose exec credit-risk-agent python /opt/airflow/agent/cli.py
```

### Autonomous event loop (always running as a service)

The `credit-risk-agent` service starts automatically with `docker compose up -d`.
Check its logs:

```bash
docker compose logs credit-risk-agent -f
```

### Rebuilding after code changes

```bash
docker compose build --no-cache
docker compose up -d credit-risk-agent
```

> Use `up -d` (not `restart`) after `.env` changes — `restart` does not re-read env vars.

---

## Security Model

| Threat | Mitigation |
|---|---|
| Agent autonomously destroys data | `clear_dag_run` classified `high`; always blocked by approval gate |
| Approval gate bypassed in headless mode | `AGENT_NON_INTERACTIVE=true` → auto-reject when Gmail unavailable |
| LangChain swallows PermissionError | Raise `RuntimeError` instead — not caught by `handle_parsing_errors` |
| Secrets in code | All credentials in `.env` (gitignored); `.env.example` has no real values |
| Unknown tool called by LLM hallucination | Unrecognised tool names default to risk `"high"` in classifier |

---

## Observability

All agent activity is visible in Jaeger at **http://localhost:16686** under
service name `credit-risk-agent`.

Filter by operation name to see:
- `agent.decision` — every tool risk classification and approval outcome
- `agent.tool_execution` — every tool result

Span attributes allow filtering by `agent.risk_level`, `agent.decision`, and
`agent.channel` without reading raw logs.
