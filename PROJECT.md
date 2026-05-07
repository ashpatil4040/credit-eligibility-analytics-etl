# Credit Risk Airflow ETL — Project Reference

## Overview

This project is a **credit risk data pipeline** built on Apache Airflow 2.9.1 running in Docker
Compose. It ingests raw Home Credit loan application data, engineers customer financial features,
and produces a risk segmentation and program eligibility analysis. The entire pipeline runs
locally on a single machine using the `LocalExecutor` — no distributed workers required.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Compose Stack                         │
│                                                                     │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐   │
│  │   postgres:16  │  │  airflow-init │  │       jaeger:1.57     │   │
│  │  (metadata DB) │  │  (one-shot)   │  │  (trace/metric store) │   │
│  └───────┬───────┘  └───────────────┘  └──────────┬────────────┘   │
│          │                                         │ :16686 (UI)    │
│          │  ┌──────────────────────────────────┐   │ :4317 (OTLP)   │
│          └──┤      airflow-apiserver            ├───┘               │
│             │      airflow-scheduler            │                   │
│             │      airflow-dag-processor        │                   │
│             │      airflow-triggerer            │                   │
│             └──────────────┬───────────────────┘                   │
│                            │ LocalExecutor (in-process)             │
│                            ▼                                        │
│              ┌─────────────────────────┐                           │
│              │   Task subprocesses     │                           │
│              │  python scripts/*.py    │                           │
│              └─────────────────────────┘                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Services

| Service | Image | Role | Port |
|---|---|---|---|
| `postgres` | postgres:16 | Airflow metadata database | internal |
| `airflow-init` | custom build | One-shot init: creates DB, admin user, default config | — |
| `airflow-apiserver` | custom build | Airflow web UI + REST API | `8081` (host) |
| `airflow-scheduler` | custom build | DAG run scheduling and task dispatch | internal |
| `airflow-dag-processor` | custom build | Parses DAG files, detects changes | internal |
| `airflow-triggerer` | custom build | Handles deferred/async operators | internal |
| `jaeger` | jaegertracing/all-in-one:1.57 | OpenTelemetry trace + metric backend | `16686` (UI), `4317` (OTLP gRPC) |
| `credit-risk-agent` | custom build | Autonomous AI agent — polls DAGs, diagnoses failures, runs approval-gated remediation | internal |

### Executor: LocalExecutor

Tasks run in the same container as the scheduler as Python subprocesses. There are no Celery
workers or Redis in the default profile. This keeps the stack lightweight for local development.
The `celery` Docker Compose profile exists but is not active.

### Image Build

```
Dockerfile
  └── FROM apache/airflow:2.9.1
      └── COPY requirements.txt
          └── RUN pip install -r requirements.txt
```

`requirements.txt` installs:
- `pandas`, `numpy` — data processing
- `opentelemetry-api==1.24.0`, `opentelemetry-sdk==1.24.0` — OTel core
- `opentelemetry-exporter-otlp-proto-grpc==1.24.0` — OTLP gRPC exporter to Jaeger
- `opentelemetry-instrumentation==0.45b0` — instrumentation helpers

---

## Repository Layout

```
credit-risk-airflow-etl/
├── docker-compose.yaml          # Full stack definition
├── Dockerfile                   # Airflow image with OTel deps
├── requirements.txt             # Python dependencies
├── .env                         # Runtime config (UIDs, ports, OTel endpoint)
│
├── dags/
│   ├── raw_ingestion_dag.py         # Stage 1 DAG
│   ├── feature_engineering_dag.py   # Stage 2 DAG
│   └── eligibility_analytics_dag.py # Stage 3 DAG
│
├── agent/                       # AI agent layer (Phases 3–9)
│   ├── event_loop.py            # Phase 9: autonomous polling service
│   ├── agent_core.py            # Phase 3/6/8: AgentExecutor + risk gate + audit
│   ├── tools.py                 # Phase 4: read-only Airflow tools
│   ├── airflow_tools.py         # Phase 5: action tools (trigger/retry/clear)
│   ├── risk_classifier.py       # Phase 6: deterministic tool risk classification
│   ├── approval_gate.py         # Phase 7: Gmail human-in-the-loop approval
│   ├── notifier.py              # Phase 7: Gmail SMTP/IMAP transport
│   ├── audit.py                 # Phase 8: OTel audit spans to Jaeger
│   ├── llm_adapter.py           # Phase 2: Groq/Ollama LLM abstraction
│   └── cli.py                   # Interactive CLI for manual agent queries
│
├── plugins/
│   └── dag_instrumentation.py   # OTel DAG/task callbacks (auto-loaded by Airflow)
│
├── scripts/
│   ├── otel_setup.py            # Shared OTel provider initialisation
│   ├── raw_ingestion.py         # Stage 1 business logic
│   ├── feature_engineering.py   # Stage 2 business logic
│   └── eligibility_analytics.py # Stage 3 business logic
│
└── data/
    ├── raw/                     # Source CSV files (mounted read-only at runtime)
    ├── staging/                 # Output of Stage 1
    └── processed/               # Output of Stages 2 and 3
```

---

## Data Flow

```
data/raw/                          data/staging/                data/processed/
────────────────────────           ─────────────────────        ──────────────────────────────
application_train.csv   ──┐        application_train.csv  ──┐
credit_card_balance.csv ──┼──────► credit_card_balance.csv──┼──► customer_financial_profile.csv
installments_payments.csv┘        installments_payments.csv┘         │
                                                                      │
                                                                      ▼
                                                             eligibility_programs.csv
                                                             risk_segmentation_summary.csv
                                                             program_eligibility_summary.csv
```

### Stage 1 — Raw Ingestion

**DAG:** `raw_ingestion_dag` · Schedule: `0 * * * *` (top of every hour)  
**Script:** `scripts/raw_ingestion.py`  
**Operator:** `BashOperator` → `python /opt/airflow/scripts/raw_ingestion.py`

What it does:
1. `validate_raw_files()` — checks all three required CSVs exist in `data/raw/`. Raises
   `FileNotFoundError` if any are missing; Airflow retries up to 2 times with 5-minute backoff.
2. `stage_raw_files()` — copies each file from `data/raw/` to `data/staging/` using
   `shutil.copy`. Records file size bytes per file for OTel metrics.

Output: Three CSVs in `data/staging/` (total ~1.3 GB).

---

### Stage 2 — Feature Engineering

**DAG:** `feature_engineering_dag` · Schedule: `15 * * * *` (15 min past every hour)  
**Script:** `scripts/feature_engineering.py`  
**Operators:** `ExternalTaskSensor` (waits for Stage 1) + `BashOperator`

The `ExternalTaskSensor` waits for `raw_ingestion_dag` to succeed with:
- `execution_delta=timedelta(minutes=15)` — matches the 15-minute offset between schedules
- `mode="reschedule"` — releases the worker slot while waiting (efficient for LocalExecutor)
- `timeout=900s`, `poke_interval=30s`

What the script does:

**`create_customer_base()`**
- Reads `application_train.csv` (307,511 rows)
- Selects 7 columns: `SK_ID_CURR`, `TARGET`, income, credit, annuity, income type, days employed
- Renames to lowercase snake_case column names

**`create_credit_card_features()`**
- Reads `credit_card_balance.csv`
- Computes `utilization_ratio = AMT_BALANCE / AMT_CREDIT_LIMIT_ACTUAL`
- Groups by customer: avg/max balance, avg/max utilization, total drawings, total payments

**`create_payment_features()`**
- Reads `installments_payments.csv`
- Computes `payment_delay_days = DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT`
- Groups by customer: avg/max delay, late payment count, underpaid count, payment totals

**`run_feature_engineering()`**
- Left-joins all three frames on `SK_ID_CURR`
- Fills nulls with `0` (customers with no credit card or payment history)
- Writes `customer_financial_profile.csv` — **307,511 rows × 20 columns**

---

### Stage 3 — Eligibility Analytics

**DAG:** `eligibility_analytics_dag` · Schedule: `30 * * * *` (30 min past every hour)  
**Script:** `scripts/eligibility_analytics.py`  
**Operators:** `ExternalTaskSensor` (waits for Stage 2) + `BashOperator`

What the script does:

**`assign_risk_level(row)`** — row-wise classification:

| Condition | Risk Level |
|---|---|
| `max_delay > 90` OR `late_count >= 5` OR `utilization > 1.0` | CRITICAL |
| `max_delay > 60` OR `utilization > 0.85` | HIGH |
| `max_delay > 30` OR `utilization > 0.60` | MEDIUM |
| Otherwise | LOW |

**`assign_program(row)`** — row-wise program assignment:

| Condition | Program |
|---|---|
| CRITICAL + income < 50,000 | SETTLEMENT_PLAN |
| delay > 60 days + income < 75,000 | HARDSHIP_PROGRAM |
| utilization > 0.85 + late_count ≤ 3 | PAYMENT_EXTENSION |
| HIGH or CRITICAL | COLLECTIONS_REVIEW |
| Otherwise | STANDARD_MONITORING |

**Outputs** (all written to `data/processed/`):

| File | Contents | Rows |
|---|---|---|
| `eligibility_programs.csv` | Full per-customer risk + program assignment | 307,511 |
| `risk_segmentation_summary.csv` | Aggregated stats per risk level | 4 |
| `program_eligibility_summary.csv` | Aggregated stats per program | 5 |

**Observed distribution (last run):**

| Risk Level | Count |
|---|---|
| LOW | 205,912 (67%) |
| CRITICAL | 90,308 (29%) |
| HIGH | 6,842 (2.2%) |
| MEDIUM | 4,449 (1.4%) |

---

## DAG Dependency Chain

```
raw_ingestion_dag        feature_engineering_dag     eligibility_analytics_dag
─────────────────        ───────────────────────     ─────────────────────────
run_raw_ingestion   ───► wait_for_raw_ingestion  ──► wait_for_feature_engineering
  (BashOperator)           (ExternalTaskSensor)         (ExternalTaskSensor)
                               │                               │
                               ▼                               ▼
                       run_feature_engineering       run_eligibility_analytics
                           (BashOperator)                (BashOperator)
```

Schedule offsets ensure the sensor always finds a completed upstream run:
- Stage 1 runs at `:00` — finishes in ~2 min
- Stage 2 waits for Stage 1's `:00` run, starts at `:15`
- Stage 3 waits for Stage 2's `:15` run, starts at `:30`

---

## Configuration

### `.env`

```
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
FERNET_KEY=                            # set for encrypted connections
AIRFLOW_WEBSERVER_PORT=8081            # host port for Airflow UI
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317
OTEL_TRACES_SAMPLER=always_on
OTEL_PYTHON_LOG_CORRELATION=true

# Airflow REST API (used by agent)
AIRFLOW_API_BASE_URL=http://airflow-apiserver:8080
AIRFLOW_API_USERNAME=airflow
AIRFLOW_API_PASSWORD=change_me_local_only

# LLM provider — "groq" (cloud) or "ollama" (local)
LLM_PROVIDER=groq
LLM_TEMPERATURE=0.0
GROQ_API_KEY=<your-key>
GROQ_MODEL=llama-3.3-70b-versatile

# Agent tuning
AGENT_MAX_ITERATIONS=20
AGENT_MAX_EXECUTION_TIME=180
AGENT_POLL_INTERVAL=60
AGENT_NON_INTERACTIVE=false   # set true in the Docker service

# Gmail approval gate
GMAIL_ADDRESS=you@gmail.com
GMAIL_APP_PASSWORD=<app-password>
APPROVAL_TIMEOUT_SECONDS=300
```

See `.env.example` for a fully annotated template with generation instructions.
See [AI_INTEGRATION.md](AI_INTEGRATION.md) for the complete AI agent reference.

### Key Airflow environment variables (set in `docker-compose.yaml`)

| Variable | Value | Effect |
|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | No workers, tasks run in-process |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Hides built-in example DAGs |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `true` | New DAGs start paused |
| `OTEL_SERVICE_NAME` | per-service override | Separates spans by Airflow component in Jaeger |
| `OTEL_RESOURCE_ATTRIBUTES` | `deployment.environment=local,...` | Stamps env on all telemetry |

---

## Running the Stack

```bash
# First run / after requirements.txt change
docker compose down
docker compose build --no-cache
docker compose up -d

# Normal restart (no dependency changes)
docker compose restart

# Check service health
docker compose ps

# Unpause DAGs (required after first deploy)
docker compose exec airflow-scheduler airflow dags unpause raw_ingestion_dag
docker compose exec airflow-scheduler airflow dags unpause feature_engineering_dag
docker compose exec airflow-scheduler airflow dags unpause eligibility_analytics_dag

# Trigger a full pipeline run manually
docker compose exec airflow-scheduler airflow dags trigger raw_ingestion_dag \
  --exec-date "2026-05-04T12:00:00+00:00"
docker compose exec airflow-scheduler airflow dags trigger feature_engineering_dag \
  --exec-date "2026-05-04T12:15:00+00:00"
docker compose exec airflow-scheduler airflow dags trigger eligibility_analytics_dag \
  --exec-date "2026-05-04T12:30:00+00:00"

# Check run status
docker compose exec airflow-scheduler \
  airflow dags list-runs -d raw_ingestion_dag --no-backfill 2>/dev/null | head -5

# View task logs
docker compose exec airflow-scheduler \
  airflow tasks logs raw_ingestion_dag run_raw_ingestion "2026-05-04T12:00:00+00:00"
```

### Access points

| URL | Description |
|---|---|
| http://localhost:8081 | Airflow UI (credentials configured in `.env`) |
| http://localhost:16686 | Jaeger trace UI |

---

## Retry and Error Behaviour

All tasks share these defaults (set in `default_args` in each DAG):

```python
"retries": 2
"retry_delay": timedelta(minutes=5)
```

- Stage 1 failure (missing files): `FileNotFoundError` is raised, span is marked `ERROR`,
  exception is recorded on the span with full stack trace. Airflow retries twice.
- Stage 2/3 sensor timeout (upstream not ready within 900s): sensor raises
  `AirflowSensorTimeout`, DAG is marked failed, `on_failure_callback` fires and emits a
  failure span to Jaeger.
- Any unhandled exception in a script: caught by the root `try/except`, recorded on the root
  span via `span.record_exception(exc)`, re-raised so Airflow marks the task failed.
