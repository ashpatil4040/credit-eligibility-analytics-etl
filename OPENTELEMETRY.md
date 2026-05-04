# OpenTelemetry Implementation Reference

## What is OpenTelemetry?

OpenTelemetry (OTel) is a vendor-neutral observability framework that standardises how
applications emit **traces**, **metrics**, and **logs**. Instead of writing code for Datadog,
Jaeger, Prometheus, or any specific backend, you write against the OTel API once — the backend
is a configuration choice made at deploy time.

The three signal types:

| Signal | What it answers | Unit |
|---|---|---|
| **Traces** | What happened, in what order, how long did each step take? | Spans (start time + duration + attributes) |
| **Metrics** | How much, how many, how fast — aggregated over time? | Counters, histograms, gauges |
| **Logs** | What did the application say at a point in time? | Log records with trace context injected |

---

## Architecture in This Project

```
┌──────────────────────────────────────────────────────────────┐
│                    Python process (script or DAG callback)    │
│                                                              │
│  Your code                                                   │
│  ─────────                                                   │
│  tracer.start_as_current_span(...)  ──► TracerProvider       │
│  meter.create_counter(...)          ──► MeterProvider        │
│  logger.info(...)                   ──► LoggerProvider       │
│                                              │               │
│  SDK layer                                   │               │
│  ─────────                                   ▼               │
│  BatchSpanProcessor ──────────────► OTLPSpanExporter         │
│  PeriodicExportingMetricReader ────► OTLPMetricExporter      │
│  BatchLogRecordProcessor ──────────► OTLPLogExporter         │
│                                              │               │
└──────────────────────────────────────────────┼───────────────┘
                                               │ gRPC :4317
                                               ▼
                                    ┌──────────────────┐
                                    │  Jaeger all-in-one│
                                    │  (collector +     │
                                    │   query + UI)     │
                                    └──────────────────┘
                                           :16686
                                        (browser)
```

### Key design decisions

**One shared initialisation module (`scripts/otel_setup.py`)**  
Airflow imports DAG files and scripts repeatedly across scheduler, dag-processor, and task
processes. A naive `TracerProvider()` on every import would register duplicate exporters. A
module-level `_initialized` flag ensures providers are registered exactly once per process.

**Per-service `OTEL_SERVICE_NAME`**  
Each Airflow container and each script sets a distinct service name. This lets you filter traces
by component in Jaeger without knowing trace IDs:
- `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer` — set
  via `OTEL_SERVICE_NAME` env var override in `docker-compose.yaml`
- `airflow-dags` — set by `setup_telemetry("airflow-dags")` in `dag_instrumentation.py`
- `raw-ingestion`, `feature-engineering`, `eligibility-analytics` — set by each script

**Subprocess boundary bridging via env vars**  
`BashOperator` spawns a new OS process. OTel context lives in memory and cannot cross that
boundary automatically. The solution: `BashOperator`'s `env=` parameter injects Airflow's
`run_id`, `dag_id`, and `task_id` as environment variables. Each script reads them in
`apply_pipeline_context()` and stamps them onto its root span. You can then search
`pipeline.run_id=<value>` in Jaeger and get all spans for that pipeline run across all services.

---

## File Roles

| File | Role |
|---|---|
| `scripts/otel_setup.py` | Initialises all three OTel providers, exposes `get_tracer`, `get_meter`, `get_logger`, `apply_pipeline_context` |
| `plugins/dag_instrumentation.py` | DAG and task success/failure callbacks that emit spans to `airflow-dags` service |
| `scripts/raw_ingestion.py` | Instrumented with traces + metrics for file validation and staging |
| `scripts/feature_engineering.py` | Instrumented with traces + metrics per transformation step |
| `scripts/eligibility_analytics.py` | Instrumented with traces + metrics for risk/program assignment |

---

## `otel_setup.py` — Deep Dive

### Resource

```python
resource = Resource.create({
    "service.name":           service_name,
    "deployment.environment": os.getenv("DEPLOYMENT_ENV", "local"),
    "pipeline.name":          "credit-risk-etl",
})
```

A `Resource` is the "identity card" of the process emitting telemetry. Every span, metric data
point, and log record emitted from that process carries these attributes automatically. In Jaeger
you see them in the **Process** section of any span. They are not per-span — they are per-process.

### TracerProvider and BatchSpanProcessor

```python
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
)
trace.set_tracer_provider(tracer_provider)
```

- `TracerProvider` — factory for `Tracer` objects. Set globally via `trace.set_tracer_provider()`
  so `trace.get_tracer(name)` anywhere in the process returns a tracer backed by this provider.
- `BatchSpanProcessor` — collects finished spans in an internal queue and flushes them
  asynchronously in batches. Your code is never blocked waiting for a network export.
- `OTLPSpanExporter` — speaks the OTLP protocol over gRPC to Jaeger's `4317` port.
- `insecure=True` — skips TLS. Fine for local Docker networking; remove for production.

### MeterProvider and PeriodicExportingMetricReader

```python
reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=endpoint, insecure=True),
    export_interval_millis=30_000,
)
metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))
```

- `PeriodicExportingMetricReader` — every 30 seconds, aggregates all accumulated metric data
  and pushes it to Jaeger. Unlike traces (which export immediately on span end), metrics are
  aggregated then pushed on a schedule.
- `MeterProvider` — factory for `Meter` objects. `metrics.get_meter(name)` anywhere returns a
  meter backed by this provider.

### LoggerProvider and LoggingHandler

```python
logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(
    BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, insecure=True))
)
logging.getLogger().addHandler(
    LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
)
```

- `LoggingHandler` bridges the standard Python `logging` module into the OTel log pipeline.
  Any `logger.info(...)` call in any module is forwarded as an OTel log record to Jaeger.
- The handler is added to the **root logger** (`logging.getLogger()`) so it catches all loggers
  across all modules without needing per-module configuration.
- Wrapped in a `try/except ImportError` because the OTel logs SDK was still `_logs` (internal)
  in version 1.24.0 — the guard makes the setup safe if the import path changes.

### TraceContextFormatter

```python
class TraceContextFormatter(logging.Formatter):
    def format(self, record):
        ctx = trace.get_current_span().get_span_context()
        if ctx.is_valid:
            record.trace_id = format(ctx.trace_id, "032x")
            record.span_id  = format(ctx.span_id,  "016x")
        else:
            record.trace_id = "0" * 32
            record.span_id  = "0" * 16
        return super().format(record)
```

This formatter is called **at format time** (not at log call time), which means it reads the
**currently active span** at the moment the log line is rendered. If a span is active (i.e. the
log call happens inside a `with tracer.start_as_current_span(...)` block), the `trace_id` and
`span_id` are injected into the log record. The resulting log line looks like:

```
2026-05-04 12:01:23 INFO [trace_id=3a7f9c2b... span_id=b2c1d4e5...] __main__ - Copied application_train.csv to staging.
```

Copy the `trace_id` from any log line → paste into Jaeger's **Lookup by Trace ID** box → you
land directly on that span's trace.

### Standard attribute constants

```python
ATTR_DAG_ID    = "pipeline.dag_id"
ATTR_TASK_ID   = "pipeline.task_id"
ATTR_RUN_ID    = "pipeline.run_id"
ATTR_STAGE     = "pipeline.stage"
ATTR_FILE_NAME = "pipeline.file_name"
ATTR_ROW_COUNT = "pipeline.row_count"
ATTR_BYTES     = "pipeline.bytes_processed"
```

All scripts import and use these constants. Consistent attribute key names mean Jaeger tag search
works reliably — searching `pipeline.row_count` returns results from all three services.

### `apply_pipeline_context(span)`

```python
def apply_pipeline_context(span) -> None:
    run_id  = os.getenv("AIRFLOW_RUN_ID")
    dag_id  = os.getenv("AIRFLOW_DAG_ID")
    task_id = os.getenv("AIRFLOW_TASK_ID")
    if run_id:
        span.set_attribute(ATTR_RUN_ID,  run_id)
    if dag_id:
        span.set_attribute(ATTR_DAG_ID,  dag_id)
    if task_id:
        span.set_attribute(ATTR_TASK_ID, task_id)
```

Called as the first line inside each script's root span. Reads the env vars injected by
`BashOperator`'s `env=` dict (set in each DAG file) and stamps them onto the span. Guards with
`if` checks so the function is also safe when scripts are run manually outside Airflow (env vars
will simply be absent).

---

## `dag_instrumentation.py` — Deep Dive

This file lives in `plugins/` so Airflow auto-loads it into the DAG processor's Python path.

### Callback pattern

All four functions (`on_dag_success`, `on_dag_failure`, `on_task_success`, `on_task_failure`)
follow the same pattern:

```python
def on_dag_success(context: dict) -> None:
    attrs = _common_attrs(context)   # extract dag_id, task_id, run_id, execution_date
    with _tracer.start_as_current_span(f"{dag_id}.dag_run") as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
        span.set_attribute("dag_run.outcome", "success")
        span.set_status(Status(StatusCode.OK))
        _logger.info(...)
```

The `context` dict is provided by Airflow and contains the full `DagRun`, `TaskInstance`, `dag`,
and `execution_date` objects. `_common_attrs()` extracts the fields you care about into a flat
dict that can be applied to any span.

### Where callbacks are wired in DAGs

```python
with DAG(
    ...
    on_success_callback=on_dag_success,   # fires when entire DAG run succeeds
    on_failure_callback=on_dag_failure,   # fires when any task exhausts retries
) as dag:

    run_raw_ingestion = BashOperator(
        ...
        on_success_callback=on_task_success,  # fires when this task succeeds
        on_failure_callback=on_task_failure,  # fires when this task fails
    )
```

DAG-level and task-level callbacks are separate. The DAG-level fires once per run; task-level
fires per task instance.

### Span naming convention

- DAG run span: `raw_ingestion_dag.dag_run`
- Task span: `raw_ingestion_dag.run_raw_ingestion`

This naming makes the Jaeger operation dropdown hierarchical and easy to filter.

---

## Traces in Each Script

### `raw_ingestion.py` — span tree

```
raw_ingestion                           ← root span; apply_pipeline_context stamps run_id here
├── validate_raw_files                  ← checks 3 files exist; sets validation.missing_file_count
└── stage_raw_files                     ← copies files; sets pipeline.bytes_processed total
    ├── copy.application_train.csv      ← individual file span with file_name + bytes
    ├── copy.credit_card_balance.csv
    └── copy.installments_payments.csv
```

**Metrics emitted:**

| Instrument | Type | Dimensions |
|---|---|---|
| `raw_ingestion.files_copied` | Counter | `file_name` |
| `raw_ingestion.file_size_bytes` | Histogram | `file_name` |

### `feature_engineering.py` — span tree

```
feature_engineering                     ← root span; apply_pipeline_context stamps run_id
├── create_customer_base                ← reads application_train.csv; row_count=307511
├── create_credit_card_features         ← reads credit_card_balance.csv; sets raw_row_count
├── create_payment_features             ← reads installments_payments.csv; sets late_payment_total
└── merge_and_save                      ← joins 3 frames; sets merge.null_fills + row_count
```

**Metrics emitted:**

| Instrument | Type | Dimensions |
|---|---|---|
| `feature_engineering.output_rows` | UpDownCounter | — |
| `feature_engineering.transform_duration_seconds` | Histogram | `step` |
| `feature_engineering.nulls_filled` | Counter | — |

`UpDownCounter` vs `Counter`: use `Counter` for values that only ever increase (events, files
copied). Use `UpDownCounter` for values that represent a current state that can decrease on
reruns (current output row count).

### `eligibility_analytics.py` — span tree

```
eligibility_analytics                   ← root span; apply_pipeline_context stamps run_id
├── load_profile                        ← reads customer_financial_profile.csv; row_count=307511
├── assign_risk_levels                  ← df.apply(assign_risk_level); sets risk.*_count per level
├── assign_programs                     ← df.apply(assign_program); sets program.*_count per program
└── write_outputs                       ← writes 3 CSVs; sets outputs.files_written=3
```

**Metrics emitted:**

| Instrument | Type | Dimensions |
|---|---|---|
| `eligibility.customers_by_risk` | Counter | `risk_level` |
| `eligibility.customers_by_program` | Counter | `program` |
| `eligibility.step_duration_seconds` | Histogram | `step` |

**Dimensional metrics** — `customers_by_risk.add(count, {"risk_level": level})` emits a
separate time series per label value. In a metrics backend (Prometheus/Grafana) you can query
"CRITICAL customers processed over time" without a separate metric per risk level.

---

## Environment Variable Configuration

Set in `docker-compose.yaml` (shared `x-airflow-common` env block) and overrideable via `.env`:

| Variable | Value | Effect |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4317` | gRPC endpoint for all three exporters |
| `OTEL_EXPORTER_OTLP_INSECURE` | `true` | Skip TLS (Docker internal network) |
| `OTEL_TRACES_EXPORTER` | `otlp` | Export traces via OTLP protocol |
| `OTEL_METRICS_EXPORTER` | `otlp` | Export metrics via OTLP protocol |
| `OTEL_LOGS_EXPORTER` | `otlp` | Export logs via OTLP protocol |
| `OTEL_TRACES_SAMPLER` | `always_on` | Capture every span (use `parentbased_traceidratio` in production) |
| `OTEL_PYTHON_LOG_CORRELATION` | `true` | Standard flag; our `TraceContextFormatter` implements this manually |
| `OTEL_RESOURCE_ATTRIBUTES` | `deployment.environment=local,pipeline.name=credit-risk-etl` | Stamped on all telemetry from all services |
| `OTEL_SERVICE_NAME` | per-service value | Overridden per service in `docker-compose.yaml` |

Note: `OTEL_EXPORTER_OTLP_ENDPOINT` is also read directly in `otel_setup.py` via
`os.getenv(...)` with `http://jaeger:4317` as the fallback, so scripts work correctly even when
run outside Docker Compose (pointing at a local Jaeger on the same host).

---

## Jaeger UI — How to Use It

### Find traces for a specific service

1. Open http://localhost:16686
2. **Service** dropdown → select `raw-ingestion`, `feature-engineering`, `eligibility-analytics`,
   or `airflow-dags`
3. Click **Find Traces**
4. Click any trace row to open the waterfall view

### Search by pipeline run ID (cross-service)

This is the key cross-service correlation feature:

1. **Service** → leave blank (or select any)
2. **Tags** → `pipeline.run_id=manual__2026-05-04T12:00:00+00:00`
3. Click **Find Traces**

You get results from both `airflow-dags` (callback spans) and `raw-ingestion` (script spans)
all linked to that one Airflow run — even though they are separate traces with different
`trace_id` values.

### Jump from a log line to a trace

1. Get task logs from the CLI or Airflow UI
2. Find a line containing `trace_id=<32 hex chars>`
3. Copy the value
4. In Jaeger UI → paste into **Lookup by Trace ID** top-right box → press Enter
5. You land directly on that script run's trace

### Read span attributes

Click any span row in the waterfall → the detail panel shows:
- **Tags** — span-level attributes you set with `span.set_attribute(...)`
- **Process** — resource attributes (`service.name`, `deployment.environment`, `pipeline.name`)
- **Logs** — events recorded with `span.add_event(...)` or `span.record_exception(...)`

### Compare two traces

Jaeger → **Compare** tab → paste two trace IDs → see them side by side. Useful for comparing
run durations before and after a code change.

---

## Error Observability

Every script wraps its root function in:

```python
try:
    ...
except Exception as exc:
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc)))
    raise
```

`span.record_exception(exc)` records the exception type, message, and full stack trace as a
**span event** (visible in Jaeger under the span's Logs section). The span's status is set to
`ERROR` which turns the span red in the waterfall view — making failures immediately visible
without reading log text.

For the DAG failure callback, the same pattern applies:

```python
def on_dag_failure(context: dict) -> None:
    exception = context.get("exception")
    with _tracer.start_as_current_span(...) as span:
        if exception:
            span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception)))
```

---

## Extending the Instrumentation

### Add a span to any new function

```python
from otel_setup import get_tracer
tracer = get_tracer(__name__)

def my_new_function():
    with tracer.start_as_current_span("my_new_function") as span:
        span.set_attribute("some.attribute", "value")
        # ... your code ...
        span.set_status(Status(StatusCode.OK))
```

### Add a new metric

```python
from otel_setup import get_meter
meter = get_meter(__name__)

# Choose the right instrument type:
my_counter   = meter.create_counter("my.events", unit="events")
my_histogram = meter.create_histogram("my.duration", unit="s")
my_gauge     = meter.create_up_down_counter("my.queue_depth", unit="items")

# Record values:
my_counter.add(1, {"label": "value"})
my_histogram.record(elapsed, {"step": "transform"})
```

### Switch from Jaeger to another backend

Change one env var — no code changes needed:

```
# Grafana Tempo
OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4317

# Any OTLP-compatible SaaS (Honeycomb, Grafana Cloud, etc.)
OTEL_EXPORTER_OTLP_ENDPOINT=https://api.honeycomb.io
OTEL_EXPORTER_OTLP_HEADERS=x-honeycomb-team=<api-key>
```

### Switch to ratio-based sampling (production)

```
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1   # sample 10% of traces
```

`parentbased_traceidratio` respects the sampling decision from the parent span if one exists
(important for distributed systems). `traceidratio` samples independently.
