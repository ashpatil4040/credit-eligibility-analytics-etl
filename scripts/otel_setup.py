import logging
import os

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

try:
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    _LOGS_SDK_AVAILABLE = True
except ImportError:
    _LOGS_SDK_AVAILABLE = False

_initialized = False

# Standard attribute key names — use these in every script so Jaeger search is consistent
ATTR_DAG_ID    = "pipeline.dag_id"
ATTR_TASK_ID   = "pipeline.task_id"
ATTR_RUN_ID    = "pipeline.run_id"
ATTR_STAGE     = "pipeline.stage"
ATTR_FILE_NAME = "pipeline.file_name"
ATTR_ROW_COUNT = "pipeline.row_count"
ATTR_BYTES     = "pipeline.bytes_processed"


class TraceContextFormatter(logging.Formatter):
    """
    Injects trace_id and span_id from the active OTel span into every
    Python log record. When there is no active span the fields are zeroed.
    """
    def format(self, record):
        ctx = trace.get_current_span().get_span_context()
        if ctx.is_valid:
            record.trace_id = format(ctx.trace_id, "032x")
            record.span_id  = format(ctx.span_id,  "016x")
        else:
            record.trace_id = "0" * 32
            record.span_id  = "0" * 16
        return super().format(record)


def setup_telemetry(service_name: str = "credit-risk-etl") -> None:
    """
    One-time initialisation of the global OTel tracer, meter, and log providers.
    Subsequent calls are no-ops — safe to call from every script entry point.
    """
    global _initialized
    if _initialized:
        return

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")

    # Resource = the metadata bundle stamped on every span/metric/log
    # Think of it as: "this telemetry came from this service running in this env"
    resource = Resource.create({
        "service.name":            service_name,
        "deployment.environment":  os.getenv("DEPLOYMENT_ENV", "local"),
        "pipeline.name":           "credit-risk-etl",
    })

    # ── Traces ────────────────────────────────────────────────────────────────
    # TracerProvider owns span creation.
    # BatchSpanProcessor buffers spans and flushes them asynchronously —
    # your pipeline code is never blocked waiting for the export to finish.
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
    )
    trace.set_tracer_provider(tracer_provider)

    # ── Metrics ───────────────────────────────────────────────────────────────
    # PeriodicExportingMetricReader pushes accumulated metric data every 30 s.
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=endpoint, insecure=True),
        export_interval_millis=30_000,
    )
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))

    # ── Logs ──────────────────────────────────────────────────────────────────
    # LoggingHandler bridges the standard Python logging system into OTel.
    # Any logger.info/warning/error call is forwarded to Jaeger as a log record.
    if _LOGS_SDK_AVAILABLE:
        logger_provider = LoggerProvider(resource=resource)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, insecure=True))
        )
        logging.getLogger().addHandler(
            LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        )

    # ── Console log format with trace context ─────────────────────────────────
    # TraceContextFormatter adds trace_id/span_id so Airflow task log lines
    # can be correlated with the Jaeger trace just by copy-pasting the ID.
    fmt = (
        "%(asctime)s %(levelname)s "
        "[trace_id=%(trace_id)s span_id=%(span_id)s] "
        "%(name)s - %(message)s"
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(TraceContextFormatter(fmt))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(console_handler)

    _initialized = True


def get_tracer(name: str):
    """Return a tracer scoped to name (use the script's __name__)."""
    return trace.get_tracer(name)


def get_meter(name: str):
    """Return a meter scoped to name."""
    return metrics.get_meter(name)


def get_logger(name: str) -> logging.Logger:
    """
    Return a standard Python logger.
    Trace context (trace_id, span_id) is injected automatically
    by TraceContextFormatter when an OTel span is active.
    """
    return logging.getLogger(name)


def apply_pipeline_context(span) -> None:
    """
    Reads AIRFLOW_RUN_ID / AIRFLOW_DAG_ID / AIRFLOW_TASK_ID injected by
    BashOperator's env= dict and stamps them onto the given span.

    Call this immediately after starting the root span in each script:

        with tracer.start_as_current_span("raw_ingestion") as span:
            apply_pipeline_context(span)
            ...
    """
    run_id  = os.getenv("AIRFLOW_RUN_ID")
    dag_id  = os.getenv("AIRFLOW_DAG_ID")
    task_id = os.getenv("AIRFLOW_TASK_ID")
    if run_id:
        span.set_attribute(ATTR_RUN_ID,  run_id)
    if dag_id:
        span.set_attribute(ATTR_DAG_ID,  dag_id)
    if task_id:
        span.set_attribute(ATTR_TASK_ID, task_id)