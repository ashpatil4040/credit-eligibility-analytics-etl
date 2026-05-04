import sys
import time
import shutil

import pandas as pd
from pathlib import Path

from otel_setup import (
    setup_telemetry, get_tracer, get_meter, get_logger,
    apply_pipeline_context,
    ATTR_STAGE, ATTR_FILE_NAME, ATTR_ROW_COUNT, ATTR_BYTES,
)
from opentelemetry.trace import Status, StatusCode

setup_telemetry("raw-ingestion")
tracer = get_tracer(__name__)
meter  = get_meter(__name__)
logger = get_logger(__name__)

# ── Metrics instruments ────────────────────────────────────────────────────────
# Counter — total files copied across all runs (monotonically increasing)
files_copied_counter = meter.create_counter(
    "raw_ingestion.files_copied",
    unit="files",
    description="Total number of raw files copied to staging",
)
# Histogram — distribution of file sizes we process
file_size_histogram = meter.create_histogram(
    "raw_ingestion.file_size_bytes",
    unit="bytes",
    description="Size of each raw file copied to staging",
)

RAW_DIR     = Path("/opt/airflow/data/raw")
STAGING_DIR = Path("/opt/airflow/data/staging")

REQUIRED_FILES = [
    "application_train.csv",
    "credit_card_balance.csv",
    "installments_payments.csv",
]


def validate_raw_files():
    with tracer.start_as_current_span("validate_raw_files") as span:
        span.set_attribute(ATTR_STAGE, "validation")
        span.set_attribute("validation.expected_file_count", len(REQUIRED_FILES))

        missing_files = []
        for file_name in REQUIRED_FILES:
            if not (RAW_DIR / file_name).exists():
                missing_files.append(file_name)

        if missing_files:
            span.set_attribute("validation.missing_files", str(missing_files))
            span.set_status(Status(StatusCode.ERROR, f"Missing: {missing_files}"))
            raise FileNotFoundError(f"Missing raw files: {missing_files}")

        span.set_attribute("validation.missing_file_count", 0)
        span.set_status(Status(StatusCode.OK))
        logger.info("All %d required raw files are present.", len(REQUIRED_FILES))


def stage_raw_files():
    with tracer.start_as_current_span("stage_raw_files") as span:
        span.set_attribute(ATTR_STAGE, "staging")
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        total_bytes = 0
        for file_name in REQUIRED_FILES:
            source      = RAW_DIR / file_name
            destination = STAGING_DIR / file_name
            file_bytes  = source.stat().st_size

            with tracer.start_as_current_span(f"copy.{file_name}") as file_span:
                file_span.set_attribute(ATTR_FILE_NAME, file_name)
                file_span.set_attribute(ATTR_BYTES, file_bytes)
                shutil.copy(source, destination)

            # Record per-file metrics
            files_copied_counter.add(1, {"file_name": file_name})
            file_size_histogram.record(file_bytes, {"file_name": file_name})

            total_bytes += file_bytes
            logger.info("Copied %s to staging (%d bytes).", file_name, file_bytes)

        span.set_attribute(ATTR_BYTES, total_bytes)
        span.set_attribute(ATTR_ROW_COUNT, len(REQUIRED_FILES))
        span.set_status(Status(StatusCode.OK))


def run_raw_ingestion():
    with tracer.start_as_current_span("raw_ingestion") as span:
        apply_pipeline_context(span)   
        span.set_attribute(ATTR_STAGE, "raw_ingestion")
        try:
            validate_raw_files()
            stage_raw_files()
            span.set_status(Status(StatusCode.OK))
            logger.info("Raw ingestion completed successfully.")
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise


if __name__ == "__main__":
    run_raw_ingestion()