import sys
import time

import pandas as pd
from pathlib import Path

from otel_setup import (
    setup_telemetry, get_tracer, get_meter, get_logger,
    apply_pipeline_context,
    ATTR_STAGE, ATTR_FILE_NAME, ATTR_ROW_COUNT,
)
from opentelemetry.trace import Status, StatusCode

setup_telemetry("eligibility-analytics")
tracer = get_tracer(__name__)
meter  = get_meter(__name__)
logger = get_logger(__name__)

# ── Metrics instruments ────────────────────────────────────────────────────────
# Counters broken out by dimension — each risk level and program is a separate label
customers_by_risk = meter.create_counter(
    "eligibility.customers_by_risk",
    unit="customers",
    description="Customers classified per risk level",
)
customers_by_program = meter.create_counter(
    "eligibility.customers_by_program",
    unit="customers",
    description="Customers assigned per recommended program",
)
analytics_duration = meter.create_histogram(
    "eligibility.step_duration_seconds",
    unit="s",
    description="Duration of each analytics step",
)

PROCESSED_DIR = Path("/opt/airflow/data/processed")


def assign_risk_level(row):
    utilization = row["max_utilization_ratio"]
    max_delay   = row["max_payment_delay"]
    late_count  = row["late_payment_count"]

    if max_delay > 90 or late_count >= 5 or utilization > 1.0:
        return "CRITICAL"
    elif max_delay > 60 or utilization > 0.85:
        return "HIGH"
    elif max_delay > 30 or utilization > 0.60:
        return "MEDIUM"
    else:
        return "LOW"


def assign_program(row):
    risk       = row["risk_level"]
    income     = row["income"]
    utilization = row["max_utilization_ratio"]
    max_delay  = row["max_payment_delay"]
    late_count = row["late_payment_count"]

    if risk == "CRITICAL" and income < 50000:
        return "SETTLEMENT_PLAN"
    elif max_delay > 60 and income < 75000:
        return "HARDSHIP_PROGRAM"
    elif utilization > 0.85 and late_count <= 3:
        return "PAYMENT_EXTENSION"
    elif risk in ["HIGH", "CRITICAL"]:
        return "COLLECTIONS_REVIEW"
    else:
        return "STANDARD_MONITORING"


def run_eligibility_analytics():
    with tracer.start_as_current_span("eligibility_analytics") as span:
        apply_pipeline_context(span)          # ← add this line
        span.set_attribute(ATTR_STAGE, "eligibility_analytics")
        try:
            # ── Load ──────────────────────────────────────────────────────────
            with tracer.start_as_current_span("load_profile") as load_span:
                input_path = PROCESSED_DIR / "customer_financial_profile.csv"
                if not input_path.exists():
                    raise FileNotFoundError("customer_financial_profile.csv not found.")

                t0 = time.perf_counter()
                df = pd.read_csv(input_path)
                duration = time.perf_counter() - t0

                load_span.set_attribute(ATTR_FILE_NAME, "customer_financial_profile.csv")
                load_span.set_attribute(ATTR_ROW_COUNT, len(df))
                load_span.set_status(Status(StatusCode.OK))
                analytics_duration.record(duration, {"step": "load_profile"})
                logger.info("Loaded customer profile: %d rows (%.2fs).", len(df), duration)

            # ── Risk assignment ───────────────────────────────────────────────
            with tracer.start_as_current_span("assign_risk_levels") as risk_span:
                t0 = time.perf_counter()
                df["risk_level"] = df.apply(assign_risk_level, axis=1)
                duration = time.perf_counter() - t0

                risk_counts = df["risk_level"].value_counts().to_dict()
                for level, count in risk_counts.items():
                    customers_by_risk.add(count, {"risk_level": level})
                    risk_span.set_attribute(f"risk.{level.lower()}_count", count)

                risk_span.set_attribute(ATTR_ROW_COUNT, len(df))
                risk_span.set_status(Status(StatusCode.OK))
                analytics_duration.record(duration, {"step": "assign_risk_levels"})
                logger.info("Risk levels assigned: %s (%.2fs).", risk_counts, duration)

            # ── Program assignment ────────────────────────────────────────────
            with tracer.start_as_current_span("assign_programs") as prog_span:
                t0 = time.perf_counter()
                df["recommended_program"] = df.apply(assign_program, axis=1)
                duration = time.perf_counter() - t0

                program_counts = df["recommended_program"].value_counts().to_dict()
                for program, count in program_counts.items():
                    customers_by_program.add(count, {"program": program})
                    prog_span.set_attribute(f"program.{program.lower()}_count", count)

                prog_span.set_attribute(ATTR_ROW_COUNT, len(df))
                prog_span.set_status(Status(StatusCode.OK))
                analytics_duration.record(duration, {"step": "assign_programs"})
                logger.info("Programs assigned: %s (%.2fs).", program_counts, duration)

            # ── Write outputs ─────────────────────────────────────────────────
            with tracer.start_as_current_span("write_outputs") as write_span:
                t0 = time.perf_counter()

                eligibility_output = PROCESSED_DIR / "eligibility_programs.csv"
                df.to_csv(eligibility_output, index=False)

                risk_summary = (
                    df.groupby("risk_level")
                    .agg(
                        customer_count=("SK_ID_CURR", "count"),
                        avg_income=("income", "mean"),
                        avg_loan_amount=("loan_amount", "mean"),
                        avg_utilization=("max_utilization_ratio", "mean"),
                        avg_payment_delay=("max_payment_delay", "mean"),
                    )
                    .reset_index()
                )
                risk_summary.to_csv(PROCESSED_DIR / "risk_segmentation_summary.csv", index=False)

                program_summary = (
                    df.groupby("recommended_program")
                    .agg(
                        customer_count=("SK_ID_CURR", "count"),
                        avg_income=("income", "mean"),
                        avg_loan_amount=("loan_amount", "mean"),
                        avg_utilization=("max_utilization_ratio", "mean"),
                    )
                    .reset_index()
                )
                program_summary.to_csv(PROCESSED_DIR / "program_eligibility_summary.csv", index=False)

                duration = time.perf_counter() - t0
                write_span.set_attribute("outputs.files_written", 3)
                write_span.set_attribute("outputs.eligibility_rows", len(df))
                write_span.set_attribute("outputs.risk_segments", len(risk_summary))
                write_span.set_attribute("outputs.program_segments", len(program_summary))
                write_span.set_status(Status(StatusCode.OK))
                analytics_duration.record(duration, {"step": "write_outputs"})
                logger.info("All 3 output files written (%.2fs).", duration)

            span.set_attribute(ATTR_ROW_COUNT, len(df))
            span.set_status(Status(StatusCode.OK))

        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise


if __name__ == "__main__":
    run_eligibility_analytics()