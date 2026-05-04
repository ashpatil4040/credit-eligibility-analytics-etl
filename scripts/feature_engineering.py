import sys
import time

import pandas as pd
import numpy as np
from pathlib import Path

from otel_setup import (
    setup_telemetry, get_tracer, get_meter, get_logger,apply_pipeline_context,
    ATTR_STAGE, ATTR_FILE_NAME, ATTR_ROW_COUNT,
)
from opentelemetry.trace import Status, StatusCode

setup_telemetry("feature-engineering")
tracer = get_tracer(__name__)
meter  = get_meter(__name__)
logger = get_logger(__name__)

# ── Metrics instruments ────────────────────────────────────────────────────────
# UpDownCounter — current output row count (can decrease on reruns)
output_rows_gauge = meter.create_up_down_counter(
    "feature_engineering.output_rows",
    unit="rows",
    description="Number of rows in the final customer financial profile",
)
# Histogram — how long each transformation step takes
transform_duration = meter.create_histogram(
    "feature_engineering.transform_duration_seconds",
    unit="s",
    description="Duration of each feature engineering transformation step",
)
# Counter — null values filled during profile merge
nulls_filled_counter = meter.create_counter(
    "feature_engineering.nulls_filled",
    unit="cells",
    description="Number of null cells filled with 0 after merge",
)

STAGING_DIR   = Path("/opt/airflow/data/staging")
PROCESSED_DIR = Path("/opt/airflow/data/processed")


def create_customer_base():
    with tracer.start_as_current_span("create_customer_base") as span:
        span.set_attribute(ATTR_STAGE, "customer_base")
        span.set_attribute(ATTR_FILE_NAME, "application_train.csv")

        t0  = time.perf_counter()
        app = pd.read_csv(STAGING_DIR / "application_train.csv")

        selected_columns = [
            "SK_ID_CURR", "TARGET", "AMT_INCOME_TOTAL", "AMT_CREDIT",
            "AMT_ANNUITY", "NAME_INCOME_TYPE", "DAYS_EMPLOYED",
        ]
        customer_base = app[selected_columns].copy()
        customer_base.rename(columns={
            "AMT_INCOME_TOTAL": "income",
            "AMT_CREDIT":       "loan_amount",
            "AMT_ANNUITY":      "annuity",
            "NAME_INCOME_TYPE": "income_type",
            "DAYS_EMPLOYED":    "days_employed",
        }, inplace=True)

        duration = time.perf_counter() - t0
        span.set_attribute(ATTR_ROW_COUNT, len(customer_base))
        span.set_status(Status(StatusCode.OK))
        transform_duration.record(duration, {"step": "create_customer_base"})
        logger.info("Customer base created: %d rows (%.2fs).", len(customer_base), duration)
        return customer_base


def create_credit_card_features():
    with tracer.start_as_current_span("create_credit_card_features") as span:
        span.set_attribute(ATTR_STAGE, "credit_card_features")
        span.set_attribute(ATTR_FILE_NAME, "credit_card_balance.csv")

        t0 = time.perf_counter()
        cc = pd.read_csv(STAGING_DIR / "credit_card_balance.csv")

        cc["utilization_ratio"] = np.where(
            cc["AMT_CREDIT_LIMIT_ACTUAL"] > 0,
            cc["AMT_BALANCE"] / cc["AMT_CREDIT_LIMIT_ACTUAL"],
            0,
        )
        features = (
            cc.groupby("SK_ID_CURR")
            .agg(
                avg_card_balance=("AMT_BALANCE", "mean"),
                max_card_balance=("AMT_BALANCE", "max"),
                avg_credit_limit=("AMT_CREDIT_LIMIT_ACTUAL", "mean"),
                avg_utilization_ratio=("utilization_ratio", "mean"),
                max_utilization_ratio=("utilization_ratio", "max"),
                total_card_drawings=("AMT_DRAWINGS_CURRENT", "sum"),
                total_card_payments=("AMT_PAYMENT_TOTAL_CURRENT", "sum"),
            )
            .reset_index()
        )

        duration = time.perf_counter() - t0
        span.set_attribute(ATTR_ROW_COUNT, len(features))
        span.set_attribute("credit_card.raw_row_count", len(cc))
        span.set_status(Status(StatusCode.OK))
        transform_duration.record(duration, {"step": "create_credit_card_features"})
        logger.info("Credit card features created: %d customers (%.2fs).", len(features), duration)
        return features


def create_payment_features():
    with tracer.start_as_current_span("create_payment_features") as span:
        span.set_attribute(ATTR_STAGE, "payment_features")
        span.set_attribute(ATTR_FILE_NAME, "installments_payments.csv")

        t0       = time.perf_counter()
        payments = pd.read_csv(STAGING_DIR / "installments_payments.csv")

        payments["payment_delay_days"] = (
            payments["DAYS_ENTRY_PAYMENT"] - payments["DAYS_INSTALMENT"]
        )
        payments["is_late_payment"] = payments["payment_delay_days"] > 0
        payments["is_underpaid"]    = payments["AMT_PAYMENT"] < payments["AMT_INSTALMENT"]

        features = (
            payments.groupby("SK_ID_CURR")
            .agg(
                avg_payment_delay=("payment_delay_days", "mean"),
                max_payment_delay=("payment_delay_days", "max"),
                late_payment_count=("is_late_payment", "sum"),
                underpaid_count=("is_underpaid", "sum"),
                total_expected_payment=("AMT_INSTALMENT", "sum"),
                total_actual_payment=("AMT_PAYMENT", "sum"),
            )
            .reset_index()
        )

        duration = time.perf_counter() - t0
        span.set_attribute(ATTR_ROW_COUNT, len(features))
        span.set_attribute("payments.raw_row_count", len(payments))
        span.set_attribute("payments.late_payment_total", int(payments["is_late_payment"].sum()))
        span.set_status(Status(StatusCode.OK))
        transform_duration.record(duration, {"step": "create_payment_features"})
        logger.info("Payment features created: %d customers (%.2fs).", len(features), duration)
        return features


def run_feature_engineering():
    with tracer.start_as_current_span("feature_engineering") as span:
        apply_pipeline_context(span)
        span.set_attribute(ATTR_STAGE, "feature_engineering")
        try:
            PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

            customer_base        = create_customer_base()
            credit_card_features = create_credit_card_features()
            payment_features     = create_payment_features()

            with tracer.start_as_current_span("merge_and_save") as merge_span:
                t0 = time.perf_counter()

                final_profile = (
                    customer_base
                    .merge(credit_card_features, on="SK_ID_CURR", how="left")
                    .merge(payment_features,     on="SK_ID_CURR", how="left")
                )

                null_count = int(final_profile.isnull().sum().sum())
                final_profile.fillna(0, inplace=True)

                output_path = PROCESSED_DIR / "customer_financial_profile.csv"
                final_profile.to_csv(output_path, index=False)

                duration = time.perf_counter() - t0
                merge_span.set_attribute(ATTR_ROW_COUNT, len(final_profile))
                merge_span.set_attribute("merge.null_fills", null_count)
                merge_span.set_attribute(ATTR_FILE_NAME, "customer_financial_profile.csv")
                merge_span.set_status(Status(StatusCode.OK))
                transform_duration.record(duration, {"step": "merge_and_save"})
                nulls_filled_counter.add(null_count)
                output_rows_gauge.add(len(final_profile))

            span.set_attribute(ATTR_ROW_COUNT, len(final_profile))
            span.set_status(Status(StatusCode.OK))
            logger.info("Customer financial profile saved: %d rows.", len(final_profile))

        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise


if __name__ == "__main__":
    run_feature_engineering()