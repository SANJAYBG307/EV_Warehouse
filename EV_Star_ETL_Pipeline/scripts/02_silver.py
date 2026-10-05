from __future__ import annotations

import argparse

import pandas as pd

from ev_common import (
    begin_stage,
    clean_layer_files,
    create_run_id,
    end_stage,
    ensure_dirs,
    get_logger,
    load_settings,
    read_parquet,
    write_quality_check,
    write_stage_metric,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean bronze data and build silver layer.")
    parser.add_argument("--run-id", default="", help="Optional run id shared across all pipeline stages.")
    return parser.parse_args()


def _clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    # Trim text and convert empty strings to null.
    out = df.copy()
    object_cols = out.select_dtypes(include=["object", "string"]).columns
    for col in object_cols:
        out[col] = out[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
        out[col] = out[col].replace("", pd.NA)
    return out


def main() -> None:
    # 1) Prepare run details and folders.
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    ensure_dirs(settings)
    clean_layer_files(settings.silver_layer_dir)
    logger = get_logger("02_silver", settings.log_dir)
    stage_ctx = begin_stage("02_silver", run_id)

    rows_read = 0
    rows_written = 0
    checks_passed = 0
    checks_failed = 0

    try:
        # 2) Read bronze data and clean it.
        table = settings.source_table
        bronze_df = read_parquet(settings.bronze_layer_dir, table)
        silver_df = _clean_frame(bronze_df)

        # 3) Save silver parquet.
        write_parquet(settings.silver_layer_dir, table, silver_df)
        rows_read = len(bronze_df)
        rows_written = len(silver_df)

        # 4) Quality check: row counts must match.
        row_count_match = len(bronze_df) == len(silver_df)
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "02_silver",
                "check_name": "row_count_match",
                "passed": row_count_match,
                "details": f"source={len(bronze_df)}, target={len(silver_df)}",
            },
        )

        if not row_count_match:
            checks_failed += 1
            raise RuntimeError("Silver row count mismatch")

        checks_passed += 1

        # 5) Save stage metric.
        metric = end_stage(
            stage_ctx,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
        )
        write_stage_metric(settings.log_dir, run_id, metric)
        logger.info("run_id=%s | silver.%s ready rows=%s", run_id, table, len(silver_df))
    except Exception as exc:  # noqa: BLE001
        # Save failed metric and re-raise.
        checks_failed += 1
        metric = end_stage(
            stage_ctx,
            status="failed",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            error_message=str(exc),
        )
        write_stage_metric(settings.log_dir, run_id, metric)
        logger.exception("run_id=%s | Stage failed: %s", run_id, exc)
        raise


if __name__ == "__main__":
    main()
