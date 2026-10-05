from __future__ import annotations

import argparse

import pandas as pd
from sqlalchemy import text

from ev_common import (
    begin_stage,
    clean_layer_files,
    create_run_id,
    end_stage,
    ensure_dirs,
    get_logger,
    get_tidb_engine,
    load_settings,
    run_with_retries,
    write_quality_check,
    write_stage_metric,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import EV source table into bronze layer.")
    parser.add_argument("--run-id", default="", help="Optional run id shared across all pipeline stages.")
    return parser.parse_args()


def read_source_table(source_table: str) -> pd.DataFrame:
    """Read one source table from TiDB."""
    settings = load_settings()
    engine = get_tidb_engine(settings)

    with engine.connect() as conn:
        query = text(f"SELECT * FROM `{source_table}`")
        data = pd.read_sql(query, conn)

    return data


def main() -> None:
    # 1) Prepare run details and folders.
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    ensure_dirs(settings)
    clean_layer_files(settings.bronze_layer_dir)
    logger = get_logger("01_import_to_bronze", settings.log_dir)
    stage_ctx = begin_stage("01_import_to_bronze", run_id)

    rows_read = 0
    rows_written = 0
    checks_passed = 0
    checks_failed = 0

    try:
        source_table = settings.source_table

        # 2) Read source table from TiDB.
        def _read_source() -> pd.DataFrame:
            return read_source_table(source_table)

        df = run_with_retries(f"read source table {source_table}", settings, logger, _read_source)

        # 3) Save as bronze parquet.
        write_parquet(settings.bronze_layer_dir, source_table, df)
        rows_read = len(df)
        rows_written = len(df)

        # 4) Record simple quality check.
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "01_import_to_bronze",
                "check_name": "source_table_loaded",
                "passed": True,
                "details": f"table={source_table}, rows={len(df)}",
            },
        )
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
        logger.info("run_id=%s | bronze.%s loaded rows=%s", run_id, source_table, len(df))
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
