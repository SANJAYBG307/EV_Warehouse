from __future__ import annotations

import argparse

from sqlalchemy import inspect, text

from ev_common import (
    begin_stage,
    create_run_id,
    end_stage,
    ensure_dirs,
    get_logger,
    get_neon_engine,
    load_settings,
    read_parquet,
    run_with_retries,
    write_quality_check,
    write_stage_metric,
)

DIM_TABLES = [
    "dim_date",
    "dim_activity",
    "dim_manufacturer",
    "dim_vehicle",
    "dim_battery",
    "dim_customer",
    "dim_location",
    "dim_station",
]
FACT_TABLE = "fact_ev_activity"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load star-schema gold data into Neon target tables.")
    parser.add_argument("--run-id", default="", help="Optional run id shared across all pipeline stages.")
    return parser.parse_args()


def main() -> None:
    # 1) Prepare run details.
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    ensure_dirs(settings)
    logger = get_logger("04_load_neon", settings.log_dir)
    stage_ctx = begin_stage("04_load_neon", run_id)

    rows_read = 0
    rows_written = 0
    checks_passed = 0
    checks_failed = 0

    try:
        # 2) Connect to Neon and check required tables.
        engine = get_neon_engine(settings)
        inspector = inspect(engine)

        required_tables = DIM_TABLES + [FACT_TABLE]
        missing_tables = [
            table for table in required_tables if not inspector.has_table(table, schema=settings.target_schema)
        ]

        tables_exist = len(missing_tables) == 0
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "04_load_neon",
                "check_name": "target_tables_exist",
                "passed": tables_exist,
                "details": f"missing={missing_tables}",
            },
        )
        if not tables_exist:
            checks_failed += 1
            raise RuntimeError(f"Missing target tables in Neon: {missing_tables}")
        checks_passed += 1

        # 3) Read prepared gold parquet files.
        data_frames = {table: read_parquet(settings.gold_layer_dir, table) for table in required_tables}
        rows_read = sum(len(df) for df in data_frames.values())

        with engine.begin() as conn:
            # 4) Truncate fact + dimensions together to avoid FK errors.
            truncate_targets = [
                f'"{settings.target_schema}"."{FACT_TABLE}"',
                *[f'"{settings.target_schema}"."{dim}"' for dim in DIM_TABLES],
            ]
            conn.execute(
                text(
                    f"TRUNCATE TABLE {', '.join(truncate_targets)} RESTART IDENTITY CASCADE"
                )
            )

            # 5) Load dimensions first.
            for dim in DIM_TABLES:
                frame = data_frames[dim]
                if not frame.empty:
                    run_with_retries(
                        f"load {dim} to Neon",
                        settings,
                        logger,
                        lambda frame=frame, dim=dim: frame.to_sql(
                            name=dim,
                            con=conn,
                            schema=settings.target_schema,
                            if_exists="append",
                            index=False,
                            method="multi",
                            chunksize=settings.batch_size,
                        ),
                    )
                rows_written += len(frame)

            # 6) Load fact after dimensions.
            fact_frame = data_frames[FACT_TABLE]
            if not fact_frame.empty:
                run_with_retries(
                    f"load {FACT_TABLE} to Neon",
                    settings,
                    logger,
                    lambda: fact_frame.to_sql(
                        name=FACT_TABLE,
                        con=conn,
                        schema=settings.target_schema,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=settings.batch_size,
                    ),
                )
            rows_written += len(fact_frame)

            # 7) Check row counts after load.
            postload_ok = True
            for table in required_tables:
                post_count = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{settings.target_schema}"."{table}"')
                ).scalar_one()
                expected_count = len(data_frames[table])
                current_ok = post_count == expected_count
                postload_ok = postload_ok and current_ok

                write_quality_check(
                    settings.log_dir,
                    run_id,
                    {
                        "run_id": run_id,
                        "stage": "04_load_neon",
                        "check_name": f"postload_count_{table}",
                        "passed": current_ok,
                        "details": f"expected={expected_count}, actual={post_count}",
                    },
                )

            if postload_ok:
                checks_passed += len(required_tables)
            else:
                checks_failed += 1
                if settings.strict_mode:
                    raise RuntimeError("Postload table count checks failed")

        # 8) Save stage metric.
        metric = end_stage(
            stage_ctx,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
        )
        write_stage_metric(settings.log_dir, run_id, metric)
        logger.info("run_id=%s | Neon load complete", run_id)
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
