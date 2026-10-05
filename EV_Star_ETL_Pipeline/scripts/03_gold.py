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

REQUIRED_SOURCE_COLUMNS = [
    "record_id",
    "transaction_id",
    "activity_type",
    "event_date",
    "manufacturer_id",
    "manufacturer_name",
    "manufacturer_country",
    "vehicle_id",
    "model_name",
    "vehicle_type",
    "body_type",
    "model_year",
    "drive_type",
    "motor_power_kw",
    "seating_capacity",
    "battery_id",
    "battery_type",
    "battery_chemistry",
    "battery_capacity_kwh",
    "battery_voltage",
    "battery_warranty_years",
    "battery_expected_life_years",
    "customer_id",
    "customer_type",
    "gender",
    "age_group",
    "income_segment",
    "customer_segment",
    "purchase_channel",
    "location_id",
    "country",
    "state",
    "district",
    "city",
    "region",
    "pincode",
    "urban_rural",
    "latitude",
    "longitude",
    "station_id",
    "station_name",
    "station_operator",
    "station_type",
    "connector_type",
    "charging_level",
    "number_of_ports",
    "max_charging_power_kw",
    "sale_quantity",
    "base_price",
    "discount_amount",
    "tax_amount",
    "final_price",
    "revenue_amount",
    "production_quantity",
    "defect_quantity",
    "production_cost",
    "charging_duration_min",
    "energy_consumed_kwh",
    "charging_cost",
    "battery_health_pct",
    "battery_degradation_pct",
    "estimated_range_km",
    "service_cost",
    "repair_duration_hours",
    "warranty_claim_flag",
    "co2_reduction_kg",
]

FACT_COLUMNS = [
    "source_record_id",
    "transaction_id",
    "event_date",
    "date_key",
    "activity_key",
    "manufacturer_key",
    "vehicle_key",
    "battery_key",
    "customer_key",
    "location_key",
    "station_key",
    "sale_quantity",
    "base_price",
    "discount_amount",
    "tax_amount",
    "final_price",
    "revenue_amount",
    "production_quantity",
    "defect_quantity",
    "production_cost",
    "charging_duration_min",
    "energy_consumed_kwh",
    "charging_cost",
    "battery_health_pct",
    "battery_degradation_pct",
    "estimated_range_km",
    "service_cost",
    "repair_duration_hours",
    "warranty_claim_flag",
    "co2_reduction_kg",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build star-schema gold datasets from silver source table.")
    parser.add_argument("--run-id", default="", help="Optional run id shared across all pipeline stages.")
    return parser.parse_args()


def _require_columns(df: pd.DataFrame, expected: list[str]) -> None:
    # Stop early if any required source column is missing.
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns in source: {missing}")


def _with_key(df: pd.DataFrame, key_name: str) -> pd.DataFrame:
    # Add a simple sequential surrogate key.
    out = df.reset_index(drop=True).copy()
    out.insert(0, key_name, out.index + 1)
    return out


def _build_dims(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    # Build date dimension from event_date.
    event_dates = pd.to_datetime(df["event_date"], errors="coerce")
    dim_date = pd.DataFrame({"full_date": event_dates.dt.date.dropna().drop_duplicates().sort_values()})
    dim_date["date_key"] = pd.to_datetime(dim_date["full_date"]).dt.strftime("%Y%m%d").astype(int)
    dim_date["day_of_month"] = pd.to_datetime(dim_date["full_date"]).dt.day.astype(int)
    dim_date["month_num"] = pd.to_datetime(dim_date["full_date"]).dt.month.astype(int)
    dim_date["month_name"] = pd.to_datetime(dim_date["full_date"]).dt.month_name()
    dim_date["quarter_num"] = pd.to_datetime(dim_date["full_date"]).dt.quarter.astype(int)
    dim_date["year_num"] = pd.to_datetime(dim_date["full_date"]).dt.year.astype(int)
    dim_date["weekday_num"] = pd.to_datetime(dim_date["full_date"]).dt.weekday.astype(int)
    dim_date["weekday_name"] = pd.to_datetime(dim_date["full_date"]).dt.day_name()
    dim_date["is_weekend"] = dim_date["weekday_num"].isin([5, 6])
    dim_date = dim_date[[
        "date_key",
        "full_date",
        "day_of_month",
        "month_num",
        "month_name",
        "quarter_num",
        "year_num",
        "weekday_num",
        "weekday_name",
        "is_weekend",
    ]].sort_values("date_key").reset_index(drop=True)

    # Build activity dimension.
    dim_activity = df[["activity_type"]].drop_duplicates().sort_values("activity_type")
    dim_activity = _with_key(dim_activity, "activity_key")

    # Build manufacturer dimension.
    dim_manufacturer = df[["manufacturer_id", "manufacturer_name", "manufacturer_country"]]
    dim_manufacturer = dim_manufacturer.drop_duplicates(subset=["manufacturer_id"]).sort_values("manufacturer_id")
    dim_manufacturer = _with_key(dim_manufacturer, "manufacturer_key")

    # Build vehicle dimension.
    dim_vehicle = df[[
        "vehicle_id",
        "model_name",
        "vehicle_type",
        "body_type",
        "model_year",
        "drive_type",
        "motor_power_kw",
        "seating_capacity",
    ]]
    dim_vehicle = dim_vehicle.drop_duplicates(subset=["vehicle_id"]).sort_values("vehicle_id")
    dim_vehicle = _with_key(dim_vehicle, "vehicle_key")

    # Build battery dimension.
    dim_battery = df[[
        "battery_id",
        "battery_type",
        "battery_chemistry",
        "battery_capacity_kwh",
        "battery_voltage",
        "battery_warranty_years",
        "battery_expected_life_years",
    ]]
    dim_battery = dim_battery.drop_duplicates(subset=["battery_id"]).sort_values("battery_id")
    dim_battery = _with_key(dim_battery, "battery_key")

    # Build customer dimension.
    dim_customer = df[[
        "customer_id",
        "customer_type",
        "gender",
        "age_group",
        "income_segment",
        "customer_segment",
        "purchase_channel",
    ]]
    dim_customer = dim_customer.drop_duplicates(subset=["customer_id"]).sort_values("customer_id")
    dim_customer = _with_key(dim_customer, "customer_key")

    # Build location dimension.
    dim_location = df[[
        "location_id",
        "country",
        "state",
        "district",
        "city",
        "region",
        "pincode",
        "urban_rural",
        "latitude",
        "longitude",
    ]]
    dim_location = dim_location.drop_duplicates(subset=["location_id"]).sort_values("location_id")
    dim_location = _with_key(dim_location, "location_key")

    # Build station dimension (only rows where station exists).
    dim_station = df[[
        "station_id",
        "station_name",
        "station_operator",
        "station_type",
        "connector_type",
        "charging_level",
        "number_of_ports",
        "max_charging_power_kw",
    ]]
    dim_station = dim_station[dim_station["station_id"].notna()].copy()
    dim_station = dim_station.drop_duplicates(subset=["station_id"]).sort_values("station_id")
    dim_station = _with_key(dim_station, "station_key")

    return {
        "dim_date": dim_date,
        "dim_activity": dim_activity,
        "dim_manufacturer": dim_manufacturer,
        "dim_vehicle": dim_vehicle,
        "dim_battery": dim_battery,
        "dim_customer": dim_customer,
        "dim_location": dim_location,
        "dim_station": dim_station,
    }


def _build_fact(df: pd.DataFrame, dims: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Start from source and add foreign keys by lookup joins.
    fact = df.copy()
    fact["event_date"] = pd.to_datetime(fact["event_date"], errors="coerce").dt.date
    fact["date_key"] = pd.to_datetime(fact["event_date"]).dt.strftime("%Y%m%d").astype(int)

    fact = fact.merge(dims["dim_activity"][["activity_key", "activity_type"]], on="activity_type", how="left")
    fact = fact.merge(dims["dim_manufacturer"][["manufacturer_key", "manufacturer_id"]], on="manufacturer_id", how="left")
    fact = fact.merge(dims["dim_vehicle"][["vehicle_key", "vehicle_id"]], on="vehicle_id", how="left")
    fact = fact.merge(dims["dim_battery"][["battery_key", "battery_id"]], on="battery_id", how="left")
    fact = fact.merge(dims["dim_customer"][["customer_key", "customer_id"]], on="customer_id", how="left")
    fact = fact.merge(dims["dim_location"][["location_key", "location_id"]], on="location_id", how="left")
    fact = fact.merge(dims["dim_station"][["station_key", "station_id"]], on="station_id", how="left")

    fact = fact.rename(columns={"record_id": "source_record_id"})

    # Convert tinyint-like flag to boolean.
    if "warranty_claim_flag" in fact.columns:
        fact["warranty_claim_flag"] = fact["warranty_claim_flag"].apply(
            lambda v: None if pd.isna(v) else bool(v)
        )

    # station_key can be null for non-charging events.
    fact["station_key"] = fact["station_key"].astype("Int64")
    fact["date_key"] = fact["date_key"].astype(int)

    return fact[FACT_COLUMNS].copy()


def main() -> None:
    # 1) Prepare run details and folders.
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    ensure_dirs(settings)
    clean_layer_files(settings.gold_layer_dir)
    logger = get_logger("03_gold", settings.log_dir)
    stage_ctx = begin_stage("03_gold", run_id)

    rows_read = 0
    rows_written = 0
    checks_passed = 0
    checks_failed = 0

    try:
        # 2) Read silver data and validate columns.
        source_df = read_parquet(settings.silver_layer_dir, settings.source_table)
        rows_read = len(source_df)
        _require_columns(source_df, REQUIRED_SOURCE_COLUMNS)

        # 3) Build dimensions and fact.
        dims = _build_dims(source_df)
        fact_df = _build_fact(source_df, dims)

        # 4) Save all gold parquet files.
        for name, dim_df in dims.items():
            write_parquet(settings.gold_layer_dir, name, dim_df)
            rows_written += len(dim_df)

        write_parquet(settings.gold_layer_dir, "fact_ev_activity", fact_df)
        rows_written += len(fact_df)

        # 5) Quality check: source rows must equal fact rows.
        row_parity_ok = len(source_df) == len(fact_df)
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "03_gold",
                "check_name": "fact_row_parity",
                "passed": row_parity_ok,
                "details": f"source_rows={len(source_df)}, fact_rows={len(fact_df)}",
            },
        )
        if row_parity_ok:
            checks_passed += 1
        else:
            checks_failed += 1
            raise RuntimeError("Fact row parity check failed")

        # 6) Quality check: required foreign keys cannot be null.
        required_fk_cols = [
            "date_key",
            "activity_key",
            "manufacturer_key",
            "vehicle_key",
            "battery_key",
            "customer_key",
            "location_key",
        ]
        null_fk_counts = {col: int(fact_df[col].isna().sum()) for col in required_fk_cols}
        fk_ok = all(v == 0 for v in null_fk_counts.values())
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "03_gold",
                "check_name": "required_fk_not_null",
                "passed": fk_ok,
                "details": str(null_fk_counts),
            },
        )
        if fk_ok:
            checks_passed += 1
        else:
            checks_failed += 1
            if settings.strict_mode:
                raise RuntimeError(f"Fact required FK columns contain nulls: {null_fk_counts}")

        # 7) Quality check: source key should be unique in fact.
        unique_source_key_ok = int(fact_df.duplicated(subset=["source_record_id"]).sum()) == 0
        write_quality_check(
            settings.log_dir,
            run_id,
            {
                "run_id": run_id,
                "stage": "03_gold",
                "check_name": "source_record_id_unique",
                "passed": unique_source_key_ok,
                "details": f"duplicate_count={int(fact_df.duplicated(subset=['source_record_id']).sum())}",
            },
        )
        if unique_source_key_ok:
            checks_passed += 1
        else:
            checks_failed += 1
            raise RuntimeError("source_record_id has duplicate values")

        # 8) Save stage metric.
        logger.info(
            "run_id=%s | gold built dims=%s fact_rows=%s",
            run_id,
            {k: len(v) for k, v in dims.items()},
            len(fact_df),
        )

        metric = end_stage(
            stage_ctx,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
        )
        write_stage_metric(settings.log_dir, run_id, metric)
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
