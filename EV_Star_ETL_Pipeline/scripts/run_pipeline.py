from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from ev_common import create_run_id, load_settings, read_jsonl, write_run_summary

STAGES = [
    ("01", "01_import_to_bronze.py"),
    ("02", "02_silver.py"),
    ("03", "03_gold.py"),
    ("04", "04_load_neon.py"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run EV star-schema ETL pipeline end-to-end.")
    parser.add_argument("--run-id", default="", help="Optional custom run id.")
    parser.add_argument("--from-stage", default="01", choices=[s[0] for s in STAGES], help="Start stage number.")
    parser.add_argument("--to-stage", default="04", choices=[s[0] for s in STAGES], help="End stage number.")
    return parser.parse_args()


def _stage_range(from_stage: str, to_stage: str) -> list[tuple[str, str]]:
    # Select only requested stages.
    stage_numbers = [s[0] for s in STAGES]
    start = stage_numbers.index(from_stage)
    end = stage_numbers.index(to_stage)
    if start > end:
        raise ValueError("--from-stage must be before or equal to --to-stage")
    return STAGES[start : end + 1]


def main() -> None:
    # 1) Read inputs and create run id.
    args = parse_args()
    run_id = args.run_id or create_run_id()
    settings = load_settings()
    scripts_dir = Path(__file__).resolve().parent

    # 2) Run each stage script in order.
    failed = False
    for stage_num, script_name in _stage_range(args.from_stage, args.to_stage):
        script_path = scripts_dir / script_name
        command = [sys.executable, str(script_path), "--run-id", run_id]
        print(f"[run_id={run_id}] Starting stage {stage_num}: {script_name}")
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as exc:
            failed = True
            print(f"[run_id={run_id}] Stage {stage_num} failed with exit code {exc.returncode}")
            break

            # 3) Build one summary file from stage metrics.
    metrics_file = settings.log_dir / f"{run_id}_stage_metrics.jsonl"
    stage_metrics = read_jsonl(metrics_file)
    summary_path = write_run_summary(
        settings.log_dir,
        run_id,
        pipeline_name="EV_Star_ETL_Pipeline",
        stage_metrics=stage_metrics,
    )
    print(f"[run_id={run_id}] Summary written to: {summary_path}")

    # Stop with non-zero code if any stage failed.
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
