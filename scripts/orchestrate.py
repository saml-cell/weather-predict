"""
Weather Prediction Pipeline Orchestrator

Usage:
    python scripts/orchestrate.py              # Run full pipeline
    python scripts/orchestrate.py --step fetch  # Run single step
    python scripts/orchestrate.py --step model
    python scripts/orchestrate.py --step verify
    python scripts/orchestrate.py --step all
"""

import subprocess
import sys
import os
import time
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_FILE = PROJECT_ROOT / "config.json"

PIPELINE_STEPS = {
    "init": {
        "script": "init_db.py",
        "description": "Initialize database",
                "per_city": False,
        "extra_args": [],
    },
    "fetch": {
        "script": "fetch_weather.py",
        "description": "Fetch weather data from all sources",
                "per_city": True,
        "extra_args": ["--store"],
    },
    "indices": {
        "script": "climate_indices.py",
        "description": "Download NOAA climate indices",
                "per_city": False,
        "extra_args": ["--force"],
    },
    "collect": {
        "script": "collect_forecasts.py",
        "description": "Collect and archive forecasts",
                "per_city": False,
        "extra_args": [],
    },
    "model": {
        "script": "seasonal_forecast.py",
        "description": "Run seasonal prediction models",
                "per_city": True,
        "extra_args": [],
    },
    "verify": {
        "script": "verify_and_score.py",
        "description": "Verify forecasts and update scores",
                "per_city": False,
        "extra_args": [],
    },
    "all": None,  # Special: runs full pipeline
}

FULL_PIPELINE = ["init", "fetch", "indices", "collect", "model", "verify"]


def load_cities() -> list[str]:
    """Load default cities from config.json."""
    try:
        with open(CONFIG_FILE) as f:
            config = json.load(f)
        return config.get("default_cities", ["Bratislava"])
    except (FileNotFoundError, json.JSONDecodeError):
        return ["Bratislava"]


def run_command(cmd: list[str], label: str) -> dict:
    """Run a single command and return result."""
    start_time = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )
        elapsed = time.time() - start_time

        if result.returncode == 0:
            print(f"    OK  {label} ({elapsed:.1f}s)")
            if result.stdout.strip():
                for line in result.stdout.strip().split("\n")[-3:]:
                    print(f"      {line}")
            return {"label": label, "success": True, "elapsed": elapsed}
        else:
            print(f"    FAIL {label} ({elapsed:.1f}s)")
            if result.stderr.strip():
                for line in result.stderr.strip().split("\n")[-3:]:
                    print(f"      ERROR: {line}")
            return {
                "label": label,
                "success": False,
                "error": result.stderr.strip()[-300:],
                "elapsed": elapsed,
            }
    except subprocess.TimeoutExpired:
        return {"label": label, "success": False, "error": "Timeout (300s)"}
    except Exception as e:
        return {"label": label, "success": False, "error": str(e)}


def run_step(step_name: str) -> dict:
    """Run a single pipeline step and return result."""
    step = PIPELINE_STEPS[step_name]
    script_path = SCRIPTS_DIR / step["script"]

    if not script_path.exists():
        return {
            "step": step_name,
            "success": False,
            "error": f"Script not found: {script_path}",
        }

    print(f"\n{'='*60}")
    print(f"  {step['description']}")
    print(f"{'='*60}")

    start_time = time.time()

    if step.get("per_city"):
        # Run once per city
        cities = load_cities()
        all_ok = True
        for city in cities:
            cmd = [sys.executable, str(script_path), city] + step["extra_args"]
            sub = run_command(cmd, city)
            if not sub["success"]:
                all_ok = False
        elapsed = time.time() - start_time
        return {"step": step_name, "success": all_ok, "elapsed": elapsed}
    else:
        # Run once (no city argument)
        cmd = [sys.executable, str(script_path)] + step["extra_args"]
        sub = run_command(cmd, step["script"])
        elapsed = time.time() - start_time
        return {
            "step": step_name,
            "success": sub["success"],
            "elapsed": elapsed,
            **({"error": sub["error"]} if not sub["success"] else {}),
        }


def run_pipeline(steps: list[str]) -> list[dict]:
    """Run multiple pipeline steps in order."""
    results = []
    for step_name in steps:
        result = run_step(step_name)
        results.append(result)
        if not result["success"]:
            print(f"\n  Pipeline stopped at '{step_name}' due to error.")
            break
    return results


def print_summary(results: list[dict]):
    """Print pipeline execution summary."""
    print(f"\n{'='*60}")
    print("  PIPELINE SUMMARY")
    print(f"{'='*60}")

    total_time = sum(r.get("elapsed", 0) for r in results)
    successes = sum(1 for r in results if r["success"])

    for r in results:
        status = "OK" if r["success"] else "FAIL"
        elapsed = f"{r.get('elapsed', 0):.1f}s" if "elapsed" in r else "N/A"
        print(f"  [{status}] {r['step']:12s}  {elapsed}")
        if not r["success"] and "error" in r:
            print(f"         {r['error'][:80]}")

    print(f"\n  {successes}/{len(results)} steps succeeded in {total_time:.1f}s")



def main():
    parser = argparse.ArgumentParser(description="Weather Prediction Pipeline Orchestrator")
    parser.add_argument(
        "--step",
        choices=list(PIPELINE_STEPS.keys()),
        default="all",
        help="Pipeline step to run (default: all)",
    )
    args = parser.parse_args()

    print("Weather Prediction Pipeline")
    print(f"Project: {PROJECT_ROOT}")

    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    if args.step == "all":
        results = run_pipeline(FULL_PIPELINE)
    else:
        results = [run_step(args.step)]

    print_summary(results)

    # Exit with error code if any step failed
    if not all(r["success"] for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
