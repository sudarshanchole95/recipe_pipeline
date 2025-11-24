"""
pipeline.py
-----------
Production-grade orchestrator for the Recipe Data Pipeline:
Export → ETL → Validation → Analytics

Behaviour:
- Pipeline continues on non-critical failures (Option A).
- Every step logs a concise professional summary.
- Full run manifest stored in output/logs/.
"""

from datetime import datetime, timezone
import time
import traceback
from pathlib import Path
import json
import importlib

# ------------------------------------------------------------
# Fallback directories if config.py is not available
# ------------------------------------------------------------
ROOT = Path(__file__).parent.resolve()
EXPORT_DIR = ROOT / "export"
ETL_DIR = ROOT / "output" / "etl"
BAD_DATA_DIR = ROOT / "output" / "bad_data"
VALIDATION_DIR = ROOT / "output" / "validation"
ANALYTICS_DIR = ROOT / "output" / "analytics"
CHARTS_DIR = ANALYTICS_DIR / "charts"
LOG_DIR = ROOT / "output" / "logs"

for p in (EXPORT_DIR, ETL_DIR, BAD_DATA_DIR, VALIDATION_DIR, ANALYTICS_DIR, CHARTS_DIR, LOG_DIR):
    p.mkdir(parents=True, exist_ok=True)


def timestamp():
    """Return clean ISO8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ------------------------------------------------------------
# Safe dynamic module loader
# ------------------------------------------------------------
def safe_import(name):
    try:
        return importlib.import_module(name)
    except Exception:
        return None


EXPORT_MOD = safe_import("export_firestore")
ETL_MOD = safe_import("etl_pipeline")
VALID_MOD = safe_import("validation")
ANALYTICS_MOD = safe_import("analytics")


# ------------------------------------------------------------
# Step executor
# ------------------------------------------------------------
def run_step(step_name: str, fn, run_args=None):
    start = time.time()

    result = {
        "step": step_name,
        "started_at": timestamp(),
        "status": "not_run",
        "error": None,
        "elapsed_s": None,
        "result": None
    }

    print(f"> Running {step_name}...")

    try:
        if fn is None:
            raise RuntimeError(f"Missing function implementation for step: {step_name}")

        output = fn(**run_args) if run_args else fn()

        result["status"] = "success"
        result["result"] = output
        print(f"✔ {step_name} completed successfully")

    except Exception as e:
        tb = traceback.format_exc()
        result["status"] = "failed"
        result["error"] = {"message": str(e), "traceback": tb}

        print(f"✖ {step_name} failed: {e}")

    finally:
        result["elapsed_s"] = round(time.time() - start, 2)
        result["finished_at"] = timestamp()

    return result


# ------------------------------------------------------------
# Main Orchestrator
# ------------------------------------------------------------
def main():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    print("\n==============================================")
    print("        🚀 Recipe Data Pipeline Runner")
    print("==============================================")
    print(f"Run ID: {run_id}")
    print(f"Start : {timestamp()}")
    print("----------------------------------------------")

    run_manifest = {
        "run_id": run_id,
        "started_at": timestamp(),
        "steps": [],
        "summary": {}
    }

    # ------- 1) EXPORT -------
    export_fn = getattr(EXPORT_MOD, "export_all", None) if EXPORT_MOD else None
    res_export = run_step("EXPORT", export_fn)
    run_manifest["steps"].append(res_export)

    # ------- 2) ETL -------
    etl_fn = getattr(ETL_MOD, "normalize_recipes", None) if ETL_MOD else None
    res_etl = run_step("ETL", etl_fn)
    run_manifest["steps"].append(res_etl)

    # ------- 3) VALIDATION -------
    valid_fn = getattr(VALID_MOD, "run_validation", None) if VALID_MOD else None
    res_valid = run_step("VALIDATION", valid_fn)
    run_manifest["steps"].append(res_valid)

    # ------- 4) ANALYTICS -------
    analytics_fn = getattr(ANALYTICS_MOD, "run_analytics", None) if ANALYTICS_MOD else None
    res_analytics = run_step("ANALYTICS", analytics_fn)
    run_manifest["steps"].append(res_analytics)

    # Summary
    statuses = [s["status"] for s in run_manifest["steps"]]

    run_manifest["summary"] = {
        "total_steps": len(statuses),
        "successful_steps": statuses.count("success"),
        "failed_steps": statuses.count("failed"),
        "finished_at": timestamp(),
        "elapsed_total_s": sum(s["elapsed_s"] or 0 for s in run_manifest["steps"])
    }

    # Persist run manifest
    out_file = LOG_DIR / f"run_report_{run_id}.json"
    out_file.write_text(json.dumps(run_manifest, indent=2))

    # Final Professional Summary
    print("\n----------------------------------------------")
    print("                 📘 Run Summary")
    print("----------------------------------------------")
    print(f"Total Steps     : {run_manifest['summary']['total_steps']}")
    print(f"Successful      : {run_manifest['summary']['successful_steps']}")
    print(f"Failed          : {run_manifest['summary']['failed_steps']}")
    print(f"Total Runtime   : {run_manifest['summary']['elapsed_total_s']} sec")

    if run_manifest['summary']['failed_steps'] > 0:
        print("\n⚠ Some steps failed. Review details in:")
    else:
        print("\n✔ Pipeline completed successfully.")

    print(f"Manifest saved → {out_file}\n")
    print("==============================================")

    return run_manifest


if __name__ == "__main__":
    main()
