"""
pipeline.py

Orchestrator for the Recipe Pipeline (Export -> ETL -> Validate -> Analytics)

Behavior: Option A (Continue on bad data). All steps are attempted; errors are recorded
and the pipeline proceeds to the next step. Final run summary is written to output/logs/.

Usage:
    python pipeline.py
"""

from datetime import datetime
import time
import traceback
from pathlib import Path
import json

# Modules in your project (these should already exist in project root)
# export_firestore.py  -> function export_all()
# etl_pipeline.py     -> function normalize_recipes()
# validation.py       -> function run_validation()
# analytics.py        -> function run_analytics()

# If you used the helper config/utils files, import them too
try:
    from config import EXPORT_DIR, ETL_DIR, BAD_DATA_DIR, VALIDATION_DIR, ANALYTICS_DIR, CHARTS_DIR, LOG_DIR
    from utils import ensure_dirs, write_json, timestamp
except Exception:
    # If config/utils are not present, create minimal folder vars
    ROOT = Path(__file__).parent.resolve()
    EXPORT_DIR = ROOT / "export"
    ETL_DIR = ROOT / "output" / "etl"
    BAD_DATA_DIR = ROOT / "output" / "bad_data"
    VALIDATION_DIR = ROOT / "output" / "validation"
    ANALYTICS_DIR = ROOT / "output" / "analytics"
    CHARTS_DIR = ANALYTICS_DIR / "charts"
    LOG_DIR = ROOT / "output" / "logs"

    def ensure_dirs(*paths):
        for p in paths:
            Path(p).mkdir(parents=True, exist_ok=True)

    def write_json(path, obj, indent=2):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=indent)

    def timestamp():
        return datetime.utcnow().isoformat() + "Z"

# Import pipeline stage modules (they must be in the same folder)
import importlib

def safe_import(name):
    try:
        return importlib.import_module(name)
    except Exception as e:
        return None

EXPORT_MOD = safe_import("export_firestore")
ETL_MOD = safe_import("etl_pipeline")
VALID_MOD = safe_import("validation")
ANALYTICS_MOD = safe_import("analytics")

# Ensure output folders exist
ensure_dirs(EXPORT_DIR, ETL_DIR, BAD_DATA_DIR, VALIDATION_DIR, ANALYTICS_DIR, CHARTS_DIR, LOG_DIR)

def run_step(name: str, fn, run_args=None, timeout=None):
    """Run a pipeline step, capture timing, result, errors; continue on error."""
    start = time.time()
    result = {"step": name, "started_at": timestamp(), "status": "not_run", "elapsed_s": None, "error": None, "result": None}
    try:
        if fn is None:
            raise RuntimeError(f"Module/function for step '{name}' not found (module import failed).")
        if run_args:
            out = fn(**run_args)
        else:
            out = fn()
        result["status"] = "success"
        result["result"] = out
    except Exception as e:
        # capture full traceback and message
        tb = traceback.format_exc()
        result["status"] = "failed"
        result["error"] = {"message": str(e), "traceback": tb}
        print(f"[ERROR] Step {name} failed: {e}")
    finally:
        result["elapsed_s"] = round(time.time() - start, 2)
        result["finished_at"] = timestamp()
    return result

def main():
    run_manifest = {
        "run_id": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
        "started_at": timestamp(),
        "steps": [],
        "summary": {}
    }

    print("=== Recipe Pipeline Orchestrator (Option A: continue on errors) ===")
    print("Working dirs:")
    print(f" - export:       {EXPORT_DIR}")
    print(f" - etl output:   {ETL_DIR}")
    print(f" - bad data:     {BAD_DATA_DIR}")
    print(f" - validation:   {VALIDATION_DIR}")
    print(f" - analytics:    {ANALYTICS_DIR}")
    print("--------------------------------------------------------------\n")

    # 1) EXPORT
    print("1) EXPORT: Export Firestore collections to export/*.json")
    export_fn = getattr(EXPORT_MOD, "export_all", None) if EXPORT_MOD else None
    res_export = run_step("export", export_fn)
    run_manifest["steps"].append(res_export)

    # 2) ETL
    print("\n2) ETL: Normalize JSON -> CSV (output/etl/)")
    etl_fn = getattr(ETL_MOD, "normalize_recipes", None) if ETL_MOD else None
    res_etl = run_step("etl", etl_fn)
    run_manifest["steps"].append(res_etl)

    # 3) VALIDATION
    print("\n3) VALIDATION: Run data quality checks")
    valid_fn = getattr(VALID_MOD, "run_validation", None) if VALID_MOD else None
    res_valid = run_step("validation", valid_fn)
    run_manifest["steps"].append(res_valid)

    # 4) ANALYTICS
    print("\n4) ANALYTICS: Generate charts + analytics_summary.md")
    analytics_fn = getattr(ANALYTICS_MOD, "run_analytics", None) if ANALYTICS_MOD else None
    res_analytics = run_step("analytics", analytics_fn)
    run_manifest["steps"].append(res_analytics)

    # Finalize run manifest: summarize statuses
    statuses = [s["status"] for s in run_manifest["steps"]]
    run_manifest["summary"]["total_steps"] = len(run_manifest["steps"])
    run_manifest["summary"]["success_steps"] = sum(1 for s in statuses if s == "success")
    run_manifest["summary"]["failed_steps"] = sum(1 for s in statuses if s == "failed")
    run_manifest["finished_at"] = timestamp()
    run_manifest["elapsed_total_s"] = sum((s.get("elapsed_s") or 0) for s in run_manifest["steps"])
    # Save manifest
    out_file = Path(LOG_DIR) / f"run_report_{run_manifest['run_id']}.json"
    write_json(out_file, run_manifest)
    print("\n=== PIPELINE RUN COMPLETE ===")
    print(f"Run ID: {run_manifest['run_id']}")
    print(f"Started: {run_manifest['started_at']}")
    print(f"Finished: {run_manifest['finished_at']}")
    print(f"Total elapsed (s): {run_manifest['elapsed_total_s']}")
    print(f"Success steps: {run_manifest['summary']['success_steps']} / {run_manifest['summary']['total_steps']}")
    if run_manifest['summary']['failed_steps'] > 0:
        print("Some steps failed. Check the run report and the per-step tracebacks:")
        for s in run_manifest["steps"]:
            if s["status"] == "failed":
                print(f" - {s['step']}: {s['error']['message'] if s['error'] else 'Unknown error'}")
                # Optionally print small snippet of traceback
                tb = s["error"]["traceback"] if s.get("error") and s["error"].get("traceback") else None
                if tb:
                    print("   Traceback (first 3 lines):")
                    for line in tb.splitlines()[:3]:
                        print("     " + line)
    else:
        print("All steps completed successfully.")

    print(f"\nRun manifest saved -> {out_file}")
    print("Inspect output/etl/, output/bad_data/, output/validation/, output/analytics/ for artifacts.")
    return run_manifest

if __name__ == "__main__":
    main()
