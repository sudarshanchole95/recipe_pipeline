# config.py
from pathlib import Path

ROOT = Path(__file__).parent.resolve()

EXPORT_DIR = ROOT / "export"
OUTPUT_DIR = ROOT / "output"
ETL_DIR = OUTPUT_DIR / "etl"
BAD_DATA_DIR = OUTPUT_DIR / "bad_data"
VALIDATION_DIR = OUTPUT_DIR / "validation"
ANALYTICS_DIR = OUTPUT_DIR / "analytics"
CHARTS_DIR = ANALYTICS_DIR / "charts"
LOG_DIR = OUTPUT_DIR / "logs"
FINAL_DIR = ROOT / "final_report"

# Firestore / service account (do NOT commit serviceAccountKey.json)
SERVICE_ACCOUNT_JSON = ROOT / "serviceAccountKey.json"

# Behavior toggles
DEDUPE_KEEP = "latest"   # "latest" or "first"
EXPORT_RETRIES = 2
RETRY_DELAY_SECONDS = 3
