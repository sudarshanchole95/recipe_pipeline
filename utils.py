# utils.py
import json
import os
from datetime import datetime
from pathlib import Path

def ensure_dirs(*paths):
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def timestamp():
    return datetime.utcnow().isoformat() + "Z"

def write_json(path, obj, indent=2):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=indent)

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def small_report(path, lines):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
