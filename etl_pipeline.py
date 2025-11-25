"""
etl_pipeline.py  


Behaviour:
- Reads export/*.json
- Validates and deduplicates data
- Writes clean CSVs
- Stores bad/duplicate rows in bad_data/
- Produces ETL manifest
- Prints clean, concise, professional logs
"""

from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from typing import List, Dict, Any, Tuple
import time

# -------------------------------------------------------
# Config fallbacks
# -------------------------------------------------------
try:
    from config import EXPORT_DIR, ETL_DIR, BAD_DATA_DIR
    from utils import ensure_dirs, write_json
except Exception:
    ROOT = Path(__file__).parent.resolve()
    EXPORT_DIR = ROOT / "export"
    ETL_DIR = ROOT / "output" / "etl"
    BAD_DATA_DIR = ROOT / "output" / "bad_data"

    def ensure_dirs(*paths):
        for p in paths:
            Path(p).mkdir(parents=True, exist_ok=True)

    def write_json(path, obj, indent=2):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=indent, ensure_ascii=False)


# -------------------------------------------------------
# Validation Helpers
# -------------------------------------------------------
VALID_DIFFICULTIES = {"easy", "medium", "hard", "unknown", ""}


def ts():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_str(x):
    return "" if pd.isna(x) else str(x)


def validate_recipe(rec):
    reasons = []
    rid = rec.get("id") or rec.get("slug") or ""

    if not rid:
        reasons.append("missing_id")
    if not _to_str(rec.get("title", "")).strip():
        reasons.append("missing_title")

    ing = rec.get("ingredients", [])
    if not isinstance(ing, list) or len(ing) == 0:
        reasons.append("no_ingredients")
    else:
        for i, x in enumerate(ing):
            if not isinstance(x, dict) or not _to_str(x.get("name")).strip():
                reasons.append("ingredient_missing_name")
                break

    steps = rec.get("steps", [])
    if not isinstance(steps, list) or len(steps) == 0:
        reasons.append("no_steps")

    for f in ("prep_time_min", "cook_time_min", "total_time_min"):
        if f in rec and rec[f] not in (None, ""):
            try:
                if float(rec[f]) < 0:
                    reasons.append("negative_time_value")
            except:
                reasons.append("invalid_time_format")

    diff = _to_str(rec.get("difficulty", "")).strip().lower()
    if diff and diff not in VALID_DIFFICULTIES:
        reasons.append("invalid_difficulty")

    return (len(reasons) == 0, reasons)


def validate_interaction(it):
    reasons = []
    if not it.get("id"):
        reasons.append("missing_id")
    if not it.get("user_id"):
        reasons.append("missing_user_id")
    if not it.get("recipe_id"):
        reasons.append("missing_recipe_id")
    if it.get("type") not in {"view", "like", "rating", "attempt"}:
        reasons.append("invalid_type")
    return (len(reasons) == 0, reasons)


# -------------------------------------------------------
# IO Helpers
# -------------------------------------------------------
def load_json(path):
    return json.loads(path.read_text()) if path.exists() else []


def read_csv(path: Path):
    # If file doesn't exist OR is empty → return empty DF
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()

    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def write_csv(path, df):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


# -------------------------------------------------------
# Core ETL
# -------------------------------------------------------
def normalize_recipes():
    start_time = time.time()
    print("\n[ETL] Starting ETL step...")

    ensure_dirs(ETL_DIR, BAD_DATA_DIR)

    # Load JSON exports
    rec_json = load_json(EXPORT_DIR / "recipes.json")
    inter_json = load_json(EXPORT_DIR / "interactions.json")

    # Load existing CSVs
    recipe_csv = ETL_DIR / "recipe.csv"
    ing_csv = ETL_DIR / "ingredients.csv"
    steps_csv = ETL_DIR / "steps.csv"
    inter_csv = ETL_DIR / "interactions.csv"

    # Fix: Safely load existing IDs only if 'id' column exists
    rec_df = read_csv(recipe_csv)
    existing_rec_ids = set(rec_df["id"]) if "id" in rec_df.columns else set()

    # NEW: Safely load existing Content Keys (Title + Cuisine) for deduplication
    existing_content_keys = set()
    if "title" in rec_df.columns and "cuisine" in rec_df.columns:
        existing_content_keys = set(zip(rec_df["title"], rec_df["cuisine"]))

    inter_df = read_csv(inter_csv)
    existing_inter_ids = set(inter_df["id"]) if "id" in inter_df.columns else set()

    recipe_rows = []
    ing_rows = []
    step_rows = []
    inter_rows = []

    bad_recipes = []
    dup_recipes = []
    bad_interactions = []
    dup_interactions = []

    # -------------------------
    # PROCESS RECIPES
    #--------------------------
    for rec in rec_json:
        rid = _to_str(rec.get("id") or rec.get("slug"))
        
        # Extract content fields for deduplication
        title = _to_str(rec.get("title")).strip()
        cuisine = _to_str(rec.get("cuisine")).strip()

        is_valid, reasons = validate_recipe(rec)

        if not rid:
            bad_recipes.append({"record": rec, "reasons": reasons})
            continue

        if rid in existing_rec_ids:
            dup_recipes.append({"record": rec, "reasons": ["duplicate_id"]})
            continue

        if not is_valid:
            bad_recipes.append({"record": rec, "reasons": reasons})
            continue

        # NEW: Content-based Deduplication Check (Title + Cuisine)
        if (title, cuisine) in existing_content_keys:
            dup_recipes.append({"record": rec, "reasons": ["duplicate_content"]})
            continue

        # Valid → flatten recipe
        recipe_rows.append({
            "id": rid,
            "title": title,
            "cuisine": cuisine,
            "difficulty": _to_str(rec.get("difficulty")),
            "prep_time_min": rec.get("prep_time_min", ""),
            "cook_time_min": rec.get("cook_time_min", ""),
            "total_time_min": rec.get("total_time_min", ""),
            "tags": ",".join(rec.get("tags", [])) if isinstance(rec.get("tags"), list) else "",
            "created_at": _to_str(rec.get("created_at")),
        })

        for ing in rec.get("ingredients", []):
            ing_rows.append({
                "recipe_id": rid,
                "ingredient_name": _to_str(ing.get("name")),
                "quantity": _to_str(ing.get("quantity")),
                "unit": _to_str(ing.get("unit")),
            })

        for st in rec.get("steps", []):
            if isinstance(st, dict):
                step_text = _to_str(st.get("description"))
                step_num = st.get("step_number", "")
            else:
                step_text = _to_str(st)
                step_num = ""
            step_rows.append({
                "recipe_id": rid,
                "step_number": step_num,
                "step_text": step_text
            })

        existing_rec_ids.add(rid)
        existing_content_keys.add((title, cuisine))

    # -------------------------
    # PROCESS INTERACTIONS
    #--------------------------
    for it in inter_json:
        iid = _to_str(it.get("id"))
        is_valid, reasons = validate_interaction(it)

        if not iid:
            bad_interactions.append({"record": it, "reasons": reasons})
            continue

        if iid in existing_inter_ids:
            dup_interactions.append({"record": it, "reasons": ["duplicate_id"]})
            continue

        if not is_valid:
            bad_interactions.append({"record": it, "reasons": reasons})
            continue

        inter_rows.append({
            "id": iid,
            "user_id": _to_str(it.get("user_id")),
            "recipe_id": _to_str(it.get("recipe_id")),
            "type": _to_str(it.get("type")),
            "timestamp": _to_str(it.get("timestamp")),
            "metadata_json": json.dumps(it.get("metadata", {})),
        })

        existing_inter_ids.add(iid)

    # -------------------------------------------------------
    # WRITE CLEAN CSV OUTPUTS
    # -------------------------------------------------------
    if recipe_rows:
        df_new = pd.DataFrame(recipe_rows)
        if recipe_csv.exists():
            df_old = read_csv(recipe_csv)
            df = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=["id"])
        else:
            df = df_new
        write_csv(recipe_csv, df)

    if ing_rows:
        df_new = pd.DataFrame(ing_rows)
        if ing_csv.exists():
            df_old = read_csv(ing_csv)
            df = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates()
        else:
            df = df_new
        write_csv(ing_csv, df)

    if step_rows:
        df_new = pd.DataFrame(step_rows)
        if steps_csv.exists():
            df_old = read_csv(steps_csv)
            df = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates()
        else:
            df = df_new
        write_csv(steps_csv, df)

    if inter_rows:
        df_new = pd.DataFrame(inter_rows)
        if inter_csv.exists():
            df_old = read_csv(inter_csv)
            df = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(subset=["id"])
        else:
            df = df_new
        write_csv(inter_csv, df)

    # -------------------------------------------------------
    # WRITE BAD DATA (clean)
    # -------------------------------------------------------
    if bad_recipes:
        write_json(BAD_DATA_DIR / "bad_recipes.json", bad_recipes)

    if dup_recipes:
        pd.DataFrame(dup_recipes).to_csv(BAD_DATA_DIR / "duplicate_recipes.csv", index=False)

    if bad_interactions:
        pd.DataFrame(bad_interactions).to_csv(BAD_DATA_DIR / "bad_interactions.csv", index=False)

    if dup_interactions:
        pd.DataFrame(dup_interactions).to_csv(BAD_DATA_DIR / "duplicate_interactions.csv", index=False)

    # -------------------------------------------------------
    # SUMMARY (Professional)
    # -------------------------------------------------------
    elapsed = round(time.time() - start_time, 2)

    print("\n[ETL] Summary")
    print("----------------------------------------")
    print(f"New Recipes:         {len(recipe_rows)}")
    print(f"Bad Recipes:         {len(bad_recipes)}")
    print(f"Duplicate Recipes:   {len(dup_recipes)}")
    print(f"New Ingredients:     {len(ing_rows)}")
    print(f"New Steps:           {len(step_rows)}")
    print(f"New Interactions:    {len(inter_rows)}")
    print(f"Duplicate Interact.: {len(dup_interactions)}")
    print(f"Bad Interactions:    {len(bad_interactions)}")
    print(f"Elapsed:             {elapsed}s")
    print("----------------------------------------")
    print("[ETL] ✔ Completed\n")

    # Manifest
    manifest = {
        "timestamp": ts(),
        "new_recipes": len(recipe_rows),
        "bad_recipes": len(bad_recipes),
        "duplicate_recipes": len(dup_recipes),
        "new_ingredients": len(ing_rows),
        "new_steps": len(step_rows),
        "new_interactions": len(inter_rows),
        "duplicate_interactions": len(dup_interactions),
        "bad_interactions": len(bad_interactions),
        "elapsed_s": elapsed
    }

    write_json(ETL_DIR / "_etl_manifest.json", manifest)

    return manifest


# -------------------------------------------------------
if __name__ == "__main__":
    normalize_recipes()