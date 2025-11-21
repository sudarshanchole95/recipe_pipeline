"""
etl_pipeline.py - Robust ETL for Recipe Pipeline (Senior-level)

Behavior:
- Reads export/*.json (recipes.json, interactions.json)
- Validates records using rules
- Avoids inserting duplicates (by id) into CSV outputs
- Writes duplicates to output/bad_data/duplicate_*.csv
- Writes invalid records to output/bad_data/bad_*.json or .csv (with reason)
- Appends only new valid rows to output/etl/*.csv
- Produces _etl_manifest.json with counts and breakdown

Usage:
    python etl_pipeline.py

Dependencies: pandas, json, pathlib, typing, datetime
"""

from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any, Tuple

# Optional project config / utils (fallback to local defaults if not present)
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

    def write_json(path: Path, obj: Any, indent: int = 2):
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=indent)


# -------------------------
# Validation rules / helpers
# -------------------------
VALID_DIFFICULTIES = {"easy", "medium", "hard", "unknown", ""}

def _to_str(x):
    return "" if pd.isna(x) else str(x)

def validate_recipe(rec: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate a recipe document. Return (is_valid, reasons_list).
    Business rules:
      - id and title must exist
      - ingredients must be non-empty list of dicts with 'name'
      - steps must be non-empty list
      - prep_time_min, cook_time_min, total_time_min if present must be non-negative numbers
      - difficulty must be in VALID_DIFFICULTIES (case-insensitive)
    """
    reasons = []
    rid = rec.get("id") or rec.get("slug") or None
    title = _to_str(rec.get("title", "")).strip()
    ingredients = rec.get("ingredients", [])
    steps = rec.get("steps", [])

    if not rid:
        reasons.append("missing_id")
    if not title:
        reasons.append("missing_title")

    if not isinstance(ingredients, list) or len(ingredients) == 0:
        reasons.append("no_ingredients")
    else:
        # Each ingredient must be dict with name
        for i, ing in enumerate(ingredients):
            if not isinstance(ing, dict) or not _to_str(ing.get("name")).strip():
                reasons.append(f"ingredient_missing_name_at_index_{i}")
                break

    if not isinstance(steps, list) or len(steps) == 0:
        reasons.append("no_steps")

    # time fields
    for tfield in ("prep_time_min", "cook_time_min", "total_time_min"):
        if tfield in rec and rec[tfield] not in (None, ""):
            try:
                val = float(rec[tfield])
                if val < 0:
                    reasons.append(f"negative_{tfield}")
            except Exception:
                reasons.append(f"invalid_number_{tfield}")

    # difficulty
    diff = _to_str(rec.get("difficulty", "")).strip().lower()
    if diff and diff not in VALID_DIFFICULTIES:
        reasons.append("invalid_difficulty")

    return (len(reasons) == 0, reasons)


def validate_interaction(it: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons = []
    if not it.get("id"):
        reasons.append("missing_id")
    if not it.get("user_id"):
        reasons.append("missing_user_id")
    if not it.get("recipe_id"):
        reasons.append("missing_recipe_id")
    if it.get("type") not in {"view", "like", "rating", "attempt"}:
        reasons.append("invalid_type")
    # rating should have numeric rating in metadata (optional)
    if it.get("type") == "rating":
        meta = it.get("metadata", {})
        if not isinstance(meta, dict) or not any(k in meta for k in ("rating", "score", "stars")):
            reasons.append("rating_missing_value")
    return (len(reasons) == 0, reasons)


# -------------------------
# IO helpers
# -------------------------
def load_json_file(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _ensure_csv(path: Path, df: pd.DataFrame):
    """Write dataframe to CSV, creating parent folders."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path, dtype=str).fillna("")
    else:
        return pd.DataFrame()


# -------------------------
# Main normalization function
# -------------------------
def normalize_recipes() -> Dict[str, Any]:
    """
    Entry point for ETL step.
    Returns a manifest dict describing counts and actions taken.
    """
    ensure_dirs(ETL_DIR, BAD_DATA_DIR)

    recipes_path = Path(EXPORT_DIR) / "recipes.json"
    interactions_path = Path(EXPORT_DIR) / "interactions.json"

    recipes = load_json_file(recipes_path)
    interactions = load_json_file(interactions_path)

    # Load existing CSVs to detect duplicates across runs
    recipe_csv = Path(ETL_DIR) / "recipe.csv"
    ingredients_csv = Path(ETL_DIR) / "ingredients.csv"
    steps_csv = Path(ETL_DIR) / "steps.csv"
    interactions_csv = Path(ETL_DIR) / "interactions.csv"

    existing_recipes_df = _read_csv_if_exists(recipe_csv)
    existing_recipe_ids = set(existing_recipes_df["id"].astype(str)) if "id" in existing_recipes_df.columns else set()

    existing_inter_df = _read_csv_if_exists(interactions_csv)
    existing_inter_ids = set(existing_inter_df["id"].astype(str)) if "id" in existing_inter_df.columns else set()

    # Prepare rows to append
    recipe_rows = []
    ingredient_rows = []
    step_rows = []
    inter_rows = []

    # Track bad/duplicate
    bad_recipes = []
    dup_recipes = []
    bad_interactions = []
    dup_interactions = []

    # Counters for component failures within a recipe (for analytics)
    component_failures = {
        "missing_title": 0,
        "no_ingredients": 0,
        "no_steps": 0,
        "invalid_time": 0,
        "invalid_difficulty": 0,
        "missing_id": 0,
        "ingredient_name_missing": 0
    }

    # Process recipes
    for rec in recipes:
        rid = rec.get("id") or rec.get("slug")
        rid_str = str(rid) if rid else ""
        is_valid, reasons = validate_recipe(rec)

        # Update component failure counts
        for r in reasons:
            if "missing_title" in r or r == "missing_title": component_failures["missing_title"] += 1
            if r == "no_ingredients": component_failures["no_ingredients"] += 1
            if r == "no_steps": component_failures["no_steps"] += 1
            if r.startswith("negative_") or r.startswith("invalid_number_"): component_failures["invalid_time"] += 1
            if r == "invalid_difficulty": component_failures["invalid_difficulty"] += 1
            if r == "missing_id": component_failures["missing_id"] += 1
            if r.startswith("ingredient_missing_name_at_index_") or r.startswith("ingredient_missing_name"): component_failures["ingredient_name_missing"] += 1

        if not rid_str:
            # cannot dedupe - treat as bad
            bad_recipes.append({"record": rec, "reasons": reasons})
            continue

        if rid_str in existing_recipe_ids:
            dup_recipes.append({"record": rec, "reasons": ["duplicate_id"]})
            continue

        if not is_valid:
            bad_recipes.append({"record": rec, "reasons": reasons})
            continue

        # Passed validation - convert to flat rows
        # recipe base
        recipe_row = {
            "id": rid_str,
            "title": _to_str(rec.get("title", "")).strip(),
            "author": _to_str(rec.get("author", "")),
            "cuisine": _to_str(rec.get("cuisine", "")),
            "difficulty": _to_str(rec.get("difficulty", "")).strip(),
            "servings": rec.get("servings", ""),
            "prep_time_min": rec.get("prep_time_min", ""),
            "cook_time_min": rec.get("cook_time_min", ""),
            "total_time_min": rec.get("total_time_min", ""),
            "tags": ",".join(rec.get("tags", [])) if isinstance(rec.get("tags", []), list) else _to_str(rec.get("tags", "")),
            "occasion": ",".join(rec.get("occasion", [])) if isinstance(rec.get("occasion", []), list) else _to_str(rec.get("occasion", "")),
            "category_type": _to_str(rec.get("category_type", "")),
            "texture": _to_str(rec.get("texture", "")),
            "served_temperature": _to_str(rec.get("served_temperature", "")),
            "nutrition_groups": ",".join(rec.get("nutrition_groups", [])) if isinstance(rec.get("nutrition_groups", []), list) else _to_str(rec.get("nutrition_groups", "")),
            "created_at": _to_str(rec.get("created_at", "")),
        }
        recipe_rows.append(recipe_row)

        # ingredients
        for ing in rec.get("ingredients", []):
            ingredient_rows.append({
                "recipe_id": rid_str,
                "ingredient_name": _to_str(ing.get("name", "")).strip(),
                "quantity": _to_str(ing.get("quantity", "")),
                "unit": _to_str(ing.get("unit", "")),
            })

        # steps: allow steps as dicts or simple strings
        for s in rec.get("steps", []):
            if isinstance(s, dict):
                step_number = s.get("step_number", "")
                step_text = _to_str(s.get("description", "")).strip()
            else:
                step_number = ""
                step_text = _to_str(s)
            step_rows.append({
                "recipe_id": rid_str,
                "step_number": step_number,
                "step_text": step_text
            })

        # mark as seen to prevent same-run duplicates
        existing_recipe_ids.add(rid_str)

    # Process interactions
    for it in interactions:
        iid = it.get("id") or ""
        iid_str = str(iid)
        is_valid, reasons = validate_interaction(it)

        if not iid_str:
            bad_interactions.append({"record": it, "reasons": reasons if reasons else ["missing_id"]})
            continue

        if iid_str in existing_inter_ids:
            dup_interactions.append({"record": it, "reasons": ["duplicate_id"]})
            continue

        if not is_valid:
            bad_interactions.append({"record": it, "reasons": reasons})
            continue

        # valid - flatten small
        inter_rows.append({
            "id": iid_str,
            "user_id": _to_str(it.get("user_id", "")),
            "recipe_id": _to_str(it.get("recipe_id", "")),
            "type": _to_str(it.get("type", "")),
            "timestamp": _to_str(it.get("timestamp", "")),
            "metadata_json": json.dumps(it.get("metadata", {}), ensure_ascii=False)
        })

        existing_inter_ids.add(iid_str)

    # -------------------------
    # Write outputs
    # -------------------------
    # Recipes/Ingredients/Steps: append only new validated rows
    summary = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "new_recipes": len(recipe_rows),
        "duplicate_recipes": len(dup_recipes),
        "bad_recipes": len(bad_recipes),
        "new_ingredients": len(ingredient_rows),
        "new_steps": len(step_rows),
        "new_interactions": len(inter_rows),
        "duplicate_interactions": len(dup_interactions),
        "bad_interactions": len(bad_interactions),
        "component_failures": component_failures
    }

    # Write or append recipe.csv
    recipe_df_new = pd.DataFrame(recipe_rows)
    ing_df_new = pd.DataFrame(ingredient_rows)
    steps_df_new = pd.DataFrame(step_rows)
    inter_df_new = pd.DataFrame(inter_rows)

    # Append if file exists else write new
    if not recipe_df_new.empty:
        if recipe_csv.exists():
            existing = _read_csv_if_exists(recipe_csv)
            combined = pd.concat([existing, recipe_df_new], ignore_index=True)
            # write combined with dedupe just in case
            combined = combined.drop_duplicates(subset=["id"], keep="first")
            _ensure_csv(recipe_csv, combined)
        else:
            _ensure_csv(recipe_csv, recipe_df_new)

    if not ing_df_new.empty:
        if ingredients_csv.exists():
            existing = _read_csv_if_exists(ingredients_csv)
            combined = pd.concat([existing, ing_df_new], ignore_index=True)
            # ingredients may have no unique id; drop exact duplicates
            combined = combined.drop_duplicates()
            _ensure_csv(ingredients_csv, combined)
        else:
            _ensure_csv(ingredients_csv, ing_df_new)

    if not steps_df_new.empty:
        if steps_csv.exists():
            existing = _read_csv_if_exists(steps_csv)
            combined = pd.concat([existing, steps_df_new], ignore_index=True)
            combined = combined.drop_duplicates()
            _ensure_csv(steps_csv, combined)
        else:
            _ensure_csv(steps_csv, steps_df_new)

    # interactions: overwrite or append (we append validated unique ids)
    if not inter_df_new.empty:
        if interactions_csv.exists():
            existing = _read_csv_if_exists(interactions_csv)
            combined = pd.concat([existing, inter_df_new], ignore_index=True)
            combined = combined.drop_duplicates(subset=["id"], keep="first")
            _ensure_csv(interactions_csv, combined)
        else:
            _ensure_csv(interactions_csv, inter_df_new)

    # Bad/duplicate writes (overwrite for each run so they reflect current run)
    if bad_recipes:
        # save as JSON with reasons
        bad_recipes_path = BAD_DATA_DIR / "bad_recipes.json"
        br = [{"record": r["record"], "reasons": r["reasons"]} for r in bad_recipes]
        write_json(bad_recipes_path, br)

    if dup_recipes:
        dup_recipes_df = pd.json_normalize([{"id": _to_str(r["record"].get("id", r["record"].get("slug",""))), "record": r["record"], "reasons": ",".join(r["reasons"])} for r in dup_recipes])
        _ensure_csv(BAD_DATA_DIR / "duplicate_recipes.csv", dup_recipes_df)

    if bad_interactions:
        bad_it_df = pd.json_normalize([{"id": _to_str(r["record"].get("id","")), "record": r["record"], "reasons": ",".join(r["reasons"])} for r in bad_interactions])
        _ensure_csv(BAD_DATA_DIR / "bad_interactions.csv", bad_it_df)

    if dup_interactions:
        dup_it_df = pd.json_normalize([{"id": _to_str(r["record"].get("id","")), "record": r["record"], "reasons": ",".join(r["reasons"])} for r in dup_interactions])
        _ensure_csv(BAD_DATA_DIR / "duplicate_interactions.csv", dup_it_df)

    # Always write manifest to ETL_DIR
    manifest_path = Path(ETL_DIR) / "_etl_manifest.json"
    write_json(manifest_path, summary)

    print("ETL Summary:", summary)
    return summary


if __name__ == "__main__":
    print("Running normalize_recipes()")
    normalize_recipes()
