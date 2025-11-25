# validation.py
"""


Outputs:
 - VALIDATION_DIR/validation_report.md         (human readable)
 - VALIDATION_DIR/validation_results.json      (machine readable)
 - VALIDATION_DIR/validation_summary.png       (color bar chart)
 - BAD_DATA_DIR/bad_steps.csv
 - BAD_DATA_DIR/bad_ingredients.csv
 - BAD_DATA_DIR/orphan_interactions.csv
 - BAD_DATA_DIR/dup_recipes.csv
"""
from pathlib import Path
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt

from config import ETL_DIR, VALIDATION_DIR, BAD_DATA_DIR
from utils import ensure_dirs, small_report

# CSV paths
RECIPE_CSV = ETL_DIR / "recipe.csv"
ING_CSV = ETL_DIR / "ingredients.csv"
STEPS_CSV = ETL_DIR / "steps.csv"
INTER_CSV = ETL_DIR / "interactions.csv"

# Severity mapping (Option A)
SEVERITY = {
    "critical": {"emoji": "🔥", "label": "Critical"},
    "high":     {"emoji": "⚠️", "label": "High"},
    "medium":   {"emoji": "🟡", "label": "Medium"},
    "low":      {"emoji": "🟢", "label": "Low"},
}

def _safe_read_csv(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            df = pd.read_csv(path)
            # strip whitespace from column names
            df.columns = df.columns.str.strip()
            return df
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def run_validation():
    ensure_dirs(VALIDATION_DIR, BAD_DATA_DIR)
    results = {
        "run_time": pd.Timestamp.now().isoformat(),
        "total_recipes": 0,
        "checks": {},
        "total_issues": 0
    }

    # load
    df_r = _safe_read_csv(RECIPE_CSV)
    df_ing = _safe_read_csv(ING_CSV)
    df_steps = _safe_read_csv(STEPS_CSV)
    df_inter = _safe_read_csv(INTER_CSV)

    results["total_recipes"] = int(len(df_r))

    # helper to register a check
    def register(name, count, severity, details=None):
        results["checks"][name] = {
            "count": int(count),
            "severity": SEVERITY[severity]["label"],
            "severity_emoji": SEVERITY[severity]["emoji"],
            "details": details or []
        }
        results["total_issues"] += int(count)

    # ---------- CHECK 1: Required columns (Critical) ----------
    required_recipe_cols = {"id", "title"}
    missing_cols = list(required_recipe_cols - set(df_r.columns))
    if missing_cols:
        register("missing_recipe_columns", len(missing_cols), "critical", details=missing_cols)
    else:
        register("missing_recipe_columns", 0, "low")

    # ---------- CHECK 2: Negative time values (High) ----------
    neg_counts = 0
    neg_examples = []
    for col in ["prep_time_min", "cook_time_min", "total_time_min"]:
        if col in df_r.columns:
            # coerce numeric then check negative
            series = pd.to_numeric(df_r[col], errors="coerce")
            invalid = df_r[series < 0]
            if not invalid.empty:
                cnt = len(invalid)
                neg_counts += cnt
                neg_examples.extend(invalid["id"].astype(str).head(5).tolist())
    register("negative_time_values", neg_counts, "high", details=neg_examples[:10])

    # ---------- CHECK 3: Steps numbering (Medium) ----------
    step_issues = pd.DataFrame()
    if not df_steps.empty and "step_number" in df_steps.columns:
        # step_number should be >= 1 and integer
        # coerce to numeric
        snum = pd.to_numeric(df_steps["step_number"], errors="coerce")
        bad_steps = df_steps[snum.isna() | (snum < 1)]
        if not bad_steps.empty:
            step_issues = bad_steps
            bad_steps.to_csv(BAD_DATA_DIR / "bad_steps.csv", index=False, encoding="utf-8")
            register("invalid_steps", len(bad_steps), "medium", details=bad_steps.head(5).to_dict(orient="records"))
        else:
            register("invalid_steps", 0, "low")
    else:
        register("invalid_steps", 0, "low")

    # ---------- CHECK 4: Ingredient names present (High) ----------
    ing_issues = pd.DataFrame()
    if not df_ing.empty:
        if "ingredient_name" in df_ing.columns:
            bad_ing = df_ing[df_ing["ingredient_name"].isna() | (df_ing["ingredient_name"].astype(str).str.strip() == "")]
            if not bad_ing.empty:
                bad_ing.to_csv(BAD_DATA_DIR / "bad_ingredients.csv", index=False, encoding="utf-8")
                register("invalid_ingredients", len(bad_ing), "high", details=bad_ing.head(5).to_dict(orient="records"))
            else:
                register("invalid_ingredients", 0, "low")
        else:
            register("invalid_ingredients", 0, "low")
    else:
        register("invalid_ingredients", 0, "low")

    # ---------- CHECK 5: Referential integrity - orphan interactions (Critical) ----------
    orphan_count = 0
    orphan_examples = []
    if not df_inter.empty:
        if "recipe_id" in df_inter.columns:
            recipe_ids = set(df_r["id"].astype(str).tolist())
            orphan_inter = df_inter[~df_inter["recipe_id"].astype(str).isin(recipe_ids)]
            if not orphan_inter.empty:
                orphan_count = len(orphan_inter)
                orphan_inter.to_csv(BAD_DATA_DIR / "orphan_interactions.csv", index=False, encoding="utf-8")
                orphan_examples = orphan_inter.head(5).to_dict(orient="records")
                register("orphan_interactions", orphan_count, "critical", details=orphan_examples)
            else:
                register("orphan_interactions", 0, "low")
        else:
            register("orphan_interactions", 0, "low")
    else:
        register("orphan_interactions", 0, "low")

    # ---------- CHECK 6: Duplicate recipe IDs (High) ----------
    dup_count = 0
    dup_examples = []
    if not df_r.empty:
        if "id" in df_r.columns:
            dup_r = df_r[df_r.duplicated(subset=["id"], keep=False)].copy()
            if not dup_r.empty:
                dup_count = len(dup_r)
                dup_r.to_csv(BAD_DATA_DIR / "dup_recipes.csv", index=False, encoding="utf-8")
                dup_examples = dup_r["id"].astype(str).unique().tolist()[:10]
                register("duplicate_recipe_ids", dup_count, "high", details=dup_examples)
            else:
                register("duplicate_recipe_ids", 0, "low")
        else:
            register("duplicate_recipe_ids", 0, "low")
    else:
        register("duplicate_recipe_ids", 0, "low")

    # ---------- Additional: invalid difficulty values (Low) ----------
    difficulty_issues = 0
    valid_difficulties = {"easy", "medium", "hard", "unknown", ""}
    if "difficulty" in df_r.columns:
        diffs = df_r["difficulty"].astype(str).str.strip().str.lower().fillna("")
        invalid_diff = df_r[~diffs.isin(valid_difficulties)]
        if not invalid_diff.empty:
            difficulty_issues = len(invalid_diff)
            register("invalid_difficulty_values", difficulty_issues, "low", details=invalid_diff.head(5).to_dict(orient="records"))
        else:
            register("invalid_difficulty_values", 0, "low")
    else:
        register("invalid_difficulty_values", 0, "low")

    # ---------- Finalize results and write files ----------
    # Write JSON machine-readable summary
    json_path = VALIDATION_DIR / "validation_results.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, ensure_ascii=False, indent=2)

    # Create human-readable Markdown report
    md_lines = []
    md_lines.append("# Validation Report")
    md_lines.append("")
    md_lines.append(f"**Run:** {results['run_time']}")
    md_lines.append(f"**Total recipes processed:** {results['total_recipes']}")
    md_lines.append(f"**Total issues found:** {results['total_issues']}")
    md_lines.append("")

    if results["total_issues"] == 0:
        md_lines.append("✔ No validation issues detected.")
    else:
        md_lines.append("## Issues by Severity")
        md_lines.append("")
        # Group by severity for display
        sev_order = ["critical", "high", "medium", "low"]
        for sev in sev_order:
            # collect checks of this severity
            for check_name, info in results["checks"].items():
                if info["severity"].lower() == SEVERITY[sev]["label"].lower():
                    emoji = info["severity_emoji"]
                    md_lines.append(f"- {emoji} **{info['severity']}** — `{check_name}` : {info['count']} issues")
                    # show a couple of details (ids / snippets) if available
                    if info.get("details"):
                        md_lines.append(f"  - Examples: `{json.dumps(info['details'][:3], ensure_ascii=False)}`")
        md_lines.append("")

    # Write the report using your util (small_report) for consistency
    report_path = VALIDATION_DIR / "validation_report.md"
    small_report(report_path, md_lines)

    # Create a small bar chart (severity aggregated counts)
    # Aggregate counts by severity
    severity_agg = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}
    for info in results["checks"].values():
        sev = info["severity"]
        severity_agg[sev] = severity_agg.get(sev, 0) + info["count"]

    # Plot chart
    labels = list(severity_agg.keys())
    counts = [severity_agg[l] for l in labels]
    colors = ["#D62828", "#FF9F1C", "#F4D35E", "#2EC4B6"]  # red, orange, yellow, green

    try:
        plt.figure(figsize=(6,3.5))
        bars = plt.bar(labels, counts, color=colors)
        plt.title("Validation Issues by Severity")
        plt.ylabel("Count")
        for bar in bars:
            h = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, h + 0.1, str(int(h)), ha="center", va="bottom")
        plt.tight_layout()
        plt.savefig(VALIDATION_DIR / "validation_summary.png", dpi=150)
        plt.close()
    except Exception:
        # If plotting fails, continue silently (chart is optional)
        pass

    # Return results summary
    return results

if __name__ == "__main__":
    out = run_validation()
    print("Validation run complete. Summary:")
    print(json.dumps(out, indent=2, ensure_ascii=False))
