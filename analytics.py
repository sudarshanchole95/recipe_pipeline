"""
analytics.py

Advanced Analytics Module for Recipe Pipeline.
- Calculates derived metrics (Engagement Rate, ROI, Complexity).
- Generates professional visualization charts.
- Prepends a Data Quality Summary (from validation_results.json) after Executive Summary.
- Outputs:
  - output/analytics/charts/*.png
  - output/analytics/analytics_summary.md
"""

from __future__ import annotations
import os
import json
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, Any

# Optional visualization niceties
try:
    import seaborn as sns
    sns.set_theme(style="whitegrid")
    HAS_SEABORN = True
except Exception:
    HAS_SEABORN = False

# Paths (adjust if your structure differs)
INPUT_DIR = os.path.join("output", "etl")
VALIDATION_JSON = os.path.join("output", "validation", "validation_results.json")
ANALYTICS_DIR = os.path.join("output", "analytics")
CHARTS_DIR = os.path.join(ANALYTICS_DIR, "charts")
MD_REPORT = os.path.join(ANALYTICS_DIR, "analytics_summary.md")

# Possible CSV names
RECIPE_CSV = os.path.join(INPUT_DIR, "recipe.csv")
RECIPE_CSV_ALT = os.path.join(INPUT_DIR, "recipes.csv")
INGREDIENTS_CSV = os.path.join(INPUT_DIR, "ingredients.csv")
STEPS_CSV = os.path.join(INPUT_DIR, "steps.csv")
INTERACTIONS_CSV = os.path.join(INPUT_DIR, "interactions.csv")


# -------------------------
# Utilities
# -------------------------
def ensure_dirs():
    os.makedirs(CHARTS_DIR, exist_ok=True)
    os.makedirs(ANALYTICS_DIR, exist_ok=True)


def safe_load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def percent(part: int, whole: int) -> float:
    if whole <= 0:
        return 0.0
    return 100.0 * (part / whole)


# -------------------------
# Load Data
# -------------------------
def load_data():
    # choose which recipe csv exists
    recipe_path = RECIPE_CSV if os.path.exists(RECIPE_CSV) else (RECIPE_CSV_ALT if os.path.exists(RECIPE_CSV_ALT) else None)
    if recipe_path is None:
        raise FileNotFoundError("recipe.csv (or recipes.csv) not found in output/etl. Run ETL first.")
    df_recipe = pd.read_csv(recipe_path).fillna("")
    df_ing = pd.read_csv(INGREDIENTS_CSV).fillna("") if os.path.exists(INGREDIENTS_CSV) else pd.DataFrame()
    df_steps = pd.read_csv(STEPS_CSV).fillna("") if os.path.exists(STEPS_CSV) else pd.DataFrame()
    df_inter = pd.read_csv(INTERACTIONS_CSV).fillna("") if os.path.exists(INTERACTIONS_CSV) else pd.DataFrame()

    # normalize column names
    df_recipe.columns = df_recipe.columns.str.strip()
    df_ing.columns = df_ing.columns.str.strip()
    df_steps.columns = df_steps.columns.str.strip()
    df_inter.columns = df_inter.columns.str.strip()

    # numeric conversions (defensive)
    for col in ["prep_time_min", "cook_time_min", "total_time_min", "servings"]:
        if col in df_recipe.columns:
            df_recipe[col] = pd.to_numeric(df_recipe[col], errors="coerce").fillna(0)

    # parse metadata_json if present
    if "metadata_json" in df_inter.columns:
        df_inter["metadata"] = df_inter["metadata_json"].apply(lambda x: json.loads(x) if x and isinstance(x, str) else {})
    else:
        df_inter["metadata"] = [{} for _ in range(len(df_inter))]

    return df_recipe, df_ing, df_steps, df_inter


# -------------------------
# Validation Summary handling
# -------------------------
def build_validation_summary(validation_path: str):
    """
    Loads validation_results.json and returns a dict suitable for embedding into the markdown,
    plus draws a bar chart of issue counts.
    """
    v = safe_load_json(validation_path)
    # If empty, make a "no issues" structure
    if not v:
        return {
            "total_recipes": 0,
            "checks": {},
            "total_issues": 0,
            "overall_score": 100.0
        }

    total_recipes = v.get("total_recipes", v.get("total_recipes", 0))
    checks = v.get("checks", {})

    # Flatten to list of categories with counts and severity emoji
    items = []
    total_issue_count = 0
    for key, meta in checks.items():
        count = meta.get("count", 0)
        severity = meta.get("severity", "Low")
        emoji = meta.get("severity_emoji", "🟢")
        details = meta.get("details", [])
        pct = percent(count, total_recipes) if total_recipes > 0 else 0.0
        items.append({
            "key": key,
            "count": count,
            "pct": pct,
            "severity": severity,
            "emoji": emoji,
            "details": details
        })
        total_issue_count += count

    # Compute an overall Data Quality Score: simpler, interpretable
    # score = 100 - (total_issue_count / max(1,total_recipes)) * 100  (clamp to 0..100)
    issue_rate = total_issue_count / max(1, total_recipes)
    quality_score = max(0.0, min(100.0, 100.0 - issue_rate * 100.0))

    # Draw bar chart (only categories with count>0 shown prominently, else show all with small bars)
    chart_path = os.path.join(CHARTS_DIR, "validation_issues.png")
    try:
        labels = [it["key"] for it in items]
        values = [it["count"] for it in items]
        plt.figure(figsize=(10, 5))
        if HAS_SEABORN:
            sns.barplot(x=values, y=labels, palette="rocket")
        else:
            plt.barh(range(len(labels)), values, color="tab:orange")
            plt.yticks(range(len(labels)), labels)
        plt.title("Validation Issues (counts by category)")
        plt.xlabel("Count")
        plt.tight_layout()
        plt.savefig(chart_path)
        plt.close()
    except Exception as e:
        # if chart fails, still continue
        chart_path = None

    summary = {
        "total_recipes": total_recipes,
        "checks_list": items,
        "total_issues": total_issue_count,
        "overall_score": round(quality_score, 2),
        "chart_path": chart_path
    }
    return summary


# -------------------------
# Analytics core (existing functionality simplified & reused)
# -------------------------
def compute_interaction_aggregates(df_inter: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df_inter.groupby("recipe_id")
        .agg(
            views=("type", lambda x: (x == "view").sum()),
            likes=("type", lambda x: (x == "like").sum()),
            attempts=("type", lambda x: (x == "attempt").sum()),
            events_count=("type", "count"),
            unique_users=("user_id", lambda x: x.nunique())
        )
        .reset_index()
    )
    ratings = df_inter[df_inter["type"] == "rating"].copy()
    if not ratings.empty:
        def extract_rating(m):
            if not isinstance(m, dict):
                return np.nan
            for k in ("rating", "rating_value", "stars", "score"):
                if k in m:
                    try:
                        return float(m[k])
                    except Exception:
                        return np.nan
            return np.nan
        ratings["rating_value"] = ratings["metadata"].apply(extract_rating)
        avg_rating = ratings.groupby("recipe_id").agg(avg_rating=("rating_value", "mean")).reset_index()
    else:
        avg_rating = pd.DataFrame(columns=["recipe_id", "avg_rating"])
    agg = agg.merge(avg_rating, on="recipe_id", how="left")
    agg["avg_rating"] = pd.to_numeric(agg.get("avg_rating", 0)).fillna(0)
    return agg


def compute_recipe_metrics(df_recipe: pd.DataFrame, df_ing: pd.DataFrame, inter_agg: pd.DataFrame) -> pd.DataFrame:
    ing_count = df_ing.groupby("recipe_id").size().reset_index(name="num_ingredients") if not df_ing.empty else pd.DataFrame(columns=["recipe_id","num_ingredients"])
    df = df_recipe.copy()
    if "id" not in df.columns:
        df["id"] = df.index.astype(str)
    df = df.merge(ing_count, left_on="id", right_on="recipe_id", how="left").drop(columns=["recipe_id"], errors="ignore")
    df["num_ingredients"] = pd.to_numeric(df.get("num_ingredients", 0), errors="coerce").fillna(0).astype(int)
    df = df.merge(inter_agg, left_on="id", right_on="recipe_id", how="left").drop(columns=["recipe_id"], errors="ignore")

    # fill missing numeric interaction fields
    for c in ["views", "likes", "attempts", "events_count", "unique_users"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        else:
            df[c] = 0
    df["avg_rating"] = pd.to_numeric(df.get("avg_rating", 0), errors="coerce").fillna(0)

    df["engagement_rate"] = df.apply(lambda r: (r["likes"] / r["views"]) if r["views"] > 0 else 0.0, axis=1)
    df["total_time_min"] = pd.to_numeric(df.get("total_time_min", 0), errors="coerce").fillna(0)
    df["complexity_score"] = (df["total_time_min"] / 10) + df["num_ingredients"]
    df["roi"] = df.apply(lambda r: (r["avg_rating"] / r["total_time_min"]) if r["total_time_min"] > 0 else 0.0, axis=1)
    return df


# -------------------------
# Reporting: write markdown with Data Quality Summary after Executive Summary
# -------------------------
def write_markdown_report(validation_summary: dict, insights: dict, df_metrics: pd.DataFrame):
    lines = []
    # Title + Exec Summary
    lines.append("# Analytics Summary\n")
    lines.append("Generated by analytics.py\n")
    lines.append("\n## Executive Summary\n")
    lines.append(insights.get("executive_summary", "This report summarizes recipe performance.") + "\n")

    # --- Data Quality Summary (NEW, placed after Executive Summary) ---
    lines.append("## 🛡 Data Quality Summary\n")
    total_recipes = validation_summary.get("total_recipes", 0)
    overall = validation_summary.get("overall_score", 100.0)
    lines.append(f"- **Overall Data Quality Score:** **{overall:.2f}%**\n")
    lines.append(f"- **Total recipes checked:** {total_recipes}\n")
    lines.append(f"- **Total issues detected:** {validation_summary.get('total_issues', 0)}\n")
    lines.append("\n### Issues by Category\n")
    for it in validation_summary.get("checks_list", []):
        lines.append(f"- {it['emoji']} **{it['key']}** — {it['count']} ({it['pct']:.2f}% of recipes) — Severity: {it['severity']}")
        # optionally include details (first 3)
        if it.get("details"):
            sample = it["details"][:3]
            lines.append(f"  - Details (sample): {sample}")
    # embed chart if exists (use relative path)
    chart_path = validation_summary.get("chart_path")
    if chart_path:
        # Use path relative to analytics dir for nicer rendering on GitHub pages if needed
        rel = os.path.relpath(chart_path, ANALYTICS_DIR).replace("\\", "/")
        lines.append("\n![Validation Issues](" + "charts/" + os.path.basename(chart_path) + ")\n")

    # --- Key Insights ---
    lines.append("\n## Key Insights\n")
    for i, ins in enumerate(insights.get("insights", []), start=1):
        lines.append(f"### Insight {i}: {ins.get('title')}\n")
        lines.append(ins.get("text", "") + "\n")

    # Recommendations
    lines.append("\n## Recommendations\n")
    for rec in insights.get("recommendations", []):
        lines.append(f"- {rec}\n")

    # Quick stats
    lines.append("\n## Quick Statistics\n")
    total_recipes_analyzed = len(df_metrics)
    events_total = int(df_metrics["events_count"].sum()) if "events_count" in df_metrics.columns else 0
    avg_engagement = df_metrics["engagement_rate"].mean() if "engagement_rate" in df_metrics.columns else 0
    lines.append(f"- Total recipes analyzed: **{total_recipes_analyzed}**")
    lines.append(f"- Total interactions analyzed: **{events_total}**")
    lines.append(f"- Average engagement rate: **{avg_engagement:.2%}**\n")

    # Write file
    with open(MD_REPORT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"✔ Markdown report written to: {MD_REPORT}")


# -------------------------
# Top-level controller
# -------------------------
def run_analytics():
    ensure_dirs()
    # load validation summary first (so chart is available to embed)
    validation_summary = build_validation_summary(VALIDATION_JSON)

    # load data
    df_recipe, df_ing, df_steps, df_inter = load_data()
    # compute aggregates & metrics
    inter_agg = compute_interaction_aggregates(df_inter)
    df_metrics = compute_recipe_metrics(df_recipe, df_ing, inter_agg)

    # Build insights (you can expand these; using a few examples)
    insights = {
        "executive_summary": "This report computes engagement, ROI, complexity and other derived metrics to surface content strategy recommendations.",
        "insights": [
            {"title": "Most common ingredients", "text": "See the Top Ingredients chart for the most-used pantry items."},
            {"title": "Difficulty distribution", "text": "Distribution by difficulty provides insight into audience preference."},
            {"title": "Prep time vs rating", "text": "Scatter plot inspects whether longer prep correlates with higher ratings."},
        ],
        "recommendations": [
            "Promote quick-win (low complexity, high ROI) recipes.",
            "Augment metadata for recipes missing 'occasion' or 'nutrition_groups'.",
            "Prioritize social promotion for high-engagement recipes."
        ]
    }

    # Generate a few charts (top ingredients, top engagement, correlation) - reuse your previous functions or simple plotting
    # Top ingredients chart (safe)
    if not df_ing.empty and "ingredient_name" in df_ing.columns:
        top = df_ing["ingredient_name"].value_counts().head(15)
        path = os.path.join(CHARTS_DIR, "top_ingredients.png")
        plt.figure(figsize=(9, 6))
        if HAS_SEABORN:
            sns.barplot(x=top.values, y=list(top.index))
        else:
            plt.barh(list(range(len(top.index))), top.values, color="tab:blue")
            plt.yticks(range(len(top.index)), list(top.index))
        plt.title("Top 15 Ingredients")
        plt.xlabel("Count")
        plt.tight_layout()
        plt.savefig(path); plt.close()

    # Top engagement
    if "engagement_rate" in df_metrics.columns:
        top_eng = df_metrics.sort_values("engagement_rate", ascending=False).head(10)
        path = os.path.join(CHARTS_DIR, "top_engagement_rate.png")
        plt.figure(figsize=(9, 6))
        if HAS_SEABORN:
            sns.barplot(x=top_eng["engagement_rate"], y=top_eng["title"])
        else:
            plt.barh(top_eng["title"], top_eng["engagement_rate"], color="tab:green")
        plt.title("Top 10 Recipes by Engagement Rate")
        plt.xlabel("Engagement Rate (likes/views)")
        plt.tight_layout(); plt.savefig(path); plt.close()

    # Correlation heatmap (numeric)
    numeric_cols = [c for c in ["prep_time_min","cook_time_min","total_time_min","num_ingredients","views","likes","avg_rating","engagement_rate","roi","complexity_score"] if c in df_metrics.columns]
    if numeric_cols:
        nm = df_metrics[numeric_cols].fillna(0)
        path = os.path.join(CHARTS_DIR, "correlation_heatmap.png")
        plt.figure(figsize=(10, 8))
        if HAS_SEABORN:
            sns.heatmap(nm.corr(), annot=True, fmt=".2f", cmap="RdBu_r", center=0)
        else:
            plt.imshow(nm.corr(), cmap="coolwarm", aspect="auto")
            plt.colorbar()
            plt.xticks(range(len(nm.corr().columns)), nm.corr().columns, rotation=45, ha="right")
            plt.yticks(range(len(nm.corr().columns)), nm.corr().columns)
        plt.title("Correlation Heatmap")
        plt.tight_layout(); plt.savefig(path); plt.close()

    # Write the markdown report (includes validation summary at top)
    write_markdown_report(validation_summary, insights, df_metrics)
    print("✔ Analytics complete. Charts saved to:", CHARTS_DIR)


if __name__ == "__main__":
    run_analytics()
