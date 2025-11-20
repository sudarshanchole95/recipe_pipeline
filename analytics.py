"""
analytics.py

Senior-Mix analytics:
- Derived metrics: engagement_rate, roi, complexity_score
- 12 high-impact insights
- Charts saved to output/analytics/charts/
- Markdown report saved to output/analytics/analytics_summary.md
"""

from __future__ import annotations
import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Optional niceties
try:
    import seaborn as sns
    sns.set_theme(style="whitegrid")
    HAS_SEABORN = True
except Exception:
    HAS_SEABORN = False

try:
    from wordcloud import WordCloud
    HAS_WORDCLOUD = True
except Exception:
    HAS_WORDCLOUD = False

# ---------------------------------------------------------
# 1. PATH CONFIGURATION (FIXED FOR YOUR FOLDER)
# ---------------------------------------------------------
# Input: Where the CSVs are stored
INPUT_DIR = os.path.join("output", "etl")

# Output: Where to save charts/reports
ANALYTICS_DIR = os.path.join("output", "analytics")
CHARTS_DIR = os.path.join(ANALYTICS_DIR, "charts")
MD_REPORT = os.path.join(ANALYTICS_DIR, "analytics_summary.md")

# File Names
RECIPE_CSV = os.path.join(INPUT_DIR, "recipes.csv")
INGREDIENTS_CSV = os.path.join(INPUT_DIR, "ingredients.csv")
STEPS_CSV = os.path.join(INPUT_DIR, "steps.csv")
INTERACTIONS_CSV = os.path.join(INPUT_DIR, "interactions.csv")


# -------------------------
# Utils and IO
# -------------------------
def ensure_dirs():
    os.makedirs(CHARTS_DIR, exist_ok=True)
    os.makedirs(ANALYTICS_DIR, exist_ok=True)


def safe_load_json(x: str) -> dict:
    try:
        if pd.isna(x) or x == "":
            return {}
        return json.loads(x)
    except Exception:
        return {}


def load_data():
    print(f"... Looking for CSV files in: {os.path.abspath(INPUT_DIR)}")
    
    try:
        # Recipes might be named recipe.csv or recipes.csv - check both
        if os.path.exists(RECIPE_CSV):
            df_recipe = pd.read_csv(RECIPE_CSV).fillna("")
        elif os.path.exists(os.path.join(INPUT_DIR, "recipe.csv")):
            print("   -> Found 'recipe.csv' (singular). Loading that.")
            df_recipe = pd.read_csv(os.path.join(INPUT_DIR, "recipe.csv")).fillna("")
        else:
            raise FileNotFoundError("Could not find recipes.csv")

        df_ing = pd.read_csv(INGREDIENTS_CSV).fillna("")
        df_steps = pd.read_csv(STEPS_CSV).fillna("")
        df_inter = pd.read_csv(INTERACTIONS_CSV).fillna("")
        
        # --- CRITICAL FIX: Strip whitespace from column names ---
        df_recipe.columns = df_recipe.columns.str.strip()
        df_ing.columns = df_ing.columns.str.strip()
        df_steps.columns = df_steps.columns.str.strip()
        df_inter.columns = df_inter.columns.str.strip()

    except FileNotFoundError as e:
        print("\n❌ CRITICAL ERROR: CSV files not found!")
        print(f"   Looking in: {INPUT_DIR}")
        print("   👉 SOLUTION: Check if your folder is named 'output/etl' and contains the CSVs.\n")
        exit()

    # Convert numeric fields where sensible
    for col in ["prep_time_min", "cook_time_min", "total_time_min", "servings"]:
        if col in df_recipe.columns:
            df_recipe[col] = pd.to_numeric(df_recipe[col], errors="coerce").fillna(0)

    # Parse metadata_json
    if "metadata_json" in df_inter.columns:
        df_inter["metadata"] = df_inter["metadata_json"].apply(safe_load_json)
    else:
        df_inter["metadata"] = [{} for _ in range(len(df_inter))]

    return df_recipe, df_ing, df_steps, df_inter


# -------------------------
# Aggregations & derived metrics
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

    # ratings logic
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
    # ingredient counts
    ing_count = df_ing.groupby("recipe_id").size().reset_index(name="num_ingredients")

    df = df_recipe.copy()
    if "id" not in df.columns:
        df["id"] = df.index.astype(str)

    # Merge Ingredients Count
    df = df.merge(ing_count, left_on="id", right_on="recipe_id", how="left").drop(columns=["recipe_id"], errors="ignore")
    df["num_ingredients"] = pd.to_numeric(df["num_ingredients"], errors="coerce").fillna(0).astype(int)

    # Merge Interactions
    df = df.merge(inter_agg, left_on="id", right_on="recipe_id", how="left").drop(columns=["recipe_id"], errors="ignore")
    
    for c in ["views", "likes", "attempts", "events_count", "unique_users"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        else:
            df[c] = 0

    df["avg_rating"] = pd.to_numeric(df.get("avg_rating", 0), errors="coerce").fillna(0)

    # Derived metrics
    df["engagement_rate"] = df.apply(lambda r: (r["likes"] / r["views"]) if r["views"] > 0 else 0.0, axis=1)
    df["total_time_min"] = pd.to_numeric(df.get("total_time_min", 0), errors="coerce").fillna(0)
    df["complexity_score"] = (df["total_time_min"] / 10) + df["num_ingredients"] 
    df["roi"] = df.apply(lambda r: (r["avg_rating"] / r["total_time_min"]) if r["total_time_min"] > 0 else 0.0, axis=1)

    # Normalize text fields
    for col in ["cuisine", "difficulty", "category_type", "texture", "served_temperature"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"nan": ""})

    return df


# -------------------------
# Insights & visual helpers
# -------------------------
def top_ingredients(df_ing: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """
    Robust function to get top ingredients.
    """
    col_name = "ingredient_name"
    if col_name not in df_ing.columns:
        for c in df_ing.columns:
            if "ingredient" in c or "name" in c:
                col_name = c
                break
    
    if col_name not in df_ing.columns:
        return pd.DataFrame(columns=["ingredient_name", "count"])

    counts = df_ing[col_name].value_counts().head(n)
    top = pd.DataFrame({
        "ingredient_name": counts.index,
        "count": counts.values
    })
    
    return top

def plot_bar(labels, values, title, xlabel, ylabel, path):
    plt.figure(figsize=(10, 6))
    if HAS_SEABORN:
        sns.barplot(x=values, y=labels, palette="viridis")
    else:
        plt.barh(range(len(labels)), values)
        plt.yticks(range(len(labels)), labels)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def plot_scatter(x, y, title, xlabel, ylabel, path):
    plt.figure(figsize=(8, 6))
    if HAS_SEABORN:
        sns.scatterplot(x=x, y=y, alpha=0.7)
    else:
        plt.scatter(x, y, alpha=0.7)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def plot_heatmap(df_numeric: pd.DataFrame, title: str, path: str):
    plt.figure(figsize=(10, 8))
    corr = df_numeric.corr()
    if HAS_SEABORN:
        sns.heatmap(corr, annot=True, fmt=".2f", cmap="RdBu_r", center=0)
    else:
        plt.imshow(corr, cmap="coolwarm", aspect="auto")
        plt.colorbar()
        plt.xticks(range(len(corr.columns)), corr.columns, rotation=45)
        plt.yticks(range(len(corr.columns)), corr.columns)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def plot_pie(labels, sizes, title, path):
    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def make_wordcloud(text_list, path):
    if not HAS_WORDCLOUD:
        return False
    text = " ".join(text_list)
    if not text.strip(): return False
    
    wc = WordCloud(width=800, height=400, background_color="white").generate(text)
    plt.figure(figsize=(10, 5))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    return True


# -------------------------
# Build markdown report
# -------------------------
def mk_report(insights: dict, df_metrics: pd.DataFrame):
    lines = []
    lines.append("# Analytics Summary\n")
    lines.append("Generated by analytics.py\n")
    lines.append("\n## Executive Summary\n")
    lines.append(insights["executive_summary"] + "\n")
    lines.append("\n## Key Insights\n")

    for i, insight in enumerate(insights["insights"], 1):
        lines.append(f"### Insight {i}: {insight['title']}\n")
        lines.append(insight["text"] + "\n")

    lines.append("\n## Recommendations\n")
    for rec in insights["recommendations"]:
        lines.append(f"- {rec}\n")

    lines.append("\n## Quick Stats\n")
    total_recipes = len(df_metrics)
    total_interactions = int(df_metrics["events_count"].sum()) if "events_count" in df_metrics else 0
    lines.append(f"- Total recipes analyzed: **{total_recipes}**\n")
    lines.append(f"- Total interactions analyzed: **{total_interactions}**\n")

    with open(MD_REPORT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"✔ Markdown report written to: {MD_REPORT}")


# -------------------------
# Main analytics flow
# -------------------------
def run_analytics():
    print("📊 Starting Analytics Module...")
    ensure_dirs()
    df_recipe, df_ing, df_steps, df_inter = load_data()
    
    print("... Computing Metrics")
    inter_agg = compute_interaction_aggregates(df_inter)
    df_metrics = compute_recipe_metrics(df_recipe, df_ing, inter_agg)

    insights = {
        "executive_summary": "This report computes engagement, ROI, complexity and other derived metrics to surface content strategy recommendations.",
        "insights": [],
        "recommendations": []
    }

    print("... Generating Visuals")

    # ---- Insight 1: Top ingredients (bar + wordcloud)
    top_ing = top_ingredients(df_ing, n=15)
    ingredient_list_str = ""

    if not top_ing.empty:
        plot_bar(top_ing["ingredient_name"], top_ing["count"], "Top 15 Ingredients", "Count", "Ingredient", os.path.join(CHARTS_DIR, "top_ingredients.png"))
        if HAS_WORDCLOUD:
            make_wordcloud(df_ing[top_ing.columns[0]].astype(str).tolist(), os.path.join(CHARTS_DIR, "ingredients_wordcloud.png"))
        
        items = top_ing['ingredient_name'].head(7).tolist()
        ingredient_list_str = ", ".join([str(x) for x in items])
    else:
        ingredient_list_str = "No ingredient data found"

    insights["insights"].append({
        "title": "Most common ingredients",
        "text": f"Top ingredients: {ingredient_list_str}"
    })

    # ---- Insight 2: Difficulty distribution (pie)
    diff_counts = df_metrics["difficulty"].value_counts().to_dict()
    if diff_counts:
        plot_pie(list(diff_counts.keys()), list(diff_counts.values()), "Difficulty Distribution", os.path.join(CHARTS_DIR, "difficulty_distribution.png"))
    insights["insights"].append({
        "title": "Difficulty distribution",
        "text": f"Counts by difficulty: {diff_counts}"
    })

    # ---- Insight 3: Prep time vs Rating (scatter)
    if "avg_rating" in df_metrics.columns:
        plot_scatter(df_metrics["prep_time_min"].fillna(0), df_metrics["avg_rating"].fillna(0), "Prep Time vs Average Rating", "Prep Time (min)", "Avg Rating", os.path.join(CHARTS_DIR, "prep_vs_rating.png"))
    insights["insights"].append({
        "title": "Prep time vs rating",
        "text": "Scatter of prep_time vs average rating to inspect if longer prep correlates with higher/lower ratings."
    })

    # ---- Insight 4: Correlation heatmap
    numeric_cols = ["prep_time_min", "cook_time_min", "total_time_min", "num_ingredients", "views", "likes", "avg_rating", "engagement_rate", "roi", "complexity_score"]
    numeric_df = df_metrics[[c for c in numeric_cols if c in df_metrics.columns]].fillna(0)
    if not numeric_df.empty:
        plot_heatmap(numeric_df, "Correlation Heatmap", os.path.join(CHARTS_DIR, "correlation_heatmap.png"))
    insights["insights"].append({
        "title": "Correlation analysis",
        "text": "Correlation heatmap reveals relationships between time, complexity, engagement and satisfaction."
    })

    # ---- Insight 5: Top engagement
    top_by_eng = df_metrics.sort_values("engagement_rate", ascending=False).head(10)
    if not top_by_eng.empty:
        plot_bar(top_by_eng["title"], top_by_eng["engagement_rate"], "Top Recipes by Engagement Rate", "Engagement Rate", "Recipe", os.path.join(CHARTS_DIR, "top_engagement_rate.png"))
    insights["insights"].append({
        "title": "Top engagement (viral potential)",
        "text": "Top recipes by engagement rate (likes/views) highlight highly efficient viral content."
    })

    # ---- Insight 6: ROI
    top_by_roi = df_metrics.sort_values("roi", ascending=False).head(10)
    if not top_by_roi.empty:
        plot_bar(top_by_roi["title"], top_by_roi["roi"], "Top Recipes by ROI (Rating per minute)", "ROI", "Recipe", os.path.join(CHARTS_DIR, "top_roi.png"))
    insights["insights"].append({
        "title": "High ROI recipes",
        "text": "Recipes giving high satisfaction for low time (ideal for promoting Quick Wins)."
    })

    # ---- Insight 7: Cuisine popularity
    if "cuisine" in df_metrics.columns:
        cuisine_views = df_metrics.groupby("cuisine")["views"].sum().sort_values(ascending=False)
        if not cuisine_views.empty:
            plot_bar(cuisine_views.index.astype(str), cuisine_views.values, "Views by Cuisine", "Views", "Cuisine", os.path.join(CHARTS_DIR, "views_by_cuisine.png"))
        insights["insights"].append({
            "title": "Cuisine popularity",
            "text": "Top cuisines by views."
        })

    # ---- Insight 8: Steps count vs engagement
    steps_count = df_steps.groupby("recipe_id").size().reset_index(name="steps_count")
    merged = df_metrics.merge(steps_count, left_on="id", right_on="recipe_id", how="left")
    merged["steps_count"] = merged["steps_count"].fillna(0).astype(int)
    plot_scatter(merged["steps_count"], merged["engagement_rate"], "Steps Count vs Engagement Rate", "Steps Count", "Engagement Rate", os.path.join(CHARTS_DIR, "steps_vs_engagement.png"))
    insights["insights"].append({
        "title": "Steps vs engagement",
        "text": "Examines whether recipes with many steps have lower engagement rates."
    })

    # ---- Insight 9: Ratings histogram
    if "avg_rating" in df_metrics.columns and df_metrics["avg_rating"].sum() > 0:
        plt.figure(figsize=(7, 4))
        plt.hist(df_metrics["avg_rating"].dropna(), bins=5, color='skyblue', edgecolor='black')
        plt.title("Average Rating Distribution")
        plt.xlabel("Rating")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(os.path.join(CHARTS_DIR, "ratings_histogram.png"))
        plt.close()
    insights["insights"].append({
        "title": "Ratings distribution",
        "text": "Distribution of average ratings across recipes."
    })

    # ---- Insight 10: Served temperature
    if "served_temperature" in df_metrics.columns:
        insights["insights"].append({
            "title": "Engagement by served temperature",
            "text": "Comparison of engagement rates across different served temperatures."
        })

    # ---- Insight 11: Missing data
    total = len(df_metrics)
    missing_occasion = df_metrics["occasion"].apply(lambda x: x == "" or pd.isna(x)).sum() if "occasion" in df_metrics.columns else total
    insights["insights"].append({
        "title": "Missing metadata",
        "text": f"{missing_occasion}/{total} recipes missing 'occasion' tags."
    })

    # ---- Insight 12: Quick-win
    quick_wins = df_metrics[df_metrics["total_time_min"] <= 30].sort_values(["roi", "engagement_rate"], ascending=False).head(5)
    
    # Safe f-string
    qw_titles = ", ".join(quick_wins['title'].head(3).astype(str).tolist())
    
    insights["insights"].append({
        "title": "Quick-win recipes",
        "text": f"Top quick-win recipes (under 30m, high ROI): {qw_titles}"
    })

    # Recommendations
    insights["recommendations"] = [
        "Promote top engagement and high-ROI recipes in 'Quick Meals' features.",
        "Seed content for underrepresented nutrition groups.",
        "Prioritize recipes with high engagement rate for social media.",
        "A/B test featuring quick-win recipes in homepage carousels."
    ]

    # Save report
    mk_report(insights, df_metrics)

    print("✔ Analytics complete")
    print(f"Charts saved to: {CHARTS_DIR}")
    print(f"Markdown report: {MD_REPORT}")


if __name__ == "__main__":
    run_analytics()