"""
analytics.py

Advanced Analytics Module for Recipe Pipeline.
- Generates 10+ professional visualization charts using Seaborn.
- Calculates 14+ deep business insights (ROI, Viral Score, Conversion Rates).
- Integrates Data Quality checks into the final report.
"""

from __future__ import annotations
import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any

# --- Configuration & Aesthetics ---
try:
    # Set font_scale to 0.8 to ensure text fits, use a clean theme
    sns.set_theme(style="whitegrid", context="talk", font_scale=0.8, palette="viridis")
    HAS_SEABORN = True
except Exception:
    HAS_SEABORN = False
    print("Warning: Seaborn not found. Charts will look basic.")

# Paths
INPUT_DIR = os.path.join("output", "etl")
VALIDATION_JSON = os.path.join("output", "validation", "validation_results.json")
ANALYTICS_DIR = os.path.join("output", "analytics")
CHARTS_DIR = os.path.join(ANALYTICS_DIR, "charts")
MD_REPORT = os.path.join(ANALYTICS_DIR, "analytics_summary.md")

# Input Files
RECIPE_CSV = os.path.join(INPUT_DIR, "recipe.csv")
RECIPE_CSV_ALT = os.path.join(INPUT_DIR, "recipes.csv")
INGREDIENTS_CSV = os.path.join(INPUT_DIR, "ingredients.csv")
STEPS_CSV = os.path.join(INPUT_DIR, "steps.csv")
INTERACTIONS_CSV = os.path.join(INPUT_DIR, "interactions.csv")

def ensure_dirs():
    os.makedirs(CHARTS_DIR, exist_ok=True)
    os.makedirs(ANALYTICS_DIR, exist_ok=True)

def safe_load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}

# ---------------------------------------------------------
# 1. Data Loading
# ---------------------------------------------------------
def load_data():
    """Loads CSVs into Pandas DataFrames with type safety."""
    recipe_path = RECIPE_CSV if os.path.exists(RECIPE_CSV) else (RECIPE_CSV_ALT if os.path.exists(RECIPE_CSV_ALT) else None)
    if not recipe_path:
        raise FileNotFoundError("ETL output not found. Run 'python pipeline.py' first.")
    
    df_r = pd.read_csv(recipe_path).fillna("")
    df_i = pd.read_csv(INGREDIENTS_CSV).fillna("") if os.path.exists(INGREDIENTS_CSV) else pd.DataFrame()
    df_s = pd.read_csv(STEPS_CSV).fillna("") if os.path.exists(STEPS_CSV) else pd.DataFrame()
    df_int = pd.read_csv(INTERACTIONS_CSV).fillna("") if os.path.exists(INTERACTIONS_CSV) else pd.DataFrame()

    # Numeric Conversions
    for col in ["prep_time_min", "cook_time_min", "total_time_min"]:
        if col in df_r.columns:
            df_r[col] = pd.to_numeric(df_r[col], errors="coerce").fillna(0)
            
    return df_r, df_i, df_s, df_int

# ---------------------------------------------------------
# 2. Advanced Metric Calculation
# ---------------------------------------------------------
def calculate_advanced_metrics(df_r, df_i, df_int):
    """Derives complex KPIs: ROI, Engagement Score, Complexity, Conversion."""
    
    # 1. Interaction Aggregates
    int_agg = df_int.groupby("recipe_id").agg(
        views=("type", lambda x: (x == "view").sum()),
        likes=("type", lambda x: (x == "like").sum()),
        attempts=("type", lambda x: (x == "attempt").sum()),
        total_interactions=("type", "count")
    ).reset_index()

    # 2. Rating Calculation
    ratings = df_int[df_int["type"] == "rating"].copy()
    if not ratings.empty and "metadata_json" in ratings.columns:
        def get_rating(x):
            try:
                if isinstance(x, str) and "{" in x:
                    d = json.loads(x)
                    return float(d.get("rating", d.get("score", 0)))
                return 0.0
            except:
                return 0.0
        ratings["score"] = ratings["metadata_json"].apply(get_rating)
        avg_rating = ratings.groupby("recipe_id")["score"].mean().reset_index(name="avg_rating")
    else:
        avg_rating = pd.DataFrame(columns=["recipe_id", "avg_rating"])

    # 3. Ingredient Counts
    ing_counts = df_i.groupby("recipe_id").size().reset_index(name="ingredient_count")

    # 4. Merge into Master DataFrame
    if "id" not in df_r.columns: df_r["id"] = df_r.index # fallback
    df = df_r.merge(int_agg, left_on="id", right_on="recipe_id", how="left")
    df = df.merge(avg_rating, left_on="id", right_on="recipe_id", how="left")
    df = df.merge(ing_counts, left_on="id", right_on="recipe_id", how="left")
    
    # Fill NaNs
    cols_to_fill = ["views", "likes", "attempts", "total_interactions", "ingredient_count", "avg_rating"]
    for c in cols_to_fill:
        if c in df.columns:
            df[c] = df[c].fillna(0)
        else:
            df[c] = 0

    # 5. Derived KPIs
    # Engagement Rate: (Likes + Attempts) / Views
    df["engagement_rate"] = df.apply(lambda x: ((x["likes"] + x["attempts"]) / x["views"] * 100) if x["views"] > 0 else 0, axis=1)
    
    # Conversion Rate: Attempts / Views
    df["conversion_rate"] = df.apply(lambda x: (x["attempts"] / x["views"] * 100) if x["views"] > 0 else 0, axis=1)

    # Complexity Score: Weighted mix of time (60%) and ingredients (40%)
    df["complexity_score"] = (
        (df["total_time_min"].clip(upper=120) / 120) * 60 + 
        (df["ingredient_count"].clip(upper=20) / 20) * 40
    )

    # ROI (Return on Investment): Rating points per minute of cooking
    df["roi_score"] = df.apply(lambda x: (x["avg_rating"] / x["total_time_min"]) if x["total_time_min"] > 0 else 0, axis=1)

    return df

# ---------------------------------------------------------
# 3. Aesthetic Chart Generation (10 Charts)
# ---------------------------------------------------------
def generate_charts(df, df_i, df_int):
    print("   Generating 10+ aesthetic charts...")
    
    def save_plot(filename):
        plt.tight_layout()
        plt.savefig(os.path.join(CHARTS_DIR, filename), dpi=150, bbox_inches="tight")
        plt.close()

    # 1. Top 10 Ingredients
    if not df_i.empty:
        top_ing = df_i["ingredient_name"].value_counts().head(10)
        plt.figure(figsize=(10, 6))
        sns.barplot(y=top_ing.index, x=top_ing.values, hue=top_ing.index, palette="mako", legend=False)
        plt.title("Most Popular Ingredients (Supply Chain)")
        plt.xlabel("Number of Recipes Using This Ingredient") # Explicit Label
        save_plot("1_top_ingredients.png")

    # 2. Difficulty Distribution
    if "difficulty" in df.columns:
        data = df["difficulty"].value_counts()
        plt.figure(figsize=(6, 6))
        plt.pie(data, labels=data.index, autopct='%1.1f%%', colors=sns.color_palette("pastel"), startangle=90, pctdistance=0.85)
        plt.gca().add_artist(plt.Circle((0,0),0.70,fc='white'))
        plt.title("Recipe Difficulty Distribution")
        save_plot("2_difficulty_donut.png")

    # 3. Prep Time vs Rating
    plt.figure(figsize=(8, 6))
    sns.regplot(data=df[df["avg_rating"] > 0], x="prep_time_min", y="avg_rating", scatter_kws={'alpha':0.5}, line_kws={'color':'red'})
    plt.title("Does More Prep Time = Better Taste?")
    plt.xlabel("Preparation Time (Minutes)") # Explicit Label
    plt.ylabel("Average User Rating (1-5 Stars)") # Explicit Label
    save_plot("3_time_vs_rating.png")

    # 4. Engagement Rate Leaders
    top_eng = df.nlargest(10, "engagement_rate")
    if not top_eng.empty:
        plt.figure(figsize=(10, 6))
        sns.barplot(data=top_eng, x="engagement_rate", y="title", hue="title", palette="viridis", legend=False)
        plt.title("Top 10 Viral Recipes (Engagement Rate)")
        plt.xlabel("Engagement Rate (%) (Likes/Views)") # Explicit Label
        plt.ylabel("")
        save_plot("4_viral_recipes.png")

    # 5. Cuisine Popularity
    if "cuisine" in df.columns:
        plt.figure(figsize=(10, 5))
        order = df["cuisine"].value_counts().index
        if not order.empty:
            sns.countplot(data=df, y="cuisine", hue="cuisine", palette="Set2", order=order, legend=False)
            plt.title("Cuisine Popularity")
            plt.xlabel("Number of Recipes") # Explicit Label
            save_plot("5_cuisine_popularity.png")

    # 6. Complexity vs. Difficulty
    if "difficulty" in df.columns:
        plt.figure(figsize=(8, 6))
        sns.boxplot(data=df, x="difficulty", y="complexity_score", hue="difficulty", palette="coolwarm", legend=False)
        plt.title("Calculated Complexity Score by Label")
        plt.ylabel("Complexity Score (0-100)") # Explicit Label
        save_plot("6_complexity_validation.png")

    # 7. Interaction Funnel
    if not df_int.empty:
        plt.figure(figsize=(8, 5))
        sns.countplot(data=df_int, x="type", hue="type", palette="autumn", legend=False)
        plt.title("User Interaction Funnel")
        plt.xlabel("Interaction Type") # Explicit Label
        plt.ylabel("Count of Events") # Explicit Label
        save_plot("7_interaction_funnel.png")

    # 8. Quick Wins Analysis [FIXED LABELS]
    plt.figure(figsize=(10, 7))
    sns.scatterplot(data=df, x="total_time_min", y="avg_rating", hue="difficulty", size="views", sizes=(50, 300), alpha=0.7)
    plt.axvline(30, color='green', linestyle='--') 
    plt.axhline(4.0, color='green', linestyle='--') 
    plt.title("Quick Wins Analysis (Target: Top-Left Quadrant)")
    # ADDED EXPLICIT LABELS HERE
    plt.xlabel("Total Cooking Time (Minutes) - Lower is Faster") 
    plt.ylabel("Average Rating (Stars) - Higher is Better")
    save_plot("8_quick_wins.png")

    # 9. Heatmap of Correlations [FIXED SIZE]
    numeric_cols = df.select_dtypes(include=np.number)
    if not numeric_cols.empty and len(numeric_cols.columns) > 1:
        plt.figure(figsize=(14, 12)) # Large size for readability
        sns.heatmap(numeric_cols.corr(), annot=True, fmt=".2f", cmap="RdBu", center=0, annot_kws={"size": 9})
        plt.title("Metric Correlation Matrix")
        save_plot("9_correlation_matrix.png")
        
    # 10. Ingredient Count Distribution [FIXED LABELS]
    plt.figure(figsize=(8, 5))
    sns.kdeplot(data=df, x="ingredient_count", fill=True, color="purple", alpha=0.4)
    plt.title("Ingredient Complexity Distribution")
    # ADDED EXPLICIT LABELS HERE
    plt.xlabel("Number of Ingredients in Recipe")
    plt.ylabel("Density (Frequency of Occurrence)")
    save_plot("10_ingredient_density.png")

# ---------------------------------------------------------
# 4. Text Insight Generation
# ---------------------------------------------------------
def extract_text_insights(df, df_i):
    """Generates text-based insights for the report."""
    insights = []
    
    top_ing = df_i["ingredient_name"].mode()[0] if not df_i.empty else "N/A"
    insights.append({"title": "Top Ingredient", "value": f"'{top_ing}' appears most frequently."})
    
    avg_time = df["prep_time_min"].mean()
    insights.append({"title": "Average Prep Time", "value": f"{avg_time:.1f} minutes."})
    
    if not df.empty:
        leader = df.loc[df["engagement_rate"].idxmax()]["title"]
        val = df["engagement_rate"].max()
        insights.append({"title": "Viral Leader", "value": f"'{leader}' ({val:.1f}% engagement)."})
    
    if "difficulty" in df.columns:
        hard_pct = (len(df[df["difficulty"]=="Hard"]) / len(df)) * 100
        insights.append({"title": "Difficulty Balance", "value": f"{hard_pct:.1f}% of recipes are 'Hard'."})
        
    quick_wins = len(df[(df["total_time_min"] < 30) & (df["avg_rating"] >= 4.0)])
    insights.append({"title": "Quick Wins", "value": f"{quick_wins} recipes are <30mins & rated 4+ stars."})
    
    insights.append({"title": "Global Quality Score", "value": f"{df['avg_rating'].mean():.2f} / 5.0 Stars."})

    corr = df["total_time_min"].corr(df["avg_rating"])
    insights.append({"title": "Time/Quality Correlation", "value": f"{corr:.2f} (Positive means longer = better)."})

    avg_complex = df["complexity_score"].mean()
    insights.append({"title": "Avg Complexity Score", "value": f"{avg_complex:.1f} / 100."})
    
    insights.append({"title": "Total Interactions", "value": f"{df['total_interactions'].sum()} user events."})
    
    if "cuisine" in df.columns:
        top_c = df["cuisine"].mode()[0] if not df["cuisine"].mode().empty else "N/A"
        insights.append({"title": "Dominant Cuisine", "value": top_c})

    avg_conv = df["conversion_rate"].mean()
    insights.append({"title": "Avg Conversion Rate", "value": f"{avg_conv:.1f}% of viewers attempt to cook."})

    if not df.empty:
        roi_leader = df.loc[df["roi_score"].idxmax()]["title"]
        insights.append({"title": "Best ROI Recipe", "value": f"'{roi_leader}' (Highest rating per minute spent)."})

    if not df.empty:
        most_liked = df.loc[df["likes"].idxmax()]["title"]
        insights.append({"title": "Crowd Favorite", "value": f"'{most_liked}' has the most likes."})
        
    if not df.empty:
        complex_r = df.loc[df["ingredient_count"].idxmax()]["title"]
        count = df["ingredient_count"].max()
        insights.append({"title": "Most Complex", "value": f"'{complex_r}' uses {int(count)} ingredients."})

    return insights

# ---------------------------------------------------------
# 5. Report Writing
# ---------------------------------------------------------
def write_report(df, insights, validation_summary):
    print("   Writing analytics_summary.md...")
    
    with open(MD_REPORT, "w", encoding="utf-8") as f:
        f.write("# 📊 Recipe Pipeline Analytics & Insights\n\n")
        f.write(f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Total Recipes Analyzed:** {len(df)}\n\n")
        
        # --- Validation Section ---
        f.write("## 🛡 1. Data Quality Summary\n")
        if validation_summary:
            overall = validation_summary.get("overall_score", 100.0)
            f.write(f"- **Data Health Score:** {overall:.1f}%\n")
            f.write(f"- **Total Issues:** {validation_summary.get('total_issues', 0)}\n")
            f.write("\n**Issues by Category:**\n")
            for it in validation_summary.get("checks_list", []):
                f.write(f"- {it['emoji']} **{it['key']}**: {it['count']} issues\n")
        else:
            f.write("No validation data found.\n")
        
        # --- Insights Section ---
        f.write("\n## 📈 2. Business Insights\n")
        for i, insight in enumerate(insights, 1):
            f.write(f"{i}. **{insight['title']}:** {insight['value']}\n")
            
        # --- Top Lists ---
        f.write("\n## 🏆 3. High-Performance Leaderboard\n")
        if not df.empty:
            cols = ["title", "difficulty", "engagement_rate", "views", "avg_rating"]
            cols = [c for c in cols if c in df.columns]
            top_5 = df.nlargest(5, "engagement_rate")[cols]
            f.write(top_5.to_markdown(index=False))
        
        # --- Recommendations ---
        f.write("\n\n## 💡 4. Strategic Recommendations\n")
        f.write("- **Promote 'Quick Wins':** Focus marketing on recipes under 30 mins with >4.0 rating.\n")
        f.write("- **Optimize Titles:** High view count but low conversion indicates misleading titles.\n")
        f.write("- **Supply Chain:** Ensure stock for the top 3 ingredients shown in Chart 1.\n")
        
        f.write("\n## 📊 5. Visualizations\n")
        f.write("All 10 professional charts are saved in: `output/analytics/charts/`\n")

# ---------------------------------------------------------
# Main Controller
# ---------------------------------------------------------
def run_analytics():
    print("🚀 Starting Advanced Analytics Module...")
    ensure_dirs()
    
    # 1. Load Data
    df_r, df_i, df_s, df_int = load_data()
    
    # 2. Load Validation Summary
    val_summary = {}
    if os.path.exists(VALIDATION_JSON):
        v_data = safe_load_json(VALIDATION_JSON)
        if v_data:
            total = v_data.get("total_recipes", 1)
            issues = 0
            checks_list = []
            for k, v in v_data.get("checks", {}).items():
                issues += v.get("count", 0)
                checks_list.append({"key": k, "count": v.get("count", 0), "emoji": v.get("severity_emoji", "⚠️")})
            val_summary = {
                "overall_score": max(0, 100 - (issues/total*100)),
                "total_issues": issues,
                "checks_list": checks_list
            }

    # 3. Compute Metrics
    df_enriched = calculate_advanced_metrics(df_r, df_i, df_int)
    
    # 4. Generate Charts
    generate_charts(df_enriched, df_i, df_int)
    
    # 5. Generate Insights
    insights = extract_text_insights(df_enriched, df_i)
    
    # 6. Save Report
    write_report(df_enriched, insights, val_summary)
    print(f"✅ Analytics Complete. Report saved to {MD_REPORT}")

if __name__ == "__main__":
    run_analytics()