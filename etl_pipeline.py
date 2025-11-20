import json
import os
import pandas as pd

# Input directory
INPUT_DIR = "output"
OUTPUT_DIR = os.path.join("output", "etl")

# Input files
RECIPES_FILE = os.path.join(INPUT_DIR, "recipes.json")
INTERACTIONS_FILE = os.path.join(INPUT_DIR, "interactions.json")

# Output files
RECIPE_CSV = os.path.join(OUTPUT_DIR, "recipe.csv")
INGREDIENTS_CSV = os.path.join(OUTPUT_DIR, "ingredients.csv")
STEPS_CSV = os.path.join(OUTPUT_DIR, "steps.csv")
INTERACTIONS_CSV = os.path.join(OUTPUT_DIR, "interactions.csv")


def ensure_output_dir():
    """Create output/etl directory if missing."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def load_json(path):
    """Load and return JSON list from file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def array_to_csv(value):
    """Convert list to comma-separated string."""
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return "" if value is None else str(value)


def build_recipe_table(recipes):
    rows = []
    for r in recipes:
        rows.append({
            "id": r.get("id", ""),
            "title": r.get("title", ""),
            "cuisine": r.get("cuisine", ""),
            "difficulty": r.get("difficulty", ""),
            "servings": r.get("servings", ""),
            "prep_time_min": r.get("prep_time_min", ""),
            "cook_time_min": r.get("cook_time_min", ""),
            "total_time_min": r.get("total_time_min", ""),
            "tags": array_to_csv(r.get("tags", [])),
            "occasion": array_to_csv(r.get("occasion", [])),
            "nutrition_groups": array_to_csv(r.get("nutrition_groups", [])),
            "category_type": r.get("category_type", ""),
            "texture": r.get("texture", ""),
            "served_temperature": r.get("served_temperature", "")
        })
    return pd.DataFrame(rows)


def build_ingredients_table(recipes):
    rows = []
    for r in recipes:
        rid = r.get("id", "")
        for ing in r.get("ingredients", []):
            rows.append({
                "recipe_id": rid,
                "ingredient_name": ing.get("name", ""),
                "quantity": str(ing.get("quantity", "")),
                "unit": str(ing.get("unit", "")),
            })
    return pd.DataFrame(rows)


def build_steps_table(recipes):
    rows = []
    for r in recipes:
        rid = r.get("id", "")
        for s in r.get("steps", []):
            rows.append({
                "recipe_id": rid,
                "step_number": s.get("step_number", ""),
                "step_text": s.get("description", ""),
            })
    return pd.DataFrame(rows)


def build_interactions_table(interactions):
    rows = []
    for it in interactions:
        rows.append({
            "id": it.get("id", ""),
            "user_id": it.get("user_id", ""),
            "recipe_id": it.get("recipe_id", ""),
            "type": it.get("type", ""),
            "timestamp": it.get("timestamp", ""),
            "metadata_json": json.dumps(it.get("metadata", {})),
            "description": it.get("description", ""),
        })
    return pd.DataFrame(rows)


def write_csv(df, path):
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"✔ Saved {path} ({len(df)} rows)")


def run_etl():
    print("Starting ETL…")

    ensure_output_dir()

    recipes = load_json(RECIPES_FILE)
    interactions = load_json(INTERACTIONS_FILE)

    print(f"Loaded {len(recipes)} recipes, {len(interactions)} interactions.")

    df_recipe = build_recipe_table(recipes)
    df_ingredients = build_ingredients_table(recipes)
    df_steps = build_steps_table(recipes)
    df_interactions = build_interactions_table(interactions)

    write_csv(df_recipe, RECIPE_CSV)
    write_csv(df_ingredients, INGREDIENTS_CSV)
    write_csv(df_steps, STEPS_CSV)
    write_csv(df_interactions, INTERACTIONS_CSV)

    print("\n🎉 ETL Completed. Check output/etl folder.")


if __name__ == "__main__":
    run_etl()

