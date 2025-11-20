import json
import pandas as pd
import os

INPUT_DIR = "output/etl"
OUTPUT_DIR = "output/validation"

RECIPE_CSV = os.path.join(INPUT_DIR, "recipe.csv")
INGREDIENT_CSV = os.path.join(INPUT_DIR, "ingredients.csv")
STEPS_CSV = os.path.join(INPUT_DIR, "steps.csv")
INTERACTIONS_CSV = os.path.join(INPUT_DIR, "interactions.csv")

REPORT_JSON = os.path.join(OUTPUT_DIR, "validation_report.json")
REPORT_TXT = os.path.join(OUTPUT_DIR, "validation_report.txt")


def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def load_csv(path):
    return pd.read_csv(path)


def validate_recipes(df):
    issues = []

    for idx, row in df.iterrows():
        rid = row["id"]

        # Required fields
        if pd.isna(rid) or rid == "":
            issues.append({"id": None, "field": "id", "error": "Missing recipe ID"})

        if pd.isna(row["title"]) or row["title"].strip() == "":
            issues.append({"id": rid, "field": "title", "error": "Missing title"})

        # Numeric validation
        for field in ["prep_time_min", "cook_time_min", "total_time_min", "servings"]:
            if pd.isna(row[field]):
                continue
            try:
                val = float(row[field])
                if val < 0:
                    issues.append({"id": rid, "field": field, "error": "Negative value"})
            except:
                issues.append({"id": rid, "field": field, "error": "Invalid number"})

        # Array fields
        for field in ["tags", "occasion", "nutrition_groups"]:
            if pd.isna(row[field]):
                continue
            if not isinstance(row[field], str):
                issues.append({"id": rid, "field": field, "error": "Not string after flattening"})

    return issues


def validate_ingredients(df):
    issues = []
    for idx, row in df.iterrows():
        if pd.isna(row["recipe_id"]) or row["recipe_id"] == "":
            issues.append({"recipe_id": None, "field": "recipe_id", "error": "Missing recipe_id"})

        if pd.isna(row["ingredient_name"]) or row["ingredient_name"].strip() == "":
            issues.append({"recipe_id": row["recipe_id"], "field": "ingredient_name", "error": "Missing ingredient name"})
    return issues


def validate_steps(df):
    issues = []
    for idx, row in df.iterrows():
        if pd.isna(row["recipe_id"]) or row["recipe_id"] == "":
            issues.append({"recipe_id": None, "field": "recipe_id", "error": "Missing recipe_id"})

        if pd.isna(row["step_number"]):
            issues.append({"recipe_id": row["recipe_id"], "field": "step_number", "error": "Missing step number"})

        if pd.isna(row["step_text"]) or row["step_text"].strip() == "":
            issues.append({"recipe_id": row["recipe_id"], "field": "step_text", "error": "Empty step text"})

    return issues


def validate_interactions(df):
    issues = []
    for idx, row in df.iterrows():

        # Required fields
        for field in ["id", "user_id", "recipe_id", "type", "timestamp"]:
            if pd.isna(row[field]) or str(row[field]).strip() == "":
                issues.append({"id": row.get("id"), "field": field, "error": "Missing required interaction field"})

        # Metadata validation
        try:
            json.loads(row["metadata_json"])
        except:
            issues.append({"id": row["id"], "field": "metadata_json", "error": "Invalid JSON"})

    return issues


def write_reports(issues):
    # JSON report
    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(issues, f, indent=4)

    # Human-readable text report
    with open(REPORT_TXT, "w", encoding="utf-8") as f:
        f.write("DATA VALIDATION REPORT\n")
        f.write("======================\n\n")
        f.write(f"Total Issues Found: {len(issues)}\n\n")

        for issue in issues:
            f.write(json.dumps(issue) + "\n")

    print("\n✔ Validation reports saved in output/validation/")


def run_validation():
    print("Starting validation...")

    ensure_output_dir()

    df_recipe = load_csv(RECIPE_CSV)
    df_ing = load_csv(INGREDIENT_CSV)
    df_steps = load_csv(STEPS_CSV)
    df_inter = load_csv(INTERACTIONS_CSV)

    issues = []
    issues += validate_recipes(df_recipe)
    issues += validate_ingredients(df_ing)
    issues += validate_steps(df_steps)
    issues += validate_interactions(df_inter)

    write_reports(issues)

    print("🎉 Validation complete.")
    print(f"Total issues detected: {len(issues)}")


if __name__ == "__main__":
    run_validation()
