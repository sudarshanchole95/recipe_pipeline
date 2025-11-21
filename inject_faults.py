"""
inject_faults.py

Utility to INTENTIONALLY corrupt data in export/ to test the ETL Quarantine logic.
Run this ONCE, then run pipeline.py to see the bad_data files appear.
"""
import json
import random
from pathlib import Path

EXPORT_DIR = Path("export")
RECIPES_JSON = EXPORT_DIR / "recipes.json"
INTER_JSON = EXPORT_DIR / "interactions.json"

def corrupt_data():
    print("😈 Injecting faults into source data...")

    # 1. Corrupt Recipes
    with open(RECIPES_JSON, "r", encoding="utf-8") as f:
        recipes = json.load(f)
        if isinstance(recipes, dict): recipes = list(recipes.values())

    # Fault A: Remove ID and Title from the first recipe
    if len(recipes) > 0:
        recipes[0]["id"] = ""
        recipes[0]["title"] = ""
        print("   - Corrupted Recipe 0: Removed ID and Title")

    # Fault B: Negative Prep Time
    if len(recipes) > 1:
        recipes[1]["prep_time_min"] = -50
        print("   - Corrupted Recipe 1: Set negative prep_time_min")

    # Fault C: Total Time < Prep + Cook (Logic Error)
    if len(recipes) > 2:
        recipes[2]["prep_time_min"] = 20
        recipes[2]["cook_time_min"] = 20
        recipes[2]["total_time_min"] = 10 # Impossible!
        print("   - Corrupted Recipe 2: Logic error (Total < Prep+Cook)")

    # Fault D: Duplicate ID
    if len(recipes) > 3:
        recipes[3]["id"] = "DUPLICATE_123"
        recipes.append(recipes[3].copy()) # Add exact copy
        print("   - Created Duplicate Recipe ID: DUPLICATE_123")

    # Fault E: Bad Ingredient (Missing Name)
    if len(recipes) > 4:
        recipes[4]["ingredients"].append({"name": "", "quantity": "10", "unit": "kg"})
        print("   - Corrupted Recipe 4: Added nameless ingredient")

    # Fault F: Empty Step
    if len(recipes) > 5:
        recipes[5]["steps"].append("")
        print("   - Corrupted Recipe 5: Added empty step")

    # Save corrupted recipes
    with open(RECIPES_JSON, "w", encoding="utf-8") as f:
        json.dump(recipes, f, indent=2)

    # 2. Corrupt Interactions
    with open(INTER_JSON, "r", encoding="utf-8") as f:
        interactions = json.load(f)
        if isinstance(interactions, dict): interactions = list(interactions.values())

    # Fault G: Orphan Interaction (Recipe ID doesn't exist)
    if len(interactions) > 0:
        interactions[0]["recipe_id"] = "NON_EXISTENT_ID_999"
        print("   - Corrupted Interaction 0: Linked to non-existent recipe")

    # Fault H: Missing User ID
    if len(interactions) > 1:
        interactions[1]["user_id"] = ""
        print("   - Corrupted Interaction 1: Removed user_id")

    with open(INTER_JSON, "w", encoding="utf-8") as f:
        json.dump(interactions, f, indent=2)

    print("😈 Fault injection complete. Now run 'python pipeline.py' to test Validation.")

if __name__ == "__main__":
    corrupt_data()