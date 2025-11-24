# add_test_recipes.py
# Adds 2 valid recipes + 3 bad recipes into Firestore for pipeline testing

from firebase_admin import credentials, firestore, initialize_app
from datetime import datetime
import random

# ------------------------------
# FIRESTORE SETUP
# ------------------------------
cred = credentials.Certificate("serviceAccountKey.json")
try:
    initialize_app(cred)
except:
    pass

db = firestore.client()
recipes_ref = db.collection("recipes")

def now():
    return firestore.SERVER_TIMESTAMP


# ------------------------------
#  VALID RECIPE 1
# ------------------------------
valid_recipe_1 = {
    "title": "Test Paneer Masala",
    "cuisine": "Indian",
    "difficulty": "easy",
    "prep_time_min": 15,
    "cook_time_min": 20,
    "total_time_min": 35,
    "servings": 2,
    "ingredients": [
        {"name": "Paneer", "quantity": "200", "unit": "grams"},
        {"name": "Tomato", "quantity": "2", "unit": "pcs"}
    ],
    "steps": [
        "Heat oil in a pan.",
        "Add tomatoes, cook and add paneer."
    ],
    "tags": ["test", "paneer"],
    "created_at": now(),
    "updated_at": now()
}

# ------------------------------
#  VALID RECIPE 2
# ------------------------------
valid_recipe_2 = {
    "title": "Test Veg Soup",
    "cuisine": "Fusion",
    "difficulty": "medium",
    "prep_time_min": 10,
    "cook_time_min": 15,
    "total_time_min": 25,
    "servings": 3,
    "ingredients": [
        {"name": "Carrot", "quantity": "1", "unit": "pcs"},
        {"name": "Corn", "quantity": "1/2", "unit": "cup"}
    ],
    "steps": [
        "Chop vegetables.",
        "Boil everything together for 15 minutes."
    ],
    "tags": ["test", "soup"],
    "created_at": now(),
    "updated_at": now()
}

# ------------------------------
#  BAD RECIPE 1 - Missing Title
# ------------------------------
bad_recipe_1 = {
    "cuisine": "Indian",
    "difficulty": "easy",
    "ingredients": [{"name": "Onion"}],
    "steps": ["Just mix everything"],
    "created_at": now(),
    "updated_at": now()
}

# ------------------------------
#  BAD RECIPE 2 - Empty Ingredients
# ------------------------------
bad_recipe_2 = {
    "title": "Broken Recipe No Ingredients",
    "cuisine": "Indian",
    "difficulty": "hard",
    "ingredients": [],
    "steps": ["Random step"],
    "created_at": now(),
    "updated_at": now()
}

# ------------------------------
#  BAD RECIPE 3 - Negative Time
# ------------------------------
bad_recipe_3 = {
    "title": "Invalid Negative Time",
    "cuisine": "Global",
    "difficulty": "easy",
    "prep_time_min": -5,
    "cook_time_min": 10,
    "total_time_min": 5,
    "ingredients": [{"name": "Salt"}],
    "steps": ["Step 1"],
    "created_at": now(),
    "updated_at": now()
}

# ------------------------------
#  UPLOAD ALL
# ------------------------------
docs = [
    ("test-paneer-" + str(random.randint(1000,9999)), valid_recipe_1),
    ("test-soup-" + str(random.randint(1000,9999)), valid_recipe_2),
    ("bad-missing-title-" + str(random.randint(1000,9999)), bad_recipe_1),
    ("bad-no-ingredients-" + str(random.randint(1000,9999)), bad_recipe_2),
    ("bad-negative-time-" + str(random.randint(1000,9999)), bad_recipe_3),
]

print("Uploading test recipes...\n")

for doc_id, data in docs:
    recipes_ref.document(doc_id).set(data)
    print(f"✔ Uploaded: {doc_id}")

print("\n🎉 Done! 2 valid + 3 bad recipes added.")
print("Now run your pipeline:")
print("  python pipeline.py")
