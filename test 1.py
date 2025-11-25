# add_minimal_test_dataset.py
# Generates: 2 GOOD recipes + 1 BAD recipe + 5 test users + ~15 interactions
# Perfect for testing your ETL pipeline.

from firebase_admin import credentials, firestore, initialize_app
from datetime import datetime, timedelta
import random
import uuid

# ------------------------------
# FIRESTORE INIT
# ------------------------------
cred = credentials.Certificate("serviceAccountKey.json")
try:
    initialize_app(cred)
except:
    pass

db = firestore.client()

now = datetime.utcnow

# ------------------------------
# 1) RECIPES (2 good + 1 bad)
# ------------------------------
print("\n=== Uploading Recipes ===")

good_recipes = [
    {
        "title": "Test Paneer Tikka",
        "cuisine": "Indian",
        "difficulty": "easy",
        "prep_time_min": 10,
        "cook_time_min": 15,
        "total_time_min": 25,
        "servings": 2,
        "ingredients": [
            {"name": "Paneer", "quantity": "200", "unit": "grams"},
            {"name": "Curd", "quantity": "3", "unit": "tbsp"},
        ],
        "steps": [
            {"step_number": 1, "text": "Marinate paneer."},
            {"step_number": 2, "text": "Grill for 10 minutes."}
        ],
        "created_at": now(),
        "updated_at": now()
    },
    {
        "title": "Test Pasta Alfredo",
        "cuisine": "Italian",
        "difficulty": "medium",
        "prep_time_min": 12,
        "cook_time_min": 18,
        "total_time_min": 30,
        "servings": 2,
        "ingredients": [
            {"name": "Pasta", "quantity": "150", "unit": "grams"},
            {"name": "Cream", "quantity": "1/2", "unit": "cup"},
        ],
        "steps": [
            {"step_number": 1, "text": "Boil pasta."},
            {"step_number": 2, "text": "Mix with Alfredo sauce."}
        ],
        "created_at": now(),
        "updated_at": now()
    }
]

bad_recipe = {
    # ❌ BAD RECIPE (missing title)
    "cuisine": "Indian",
    "difficulty": "easy",
    "prep_time_min": 5,
    "ingredients": [{"name": "Something"}],
    "steps": [{"step_number": 1, "text": "Just cook."}],
    "created_at": now(),
    "updated_at": now()
}

recipe_ids = []

# Upload good recipes
for recipe in good_recipes:
    doc_id = f"good-{uuid.uuid4().hex[:6]}"
    db.collection("recipes").document(doc_id).set(recipe)
    recipe_ids.append(doc_id)
    print(f"✔ Good Recipe Added: {doc_id} → {recipe['title']}")

# Upload bad recipe
bad_id = f"bad-{uuid.uuid4().hex[:6]}"
db.collection("recipes").document(bad_id).set(bad_recipe)
recipe_ids.append(bad_id)
print(f"⚠ Bad Recipe Added: {bad_id} (missing title)")

# ------------------------------
# 2) USERS (5 test users)
# ------------------------------
print("\n=== Uploading Users ===")

user_ids = []
user_list = ["user_a", "user_b", "user_c", "user_d", "user_e"]

for u in user_list:
    uid = f"u-{uuid.uuid4().hex[:6]}"
    db.collection("users").document(uid).set({
        "id": uid,
        "name": u,
        "created_at": now()
    })
    user_ids.append(uid)
    print(f"✔ User Added: {uid}")

# ------------------------------
# 3) INTERACTIONS (~15 events)
# ------------------------------
print("\n=== Uploading Interactions ===")

interaction_types = ["view", "like", "rating"]

count = 0
num_interactions = random.randint(15, 20)

for _ in range(num_interactions):
    rid = random.choice(recipe_ids[:2])  # only good recipes get interactions
    uid = random.choice(user_ids)
    itype = random.choice(interaction_types)

    iid = f"int-{uuid.uuid4().hex[:8]}"

    data = {
        "id": iid,
        "user_id": uid,
        "recipe_id": rid,
        "type": itype,
        "timestamp": now() - timedelta(days=random.randint(0, 90)),
        "metadata": {},
        "description": f"{uid} {itype} on recipe {rid}"
    }

    if itype == "rating":
        data["metadata"]["rating"] = random.randint(3, 5)

    db.collection("interactions").document(iid).set(data)
    count += 1

print(f"✔ {count} interactions added.")

print("\n🎉 DONE! Data ready for ETL pipeline test.")
print("Run now: python pipeline.py")
