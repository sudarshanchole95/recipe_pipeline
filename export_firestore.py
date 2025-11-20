import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
from google.cloud.firestore import DocumentSnapshot
from datetime import datetime

# ------------------------------------------
# Initialize Firebase Admin
# ------------------------------------------
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

print("Connected to Firestore for export.")

# ------------------------------------------
# Create output folder
# ------------------------------------------
output_dir = "output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# ------------------------------------------
# Helper: convert Firestore types → JSON safe
# ------------------------------------------
def convert_value(value):
    """Convert Firestore values to JSON-serializable values."""
    if isinstance(value, datetime):
        return value.isoformat()

    # Firestore timestamp type
    if hasattr(value, "timestamp"):
        return value.isoformat()

    # recursively convert lists
    if isinstance(value, list):
        return [convert_value(v) for v in value]

    # recursively convert dicts
    if isinstance(value, dict):
        return {k: convert_value(v) for k, v in value.items()}

    return value


# ------------------------------------------
# Export Collection Function
# ------------------------------------------
def export_collection(collection_name, output_filename):
    print(f"Exporting collection: {collection_name}")

    docs = db.collection(collection_name).stream()
    data = []

    for doc in docs:
        record = doc.to_dict()

        # Convert Firestore datetime → JSON serializable
        safe_record = convert_value(record)

        # Add document ID manually
        safe_record["id"] = doc.id

        data.append(safe_record)

    # Save JSON
    with open(os.path.join(output_dir, output_filename), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"✔ Saved to {output_filename}")


# ------------------------------------------
# Export all collections
# ------------------------------------------
export_collection("recipes", "recipes.json")
export_collection("users", "users.json")
export_collection("interactions", "interactions.json")

print("🎉 Export Complete!")
