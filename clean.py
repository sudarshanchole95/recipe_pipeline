import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURATION ---
CRED_PATH = 'serviceAccountKey.json'
COLLECTIONS_TO_DELETE = ['recipes', 'users', 'interactions']
BATCH_SIZE = 50

# Initialize Firebase (if not already initialized)
if not firebase_admin._apps:
    cred = credentials.Certificate(CRED_PATH)
    firebase_admin.initialize_app(cred)

db = firestore.client()

def delete_collection(coll_ref, batch_size):
    """
    Deletes all documents in a collection in batches.
    """
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        print(f"   Deleting doc: {doc.id}")
        doc.reference.delete()
        deleted += 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)
    
    return deleted

def reset_database():
    print("⚠️  WARNING: This will DELETE ALL DATA in:", COLLECTIONS_TO_DELETE)
    confirm = input("Type 'DELETE' to confirm: ")
    
    if confirm != 'DELETE':
        print("❌ Operation cancelled.")
        return

    print("\n🚀 Starting Database Wipe...")

    for collection_name in COLLECTIONS_TO_DELETE:
        print(f"\n🗑  Clearing collection: {collection_name}...")
        coll_ref = db.collection(collection_name)
        delete_collection(coll_ref, BATCH_SIZE)
        print(f"✔ {collection_name} cleared.")

    print("\n✨ Database is now empty.")

if __name__ == "__main__":
    reset_database()
