"""
reset_firestore.py
------------------------------------------------
Utility to PURGE specific collections from Firestore.
Uses batching for performance and iterative logic for safety.
------------------------------------------------
"""

import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import time

# --- CONFIGURATION ---
CRED_PATH = 'serviceAccountKey.json'
COLLECTIONS_TO_DELETE = ['recipes', 'users', 'interactions']
BATCH_SIZE = 400 # Firestore batch limit is 500

# --- LOGGING UTILS ---
LOG_FMT = "%Y-%m-%d %H:%M:%S"

def log_step(message: str, level: str = "INFO"):
    ts = datetime.datetime.now().strftime(LOG_FMT)
    print(f"[{ts}] [{level:<5}] {message}")

# --- INIT ---
try:
    if not firebase_admin._apps:
        cred = credentials.Certificate(CRED_PATH)
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    log_step(f"Firebase Init Failed: {e}", "CRIT")
    exit(1)

def delete_collection(coll_ref, batch_size):
    """
    Iteratively deletes documents in batches to avoid RecursionError
    and optimize network calls using WriteBatch.
    """
    total_deleted = 0

    while True:
        # Retrieve a batch of documents
        docs = list(coll_ref.limit(batch_size).stream())
        count = len(docs)

        if count == 0:
            break

        # Use batch write for atomic/faster deletion
        batch = db.batch()
        for doc in docs:
            batch.delete(doc.reference)
        
        batch.commit()
        total_deleted += count
        
        # Log progress only (reduce noise)
        log_step(f"  Batch committed: {count} docs removed from '{coll_ref.id}'", "DEBUG")

    return total_deleted

def reset_database():
    log_step("Initiating Database Purge Sequence...", "WARN")
    log_step(f"Target Collections: {COLLECTIONS_TO_DELETE}", "WARN")
    
    # Production guard rail
    confirm = input(f"[{datetime.datetime.now().strftime(LOG_FMT)}] [INPUT] Type 'DELETE' to confirm destructive action: ")
    
    if confirm != 'DELETE':
        log_step("Operation aborted by user.", "INFO")
        return

    start_time = time.time()
    log_step("Starting purge operation...", "INFO")

    total_cleared = 0
    
    for col_name in COLLECTIONS_TO_DELETE:
        ref = db.collection(col_name)
        count = delete_collection(ref, BATCH_SIZE)
        total_cleared += count
        log_step(f"Collection '{col_name}' wiped completely. (Total: {count})", "INFO")

    elapsed = time.time() - start_time
    log_step(f"Database Reset Complete. {total_cleared} documents deleted in {elapsed:.2f}s.", "DONE")

if __name__ == "__main__":
    reset_database()