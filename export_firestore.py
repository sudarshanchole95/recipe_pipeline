"""
export_firestore.py
-------------------------------------------------------
Production-Grade Incremental Export (Hash-Based Only)

Features:
- Always scans full Firestore collection
- Detects changes using stable MD5 document hash
- Exports only NEW or UPDATED documents
- Converts all timestamps safely to ISO format
- Stores state in output/state/pipeline_state.json
- Clean, professional logging
"""

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import firebase_admin
from firebase_admin import credentials, firestore as admin_fs

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
EXPORT_DIR = ROOT / "export"
STATE_DIR = ROOT / "output" / "state"
STATE_FILE = STATE_DIR / "pipeline_state.json"
SERVICE_ACCOUNT = ROOT / "serviceAccountKey.json"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------

def to_iso(val):
    """Convert Firestore datetime → ISO string."""
    if hasattr(val, "isoformat"):
        return val.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return val

def doc_hash(doc_dict):
    """Generate a stable MD5 hash for a document."""
    return hashlib.md5(json.dumps(doc_dict, sort_keys=True).encode()).hexdigest()

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"doc_hashes": {}}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))

def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(str(SERVICE_ACCOUNT))
        firebase_admin.initialize_app(cred)
    return admin_fs.client()

# ------------------------------------------------------------
# EXPORT LOGIC (FULL SCAN + HASH MATCH)
# ------------------------------------------------------------

def export_collection(client, collection_name, prev_state):
    """Scan entire collection and detect changes using hash."""
    
    prev_hashes = prev_state.get("doc_hashes", {}).get(collection_name, {})
    coll = client.collection(collection_name)

    docs = list(coll.stream())

    new_docs = {}
    updated_docs = {}
    unchanged = 0
    total_seen = 0

    for doc in docs:
        total_seen += 1
        data = doc.to_dict()
        data["id"] = doc.id

        # Convert timestamps
        for k, v in list(data.items()):
            if "time" in k.lower() or "at" in k.lower():
                data[k] = to_iso(v)

        h = doc_hash(data)
        old_h = prev_hashes.get(doc.id)

        if old_h is None:
            new_docs[doc.id] = data
        elif old_h != h:
            updated_docs[doc.id] = data
        else:
            unchanged += 1

    # Save only changed docs
    out_path = EXPORT_DIR / f"{collection_name}.json"
    to_export = list(new_docs.values()) + list(updated_docs.values())
    out_path.write_text(json.dumps(to_export, indent=2))

    # Update hash map
    new_hash_map = prev_hashes.copy()
    for did, dat in new_docs.items():
        new_hash_map[did] = doc_hash(dat)
    for did, dat in updated_docs.items():
        new_hash_map[did] = doc_hash(dat)

    return {
        "collection": collection_name,
        "new": len(new_docs),
        "updated": len(updated_docs),
        "unchanged": unchanged,
        "total": total_seen,
        "new_hash_map": new_hash_map,
        "has_changes": (len(new_docs) + len(updated_docs)) > 0
    }

# ------------------------------------------------------------
# EXPORT ALL
# ------------------------------------------------------------
def export_all():
    # print("[EXPORT] Scanning Firestore for changes...")

    client = init_firestore()
    prev_state = load_state()

    updated_state = {"doc_hashes": {}}
    summaries = {}
    any_changes = False

    for col in ["recipes", "users", "interactions"]:
        result = export_collection(client, col, prev_state)
        
        print(
            f"[EXPORT] {col:<12} New={result['new']} "
            f"Updated={result['updated']} "
            f"Unchanged={result['unchanged']} "
            f"Total={result['total']}"
        )

        updated_state["doc_hashes"][col] = result["new_hash_map"]
        summaries[col] = result

        if result["has_changes"]:
            any_changes = True

    save_state(updated_state)

    if any_changes:
        print("[EXPORT] ✔ Changes found → ETL will run")
    else:
        print("[EXPORT] ✔ No changes → ETL will be skipped")

    return {
        "changes_found": any_changes,
        "summary": summaries
    }


if __name__ == "__main__":
    export_all()
