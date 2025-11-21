# export_firestore.py
import time
import json
from google.cloud import firestore
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore as admin_fs
from datetime import datetime
from config import EXPORT_DIR, SERVICE_ACCOUNT_JSON
from utils import ensure_dirs, write_json, timestamp
import sys

def iso(dt):
    if dt is None:
        return None
    # Firestore timestamps may be datetime-like
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)

def init_firestore():
    # Initialize firebase-admin using provided service account key
    cred_path = str(SERVICE_ACCOUNT_JSON)
    if not SERVICE_ACCOUNT_JSON.exists():
        raise FileNotFoundError(f"serviceAccountKey.json not found at: {cred_path}")
    cred = credentials.Certificate(cred_path)
    try:
        firebase_admin.get_app()
    except ValueError:
        firebase_admin.initialize_app(cred)
    return admin_fs.client()

def export_collection(client, collection_name, out_path):
    coll = client.collection(collection_name)
    docs = coll.stream()
    out = []
    count = 0
    for d in docs:
        data = d.to_dict()
        # convert Firestore timestamp objects to ISO strings if present
        for k, v in list(data.items()):
            if isinstance(v, datetime):
                data[k] = iso(v)
        data["id"] = d.id
        out.append(data)
        count += 1
    write_json(out_path, out)
    return count

def export_all():
    ensure_dirs(EXPORT_DIR)
    client = init_firestore()
    print("Connected to Firestore. Exporting collections...")
    collections = ["recipes", "users", "interactions"]
    summary = {}
    for col in collections:
        out_path = EXPORT_DIR / f"{col}.json"
        try:
            cnt = export_collection(client, col, out_path)
            summary[col] = cnt
            print(f"  Exported {cnt} docs -> {out_path}")
        except Exception as e:
            print(f"  ERROR exporting {col}: {e}")
            summary[col] = f"ERROR: {e}"
    summary["exported_at"] = timestamp()
    return summary

if __name__ == "__main__":
    s = export_all()
    print("Export summary:", s)
