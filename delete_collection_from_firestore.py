import firebase_admin
from firebase_admin import credentials, firestore

# Firestore Setup (Replace with your Firebase credentials JSON file)
FIREBASE_CREDENTIALS = "project_secrets/rccl-debug-firebase-adminsdk-m67fx-a59bd870ab.json"

# Initilize Firestore
cred = credentials.Certificate(FIREBASE_CREDENTIALS)
firebase_admin.initialize_app(cred)
db = firestore.client()

# Collection name to delete
COLLECTION_NAME = "BookingWalkUp"

def delete_collection(coll_ref, batch_size):
    try:
        docs = coll_ref.limit(batch_size).stream()
        deleted = 0

        for doc in docs:
            print(f"Deleting doc {doc.id}")
            doc.reference.delete()
            deleted += 1

        if deleted >= batch_size:
            return delete_collection(coll_ref, batch_size)
    except Exception as e:
        print(f"❌ Error deleting collection: {e}")

delete_collection(db.collection(COLLECTION_NAME), 100)