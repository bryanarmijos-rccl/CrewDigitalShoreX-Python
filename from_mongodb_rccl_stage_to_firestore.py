import firebase_admin
from firebase_admin import credentials, firestore
from pymongo import MongoClient
import certifi

# MongoDB Connection (Replace with your credentials)
MONGO_URI = "<your_credential_secrets>"
MONGO_DB = "<database_name>"

# Firestore Setup (Replace with your Firebase credentials JSON file)
FIREBASE_CREDENTIALS = "<your_credential_secrets>"

# Initialize Firestore
cred = credentials.Certificate(FIREBASE_CREDENTIALS)
firebase_admin.initialize_app(cred)
db_firestore = firestore.client()

# Connection to MongoDB
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db_mongo = client[MONGO_DB]

def migrate_collection(mongo_collection, firestore_collection, field_mapping):
    """
    Migrate data from a MongoDB collection to Firestore.
    :param mongo_collection: The name of the MongoDB collection
    :param firestore_collection: The Firestore collection name
    :param field_mapping: Dictionary to map MongoDB fields to Firestore fields
    """
    documents = db_mongo[mongo_collection].find()
    for doc in documents:
        data = {firestore_key: doc[mongo_key] for mongo_key, firestore_key in field_mapping.items() if mongo_key in doc}
        db_firestore.collection(firestore_collection).document(str(doc["_id"])).set(data)

def migrate_data():
    try:
        migrate_collection("ConfigUserGroup", "ConfigUserGroup", {
            "userName": "userName",
            "group": "group",
            "isAdmin": "isAdmin",
            "createOffering": "createOffering",
            "createUsers": "createUsers"
        })
        
        migrate_collection("ConfigAccessControl", "ConfigAccessControl", {
            "groupName": "groupName",
            "productGroups": "productGroups"
        })
        
        migrate_collection("ProductGroup", "ProductGroup", {
            "productAlias": "productAlias",
            "products": "products",
            "offering": "offering",
            "description": "description",
            "entryType": "entryType",
            "flexInventory": "flexInventory",
            "defaultSlots": "defaultSlots",
            "master": "master",
            "brand": "brand"
        })
        
        migrate_collection("Product", "Product", {
            "productCode": "productCode",
            "productStatus": "productStatus",
            "description": "description",
            "groupName": "groupName",
            "leadTimeOffset": "leadTimeOffset"
        })
        
        migrate_collection("Offering", "Offering", {
            "offeringUID": "offeringUID",
            "offeringDuration": "offeringDuration",
            "slots": "slots",
            "offeringTimeMillis": "offeringTimeMillis",
            "subGroup": "subGroup"
        })

        print("Migration completed!")
    except Exception as e:
        print("Error migrating data:", e)
    finally:
        client.close()

migrate_data()
