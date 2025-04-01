import firebase_admin
from firebase_admin import credentials, firestore
from pymongo import MongoClient
import certifi
import time

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

    print(f"Migration of '{mongo_collection}' started")
    start_time = time.time()  # Start time measurement
    documents = db_mongo[mongo_collection].find()
    batch = db_firestore.batch()
    count = 0
    batch_size = 200
    batch_count = 0

    for doc in documents:
        data = {firestore_key: doc[mongo_key] for mongo_key, firestore_key in field_mapping.items() if mongo_key in doc}
        doc_ref = db_firestore.collection(firestore_collection).document(str(doc["_id"]))
        batch.set(doc_ref, data)
        count += 1

        # Commit batch after reaching the batch size
        if count % batch_size == 0:
            batch.commit()
            batch_count += 1
            print(f"Commited batch #{batch_count} count: {count}")
            time.sleep(1)  # Avoid hitting Firestore's write limit
            batch = db_firestore.batch()  # Start a new batch

    # Commit any remaining writes
    if count % batch_size != 0:
        batch.commit()

    end_time = time.time()  # End time measurement
    elapsed_time = end_time - start_time  # Calculate elapsed time

    print(f"Migration of '{mongo_collection}' completed in {elapsed_time:.2f} seconds!")

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

        migrate_collection("Booking", "Booking", {
            "productId": "productId",
            "productCode": "productCode",
            "leadTime": "leadTime",
            "shipCode": "shipCode",
            "bookingId": "bookingId",
            "cruiseBookingId": "cruiseBookingId",
            "offeringTime": "offeringTime",
            "sailDate": "sailDate",
            "offeringDuration": "offeringDuration",
            "productType": "productType",
            "bookingStatus": "bookingStatus",
            "lagTime": "lagTime",
            "offeringDate": "offeringDate",
            "productName": "productName",
            "status": "status",
            "guestId": "guestId",
            "offeringId": "offeringId"
        })

        migrate_collection("BookingWalkUp", "BookingWalkUp", {
            "master": "master",
            "bookingId": "bookingId",
            "bookingStatus": "bookingStatus",
            "checkInStatus": "checkInStatus",
            "cruiseBookingId": "cruiseBookingId",
            "guestId": "guestId",
            "offeringDate": "offeringDate",
            "offeringId": "offeringId",
            "offeringTime": "offeringTime",
            "productCode": "productCode",
            "productId": "productId",
            "productName": "productName",
            "sailDate": "sailDate",
            "shipCode": "shipCode",
            "status": "status"
        })
        
        migrate_collection("Guest", "Guest", {
            "firstName": "firstName",
            "lastName": "lastName",
            "sailDate": "sailDate",
            "paxId": "paxId",
            "longFolioNumber": "longFolioNumber",
            "sailingId": "sailingId",
            "cabinNumber": "cabinNumber",
            "cruiseBookingId": "cruiseBookingId",
            "shipCode": "shipCode",
            "folioNumber": "folioNumber",
            "paxIdUID": "paxIdUID",
            "dateOfBirth": "dateOfBirth"
        })
        
        migrate_collection("CheckIn", "CheckIn", {
            "master": "master",
            "checkInID": "checkInID",
            "checkInTime": "checkInTime",
            "checkInType": "checkInType",
            "declineNotes": "declineNotes",
            "declineReason": "declineReason",
            "folioNumber": "folioNumber",
            "offeringTimeMillis": "offeringTimeMillis",
            "paxId": "paxId",
            "productCode": "productCode",
            "shipCode": "shipCode",
            "status": "status"
        })
        
        migrate_collection("WalkUpData", "WalkUpData", {
            "master": "master",
            "capturedImage": "capturedImage",
            "checkInID": "checkInID",
            "firstName": "firstName",
            "lastName": "lastName",
            "middleName": "middleName",
            "paxId": "paxId"
        })

        print("Migration completed!")
    except Exception as e:
        print("Error migrating data:", e)
    finally:
        client.close()

migrate_data()
