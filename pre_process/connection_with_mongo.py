import ijson
from pymongo import MongoClient
import time
import json
import bson.json_util as json_util

start = time.time()
# --- CONFIG ---
JSON_PATH = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_synopses/ais_synopses_final.json"   # your 5GB file
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "admin"
COLLECTION_NAME = "navigation_related_and_meta"
BATCH_SIZE = 5000

print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

print("Connected!")
print("Starting streaming import...")

batch = []
count = 0

with open(JSON_PATH, "r") as f:
    data = json.load(f)
    # ijson.items(..., "item") => reads each element of a JSON ARRAY
    timer = 0 
    for obj in data:
        # Mongo native JSON parser → handles Decimal, dates, arrays etc
        bson_doc = json.loads(json.dumps(obj))
        batch.append(bson_doc)

        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch)
            count += len(batch)
            print(f"Inserted {count} documents")
            batch = []
            
# insert the last leftovers
if batch:
    collection.insert_many(batch)
    count += len(batch)
    print(f"Inserted final batch. Total documents: {count}")

end = time.time()
print("DONE — Import completed successfully! - Time taken: {:.2f} seconds".format(end - start))
