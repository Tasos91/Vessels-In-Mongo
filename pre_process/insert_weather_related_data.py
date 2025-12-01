import json
import time
from pymongo import MongoClient

start = time.time()

# --- CONFIG ---
JSON_PATH = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/weather_json_files/weather_releated_pre_processed.json"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "admin"
COLLECTION_NAME = "weather_related"
BATCH_SIZE = 5000

print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
print("Connected!\n")

print("Loading JSON file...")
with open(JSON_PATH, "r") as f:
    data = json.load(f)   # data must be a list of dicts

print(f"Loaded {len(data)} documents.\nStarting batch insert...")

batch = []
total_inserted = 0

for doc in data:
    batch.append(doc)

    if len(batch) == BATCH_SIZE:
        collection.insert_many(batch)
        total_inserted += len(batch)
        print(f"Inserted batch → Total: {total_inserted}")
        batch = []  # clear batch

# Insert remaining documents
if batch:
    collection.insert_many(batch)
    total_inserted += len(batch)
    print(f"Inserted final batch → Total: {total_inserted}")

end = time.time()
print(f"\nDONE — Import completed! Total documents: {total_inserted}")
print("Time taken: {:.2f} seconds".format(end - start))