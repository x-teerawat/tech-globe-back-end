from pymongo import MongoClient

# Connection to the source database
source_client = MongoClient('mongodb://ubuntu:techglobetrading@13.229.230.27:27017')
source_db = source_client['tg']
source_collection = source_db['transactions']

# Connection to the target database
target_client = MongoClient('mongodb://ubuntu:techglobetrading@13.229.230.27:27017')
target_db = target_client['tg-back-end']
target_collection = target_db['transactions']

# Copy documents from source collection to target collection
documents = list(source_collection.find())
if len(documents) > 0:
    target_collection.insert_many(documents)

print("Collection duplicated successfully!")
