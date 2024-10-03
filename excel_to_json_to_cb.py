import pandas as pd
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.options import InsertOptions
import json
import time
import hashlib
import os
from datetime import timedelta
import uuid  # Import the uuid module

# Couchbase connection parameters
CB_HOST = "localhost"
CB_USER = "demo"
CB_PASS = "password"
CB_BUCKET = "example"
CB_SCOPE = "_default"
CB_COLLECTION = "_default"

SCRIPT_NAME = "python-user"
SCRIPT_VERSION = "1.0"

# CSV file path
CSV_FILE = "demo_data/customers-10000.csv"

# Function to calculate MD5 hash of a file
def get_file_md5(filename):
    md5_hash = hashlib.md5()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

# Calculate MD5 hash of the CSV file
csv_md5 = get_file_md5(CSV_FILE)

# Get the file name from the path
csv_file_name = os.path.basename(CSV_FILE)

# Connect to Couchbase
cluster = Cluster(f"couchbase://{CB_HOST}", ClusterOptions(PasswordAuthenticator(CB_USER, CB_PASS)))
bucket = cluster.bucket(CB_BUCKET)
collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)

# Read CSV file
df = pd.read_csv(CSV_FILE)

# Convert DataFrame to JSON
json_data = df.to_json(orient='records')
records = json.loads(json_data)

# Add audit information to each record
for record in records:
    record["audit"] = {
        "cr": {
            "dt": time.time(),
            "ver": SCRIPT_VERSION,
            "by": SCRIPT_NAME,
            "src": csv_file_name,
            "md5": csv_md5
        }
    }

# Insert each record into Couchbase
for record in records:
    try:
        if "Customer Id" in record and record["Customer Id"] and str(record["Customer Id"]).strip():
            key = f"c:{record['Customer Id']}"
        else:
            key = f"c:{uuid.uuid4()}"  # Generate a UUID
            record["key_exception"] = True
        
        result = collection.insert(key, record, InsertOptions(timeout=timedelta(seconds=5)))
        print(f"Inserted document with key: {key}, CAS: {result.cas}")
    except Exception as e:
        print(f"Error inserting document with key {key}: {str(e)}")

print("Data insertion complete.")

# Close the Couchbase connection
cluster.close()
print("Couchbase connection closed.")