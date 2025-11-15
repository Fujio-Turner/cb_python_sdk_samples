"""
Connects to a Couchbase cluster, processes a CSV or Excel file, and inserts the data into a Couchbase collection.

The script performs the following steps:
1. Connects to a Couchbase cluster using the provided connection parameters.
2. Processes a CSV or Excel file, converts the data to JSON, and calculates the MD5 hash of the file.
3. Iterates through the records, adds audit information, and inserts each record into the Couchbase collection.
4. Handles various exceptions that may occur during the insertion process, such as document already exists, timeout, network errors, and value/type errors.
5. Closes the Couchbase connection when the data insertion is complete.

pip install couchbase
pip install pandas
pip install openpyxl
Docs on CSV/Excel: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html
"""
import pandas as pd
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.options import InsertOptions
from couchbase.management.buckets import BucketSettings, BucketType, StorageBackend
from couchbase.exceptions import (
    DocumentExistsException, 
    TimeoutException,
    BucketNotFoundException,
    BucketAlreadyExistsException
)
import time
import hashlib
import os
from datetime import timedelta
import uuid
import json

# Couchbase connection parameters
# For local/self-hosted Couchbase Server:
CB_HOST = "localhost"
CB_USER = "Administrator"
CB_PASS = "password"

# For Capella (cloud), uncomment and update these instead:
# CB_HOST = "cb.your-endpoint.cloud.couchbase.com"  # Your Capella hostname
# CB_USER = "your-capella-username"
# CB_PASS = "your-capella-password"

CB_BUCKET = "example"
CB_SCOPE = "_default"
CB_COLLECTION = "_default"

SCRIPT_NAME = "python-user"
SCRIPT_VERSION = "1.0"

# File paths
CSV_FILE = "demo_data/customers-10000.csv"
EXCEL_FILE = "demo_data/table01September2024.xlsx"

def get_file_md5(filename):
    md5_hash = hashlib.md5()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

# Connect to Couchbase
cluster = None
try:
    auth = PasswordAuthenticator(CB_USER, CB_PASS)
    options = ClusterOptions(auth)
    
    # For local/self-hosted Couchbase Server:
    cluster = Cluster(f"couchbase://{CB_HOST}", options)
    
    # For Capella (cloud), use this instead (uncomment and comment out the line above):
    # options.apply_profile('wan_development')  # Helps avoid latency issues with Capella
    # cluster = Cluster(f"couchbases://{CB_HOST}", options)  # Note: couchbaseS (secure)
    
    cluster.wait_until_ready(timedelta(seconds=10))
    
    # Check if bucket exists, create if it doesn't
    bucket_mgr = cluster.buckets()
    try:
        bucket_info = bucket_mgr.get_bucket(CB_BUCKET)
        print(f"Bucket '{CB_BUCKET}' already exists")
    except BucketNotFoundException:
        print(f"Bucket '{CB_BUCKET}' not found. Creating with 100MB...")
        try:
            settings = BucketSettings(
                name=CB_BUCKET,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100,
                storage_backend=StorageBackend.COUCHSTORE
            )
            bucket_mgr.create_bucket(settings)
            print(f"✓ Bucket '{CB_BUCKET}' created successfully (100MB, Couchbase Store)")
            # Wait for bucket to be ready
            time.sleep(3)
        except BucketAlreadyExistsException:
            print(f"Bucket '{CB_BUCKET}' already exists (created by another process)")
        except Exception as create_error:
            print(f"Failed to create bucket: {create_error}")
            exit(1)
    
    bucket = cluster.bucket(CB_BUCKET)
    collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)
    print("Successfully connected to Couchbase collection")
except Exception as e:
    print(f"Failed to connect to Couchbase: {str(e)}")
    exit(1)

# Process the file (CSV or Excel)
try:
    # Uncomment the appropriate section based on the file type you want to process

    # Process CSV file
    file_md5 = get_file_md5(CSV_FILE)
    file_name = os.path.basename(CSV_FILE)
    df = pd.read_csv(CSV_FILE)
    
    # Process Excel file
    #file_md5 = get_file_md5(EXCEL_FILE)
    #file_name = os.path.basename(EXCEL_FILE)
    #df = pd.read_excel(EXCEL_FILE)
    
    json_data = df.to_json(orient='records')
    records = json.loads(json_data)
    print(f"Successfully processed file: {file_name}")
except Exception as e:
    print(f"Error processing file: {str(e)}")
    if cluster:
        cluster.close()
    exit(1)

# Add audit information and insert records
for record in records:
    record["audit"] = {
        "cr": {
            "dt": time.time(),
            "ver": SCRIPT_VERSION,
            "by": SCRIPT_NAME,
            "src": file_name,
            "md5": file_md5
        }
    }

    try:
        
        if "Customer Id" in record and record["Customer Id"] and str(record["Customer Id"]).strip():
            key = f"c:{record['Customer Id']}"
        else:
            key = f"c:{uuid.uuid4()}"
            record["key_exception"] = True
        

        #key = "r:"+str(uuid.uuid4())
        
        result = collection.insert(key, record, InsertOptions(timeout=timedelta(seconds=5)))
        print(f"Inserted document with key: {key}, CAS: {result.cas}")
    
    except DocumentExistsException:
        print(f"Document with key {key} already exists. Skipping.")
    except TimeoutException:
        print(f"Timeout occurred while inserting document with key {key}. Retrying...")
    except TypeError as te:
        print(f"Type error for key {key}: {str(te)}. Skipping this record.")
    except Exception as e:
        print(f"Unexpected error inserting document with key {key}: {str(e)}")

print("Data insertion complete.")

# Close the Couchbase connection
if cluster:
    cluster.close()
    print("Couchbase connection closed.")