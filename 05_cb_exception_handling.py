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
from couchbase.exceptions import (
    DocumentExistsException, 
    DocumentNotFoundException,
    TimeoutException, 
    ServiceUnavailableException,
    ParsingFailedException,
    CouchbaseException
)
import json
import time
import hashlib
import os
from datetime import timedelta
import uuid

# Couchbase connection parameters
CB_HOST = "localhost"
CB_USER = "Administrator"
CB_PASS = "password"
CB_BUCKET = "travel-sample"  # Change to your bucket name
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
    cluster = Cluster(f"couchbase://{CB_HOST}", ClusterOptions(PasswordAuthenticator(CB_USER, CB_PASS)))
    bucket = cluster.bucket(CB_BUCKET)
    collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)
    print("Successfully connected to Couchbase")
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
    # file_md5 = get_file_md5(EXCEL_FILE)
    # file_name = os.path.basename(EXCEL_FILE)
    # df = pd.read_excel(EXCEL_FILE)
    
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
        
        result = collection.insert(key, record, InsertOptions(timeout=timedelta(seconds=5)))
        print(f"Inserted document with key: {key}, CAS: {result.cas}")
    
    except DocumentExistsException:
        print(f"Document with key {key} already exists. Skipping.")
    except TimeoutException:
        print(f"Timeout occurred while inserting document with key {key}. Retrying...")
        try:
            result = collection.insert(key, record, InsertOptions(timeout=timedelta(seconds=10)))
            print(f"Retry successful. Inserted document with key: {key}, CAS: {result.cas}")
        except Exception as retry_error:
            print(f"Retry failed for key {key}: {str(retry_error)}")
    except ServiceUnavailableException as ne:
        print(f"Service unavailable error occurred while inserting document with key {key}: {str(ne)}")
    except ValueError as ve:
        print(f"Value error for key {key}: {str(ve)}. Skipping this record.")
    except TypeError as te:
        print(f"Type error for key {key}: {str(te)}. Skipping this record.")
    except Exception as e:
        print(f"Unexpected error inserting document with key {key}: {str(e)}")

print("Data insertion complete.")

# Example 1: Handle DocumentNotFoundException - Get a document that doesn't exist
print("\n--- Example 1: Handling DocumentNotFoundException ---")
try:
    result = collection.get("does_not_exist")
    print(f"Document found: {result.content}")
except DocumentNotFoundException as e:
    print(f"Document not found (expected): Key 'does_not_exist' does not exist in the collection")
except CouchbaseException as e:
    print(f"Couchbase error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")

# Example 2: Handle ParsingFailedException - Bad query syntax
print("\n--- Example 2: Handling ParsingFailedException (Bad Query) ---")
try:
    # Intentionally bad query - missing FROM clause
    bad_query = "SELECT * WHERE type = 'airline'"
    result = cluster.query(bad_query)
    for row in result:
        print(row)
except ParsingFailedException as e:
    print(f"Query parsing error (expected): Invalid query syntax")
    print(f"Details: {e}")
except CouchbaseException as e:
    print(f"Couchbase error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")

# Example 3: Handle query with non-existent bucket
print("\n--- Example 3: Handling Query with Non-Existent Bucket ---")
try:
    query = "SELECT * FROM `non_existent_bucket` LIMIT 1"
    result = cluster.query(query)
    for row in result:
        print(row)
except CouchbaseException as e:
    print(f"Query error (expected): Bucket or keyspace not found")
    print(f"Details: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")

# Example 4: Demonstrate proper exception handling with get and default value
print("\n--- Example 4: Get with Default Value Pattern ---")
def get_document_or_default(collection, key, default=None):
    """
    Get a document from Couchbase, return default value if not found.
    """
    try:
        result = collection.get(key)
        return result.content_as[dict]
    except DocumentNotFoundException:
        print(f"Document '{key}' not found, returning default value")
        return default
    except TimeoutException:
        print(f"Timeout getting document '{key}'")
        return default
    except CouchbaseException as e:
        print(f"Couchbase error getting '{key}': {e}")
        return default

# Test the helper function
doc = get_document_or_default(collection, "does_not_exist", {"default": True})
print(f"Returned value: {doc}")

# Example 5: Handle CAS mismatch (optimistic locking)
print("\n--- Example 5: Handling CAS Mismatch (Optimistic Locking) ---")
try:
    from couchbase.exceptions import CASMismatchException
    from couchbase.options import ReplaceOptions
    
    # Create a test document
    test_key = "test_cas_doc"
    test_doc = {"value": "original", "counter": 0}
    
    try:
        collection.upsert(test_key, test_doc)
        print(f"Created test document: {test_key}")
        
        # Get the document with CAS
        result1 = collection.get(test_key)
        cas1 = result1.cas
        
        # Simulate another process updating the document
        collection.replace(test_key, {"value": "updated by another process", "counter": 1})
        
        # Try to update with old CAS (this will fail)
        try:
            collection.replace(
                test_key, 
                {"value": "my update", "counter": 2},
                ReplaceOptions(cas=cas1)
            )
            print("Update succeeded (unexpected)")
        except CASMismatchException:
            print("CAS mismatch detected (expected): Document was modified by another process")
            print("In production, you would typically retry the operation with fresh data")
            
    finally:
        # Cleanup test document
        try:
            collection.remove(test_key)
        except:
            pass
            
except Exception as e:
    print(f"Error in CAS example: {e}")

print("\n--- Exception Handling Examples Complete ---")

# Close the Couchbase connection
if cluster:
    cluster.close()
    print("\nCouchbase connection closed.")