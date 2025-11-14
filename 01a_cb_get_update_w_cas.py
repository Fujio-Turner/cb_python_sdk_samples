"""
Demonstrates a GET with CAS, update with CAS, and handling CAS mismatch in Couchbase using Python SDK.
Shows ordered steps: GET (JSON and CAS), update with CAS (new CAS and updated JSON), failed update with original CAS,
and a commented-out successful update with new CAS.
Increments 'qty' field in each update to show document changes.
"""
from datetime import timedelta
import time

# Needed for cluster connection and exceptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.exceptions import CasMismatchException, DocumentNotFoundException, CouchbaseException

# Update this to your cluster
# For local/self-hosted Couchbase Server:
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

# For Capella (cloud), uncomment and update these instead:
# ENDPOINT = "cb.your-endpoint.cloud.couchbase.com"  # Your Capella hostname
# USERNAME = "your-capella-username"
# PASSWORD = "your-capella-password"

BUCKET_NAME = "travel-sample"
CB_SCOPE = "_default"
CB_COLLECTION = "_default"
# User Input ends here.

# Connect options - authentication
auth = PasswordAuthenticator(USERNAME, PASSWORD)

# Get a reference to our cluster
try:
    options = ClusterOptions(auth)
    
    # For local/self-hosted Couchbase Server:
    cluster = Cluster('couchbase://{}'.format(ENDPOINT), options)
    
    # For Capella (cloud), use this instead (uncomment and comment out the line above):
    # options.apply_profile('wan_development')  # Helps avoid latency issues with Capella
    # cluster = Cluster('couchbases://{}'.format(ENDPOINT), options)  # Note: couchbaseS (secure)

    # Wait until the cluster is ready for use
    print("\nSTEP 0: Connecting to Cluster")
    cluster.wait_until_ready(timedelta(seconds=10))
    print("Cluster connection established")
except CouchbaseException as e:
    print(f"Failed to connect to cluster: {e}")
    exit(1)

# Get a reference to our bucket and collection
try:
    cb = cluster.bucket(BUCKET_NAME)
    cb_coll = cb.scope(CB_SCOPE).collection(CB_COLLECTION)
    print(f"Connected to bucket: {BUCKET_NAME}, scope: {CB_SCOPE}, collection: {CB_COLLECTION}")
except CouchbaseException as e:
    print(f"Failed to connect to bucket or collection: {e}")
    cluster.close()
    exit(1)

# Upsert document function
def upsert_document(key, doc):
    print("\n  - Upsert Document")
    start_time = time.time()
    try:
        result = cb_coll.upsert(key, doc)
        print(f"    Upserted document with key: {key}")
        print(f"    CAS after upsert: {result.cas}")
        return result.cas
    except CouchbaseException as e:
        print(f"    Error during upsert: {e}")
        return None
    finally:
        execution_time = time.time() - start_time
        print(f"    Upsert operation took {execution_time:.6f} seconds")

# Get document function
def get_airline_by_key(key):
    print("\n  - Get Document")
    start_time = time.time()
    try:
        result = cb_coll.get(key)
        content = result.content_as[dict]
        cas = result.cas
        print(f"    Retrieved document with key: {key}")
        print(f"    JSON content: {content}")
        print(f"    CAS: {cas}")
        return content, cas
    except DocumentNotFoundException as e:
        print(f"    Document with key {key} not found: {e}")
        return None, None
    except CouchbaseException as e:
        print(f"    Error during get: {e}")
        return None, None
    finally:
        execution_time = time.time() - start_time
        print(f"    Get operation took {execution_time:.6f} seconds")

# Update document function with CAS
def update_document(key, doc, cas):
    print("\n  - Update Document with CAS")
    start_time = time.time()
    try:
        result = cb_coll.replace(key, doc, cas=cas)
        print(f"    Successfully updated document with new CAS: {result.cas}")
        return True, result.cas
    except CasMismatchException as e:
        print(f"    CAS mismatch: Document was modified by another process")
        print(f"    Failure details: {e}")
        return False, None
    except CouchbaseException as e:
        print(f"    Error during update: {e}")
        return False, None
    finally:
        execution_time = time.time() - start_time
        print(f"    Update operation took {execution_time:.6f} seconds")

# Main logic
key = "airline_8091"
doc = {
    "type": "airline",
    "id": 8091,
    "callsign": "CBS",
    "iata": None,
    "icao": None,
    "qty": 0,
    "name": "Couchbase Airways",
    "timestamp": time.time()
}

# STEP 1: Upsert the initial document
print("\nSTEP 1: Upsert Initial Document")
initial_cas = upsert_document(key, doc)
if initial_cas is None:
    print("Cannot proceed due to failed upsert operation")
    cluster.close()
    exit(1)

# STEP 2: Get the document and its CAS
print("\nSTEP 2: Get Document and CAS")
content, cas = get_airline_by_key(key)
if content is None or cas is None:
    print("Cannot proceed with update due to failed GET operation")
    cluster.close()
    exit(1)

# STEP 3: Update the document with CAS and show new CAS
print("\nSTEP 3: Update Document with CAS")
updated_doc = content.copy()
updated_doc["name"] = "Couchbase Airways Updated"
updated_doc["qty"] = updated_doc.get("qty", 0) + 1  # Increment qty by 1
updated_doc["timestamp"] = time.time()
success, new_cas = update_document(key, updated_doc, cas)
if success:
    print(f"\nSTEP 3 RESULT: Update completed successfully")
    print(f"STEP 3 RESULT: New CAS: {new_cas}")
    # Fetch and print the updated document to show the change
    print("\n  - Get Updated Document")
    updated_content, _ = get_airline_by_key(key)
    if updated_content:
        print(f"STEP 3 RESULT: Updated JSON content: {updated_content}")
else:
    print("\nSTEP 3 RESULT: Update failed")

# STEP 4: Simulate CAS mismatch with the original CAS
print("\nSTEP 4: Simulate CAS Mismatch with Original CAS")
# Modify the document to change its CAS
doc["qty"] = doc.get("qty", 0) + 1  # Increment qty by 1
doc["timestamp"] = time.time()
upsert_document(key, doc)

# Try updating with the original CAS (should fail)
updated_doc["name"] = "Couchbase Airways Failed Update"
updated_doc["qty"] = updated_doc.get("qty", 0) + 1  # Increment qty by 1
success, _ = update_document(key, updated_doc, cas)
if not success:
    print("\nSTEP 4 RESULT: CAS mismatch demonstrated successfully")

'''
STEP 4B: Successful Update with New CAS from STEP 3
# Uncomment this section to demonstrate a successful update using the new CAS from STEP 3
print("\nSTEP 4B: Successful Update with New CAS from STEP 3")
# Use the new CAS from STEP 3
updated_doc["name"] = "Couchbase Airways Second Update"
updated_doc["qty"] = updated_doc.get("qty", 0) + 1  # Increment qty by 1
updated_doc["timestamp"] = time.time()
success, newer_cas = update_document(key, updated_doc, new_cas)
if success:
    print(f"\nSTEP 4B RESULT: Update completed successfully")
    print(f"STEP 4B RESULT: New CAS: {newer_cas}")
    # Fetch and print the updated document to show the change
    print("\n  - Get Updated Document")
    updated_content, _ = get_airline_by_key(key)
    if updated_content:
        print(f"STEP 4B RESULT: Updated JSON content: {updated_content}")
else:
    print("\nSTEP 4B RESULT: Update failed")
'''

# Cleanly close the connection to the Couchbase cluster
print("\nSTEP 5: Closing Connection")
try:
    cluster.close()
    print("Connection closed successfully")
except CouchbaseException as e:
    print(f"Error closing connection: {e}")