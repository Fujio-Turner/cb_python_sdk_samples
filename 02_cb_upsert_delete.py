"""
This module provides functions to upsert and delete documents in a Couchbase database.

The `upsert_document` function takes a key and a document dictionary, and upserts
the document to the specified Couchbase collection. It prints the CAS
(compare-and-swap) value of the upserted document and the execution time of the
operation.

The `delete_airline_by_key` function takes a key and deletes the corresponding
document from the specified Couchbase collection. It prints the CAS value of the
deleted document and the execution time of the operation.

The module also includes the necessary setup code to connect to a Couchbase
cluster and obtain references to the bucket and collection.
"""
from datetime import timedelta
import time

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import ClusterOptions

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
CB_SCOPE = "inventory"
CB_COLLECTION = "airline"
# User Input ends here.

# Connect options - authentication
auth = PasswordAuthenticator(USERNAME, PASSWORD)

# get a reference to our cluster
options = ClusterOptions(auth)

# For local/self-hosted Couchbase Server:
cluster = Cluster('couchbase://{}'.format(ENDPOINT), options)

# For Capella (cloud), use this instead (uncomment and comment out the line above):
# options.apply_profile('wan_development')  # Helps avoid latency issues with Capella
# cluster = Cluster('couchbases://{}'.format(ENDPOINT), options)  # Note: couchbaseS (secure)

# Wait until the cluster is ready for use.
cluster.wait_until_ready(timedelta(seconds=10))

# get a reference to our bucket
cb = cluster.bucket(BUCKET_NAME)

# get a reference to our collection
cb_coll = cb.scope(CB_SCOPE).collection(CB_COLLECTION)

def upsert_document(key, doc):
    """Upsert a document to the collection."""
    print("\nUpsert CAS: ")
    start_time = time.time()
    try:
        result = cb_coll.upsert(key, doc)
        print(result.cas)
    except Exception as e:
        print(e)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Upsert operation took {execution_time:.6f} seconds")

def delete_airline_by_key(key):
    """Delete a document from the collection by key."""
    print("\nDelete Result: ")
    start_time = time.time()
    try:
        result = cb_coll.remove(key)
        print(f"Document with key '{key}' deleted successfully")
        print("CAS:", result.cas)
    except Exception as e:
        print(e)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Delete operation took {execution_time:.6f} seconds")

key = "airline_8091"
doc = {
    "type": "airline",
    "id": 8091,
    "callsign": "CBS",
    "iata": None,
    "icao": None,
    "name": "Couchbase Airways",
    "timestamp": time.time()
}

upsert_document(key, doc)
delete_airline_by_key(key)

# Cleanly close the connection to the Couchbase cluster
print("\nClosing connection to Couchbase cluster...")
cluster.close()
print("Connection closed successfully.")