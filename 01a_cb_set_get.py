"""
Upserts a document to the specified Couchbase collection and retrieves the document by its key.

The `upsert_document` function takes a key and a document dictionary as input, and upserts 
the document to the specified Couchbase collection. It prints the CAS (Compare-And-Swap) 
value of the upserted document and the time taken to perform the operation.

The `get_airline_by_key` function takes a key as input, and retrieves the document 
from the specified Couchbase collection by its key. It prints the content of the 
retrieved document and its CAS value, as well as the time taken to perform the operation.

The code also demonstrates how to connect to a Couchbase cluster, get a reference to a 
bucket and collection, and how to cleanly close the connection to the cluster.
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

# get a reference to our bucket using the default collection
# cb_coll = cb.default_collection()

# get a reference to our collection
cb_coll = cb.scope(CB_SCOPE).collection(CB_COLLECTION)

# upsert document function
def upsert_document(key, doc):
    print("\nUpsert CAS: ")
    start_time = time.time()
    try:
        # Example of upsert operation with TTL of 1 hour
        # result = cb_coll.upsert(key, doc, expiry=timedelta(hours=1))
        result = cb_coll.upsert(key, doc)
        print(result.cas)
    except Exception as e:
        print(e)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Upsert operation took {execution_time:.6f} seconds")

# get document function
def get_airline_by_key(key):
    print("\nGet Result: ")
    start_time = time.time()
    try:
        result = cb_coll.get(key)
        print(result.content_as[dict])
        print("CAS:", result.cas)
    except Exception as e:
        print(e)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Get operation took {execution_time:.6f} seconds")

# query for new document by callsign

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
get_airline_by_key(key)

# Cleanly close the connection to the Couchbase cluster
print("\nClosing connection to Couchbase cluster...")
cluster.close()
print("Connection closed successfully.")