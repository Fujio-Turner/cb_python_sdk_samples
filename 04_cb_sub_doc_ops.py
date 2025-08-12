"""
This module provides functions for interacting with a Couchbase cluster and 
performing subdocument operations on a specific collection.

The `upsert_document` function inserts or updates a document in the specified collection.

The `sub_get_airline` function retrieves the `name` and `timestamp` fields from a 
document in the specified collection.

The `sub_update_airline` function updates the `name` and `timestamp` fields of a document 
in the specified collection, using the provided CAS value to ensure the update is atomic.

The module also includes the necessary setup code to connect to the Couchbase cluster and 
obtain references to the bucket and collection.

###############
#####LIMIT#####
###############
The maximum number of subdocument operations that can be executed in a single MutateIn or 
LookupIn request is 16. This means you can combine up to 16 individual subdocument operations 
(like upsert, insert, remove, get, etc.) within one MutateIn or LookupIn call. If you try 
to perform more than 16 operations in a single request, it will fail.
"""
from datetime import timedelta
import time

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions)
# needed for subdocument operations
import couchbase.subdocument as SD

# Update this to your cluster
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"
BUCKET_NAME = "travel-sample"
CB_SCOPE = "inventory"
CB_COLLECTION = "airline"
# User Input ends here.

# Connect options - authentication
auth = PasswordAuthenticator(USERNAME, PASSWORD)

# get a reference to our cluster
options = ClusterOptions(auth)
# Sets a pre-configured profile called "wan_development" to help avoid latency issues
# when accessing Capella from a different Wide Area Network
# or Availability Zone(e.g. your laptop).
options.apply_profile('wan_development')
cluster = Cluster('couchbases://{}'.format(ENDPOINT), options)

# Wait until the cluster is ready for use.
cluster.wait_until_ready(timedelta(seconds=10))

# get a reference to our bucket
cb = cluster.bucket(BUCKET_NAME)

# get a reference to our bucket using thedefault collection
#cb_coll = cb.default_collection()

# get a reference to our collection
cb_coll = cb.scope(CB_SCOPE).collection(CB_COLLECTION)

# upsert document function
def upsert_document(key, doc):
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

# subdocument get function for name and timestamp
def sub_get_airline(key):
    print("\nGet Result: ")
    start_time = time.time()
    try:
        result = cb_coll.lookup_in(key, [
            SD.get("name"),
            SD.get("timestamp")
        ])
        print("Name:", result.content_as[str](0))
        print("Timestamp:", result.content_as[str](1))
        print("CAS:", result.cas)
        return result.cas
    except Exception as e:
        print(e)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Get operation took {execution_time:.6f} seconds")

# subdocument update function for name and timestamp
def sub_update_airline(key, cbCas, data):
    print("\nSubDoc Update w/CAS Result: ")
    start_time = time.time()
    try:
        result = cb_coll.mutate_in(key, [
            SD.upsert("name", data["name"]),
            SD.upsert("timestamp", data["timestamp"]),
        ], cas=cbCas)
        print(result.cas)
    except Exception as e:
        print(e)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Update operation took {execution_time:.6f} seconds")

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

#insert document a first time
upsert_document(key, doc)
#get document and update name and timestamp and return cas
cbCas = sub_get_airline(key)

data = {}
data["name"] = "Couchbase Airways International"
data["timestamp"] = time.time()
#update document with new name and timestamp with cas
sub_update_airline(key, cbCas, data)

#Cleanly close the connection to the Couchbase cluster
print("\nClosing connection to Couchbase cluster...")
cluster.close()
print("Connection closed successfully.")