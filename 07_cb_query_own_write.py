"""
Upserts a document with the given key and document data in the Couchbase cluster.

Args:
    key (str): The key of the document to be upserted.
    doc (dict): The document data to be upserted.

Returns:
    couchbase.result.MutationResult: The result of the upsert operation.
"""
from datetime import timedelta
import time

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, QueryOptions)
from couchbase.n1ql import QueryScanConsistency

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


# upsert document function
def upsert_document(key,doc):
    print("\nUpsert CAS: ")
    start_time = time.time()
    try:
        result = cb_coll.upsert(key, doc)
        print(result.cas)
        return result
    except Exception as e:
        print(e)
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Upsert operation took {execution_time:.6f} seconds")


key = "airline_8091"
doc = {
    "type": "airline",
    "id": 8091,
    "callsign": "CBS",
    "iata": None,
    "icao": None,
    "faa":"couchbase",
    "name": "Couchbase Airways",
    "timestamp":time.time()
}

upsert_document(key,doc)

# Execute the query
options = QueryOptions(
    named_parameters={"cbKey": key},
    scan_consistency=QueryScanConsistency.REQUEST_PLUS,
    timeout=timedelta(seconds=30)
)
print("Query Results w/USE KEYS:")
try:
    start_time = time.time()
    query_result = cluster.query("SELECT meta().id, meta().cas,name, timestamp FROM `travel-sample`.`inventory`.`airline` USE KEYS[$cbKey]", options)
    end_time = time.time()
    query_time = end_time - start_time
    print(f"Query executed in {query_time:.4f} seconds")
    for row in query_result:
        print(row)
except Exception as e:
    print(f"An error occurred during the query: {e}")

print("Query Results WHERE name = 'Couchbase Airways'")
try:
    start_time = time.time()
    query_result = cluster.query("SELECT meta().id, meta().cas,name, timestamp FROM `travel-sample`.`inventory`.`airline` WHERE name = 'Couchbase Airways'", options)
    end_time = time.time()
    query_time = end_time - start_time
    print(f"Query executed in {query_time:.4f} seconds")
    for row in query_result:
        print(row)
except Exception as e:
    print(f"An error occurred during the query: {e}")
    
#Cleanly close the connection to the Couchbase cluster
print("\nClosing connection to Couchbase cluster...")
cluster.close()
