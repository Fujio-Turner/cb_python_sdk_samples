"""
Provides exception handling for various Couchbase operations, including:
- Connecting to the Couchbase cluster
- Upserting documents
- Getting documents
- Performing subdocument operations

The code defines several helper functions to handle common Couchbase exceptions, such as:
- `upsert_document`: Upserts a document with error handling
- `get_airline_by_key`: Gets a document by key with error handling
- `sub_get_airline`: Gets subdocument fields with error handling
- `sub_update_airline`: Updates subdocument fields with error handling

The code also includes a section that demonstrates the usage of these helper functions.
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

# needed for exception handling
from couchbase.exceptions import (
    CouchbaseException,
    TimeoutException,
    AuthenticationException,
    DocumentNotFoundException,
    DocumentExistsException,
    CasMismatchException,
    InvalidArgumentException,
    PathNotFoundException,
    BucketNotFoundException,
    InvalidValueException,
    DocumentLockedException
)

# Update this to your cluster
ENDPOINT = "localhost"
USERNAME = "demo"
PASSWORD = "password"
BUCKET_NAME = "travel-sample"
CB_SCOPE = "inventory"
CB_COLLECTION = "airline"
# User Input ends here.

try:
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

    # get a reference to our collection
    cb_coll = cb.scope(CB_SCOPE).collection(CB_COLLECTION)
except AuthenticationException as e:
    print(f"Authentication error: {e}")
except TimeoutException as e:
    print(f"Timeout error: {e}")
except BucketNotFoundException as e:
    print(f"Bucket not found: {e}")
except CouchbaseException as e:
    print(f"Couchbase error: {e}")

# upsert document function
def upsert_document(key, doc):
    print("\nUpsert CAS: ")
    start_time = time.time()
    try:
        result = cb_coll.upsert(key, doc)
        print(result.cas)
    except DocumentExistsException as e:
        print(f"Document already exists: {e}")
    except InvalidValueException as e:
        print(f"Invalid document value: {e}")
    except TimeoutException as e:
        print(f"Operation timed out: {e}")
    except AuthenticationException as e:
        print(f"Authentication failed: {e}")
    except CouchbaseException as e:
        print(f"Couchbase error occurred: {e}")
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
        print(result.content_as[str])
        print("CAS:", result.cas)
    except DocumentNotFoundException:
        print(f"Document with key '{key}' not found.")
    except TimeoutException:
        print("Operation timed out.")
    except CouchbaseException as e:
        print(f"Couchbase error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Get operation took {execution_time:.6f} seconds")

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
    except DocumentNotFoundException:
        print(f"Document with key '{key}' not found.")
    except PathNotFoundException:
        print("One or more paths in the document were not found.")
    except TimeoutException:
        print("Operation timed out.")
    except InvalidArgumentException:
        print("Invalid argument provided.")
    except CouchbaseException as e:
        print(f"Couchbase error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
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
    except DocumentNotFoundException:
        print(f"Document with key '{key}' not found.")
    except CasMismatchException:
        print("The CAS value provided does not match the current document's CAS.")
    except PathNotFoundException:
        print("One or more paths in the document were not found.")
    except DocumentLockedException:
        print("The document is currently locked.")
    except TimeoutException:
        print("Operation timed out.")
    except InvalidArgumentException:
        print("Invalid argument provided.")
    except CouchbaseException as e:
        print(f"Couchbase error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
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
    "timestamp":time.time()
}

upsert_document(key,doc)
get_airline_by_key(key)
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


'''
Common Couchbase Exceptions
Base Exception

CouchbaseException: The base exception for all Couchbase-specific errors4.

##Network and Timeout Exceptions
TimeoutException: Raised when an operation times out4.

##Authentication Exceptions
AuthenticationException: Indicates an error occurred during user authentication4.

##Document Exceptions
DocumentNotFoundException: Raised when a requested document is not found4.
DocumentExistsException: Indicates that a document already exists when an operation expected it not to4.

##Durability Exceptions
DurabilityImpossibleException: Raised when the requested durability requirements cannot be satisfied4.
DurabilityAmbiguousException: Indicates an ambiguous result for a durable operation4.

##Subdocument Exceptions
PathNotFoundException: Indicates that a specified path in a subdocument operation doesn't exist4.
PathExistsException: Raised when a path in a subdocument operation already exists, but the operation expected it not to4.

##Other Exceptions
CasMismatchException: Raised when there's a conflict in a Compare-And-Swap (CAS) operation4.
TemporaryFailureException: Indicates a temporary failure, suggesting the operation can be retried4.
InvalidArgumentException: Raised when an argument provided to an operation is invalid4.
InternalServerFailureException: Indicates an internal server error4.
'''