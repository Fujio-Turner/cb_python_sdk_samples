"""
Retrieves an airline document from a Couchbase cluster, with retry logic and replica read support.

This function takes a document key as input and attempts to retrieve the corresponding 
document from the Couchbase cluster. If the initial retrieval fails due to a timeout, 
it will retry up to 3 times. If all retries fail, it will attempt to retrieve the document from a replica.

Args:
    key (str): The key of the document to retrieve.
    timeout (int): The timeout in seconds for the document retrieval operation.

Returns:
    couchbase.result.GetResult: The retrieved document, or None if the document could not be found.
"""
from datetime import timedelta
import time

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions)

# needed for exception handling
from couchbase.exceptions import (
    CouchbaseException,
    TimeoutException,
    AuthenticationException,
    DocumentNotFoundException,
    BucketNotFoundException
)

# Update this to your cluster
ENDPOINT = "localhost"
USERNAME = "Administrator"
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


# get document function
def get_airline_by_key(key, timeout=5):
    print("\nGet Result: ")
    start_time = time.time()
    retries = 3
    result = None

    for attempt in range(retries + 1):
        try:
            result = cb_coll.get(key, timeout=timedelta(seconds=timeout))
            print(result.content_as[str])
            print("CAS:", result.cas)
            break
        except DocumentNotFoundException as e:
            print(f"Document not found: {e}")
            break
        except (TimeoutException) as e:
            if attempt < retries:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
            else:
                print(f"All {retries} attempts failed. Trying replica read.")
                try:
                    result = cb_coll.get_any_replica(key, timeout=timedelta(seconds=timeout))
                    print("Replica read result:")
                    print(result.content_as[str])
                    print("CAS:", result.cas)
                except DocumentNotFoundException as e:
                    print(f"Document not found in any replica: {e}")
                except TimeoutException as e:
                    print(f"Replica read timed out: {e}")
                except CouchbaseException as e:
                    print(f"Couchbase error during replica read: {e}")
        except CouchbaseException as e:
            print(f"Couchbase error: {e}")
            break
    else:
        print("All attempts failed, including replica read.")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Get operation took {execution_time:.6f} seconds")
    return result

# query for new document by callsign

key = "airline_8091"
# get document function with timeout set to 5 seconds
get_airline_by_key(key,5)

#Cleanly close the connection to the Couchbase cluster
print("\nClosing connection to Couchbase cluster...")
cluster.close()
print("Connection closed successfully.")