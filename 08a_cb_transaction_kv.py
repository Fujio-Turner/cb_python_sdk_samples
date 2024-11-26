"""
Provides functions for performing Couchbase transactions, including upsert and move operations.

The `upsert_document` function upserts a document to the specified Couchbase collection. 
It handles various exceptions that may occur during the operation.

The `move_numbers` function performs a Couchbase transaction to move a specified amount
 from one document to another. It uses the Couchbase transactions API to ensure atomicity 
 and consistency of the operation.
"""
from datetime import timedelta
import time
from datetime import datetime

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions)
# needed for subdocument operations
import couchbase.subdocument as SD
# needed for transactions
from couchbase.transactions import TransactionResult
from couchbase.options import TransactionOptions

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
    TransactionFailed,
    TransactionCommitAmbiguous,
    TransactionExpired,
    TransactionOperationFailed
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

def move_numbers(key1, key2, amount):
    print("\nMove numbers: ")
    start_time = time.time()
    try:
        def txn_logic(ctx):
            # Get the documents
            doc1 = ctx.get(cb_coll, key1)
            doc2 = ctx.get(cb_coll, key2)
            
            # Modify the documents
            doc1_content = doc1.content_as[dict]
            doc2_content = doc2.content_as[dict]
            
            doc1_content['stuff'] -= amount
            doc2_content['stuff'] += amount
            
            doc1_content['timestamp'] = datetime.now().isoformat(timespec='milliseconds')
            doc2_content['timestamp'] = datetime.now().isoformat(timespec='milliseconds')
            
            # Replace the documents
            ctx.replace(doc1, doc1_content)
            ctx.replace(doc2, doc2_content)

        # Configure the transaction with lower durability
        options = TransactionOptions(durability_level=None)

        # Run the transaction with the new options
        result = cluster.transactions.run(txn_logic, options)
        if result.is_committed():
            print("Transaction completed successfully")
        else:
            print("Transaction failed to commit")

    except DocumentNotFoundException as e:
        print(f"Document not found: {e}")
    except InvalidValueException as e:
        print(f"Invalid document value: {e}")
    except TimeoutException as e:
        print(f"Operation timed out: {e}")
    except TransactionFailed as ex:
        print(f'Transaction did not reach commit point. Error: {ex}')
    except TransactionCommitAmbiguous as ex:
        print(f'Transaction possibly committed. Error: {ex}')
    except CouchbaseException as e:
        print(f"Couchbase error occurred: {e}")
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Move numbers operation took {execution_time:.6f} seconds")

key1 = "0000:foo"
doc1 = {
    "stuff": 10,
    "timestamp": datetime.now().isoformat(timespec='milliseconds')
}
upsert_document(key1, doc1)

key2 = "0001:foo"
doc2 = {
    "stuff": 2,
    "timestamp": datetime.now().isoformat(timespec='milliseconds')
}
upsert_document(key2, doc2)

move_numbers(key1, key2, 3)