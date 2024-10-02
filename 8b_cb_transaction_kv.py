"""
Performs a Couchbase transaction to move a numeric value from one document to another.

The `move_numbers_n1ql` function performs a Couchbase transaction to update two 
documents. It decrements the `stuff` field in one document by the specified `amount`,
 and increments the `stuff` field in another document by the same `amount`. The function
 uses N1QL queries to perform the updates and returns the updated `stuff` values for each document.

The function also updates the `timestamp` field in both documents to the current time.

If the transaction is successful, the function prints a message indicating that the 
transaction was committed successfully. If the transaction fails, the function prints 
an error message indicating the reason for the failure.

Args:
    key1 (str): The key of the first document to be updated.
    key2 (str): The key of the second document to be updated.
    amount (int): The amount to be moved from the first document to the second document.

Raises:
    DocumentNotFoundException: If either of the documents specified by `key1` or `key2` does not exist.
    InvalidValueException: If the document value is invalid.
    TimeoutException: If the operation times out.
    TransactionFailed: If the transaction fails to reach the commit point.
    TransactionCommitAmbiguous: If the transaction possibly committed, but the outcome is ambiguous.
    CouchbaseException: If a general Couchbase error occurs.
"""
from datetime import timedelta
import time
from datetime import datetime

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, QueryOptions)
# needed for subdocument operations
import couchbase.subdocument as SD
# needed for transactions
from couchbase.transactions import TransactionResult


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

def move_numbers_n1ql(key1, key2, amount):
    print("\nMove numbers (N1QL): ")
    start_time = time.time()

    def txn_logic(ctx):
        # Perform the first update
        query1 = f"""
        UPDATE `{bucket_name}`.`{cb_scope}`.`{cb_collection}`
        USE KEYS "{key1}"
        SET stuff = stuff - {amount},
            timestamp = "{datetime.now().isoformat(timespec='milliseconds')}"
        RETURNING META().id, stuff
        """
        result1 = ctx.query(query1)
        for row in result1:
            print(f"Updated {row['id']}: new stuff value = {row['stuff']}")

        # Perform the second update
        query2 = f"""
        UPDATE `{bucket_name}`.`{cb_scope}`.`{cb_collection}`
        USE KEYS "{key2}"
        SET stuff = stuff + {amount},
            timestamp = "{datetime.now().isoformat(timespec='milliseconds')}"
        RETURNING META().id, stuff
        """
        result2 = ctx.query(query2)
        for row in result2:
            print(f"Updated {row['id']}: new stuff value = {row['stuff']}")

    try:
        result: TransactionResult = cluster.transactions.run(txn_logic)
        
        if result.is_committed():
            print("Transaction committed successfully")
        else:
            print("Transaction did not commit")

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

move_numbers_n1ql(key1, key2, 3)