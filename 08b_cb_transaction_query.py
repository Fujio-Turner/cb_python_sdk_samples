"""
Demonstrates ACID transactions using SQL++ (N1QL) queries in Couchbase.

This script shows how to perform transactional updates using SQL++ queries
instead of key-value operations, allowing for:
- SQL-based atomic updates across multiple documents
- Query-based transaction logic
- RETURNING clause to verify updates within the transaction

IMPORTANT - Transaction Durability Levels:
============================================
Durability controls how safely transactions are persisted before commit.

Available Levels:
- **None (Default)**: 
  - No durability guarantees
  - Fastest performance (~10ms)
  - Transaction committed in memory only
  - Works without replicas ✅
  - Use for: Development, non-critical data, maximum performance
  
- **MAJORITY**:
  - Transaction replicated to majority of replica nodes
  - **Requires: At least 1 replica configured on bucket** ⚠️
  - Slower (~20-30ms, 2-3x impact)
  - Protects against single node failure
  - Use for: Important data, production workloads
  
- **PERSIST_TO_MAJORITY**:
  - Transaction persisted to disk on majority of nodes
  - **Requires: At least 1 replica configured on bucket** ⚠️
  - Slowest (~50-100ms, 5-10x impact)
  - Survives node crashes and restarts
  - Use for: Critical financial data, audit trails, compliance requirements

Cost/Performance Impact:
- None: ~10ms (baseline) - No replicas needed ✅
- MAJORITY: ~20-30ms (2-3x slower) - Requires replicas
- PERSIST_TO_MAJORITY: ~50-100ms (5-10x slower) - Requires replicas

Performance vs Safety Tradeoff:
- Higher durability = Better data safety + Slower performance
- Lower durability = Faster performance + Risk of data loss on node failure
- Production recommendation: MAJORITY (good balance)

Configuration Example:
```python
from couchbase.durability import Durability
options = TransactionOptions(durability_level=Durability.MAJORITY)
result = cluster.transactions.run(txn_logic, options)
```

This script uses durability_level=None for compatibility with single-node setups.
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

try:
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
except AuthenticationException as e:
    print(f"Authentication error: {e}")
    exit(1)
except TimeoutException as e:
    print(f"Timeout error: {e}")
    exit(1)
except BucketNotFoundException as e:
    print(f"Bucket not found: {e}")
    exit(1)
except CouchbaseException as e:
    print(f"Couchbase error: {e}")
    exit(1)

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
        UPDATE `{BUCKET_NAME}`.`{CB_SCOPE}`.`{CB_COLLECTION}`
        USE KEYS "{key1}"
        SET stuff = stuff - {amount},
            timestamp = "{datetime.now().isoformat(timespec='milliseconds')}"
        RETURNING META().id, stuff
        """
        result1 = ctx.query(query1)
        # TransactionQueryResult needs .rows() to iterate
        for row in result1.rows():
            print(f"Updated {row['id']}: new stuff value = {row['stuff']}")

        # Perform the second update
        query2 = f"""
        UPDATE `{BUCKET_NAME}`.`{CB_SCOPE}`.`{CB_COLLECTION}`
        USE KEYS "{key2}"
        SET stuff = stuff + {amount},
            timestamp = "{datetime.now().isoformat(timespec='milliseconds')}"
        RETURNING META().id, stuff
        """
        result2 = ctx.query(query2)
        # TransactionQueryResult needs .rows() to iterate
        for row in result2.rows():
            print(f"Updated {row['id']}: new stuff value = {row['stuff']}")

    try:
        result: TransactionResult = cluster.transactions.run(txn_logic)
        # TransactionResult is returned on success
        print("Transaction committed successfully")
        print(f"Transaction ID: {result.transaction_id}")

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