"""
Connects to a Couchbase cluster, retrieves data from the "travel-sample" bucket, and 
executes a SQL++ (N1QL) query to find all airlines from the "France" country.

The code demonstrates the following:
- Connecting to a Couchbase cluster using the Couchbase Python SDK
- Retrieving a reference to a specific bucket, scope, and collection
- Executing a SQL++ (N1QL) query with a parameter
- Handling query exceptions and printing the query execution time

The code also includes a section with detailed documentation on the various options 
available for the `Cluster.query()` method, which can be used to customize the behavior 
of SQL++ (N1QL) queries.
"""
from datetime import timedelta
import time
import json

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.n1ql import QueryProfile

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
#cb_coll = cb.default_collection()

# get a reference to our collection
cb_coll = cb.scope(CB_SCOPE).collection(CB_COLLECTION)

# Define the country variable
country = "France"

# Execute the query
print("Query Results:")
try:
    start_time = time.time()
    query_result = cluster.query(
        "SELECT meta().id , country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country", 
        QueryOptions(named_parameters={"country": country})
    )
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



'''
The Couchbase Python SDK provides several options you can use with the `Cluster.query()` 
method to customize your SQL++ (N1QL) queries. Here are the main options available 
through the `QueryOptions` class:

## Cluster.query() Options

1. **named_parameters**
   - Allows you to specify named parameters for your query.
   ```python
   QueryOptions(named_parameters={"type": "hotel"})
   ```

2. **positional_parameters**
   - Lets you provide positional parameters for your query.
   ```python
   QueryOptions(positional_parameters=["hotel"])
   ```

3. **scan_consistency**
   - Controls the consistency of the query results.
   ```python
   QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
   ```
Couchbase Scan Consistency Options
NOT_BOUNDED (Default)
    Provides the fastest performance but with no consistency guarantees.
    The query uses whatever data is available in the index at the time of query execution.
    May not reflect recent mutations.

REQUEST_PLUS
    Ensures that the query results include all data changes made prior to the query execution.
    Waits for the index to be updated with all mutations that occurred before the query was issued.
    Provides strong consistency but may have higher latency.

STATEMENT_PLUS
    Similar to REQUEST_PLUS, but applies consistency on a per-statement basis in a query batch.
    Useful in multi-statement queries where consistency is needed for each individual statement.

SCAN_VECTORS
    Provides fine-grained control over consistency using scan vectors.
    Allows specifying exact consistency requirements for each partition.
    Advanced option, typically used in specialized scenarios.

4. **client_context_id**
   - Sets a client context ID for the query.
   ```python
   QueryOptions(client_context_id="my-context-id")
   ```

5. **metrics**
   - Enables or disables query metrics.
   ```python
   QueryOptions(metrics=True)
   ```

6. **readonly**
   - Specifies if the query is read-only.
   ```python
   QueryOptions(readonly=True)
   ```

7. **adhoc**
   - Indicates if the query is ad-hoc or prepared.
   ```python
   QueryOptions(adhoc=False)
   ```

8. **timeout**
   - Sets a timeout for the query execution.
   ```python
   QueryOptions(timeout=timedelta(seconds=30))
   ```

9. **profile**
   - Enables query profiling.
   ```python
   QueryOptions(profile=QueryProfile.TIMINGS)
   ```

10. **max_parallelism**
    - Sets the maximum parallelism for the query.
    ```python
    QueryOptions(max_parallelism=4)
    ```

11. **scan_cap**
    - Specifies the scan cap for the query.
    ```python
    QueryOptions(scan_cap=10000)
    ```

12. **pipeline_batch**
    - Sets the pipeline batch size.
    ```python
    QueryOptions(pipeline_batch=100)
    ```

13. **pipeline_cap**
    - Specifies the pipeline cap.
    ```python
    QueryOptions(pipeline_cap=1000)
    ```

14. **raw**
    - Allows setting raw query options.
    ```python
    QueryOptions(raw={"option_name": "value"})
    ```

15. **Read from Replica Index**

The `read_from_replica` option allows you to specify how queries should read data from 
replica indexes. This can be useful for improving read performance and availability.

Example usage:


from couchbase.options import QueryOptions
from couchbase.n1ql import ReadFromReplica

result = cluster.query(
    "SELECT * FROM `bucket` WHERE type = 'hotel'",
    QueryOptions(read_from_replica=ReadFromReplica.ANY)
)


ReadFromReplica Options:
- NONE: Default behavior. Only reads from active indexes and data.
- ANY: Allows reading from any available replica or active index/data.
- ALL: Reads from all available replicas and active indexes/data, comparing results.

Considerations:
- Using replicas may return slightly stale data due to replication lag.
- The ALL option provides the strongest consistency but at the cost of performance.
- This feature is best used for read-heavy workloads or scenarios where some nodes might be unavailable.

You can combine multiple options in a single `QueryOptions` instance:

```python
options = QueryOptions(
    named_parameters={"type": "hotel"},
    scan_consistency=QueryScanConsistency.REQUEST_PLUS,
    metrics=True,
    timeout=timedelta(seconds=30)
)

result = cluster.query("SELECT * FROM `travel-sample` WHERE type = $type LIMIT 10", options)
```

These options allow you to fine-tune your queries for performance, consistency, and 
specific use cases. Always refer to the latest Couchbase Python SDK documentation for 
the most up-to-date list of available options and their usage[1].

'''