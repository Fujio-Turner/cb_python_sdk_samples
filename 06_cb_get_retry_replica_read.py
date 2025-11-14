"""
Demonstrates reading from replica nodes in Couchbase for high availability scenarios.

This script shows how to use replica reads to maintain data availability even when
the active node is unreachable or experiencing timeouts. Replica reads are useful for:
- High availability: Access data even if the active node is down
- Load balancing: Distribute read traffic across replica nodes
- Disaster recovery: Failover to replicas during outages
- Performance: Reduce latency by reading from geographically closer replicas

Key functions demonstrated:
1. get() - Standard read from active node with retry logic
2. get_any_replica() - Read from any available replica (fastest response)
3. get_all_replicas() - Read from all replicas (compare data across nodes)

Note: Replicas may have slightly stale data due to replication lag.
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
except TimeoutException as e:
    print(f"Timeout error: {e}")
except BucketNotFoundException as e:
    print(f"Bucket not found: {e}")
except CouchbaseException as e:
    print(f"Couchbase error: {e}")


# Example 1: Normal get with retry logic
def get_with_retry(key, timeout=5, max_retries=3):
    """
    Get a document with retry logic, falling back to replica read if all retries fail.
    """
    print(f"\n--- Example 1: Get '{key}' with Retry Logic ---")
    start_time = time.time()
    result = None

    for attempt in range(max_retries):
        try:
            result = cb_coll.get(key, timeout=timedelta(seconds=timeout))
            print(f"✓ Successfully retrieved from active node")
            print(f"  Content: {result.content_as[dict]}")
            print(f"  CAS: {result.cas}")
            break
        except DocumentNotFoundException as e:
            print(f"✗ Document not found: {key}")
            break
        except TimeoutException as e:
            if attempt < max_retries - 1:
                print(f"  Attempt {attempt + 1}/{max_retries} timed out, retrying...")
            else:
                print(f"✗ All {max_retries} attempts timed out. Trying replica read...")
                try:
                    result = cb_coll.get_any_replica(key, timeout=timedelta(seconds=timeout))
                    print(f"✓ Successfully retrieved from replica")
                    print(f"  Content: {result.content_as[dict]}")
                    print(f"  CAS: {result.cas}")
                    print(f"  Note: Data may be slightly stale due to replication lag")
                except Exception as replica_error:
                    print(f"✗ Replica read also failed: {replica_error}")
        except CouchbaseException as e:
            print(f"✗ Couchbase error: {e}")
            break

    end_time = time.time()
    print(f"  Total time: {end_time - start_time:.3f}s")
    return result


# Example 2: Read from any replica (fastest response)
def get_any_replica_example(key):
    """
    Get document from any available replica - returns fastest response.
    Useful for load balancing and high availability.
    """
    print(f"\n--- Example 2: Get '{key}' from Any Replica ---")
    start_time = time.time()
    
    try:
        result = cb_coll.get_any_replica(key)
        print(f"✓ Retrieved from replica (fastest available)")
        print(f"  Content: {result.content_as[dict]}")
        print(f"  CAS: {result.cas}")
        print(f"  is_replica: {result.is_replica}")
        end_time = time.time()
        print(f"  Time: {end_time - start_time:.3f}s")
        return result
    except DocumentNotFoundException:
        print(f"✗ Document '{key}' not found in any replica")
    except CouchbaseException as e:
        print(f"✗ Error: {e}")
    
    return None


# Example 3: Read from all replicas (compare across nodes)
def get_all_replicas_example(key):
    """
    Get document from all replicas - useful for comparing data consistency.
    Returns an iterable of results from active + all replica nodes.
    """
    print(f"\n--- Example 3: Get '{key}' from All Replicas ---")
    start_time = time.time()
    
    try:
        results = cb_coll.get_all_replicas(key)
        replica_count = 0
        active_count = 0
        
        for idx, result in enumerate(results, 1):
            if result.is_replica:
                replica_count += 1
                print(f"  Replica {replica_count}:")
            else:
                active_count += 1
                print(f"  Active node:")
            
            print(f"    CAS: {result.cas}")
            print(f"    Content: {result.content_as[dict]}")
        
        total_nodes = active_count + replica_count
        end_time = time.time()
        print(f"✓ Retrieved from {total_nodes} nodes ({active_count} active, {replica_count} replica)")
        print(f"  Total time: {end_time - start_time:.3f}s")
        
    except DocumentNotFoundException:
        print(f"✗ Document '{key}' not found")
    except CouchbaseException as e:
        print(f"✗ Error: {e}")


# Example 4: Simulate timeout scenario with very aggressive timeout
def simulate_timeout_scenario(key):
    """
    Simulate timeout by using extremely short timeout, demonstrating replica fallback.
    """
    print(f"\n--- Example 4: Simulate Timeout with Replica Fallback ---")
    print("  Using 1ms timeout to force timeout and demonstrate replica read")
    
    try:
        # Try with impossibly short timeout to force failure
        result = cb_coll.get(key, timeout=timedelta(milliseconds=1))
        print(f"✓ Unexpectedly succeeded with 1ms timeout")
    except TimeoutException:
        print(f"✗ Expected timeout occurred (1ms timeout)")
        print(f"  Falling back to replica read...")
        try:
            result = cb_coll.get_any_replica(key, timeout=timedelta(seconds=5))
            print(f"✓ Replica read succeeded!")
            print(f"  Content: {result.content_as[dict]}")
            print(f"  is_replica: {result.is_replica}")
        except Exception as e:
            print(f"✗ Replica read also failed: {e}")
    except Exception as e:
        print(f"✗ Unexpected error: {e}")


# Run examples
key = "airline_10"  # Use existing document from travel-sample

print("=" * 70)
print("REPLICA READ EXAMPLES")
print("=" * 70)
print("\nNote: Your cluster must have replicas configured to see replica reads.")
print("Check Couchbase UI > Buckets > travel-sample to verify replica count.")

# Example 1: Standard get with retry and replica fallback
get_with_retry(key, timeout=5, max_retries=3)

# Example 2: Get from any replica (fastest)
get_any_replica_example(key)

# Example 3: Get from all replicas (compare consistency)
get_all_replicas_example(key)

# Example 4: Simulate timeout scenario
simulate_timeout_scenario(key)

print("\n" + "=" * 70)
print("REPLICA READ EXAMPLES COMPLETE")
print("=" * 70)

#Cleanly close the connection to the Couchbase cluster
print("\nClosing connection to Couchbase cluster...")
cluster.close()
print("Connection closed successfully.")