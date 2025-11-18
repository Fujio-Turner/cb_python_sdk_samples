"""
Couchbase Python SDK - Binary Counter Operations (Increment/Decrement)

This script demonstrates how to use binary counter operations in Couchbase:
- increment(): Increment an ASCII counter value
- decrement(): Decrement an ASCII counter value
- Initial values for non-existent counters
- Custom delta values

Counter operations are atomic and useful for maintaining counters without
race conditions.
"""

from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (
    ClusterOptions,
    IncrementOptions,
    DecrementOptions,
    DeltaValue,
    SignedInt64
)
from couchbase.exceptions import (
    CouchbaseException,
    DocumentNotFoundException,
    TimeoutException
)

# Configuration
# For local/self-hosted Couchbase Server:
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"
# For Capella (cloud), uncomment and modify:
# ENDPOINT = "cb.xxxxx.cloud.couchbase.com"
# USERNAME = "your-username"
# PASSWORD = "your-password"

BUCKET_NAME = "travel-sample"
CB_SCOPE = "inventory"
CB_COLLECTION = "airline"


def increment_basic_example(cluster):
    """Basic increment operation with initial value"""
    print("\n--- Example 1: Basic Increment ---")
    
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)
    
    counter_key = "counter-doc"
    
    try:
        result = collection.binary().increment(
            counter_key,
            IncrementOptions(initial=SignedInt64(100))
        )
        print(f"Counter value after increment: {result.content}")
        
        result = collection.binary().increment(counter_key)
        print(f"Counter value after second increment: {result.content}")
        
        collection.remove(counter_key)
        
    except CouchbaseException as e:
        print(f"Error during increment: {e}")


def increment_custom_delta(cluster):
    """Increment with custom delta value"""
    print("\n--- Example 2: Custom Delta Increment ---")
    
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)
    
    counter_key = "counter-doc-delta"
    
    try:
        result = collection.binary().increment(
            counter_key,
            IncrementOptions(initial=SignedInt64(0), delta=DeltaValue(5))
        )
        print(f"Counter value (delta=5): {result.content}")
        
        result = collection.binary().increment(
            counter_key,
            IncrementOptions(delta=DeltaValue(10))
        )
        print(f"Counter value after increment by 10: {result.content}")
        
        collection.remove(counter_key)
        
    except CouchbaseException as e:
        print(f"Error during increment: {e}")


def decrement_example(cluster):
    """Decrement counter operation"""
    print("\n--- Example 3: Decrement ---")
    
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)
    
    counter_key = "counter-doc-decrement"
    
    try:
        result = collection.binary().increment(
            counter_key,
            IncrementOptions(initial=SignedInt64(100))
        )
        print(f"Initial counter value: {result.content}")
        
        result = collection.binary().decrement(counter_key)
        print(f"Counter value after decrement: {result.content}")
        
        result = collection.binary().decrement(
            counter_key,
            DecrementOptions(delta=DeltaValue(5))
        )
        print(f"Counter value after decrement by 5: {result.content}")
        
        collection.remove(counter_key)
        
    except CouchbaseException as e:
        print(f"Error during decrement: {e}")


def counter_exception_handling(cluster):
    """Exception handling for counter operations"""
    print("\n--- Example 4: Exception Handling ---")
    
    bucket = cluster.bucket(BUCKET_NAME)
    collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)
    
    try:
        result = collection.binary().increment("non-existent-counter")
        print(f"Counter value: {result.content}")
        
    except DocumentNotFoundException:
        print("Document not found (no initial value provided)")
        
        result = collection.binary().increment(
            "non-existent-counter",
            IncrementOptions(initial=SignedInt64(0))
        )
        print(f"Counter created with initial value: {result.content}")
        
        collection.remove("non-existent-counter")
        
    except TimeoutException:
        print("Operation timed out")
    except CouchbaseException as e:
        print(f"Couchbase error: {e}")


def main():
    """Main function to demonstrate counter operations"""
    print("Couchbase Binary Counter Operations Demo")
    print("=" * 50)
    
    auth = PasswordAuthenticator(USERNAME, PASSWORD)
    options = ClusterOptions(auth)
    
    # For Capella (cloud), uncomment:
    # options.apply_profile('wan_development')
    # cluster = Cluster(f'couchbases://{ENDPOINT}', options)
    
    cluster = Cluster(f'couchbase://{ENDPOINT}', options)
    
    try:
        cluster.wait_until_ready(timedelta(seconds=10))
        
        increment_basic_example(cluster)
        increment_custom_delta(cluster)
        decrement_example(cluster)
        counter_exception_handling(cluster)
        
        print("\n" + "=" * 50)
        print("All counter operations completed successfully!")
        
    except CouchbaseException as e:
        print(f"Cluster connection error: {e}")
    finally:
        cluster.close()


if __name__ == "__main__":
    main()
