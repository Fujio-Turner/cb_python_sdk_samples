"""
Demonstrates asynchronous operations in Couchbase using the acouchbase module.

This script shows how to:
1. Connect to Couchbase cluster asynchronously using a class structure
2. Perform 20 concurrent async upsert operations
3. Perform 20 concurrent async get operations
4. Measure performance benefits of async operations
5. Properly manage async resources with class-based approach

Async operations are beneficial for:
- High-throughput applications
- I/O-bound workloads
- Concurrent operations on multiple documents
- Improved resource utilization
- Better scalability

Key Differences from Sync SDK:
- Import from 'acouchbase' instead of 'couchbase'
- Use 'await' keyword for all operations
- Define functions with 'async def'
- Run with asyncio.run()
- Class-based structure for better organization
"""
import asyncio
import time
from datetime import timedelta

# Async Couchbase imports
from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.exceptions import CouchbaseException, DocumentNotFoundException


class AsyncCouchbaseClient:
    """
    Async Couchbase client for high-performance concurrent operations.
    
    This class encapsulates async connection management and provides
    methods for concurrent document operations.
    """
    
    def __init__(self, endpoint, username, password, bucket_name, scope_name, collection_name):
        """
        Initialize the async Couchbase client.
        
        Args:
            endpoint: Couchbase server endpoint (e.g., "localhost")
            username: Couchbase username
            password: Couchbase password
            bucket_name: Name of the bucket
            scope_name: Name of the scope
            collection_name: Name of the collection
        """
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.bucket_name = bucket_name
        self.scope_name = scope_name
        self.collection_name = collection_name
        
        self.cluster = None
        self.bucket = None
        self.collection = None
    
    async def connect(self, use_tls=False, wan_profile=False):
        """
        Connect to the Couchbase cluster asynchronously.
        
        Args:
            use_tls: Use couchbases:// (for Capella/cloud)
            wan_profile: Apply wan_development profile (for Capella)
        """
        auth = PasswordAuthenticator(self.username, self.password)
        options = ClusterOptions(auth)
        
        # Apply WAN profile for Capella if requested
        if wan_profile:
            options.apply_profile('wan_development')
        
        # Determine connection string
        protocol = 'couchbases' if use_tls else 'couchbase'
        connection_string = f'{protocol}://{self.endpoint}'
        
        # Connect to cluster
        self.cluster = await Cluster.connect(connection_string, options)
        
        # Wait until cluster is ready
        await self.cluster.wait_until_ready(timedelta(seconds=10))
        
        # Get bucket and collection references
        self.bucket = self.cluster.bucket(self.bucket_name)
        self.collection = self.bucket.scope(self.scope_name).collection(self.collection_name)
        
        return self
    
    async def close(self):
        """Close the cluster connection."""
        if self.cluster:
            await self.cluster.close()
    
    async def upsert_documents_concurrent(self, start_id=9000, count=20):
        """
        Upsert multiple documents concurrently using async operations.
        
        Args:
            start_id: Starting ID for generated documents
            count: Number of documents to upsert
        
        Returns:
            List of CAS values from successful upserts
        """
        print(f"\n--- Async Upsert: {count} Documents ---")
        start_time = time.time()
        
        # Create tasks for concurrent upserts
        tasks = []
        for i in range(count):
            doc_id = start_id + i
            key = f"async_airline_{doc_id}"
            doc = {
                "type": "airline",
                "id": doc_id,
                "callsign": f"ASYNC{i:03d}",
                "iata": None,
                "icao": None,
                "name": f"Async Airways #{i}",
                "country": "United States",
                "timestamp": time.time()
            }
            # Create async task for each upsert
            task = self.collection.upsert(key, doc)
            tasks.append((key, task))
        
        # Execute all upserts concurrently
        results = []
        for key, task in tasks:
            try:
                result = await task
                results.append(result.cas)
                print(f"  ✓ Upserted: {key} (CAS: {result.cas})")
            except CouchbaseException as e:
                print(f"  ✗ Failed to upsert {key}: {e}")
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        print(f"\n✓ Upserted {len(results)}/{count} documents in {elapsed:.4f} seconds")
        print(f"  Average: {elapsed/count:.4f} seconds per document")
        print(f"  Throughput: {count/elapsed:.2f} ops/sec")
        
        return results
    
    async def get_documents_concurrent(self, start_id=9000, count=20):
        """
        Retrieve multiple documents concurrently using async operations.
        
        Args:
            start_id: Starting ID for document keys
            count: Number of documents to retrieve
        
        Returns:
            List of retrieved documents
        """
        print(f"\n--- Async Get: {count} Documents ---")
        start_time = time.time()
        
        # Create tasks for concurrent gets
        tasks = []
        for i in range(count):
            doc_id = start_id + i
            key = f"async_airline_{doc_id}"
            # Create async task for each get
            task = self.collection.get(key)
            tasks.append((key, task))
        
        # Execute all gets concurrently
        documents = []
        for key, task in tasks:
            try:
                result = await task
                doc = result.content_as[dict]
                documents.append(doc)
                print(f"  ✓ Retrieved: {key} - {doc['name']}")
            except DocumentNotFoundException:
                print(f"  ✗ Not found: {key}")
            except CouchbaseException as e:
                print(f"  ✗ Failed to get {key}: {e}")
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        print(f"\n✓ Retrieved {len(documents)}/{count} documents in {elapsed:.4f} seconds")
        print(f"  Average: {elapsed/count:.4f} seconds per document")
        print(f"  Throughput: {count/elapsed:.2f} ops/sec")
        
        return documents
    
    async def remove_documents_concurrent(self, start_id=9000, count=20):
        """
        Remove test documents created during the demo.
        
        Args:
            start_id: Starting ID for document keys
            count: Number of documents to remove
        """
        print(f"\n--- Cleanup: Removing {count} Test Documents ---")
        
        tasks = []
        for i in range(count):
            doc_id = start_id + i
            key = f"async_airline_{doc_id}"
            task = self.collection.remove(key)
            tasks.append((key, task))
        
        removed_count = 0
        for key, task in tasks:
            try:
                await task
                removed_count += 1
            except DocumentNotFoundException:
                pass  # Document already doesn't exist
            except CouchbaseException as e:
                print(f"  ✗ Failed to remove {key}: {e}")
        
        print(f"✓ Removed {removed_count} test documents")


async def main():
    """
    Main async function demonstrating concurrent Couchbase operations.
    """
    print("=" * 70)
    print("ASYNC COUCHBASE OPERATIONS DEMO")
    print("=" * 70)
    print("\nThis demo shows the performance benefits of async operations")
    print("by performing 20 concurrent upserts and 20 concurrent gets.\n")
    
    # Configuration
    # For local/self-hosted Couchbase Server:
    ENDPOINT = "localhost"
    USERNAME = "Administrator"
    PASSWORD = "password"
    USE_TLS = False
    WAN_PROFILE = False
    
    # For Capella (cloud), uncomment and update these:
    # ENDPOINT = "cb.your-endpoint.cloud.couchbase.com"
    # USERNAME = "your-capella-username"
    # PASSWORD = "your-capella-password"
    # USE_TLS = True
    # WAN_PROFILE = True
    
    BUCKET_NAME = "travel-sample"
    CB_SCOPE = "inventory"
    CB_COLLECTION = "airline"
    
    # Create async client instance
    client = AsyncCouchbaseClient(
        endpoint=ENDPOINT,
        username=USERNAME,
        password=PASSWORD,
        bucket_name=BUCKET_NAME,
        scope_name=CB_SCOPE,
        collection_name=CB_COLLECTION
    )
    
    try:
        # Connect to Couchbase cluster
        print("Connecting to Couchbase cluster...")
        await client.connect(use_tls=USE_TLS, wan_profile=WAN_PROFILE)
        print("✓ Connected to Couchbase cluster\n")
        
        # Example 1: Async upsert 20 documents concurrently
        await client.upsert_documents_concurrent(start_id=9000, count=20)
        
        # Small delay to ensure documents are persisted
        await asyncio.sleep(0.5)
        
        # Example 2: Async get 20 documents concurrently
        documents = await client.get_documents_concurrent(start_id=9000, count=20)
        
        # Display sample of retrieved documents
        if documents:
            print(f"\nSample retrieved documents:")
            for doc in documents[:3]:  # Show first 3
                print(f"  - {doc['name']} ({doc['callsign']})")
        
        # Example 3: Cleanup - remove test documents
        await client.remove_documents_concurrent(start_id=9000, count=20)
        
        print("\n" + "=" * 70)
        print("ASYNC OPERATIONS COMPLETE")
        print("=" * 70)
        print("\nKey Takeaways:")
        print("  ✓ Async operations execute concurrently")
        print("  ✓ Better performance for I/O-bound operations")
        print("  ✓ Improved resource utilization")
        print("  ✓ Scales well for high-throughput applications")
        print("  ✓ Class-based design provides clean encapsulation")
        
    except CouchbaseException as e:
        print(f"\n✗ Couchbase error: {e}")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
    finally:
        # Always close the cluster connection
        await client.close()
        print("\n✓ Cluster connection closed")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
