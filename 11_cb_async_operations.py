"""
Demonstrates asynchronous operations in Couchbase with observability features.

This script shows how to:
1. Create a reusable async Couchbase client class
2. Define individual async methods for upsert, get, and remove
3. Perform multiple concurrent operations using asyncio.gather()
4. Enable performance tracking with timing metrics
5. Configure slow operations logging and orphan response tracking
6. Toggle debug output with a global flag

Async operations are beneficial for:
- High-throughput applications
- I/O-bound workloads
- Concurrent operations on multiple documents
- Improved resource utilization
- Better scalability

WHAT TO EXPECT WHEN YOU RUN THIS:
==================================
With DEBUG=True, you'll see:
1. INFO logs: Connection status, batch summaries, timing
2. WARNING logs: Connection attempts to unavailable ports (normal for localhost)
3. Performance metrics: Throughput (ops/sec) for each example
4. Final metrics JSON: Operation percentiles and counts (p50, p90, p99, p100)
5. Slow operations: JSON output if any operation exceeds thresholds
6. Orphaned responses: Logged if requests timeout (10s intervals)

The final "Metrics" JSON shows:
- "percentiles_us": Operation latency at 50th, 90th, 99th, 100th percentiles (microseconds)
- "total_count": Number of operations performed
- Operations grouped by service: kv (get/upsert/remove), query, search, management

Observability Features:
- Operation timing with DEBUG flag
- Slow operations threshold logging (JSON output)
- Orphaned response tracking (timeout detection)
- Performance metrics per operation
- Exponential backoff retry for transient failures (timeouts, server errors)
- Configurable retry attempts and backoff intervals

Retry Strategy:
- Attempt 1: 0.1s delay (100ms)
- Attempt 2: 0.2s delay (200ms)
- Attempt 3: 0.4s delay (400ms)
- Formula: delay = initial_backoff * (2 ** attempt)
- Retries: Timeouts, ServiceUnavailable, InternalServerFailure
- No Retry: DocumentNotFound (not a transient error)

Learn More:
- Slow Operations Logging: https://docs.couchbase.com/python-sdk/current/howtos/slow-operations-logging.html
- Orphaned Response Tracking: https://docs.couchbase.com/python-sdk/current/howtos/observability-orphan-logger.html
- Async Operations: https://docs.couchbase.com/sdk-api/couchbase-python-client/acouchbase_api/acouchbase_core.html
"""
import asyncio
import time
import logging
import sys
from datetime import timedelta

# Async Couchbase imports
from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, ClusterTracingOptions
from couchbase.exceptions import (
    CouchbaseException,
    DocumentNotFoundException,
    DocumentExistsException,
    TimeoutException,
    AmbiguousTimeoutException,
    UnAmbiguousTimeoutException,
    AuthenticationException,
    BucketNotFoundException,
    ServiceUnavailableException,
    InternalServerFailureException
)
import couchbase

# Global debug flag - set to True to enable detailed timing and logging
DEBUG = True


# Configure logging for Couchbase operations
if DEBUG:
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='%(levelname)s::%(asctime)s::%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger()
    couchbase.configure_logging(logger.name, level=logger.level)
else:
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger()


class AsyncCouchbaseClient:
    """
    Async Couchbase client with built-in observability and performance tracking.
    
    Features:
    - Individual async methods for each operation type
    - Optional timing metrics controlled by DEBUG flag
    - Exponential backoff retry for transient failures
    - Comprehensive exception handling
    - Slow operations and orphan response logging
    """
    
    def __init__(self, endpoint, username, password, bucket_name, scope_name, collection_name, max_retries=3, initial_backoff=0.1):
        """
        Initialize the async Couchbase client.
        
        Args:
            endpoint: Couchbase server endpoint (e.g., "localhost")
            username: Couchbase username
            password: Couchbase password
            bucket_name: Name of the bucket
            scope_name: Name of the scope
            collection_name: Name of the collection
            max_retries: Maximum retry attempts for transient failures (default: 3)
            initial_backoff: Initial backoff delay in seconds (default: 0.1s)
        """
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.bucket_name = bucket_name
        self.scope_name = scope_name
        self.collection_name = collection_name
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        
        self.cluster = None
        self.bucket = None
        self.collection = None
    
    async def connect(self, use_tls=False, wan_profile=False):
        """
        Connect to the Couchbase cluster asynchronously with observability features.
        
        Args:
            use_tls: Use couchbases:// (for Capella/cloud)
            wan_profile: Apply wan_development profile (for Capella)
        
        Raises:
            AuthenticationException: If credentials are invalid
            TimeoutException: If connection times out
            BucketNotFoundException: If bucket doesn't exist
            ServiceUnavailableException: If cluster is unreachable
        """
        if DEBUG:
            logger.info(f"Connecting to Couchbase cluster at {self.endpoint}...")
        
        start_time = time.time() if DEBUG else None
        
        try:
            auth = PasswordAuthenticator(self.username, self.password)
            
            # Configure tracing options for slow operations and orphan logging
            tracing_opts = ClusterTracingOptions(
                # Slow operations thresholds
                tracing_threshold_kv=timedelta(milliseconds=100),
                tracing_threshold_query=timedelta(milliseconds=500),
                tracing_threshold_search=timedelta(milliseconds=500),
                tracing_threshold_analytics=timedelta(seconds=1),
                tracing_threshold_view=timedelta(seconds=1),
                tracing_threshold_queue_size=10,
                
                # Orphaned response logging
                tracing_orphaned_queue_flush_interval=timedelta(seconds=10),
                tracing_orphaned_queue_size=10
            )
            
            options = ClusterOptions(
                authenticator=auth,
                tracing_options=tracing_opts
            )
            
            if wan_profile:
                options.apply_profile('wan_development')
            
            protocol = 'couchbases' if use_tls else 'couchbase'
            connection_string = f'{protocol}://{self.endpoint}'
            
            self.cluster = await Cluster.connect(connection_string, options)
            await self.cluster.wait_until_ready(timedelta(seconds=10))
            
            self.bucket = self.cluster.bucket(self.bucket_name)
            self.collection = self.bucket.scope(self.scope_name).collection(self.collection_name)
            
            if DEBUG:
                elapsed = time.time() - start_time
                logger.info(f"✓ Connected to cluster in {elapsed:.4f}s")
                logger.info(f"  Slow operations logging enabled (KV > 100ms, Query > 500ms)")
                logger.info(f"  Orphaned response tracking enabled (10s intervals)")
            
            return self
            
        except AuthenticationException as e:
            logger.error(f"✗ Authentication failed: Invalid credentials")
            raise
        except BucketNotFoundException as e:
            logger.error(f"✗ Bucket '{self.bucket_name}' not found")
            raise
        except TimeoutException as e:
            logger.error(f"✗ Connection timeout - cluster may be unreachable")
            raise
        except ServiceUnavailableException as e:
            logger.error(f"✗ Service unavailable - check cluster status")
            raise
    
    async def close(self):
        """Close the cluster connection."""
        if self.cluster:
            if DEBUG:
                logger.info("Closing cluster connection...")
            await self.cluster.close()
            if DEBUG:
                logger.info("✓ Cluster connection closed")
    
    async def upsert_document(self, key, document, enable_retry=True):
        """
        Upsert a single document asynchronously with exponential backoff retry.
        
        Args:
            key: Document key
            document: Document content (dict)
            enable_retry: Enable retry with exponential backoff (default: True)
        
        Returns:
            Result with CAS value or None on error
        """
        start_time = time.time() if DEBUG else None
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                result = await self.collection.upsert(key, document)
                
                if DEBUG:
                    elapsed = time.time() - start_time
                    retry_info = f" (after {attempt} retries)" if attempt > 0 else ""
                    logger.debug(f"Upsert {key}{retry_info}: {elapsed:.4f}s (CAS: {result.cas})")
                
                return result
                
            except (TimeoutException, AmbiguousTimeoutException, UnAmbiguousTimeoutException) as e:
                last_exception = e
                
                if enable_retry and attempt < self.max_retries:
                    backoff_delay = self.initial_backoff * (2 ** attempt)
                    if DEBUG:
                        logger.warning(f"Timeout upserting {key}, retry {attempt + 1} in {backoff_delay:.2f}s")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    if DEBUG:
                        elapsed = time.time() - start_time
                        logger.warning(f"Timeout upserting {key} after {elapsed:.4f}s and {attempt} retries")
                    print(f"  ✗ Timeout upserting {key} after {attempt} retries")
                    return None
                    
            except (ServiceUnavailableException, InternalServerFailureException) as e:
                last_exception = e
                
                if enable_retry and attempt < self.max_retries:
                    backoff_delay = self.initial_backoff * (2 ** attempt)
                    if DEBUG:
                        logger.warning(f"Transient error upserting {key}, retry {attempt + 1} in {backoff_delay:.2f}s")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    logger.error(f"Server error upserting {key} after {attempt} retries")
                    print(f"  ✗ Server error upserting {key}")
                    return None
                    
            except CouchbaseException as e:
                logger.error(f"Upsert failed for {key}: {e}")
                print(f"  ✗ Upsert failed for {key}: {e}")
                return None
        
        return None
    
    async def get_document(self, key, enable_retry=True):
        """
        Get a single document asynchronously with exponential backoff retry.
        
        Args:
            key: Document key
            enable_retry: Enable retry with exponential backoff (default: True)
        
        Returns:
            Document content (dict) or None if not found
        """
        start_time = time.time() if DEBUG else None
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                result = await self.collection.get(key)
                
                if DEBUG:
                    elapsed = time.time() - start_time
                    retry_info = f" (after {attempt} retries)" if attempt > 0 else ""
                    logger.debug(f"Get {key}{retry_info}: {elapsed:.4f}s")
                
                return result.content_as[dict]
                
            except DocumentNotFoundException:
                # Document doesn't exist - don't retry
                if DEBUG:
                    elapsed = time.time() - start_time
                    logger.debug(f"Document not found {key} ({elapsed:.4f}s)")
                print(f"  ✗ Document not found: {key}")
                return None
                
            except (TimeoutException, AmbiguousTimeoutException, UnAmbiguousTimeoutException) as e:
                last_exception = e
                
                if enable_retry and attempt < self.max_retries:
                    backoff_delay = self.initial_backoff * (2 ** attempt)
                    if DEBUG:
                        logger.warning(f"Timeout getting {key}, retry {attempt + 1} in {backoff_delay:.2f}s")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    if DEBUG:
                        elapsed = time.time() - start_time
                        logger.warning(f"Timeout getting {key} after {elapsed:.4f}s and {attempt} retries")
                    print(f"  ✗ Timeout getting {key} after {attempt} retries")
                    return None
                    
            except (ServiceUnavailableException, InternalServerFailureException) as e:
                last_exception = e
                
                if enable_retry and attempt < self.max_retries:
                    backoff_delay = self.initial_backoff * (2 ** attempt)
                    if DEBUG:
                        logger.warning(f"Transient error getting {key}, retry {attempt + 1} in {backoff_delay:.2f}s")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    logger.error(f"Server error getting {key} after {attempt} retries")
                    print(f"  ✗ Server error getting {key}")
                    return None
                    
            except CouchbaseException as e:
                logger.error(f"Get failed for {key}: {e}")
                print(f"  ✗ Get failed for {key}: {e}")
                return None
        
        return None
    
    async def remove_document(self, key, enable_retry=True):
        """
        Remove a single document asynchronously with exponential backoff retry.
        
        Args:
            key: Document key
            enable_retry: Enable retry with exponential backoff (default: True)
        
        Returns:
            True if removed, False otherwise
        """
        start_time = time.time() if DEBUG else None
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                await self.collection.remove(key)
                
                if DEBUG:
                    elapsed = time.time() - start_time
                    retry_info = f" (after {attempt} retries)" if attempt > 0 else ""
                    logger.debug(f"Remove {key}{retry_info}: {elapsed:.4f}s")
                
                return True
                
            except DocumentNotFoundException:
                # Document doesn't exist - don't retry
                if DEBUG:
                    logger.debug(f"Document not found (already removed): {key}")
                return False
                
            except (TimeoutException, AmbiguousTimeoutException, UnAmbiguousTimeoutException) as e:
                last_exception = e
                
                if enable_retry and attempt < self.max_retries:
                    backoff_delay = self.initial_backoff * (2 ** attempt)
                    if DEBUG:
                        logger.warning(f"Timeout removing {key}, retry {attempt + 1} in {backoff_delay:.2f}s")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    if DEBUG:
                        elapsed = time.time() - start_time
                        logger.warning(f"Timeout removing {key} after {elapsed:.4f}s and {attempt} retries")
                    print(f"  ✗ Timeout removing {key} after {attempt} retries")
                    return False
                    
            except (ServiceUnavailableException, InternalServerFailureException) as e:
                last_exception = e
                
                if enable_retry and attempt < self.max_retries:
                    backoff_delay = self.initial_backoff * (2 ** attempt)
                    if DEBUG:
                        logger.warning(f"Transient error removing {key}, retry {attempt + 1} in {backoff_delay:.2f}s")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    logger.error(f"Server error removing {key} after {attempt} retries")
                    print(f"  ✗ Server error removing {key}")
                    return False
                    
            except CouchbaseException as e:
                logger.error(f"Remove failed for {key}: {e}")
                print(f"  ✗ Remove failed for {key}: {e}")
                return False
        
        return False


async def demo_concurrent_operations(client):
    """
    Demonstrate various concurrent operation patterns with timing.
    
    Args:
        client: AsyncCouchbaseClient instance
    """
    
    # Example 1: Concurrent upserts
    print("\n" + "=" * 70)
    print("EXAMPLE 1: 20 Concurrent Upserts")
    print("=" * 70)
    
    overall_start = time.time()
    
    # Create 20 documents to upsert
    upsert_tasks = []
    for i in range(20):
        doc_id = 9000 + i
        key = f"async_airline_{doc_id}"
        doc = {
            "type": "airline",
            "id": doc_id,
            "callsign": f"ASYNC{i:03d}",
            "name": f"Async Airways #{i}",
            "country": "United States",
            "timestamp": time.time()
        }
        upsert_tasks.append(client.upsert_document(key, doc))
    
    # Execute all upserts concurrently
    results = await asyncio.gather(*upsert_tasks)
    successful = sum(1 for r in results if r is not None)
    
    elapsed = time.time() - overall_start
    print(f"\n✓ Completed {successful}/20 upserts in {elapsed:.4f}s")
    print(f"  Throughput: {20/elapsed:.2f} ops/sec")
    if DEBUG:
        logger.info(f"Upsert batch completed: {successful}/20 successful, {elapsed:.4f}s total")
    
    # Small delay to ensure persistence
    await asyncio.sleep(0.5)
    
    
    # Example 2: Concurrent gets
    print("\n" + "=" * 70)
    print("EXAMPLE 2: 20 Concurrent Gets")
    print("=" * 70)
    
    overall_start = time.time()
    
    # Create 20 get tasks
    get_tasks = []
    for i in range(20):
        doc_id = 9000 + i
        key = f"async_airline_{doc_id}"
        get_tasks.append(client.get_document(key))
    
    # Execute all gets concurrently
    documents = await asyncio.gather(*get_tasks)
    successful_docs = [d for d in documents if d is not None]
    
    elapsed = time.time() - overall_start
    print(f"\n✓ Retrieved {len(successful_docs)}/20 documents in {elapsed:.4f}s")
    print(f"  Throughput: {20/elapsed:.2f} ops/sec")
    if DEBUG:
        logger.info(f"Get batch completed: {len(successful_docs)}/20 successful, {elapsed:.4f}s total")
    
    # Show sample documents
    if successful_docs:
        print(f"\nSample documents:")
        for doc in successful_docs[:3]:
            print(f"  - {doc['name']} ({doc['callsign']})")
    
    
    # Example 3: Mixed concurrent operations (upsert, get, remove)
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Mixed Concurrent Operations (15 total)")
    print("=" * 70)
    print("Performing upserts, gets, and removes simultaneously\n")
    
    overall_start = time.time()
    
    mixed_tasks = []
    
    # Add 5 upserts
    for i in range(5):
        doc_id = 9100 + i
        key = f"async_airline_{doc_id}"
        doc = {"type": "airline", "id": doc_id, "name": f"Mixed Test {i}"}
        mixed_tasks.append(('upsert', client.upsert_document(key, doc)))
    
    # Add 5 gets (from previously created docs)
    for i in range(5):
        doc_id = 9000 + i
        key = f"async_airline_{doc_id}"
        mixed_tasks.append(('get', client.get_document(key)))
    
    # Add 5 removes (cleanup from Example 1/2)
    for i in range(5, 10):
        doc_id = 9000 + i
        key = f"async_airline_{doc_id}"
        mixed_tasks.append(('remove', client.remove_document(key)))
    
    # Execute all mixed operations concurrently
    if DEBUG:
        logger.info("Executing 15 mixed operations concurrently (5 upserts, 5 gets, 5 removes)...")
    
    results = await asyncio.gather(*[task for _, task in mixed_tasks])
    
    elapsed = time.time() - overall_start
    
    # Count results by operation type
    upsert_count = sum(1 for i, (op, _) in enumerate(mixed_tasks) if op == 'upsert' and results[i] is not None)
    get_count = sum(1 for i, (op, _) in enumerate(mixed_tasks) if op == 'get' and results[i] is not None)
    remove_count = sum(1 for i, (op, _) in enumerate(mixed_tasks) if op == 'remove' and results[i])
    
    print(f"✓ Mixed operations completed in {elapsed:.4f}s:")
    print(f"  - Upserts: {upsert_count}/5")
    print(f"  - Gets: {get_count}/5")
    print(f"  - Removes: {remove_count}/5")
    print(f"  Total throughput: {15/elapsed:.2f} ops/sec")
    
    if DEBUG:
        logger.info(f"Mixed batch: U={upsert_count}/5, G={get_count}/5, R={remove_count}/5, {elapsed:.4f}s")
    
    
    # Example 4: Final cleanup
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Final Cleanup (Concurrent Removal)")
    print("=" * 70)
    
    overall_start = time.time()
    
    cleanup_tasks = []
    # Clean up all test documents created (9000-9019 and 9100-9104)
    for doc_id in list(range(9000, 9020)) + list(range(9100, 9105)):
        key = f"async_airline_{doc_id}"
        cleanup_tasks.append(client.remove_document(key))
    
    results = await asyncio.gather(*cleanup_tasks)
    removed = sum(1 for r in results if r)
    
    elapsed = time.time() - overall_start
    print(f"✓ Cleaned up {removed} test documents in {elapsed:.4f}s")
    
    if DEBUG:
        logger.info(f"Cleanup completed: {removed} documents removed, {elapsed:.4f}s")


async def main():
    """
    Main async function demonstrating concurrent Couchbase operations.
    """
    print("=" * 70)
    print("ASYNC COUCHBASE OPERATIONS WITH OBSERVABILITY")
    print("=" * 70)
    print(f"\nDEBUG Mode: {'ON' if DEBUG else 'OFF'}")
    if DEBUG:
        print("  ✓ Detailed timing for each operation")
        print("  ✓ Slow operations logging (JSON format)")
        print("  ✓ Orphaned response tracking")
        print("  ✓ Performance metrics")
    print("\nThis demo shows:")
    print("  - AsyncCouchbaseClient class for clean encapsulation")
    print("  - Individual async methods (upsert, get, remove)")
    print("  - Concurrent execution with asyncio.gather()")
    print("  - Mixed operation types running simultaneously")
    print("  - Comprehensive observability and error tracking\n")
    
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
        await client.connect(use_tls=USE_TLS, wan_profile=WAN_PROFILE)
        
        # Run demonstration of concurrent operations
        await demo_concurrent_operations(client)
        
        print("\n" + "=" * 70)
        print("KEY TAKEAWAYS")
        print("=" * 70)
        print("  ✓ Class-based design provides clean encapsulation")
        print("  ✓ Individual async methods compose easily")
        print("  ✓ asyncio.gather() enables true concurrency")
        print("  ✓ Can mix different operation types simultaneously")
        print("  ✓ Significantly better performance than sequential operations")
        print("  ✓ Built-in observability with timing and logging")
        print("  ✓ Slow operations and orphaned responses auto-logged")
        
        if DEBUG:
            print("\n  Check logs above for:")
            print("    - Individual operation timings")
            print("    - Slow operations (if any exceeded thresholds)")
            print("    - Orphaned responses (if any timeouts occurred)")
        
    except CouchbaseException as e:
        logger.error(f"Couchbase error: {e}")
        print(f"\n✗ Couchbase error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n✗ Unexpected error: {e}")
    finally:
        # Always close the cluster connection
        await client.close()


if __name__ == "__main__":
    print("\nTo disable debug output, set DEBUG = False at the top of the script\n")
    # Run the async main function
    asyncio.run(main())
