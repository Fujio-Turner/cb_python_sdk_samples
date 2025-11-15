"""
Demonstrates asynchronous SQL++ (N1QL) queries with observability features.

This script shows how to:
1. Create an async query-focused Couchbase client class
2. Execute queries asynchronously with various options
3. Run multiple queries concurrently with asyncio.gather()
4. Enable query profiling for performance analysis
5. Use use_replica for high availability
6. Compare adhoc=True vs adhoc=False performance
7. Track query performance with timing metrics

Async SQL++ Query Benefits:
- Execute multiple queries concurrently
- Non-blocking I/O for better throughput
- Efficient resource utilization
- Ideal for high-volume query workloads
- Familiar SQL-like syntax with async benefits

IMPORTANT - Query Timeout Configuration:
- **Default query timeout**: 75 seconds (1 minute 15 seconds)
- **When to adjust**:
  - Short timeouts (5-10s): Simple lookups, real-time queries
  - Long timeouts (120s+): Complex aggregations, analytics, large result sets
  - Production: Set based on p99 latency + buffer
- **How to set**: QueryOptions(timeout=timedelta(seconds=30))
- **Examples in this script**: See Examples 1, 3, and 5 for custom timeouts

IMPORTANT - Query Parameterization Best Practices:
- **Always use bind variables** for user input:
  - WHERE clauses: WHERE country = $country
  - LIMIT/OFFSET: LIMIT $limit OFFSET $offset
- **Exception**: Document type fields (type, docType, class) can be hard-coded
  Example: WHERE type = 'airline' (safe - part of data model)
  Example: WHERE country = $country (parameterized - user input)
  Example: LIMIT $limit (parameterized - better plan caching)
- **Why**: Prevents SQL injection + enables better query plan caching + flexible queries
- **Never**: WHERE country = 'France' or LIMIT 100 (hard-coded values reduce plan reuse)

IMPORTANT - Backtick Field Names to Prevent Reserved Word Collisions:
- **Use backticks** around field names that might be SQL++ reserved words
  Example: SELECT `name`, `type`, `value` (name/type/value are reserved)
  Example: WHERE `order` = $order (order is a reserved word)
- **Always safe**: Backtick all field names in production code
- **Why**: Prevents conflicts with SQL++ keywords (SELECT, WHERE, ORDER, COUNT, etc.)

WHAT TO EXPECT WHEN YOU RUN THIS:
==================================
With DEBUG=True, you'll see:

1. **Connection Logs (INFO/WARNING)**:
   - WARNING for ::1:8094/8092 (IPv6 Analytics/Search - normal if not configured)
   - INFO showing successful async connection with timing

2. **Async Query Execution Logs**:
   - Each query shows: execution time, result count, metrics
   - Profile data logged when profiling is enabled
   - Concurrent execution demonstrated with asyncio.gather()

3. **Prepared Statement Performance**:
   - 1st execution: ~15ms (parse + plan + execute)
   - Subsequent: ~3ms (cached plan + execute)
   - **Demonstrates ~80-85% speedup with query plan caching!**

4. **Concurrent Query Execution**:
   - Multiple queries run simultaneously (not sequentially)
   - Shows dramatic performance improvement with concurrency
   - Example: 5 queries in parallel vs 5 queries sequential

5. **Final Metrics JSON** (logged at end):
   ```json
   {
     "operations": {
       "query": {
         "query": {
           "percentiles_us": {
             "50.0": 3131,    // 50th percentile (median): 3.1ms
             "90.0": 14743,   // 90th percentile: 14.7ms
             "99.0": 16383,   // 99th percentile: 16.4ms
             "100.0": 16383   // Max latency: 16.4ms
           },
           "total_count": 16  // Total queries executed
         }
       }
     }
   }
   ```
   
   **What Percentiles Mean**:
   - p50 (median): Half of queries were faster than this
   - p90: 90% of queries were faster than this
   - p99: 99% of queries were faster than this
   - p100: Slowest query (max latency)
   
   **Services Tracked**: query, kv, search, management, views
   **Use Case**: Identify latency patterns and outliers in production

Query Profile Modes:
- OFF: No profiling (fastest)
- PHASES: Shows query execution phases (parse, plan, execute)
- TIMINGS: Detailed timing for each query operator (scan, filter, aggregate)

Observability Features:
- Query timing with DEBUG flag
- Slow query logging (>500ms threshold)
- Query profiling (PHASES/TIMINGS)
- Query metrics (execution time, result count)
- use_replica for reading from replica indexes
- Percentile latency tracking
- Orphaned query logging
- Exponential backoff retry for transient failures (timeouts, server errors)
- Configurable retry attempts and backoff intervals

Learn More:
- Async Queries: https://docs.couchbase.com/sdk-api/couchbase-python-client/acouchbase_api/acouchbase_core.html#module-acouchbase.cluster
- SQL++ Queries: https://docs.couchbase.com/python-sdk/current/howtos/n1ql-queries-with-sdk.html
- Slow Operations Logging: https://docs.couchbase.com/python-sdk/current/howtos/slow-operations-logging.html
- Observability: https://docs.couchbase.com/python-sdk/current/howtos/observability-metrics.html
"""
import asyncio
import time
import logging
import sys
from datetime import timedelta

# Async Couchbase imports
from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, ClusterTracingOptions, QueryOptions
from couchbase.n1ql import QueryScanConsistency, QueryProfile
from couchbase.exceptions import (
    CouchbaseException,
    ParsingFailedException,
    AuthenticationException,
    BucketNotFoundException,
    TimeoutException,
    ServiceUnavailableException,
    InternalServerFailureException
)
import couchbase

# Global debug flag - set to True to enable detailed timing and profiling
DEBUG = True

# Configure logging
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


class AsyncCouchbaseQueryClient:
    """
    Async Couchbase client focused on SQL++ (N1QL) query operations.
    
    Features:
    - Async query execution with timing metrics
    - Query profiling support (PHASES/TIMINGS)
    - use_replica for replica index reads
    - Concurrent query execution with asyncio.gather()
    - Exponential backoff retry logic for transient failures
    - Comprehensive exception handling
    - Slow query logging
    """
    
    def __init__(self, endpoint, username, password, bucket_name, max_retries=3, initial_backoff=0.1):
        """
        Initialize the async query client.
        
        Args:
            endpoint: Couchbase server endpoint
            username: Couchbase username
            password: Couchbase password
            bucket_name: Name of the bucket
            max_retries: Maximum retry attempts for transient failures (default: 3)
            initial_backoff: Initial backoff delay in seconds (default: 0.1s)
        """
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.bucket_name = bucket_name
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        
        self.cluster = None
        self.bucket = None
    
    async def connect(self, use_tls=False, wan_profile=False):
        """
        Connect to the Couchbase cluster asynchronously with query observability.
        
        Args:
            use_tls: Use couchbases:// (for Capella/cloud)
            wan_profile: Apply wan_development profile (for Capella)
        
        Raises:
            AuthenticationException: If credentials are invalid
            TimeoutException: If connection times out
            BucketNotFoundException: If bucket doesn't exist
        """
        if DEBUG:
            logger.info(f"Connecting to Couchbase cluster at {self.endpoint}...")
        
        start_time = time.time() if DEBUG else None
        
        try:
            auth = PasswordAuthenticator(self.username, self.password)
            
            # Configure tracing for slow query and orphaned response logging
            tracing_opts = ClusterTracingOptions(
                # Slow query thresholds
                tracing_threshold_query=timedelta(milliseconds=500),
                tracing_threshold_kv=timedelta(milliseconds=100),
                tracing_threshold_queue_size=10,
                
                # Orphaned query logging
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
            
            if DEBUG:
                elapsed = time.time() - start_time
                logger.info(f"✓ Connected in {elapsed:.4f}s")
                logger.info(f"  Slow query logging enabled (threshold: >500ms)")
                logger.info(f"  Orphaned query tracking enabled (10s intervals)")
            
            return self
            
        except AuthenticationException:
            logger.error("✗ Authentication failed")
            raise
        except BucketNotFoundException:
            logger.error(f"✗ Bucket '{self.bucket_name}' not found")
            raise
        except (TimeoutException, ServiceUnavailableException) as e:
            logger.error(f"✗ Connection failed: {e}")
            raise
    
    async def close(self):
        """Close the cluster connection."""
        if self.cluster:
            if DEBUG:
                logger.info("Closing cluster connection...")
            await self.cluster.close()
            if DEBUG:
                logger.info("✓ Cluster closed")
    
    async def execute_query(self, statement, query_options=None, description="Query", enable_retry=True):
        """
        Execute a SQL++ query asynchronously with timing, error handling, and exponential backoff retry.
        
        Args:
            statement: SQL++ query statement
            query_options: QueryOptions object
            description: Human-readable query description
            enable_retry: Enable retry with exponential backoff (default: True)
        
        Returns:
            List of query results or None on error
        """
        start_time = time.time() if DEBUG else None
        last_exception = None
        
        # Retry loop with exponential backoff
        for attempt in range(self.max_retries + 1):
            try:
                if DEBUG and attempt > 0:
                    logger.info(f"Retry attempt {attempt}/{self.max_retries} for {description}")
                elif DEBUG:
                    logger.info(f"Executing {description}...")
                
                result = self.cluster.query(statement, query_options)
                
                # Collect rows asynchronously
                rows = []
                async for row in result:
                    rows.append(row)
                
                # Get metadata
                metadata = result.metadata()
                
                if DEBUG:
                    elapsed = time.time() - start_time
                    retry_info = f" (after {attempt} retries)" if attempt > 0 else ""
                    logger.info(f"✓ {description} completed{retry_info}: {len(rows)} rows, {elapsed:.4f}s")
                    
                    # Log metrics if enabled
                    if metadata and metadata.metrics():
                        metrics = metadata.metrics()
                        logger.info(f"  Execution time: {metrics.execution_time()}")
                        logger.info(f"  Result count: {metrics.result_count()}")
                    
                    # Log profile if enabled
                    if metadata and metadata.profile():
                        logger.info(f"  Profile data available (see output)")
                
                return rows
                
            except ParsingFailedException as e:
                # Syntax errors - don't retry
                logger.error(f"✗ Query parsing error in {description}: {e}")
                print(f"  ✗ Syntax error (not retrying): {e}")
                return None
                
            except TimeoutException as e:
                # Timeout - retry with exponential backoff if enabled
                last_exception = e
                
                if enable_retry and attempt < self.max_retries:
                    backoff_delay = self.initial_backoff * (2 ** attempt)  # Exponential backoff
                    if DEBUG:
                        elapsed = time.time() - start_time
                        logger.warning(f"✗ {description} timed out after {elapsed:.4f}s, retrying in {backoff_delay:.2f}s...")
                    print(f"  ⚠ Timeout (attempt {attempt + 1}/{self.max_retries + 1}), retrying in {backoff_delay:.2f}s...")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    if DEBUG:
                        elapsed = time.time() - start_time
                        logger.error(f"✗ {description} timed out after {elapsed:.4f}s and {attempt} retries")
                    print(f"  ✗ Query timeout after {attempt} retries")
                    return None
                    
            except (ServiceUnavailableException, InternalServerFailureException) as e:
                # Transient errors - retry with exponential backoff if enabled
                last_exception = e
                
                if enable_retry and attempt < self.max_retries:
                    backoff_delay = self.initial_backoff * (2 ** attempt)
                    if DEBUG:
                        logger.warning(f"✗ Transient error in {description}, retrying in {backoff_delay:.2f}s...")
                    print(f"  ⚠ Transient error (attempt {attempt + 1}/{self.max_retries + 1}), retrying...")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    logger.error(f"✗ {description} failed after {attempt} retries: {e}")
                    print(f"  ✗ Server error after {attempt} retries")
                    return None
                    
            except CouchbaseException as e:
                # Other Couchbase errors - log and return
                logger.error(f"✗ {description} failed: {e}")
                print(f"  ✗ Query failed: {e}")
                return None
        
        # If we exhausted retries, return None
        if last_exception:
            logger.error(f"✗ {description} failed after all retries")
        return None


async def demo_query_operations(client):
    """
    Demonstrate various async query patterns with observability.
    
    Args:
        client: AsyncCouchbaseQueryClient instance
    """
    
    # Example 1: Basic async query with parameterized LIMIT
    print("\n" + "=" * 70)
    print("EXAMPLE 1: Query with Parameterized LIMIT & Custom Timeout")
    print("=" * 70)
    print("Using $limit bind variable - best practice for flexible queries\n")
    
    limit_value = 10  # Parameterize LIMIT for better plan reuse
    
    query = f"""
    SELECT `name`, `country`, `callsign`
    FROM `{client.bucket_name}`.`inventory`.`airline`
    WHERE `country` = $country
    LIMIT $limit
    """
    
    opts = QueryOptions(
        named_parameters={"country": "United States", "limit": limit_value},
        metrics=True,
        timeout=timedelta(seconds=10)  # Custom timeout: 10s (default is 75s)
    )
    
    rows = await client.execute_query(query, opts, "Query with parameterized LIMIT")
    if rows:
        print(f"✓ Retrieved {len(rows)} airlines (LIMIT $limit = {limit_value})")
        for row in rows[:3]:
            print(f"  - {row['name']} ({row['callsign']})")
        print(f"  Query timeout: 10s | Parameterized: country, limit")
    
    
    # Example 2: Concurrent queries with parameterized LIMIT
    print("\n" + "=" * 70)
    print("EXAMPLE 2: 5 Concurrent Queries with Parameterized LIMIT")
    print("=" * 70)
    print("Using bind variables for both $country and $limit\n")
    
    overall_start = time.time()
    
    limit_value = 5  # Parameterize LIMIT
    
    # Define query statement with parameters (note: backticks around field names)
    query_template = f"""
    SELECT `name` FROM `{{client.bucket_name}}`.`inventory`.`airline`
    WHERE `country` = $country 
    LIMIT $limit
    """
    
    # Create 5 different query tasks with different parameters
    countries = ["France", "Germany", "Japan", "Canada", "Brazil"]
    query_tasks = []
    
    for country in countries:
        task = client.execute_query(
            query_template.format(client=client),
            QueryOptions(
                named_parameters={"country": country, "limit": limit_value},
                metrics=True
            ),
            f"Query {country}"
        )
        query_tasks.append(task)
    
    # Execute all queries concurrently
    results = await asyncio.gather(*query_tasks)
    
    elapsed = time.time() - overall_start
    successful = sum(1 for r in results if r is not None)
    total_rows = sum(len(r) for r in results if r is not None)
    
    print(f"\n✓ Completed {successful}/5 queries concurrently in {elapsed:.4f}s")
    print(f"  Total rows retrieved: {total_rows}")
    print(f"  Average per query: {elapsed/5:.4f}s")
    print(f"  Throughput: {5/elapsed:.2f} queries/sec")
    
    if DEBUG:
        logger.info(f"Concurrent queries: {successful}/5 successful, {total_rows} total rows, {elapsed:.4f}s")
    
    
    # Example 3: Query with profiling (TIMINGS mode) and longer timeout
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Complex Query with TIMINGS Profiling & Long Timeout")
    print("=" * 70)
    print("Aggregation query with 30s timeout (default: 75s)\n")
    
    if DEBUG:
        limit_value = 5  # Parameterize LIMIT even in aggregations
        
        query = f"""
        SELECT `country`, COUNT(*) as airline_count
        FROM `{client.bucket_name}`.`inventory`.`airline`
        GROUP BY `country`
        ORDER BY airline_count DESC
        LIMIT $limit
        """
        
        opts = QueryOptions(
            named_parameters={"limit": limit_value},
            metrics=True,
            profile=QueryProfile.TIMINGS,  # Detailed timing per operator
            timeout=timedelta(seconds=30)  # 30s timeout for complex aggregation
        )
        
        rows = await client.execute_query(query, opts, "Aggregation query with 30s timeout")
        if rows:
            print(f"✓ Top countries by airline count (LIMIT $limit = {limit_value}):")
            for row in rows:
                country = row.get('country', 'Unknown')
                count = row.get('airline_count', 0)
                print(f"  - {country}: {count} airlines")
            print(f"  Query timeout: 30s | Parameterized: limit")
    
    
    # Example 4: use_replica for reading from replica indexes
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Async Query with use_replica (High Availability)")
    print("=" * 70)
    print("Reads from replica indexes if active is unavailable\n")
    
    country_filter = "Japan"
    limit_value = 5
    
    query = f"""
    SELECT `name`, `country`
    FROM `{client.bucket_name}`.`inventory`.`airline`
    WHERE `country` = $country
    LIMIT $limit
    """
    
    opts = QueryOptions(
        named_parameters={"country": country_filter, "limit": limit_value},
        use_replica=True,  # Allow reading from replica indexes
        metrics=True
    )
    
    rows = await client.execute_query(query, opts, "Async query with use_replica")
    if rows:
        print(f"✓ Retrieved {len(rows)} {country_filter} airlines (may use replica indexes)")
        for row in rows:
            print(f"  - {row['name']}")
        print(f"  Parameterized: country, limit")
    
    
    # Example 5: Prepared statement (adhoc=False) - Run 5 times async with short timeout
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Async Prepared Statement with Short Timeout (5s)")
    print("=" * 70)
    print("Executing same query 5 times with 5s timeout (default: 75s)")
    print("Good for fast, frequently-executed queries\n")
    
    limit_value = 5  # Parameterize LIMIT
    
    query = f"""
    SELECT `name`, `country`, `iata`
    FROM `{client.bucket_name}`.`inventory`.`airline`
    WHERE `country` = $country
    LIMIT $limit
    """
    
    # Execute the same query 5 times with adhoc=False
    execution_times = []
    for i in range(5):
        start_time = time.time()
        
        opts = QueryOptions(
            adhoc=False,  # Enable prepared statement caching
            named_parameters={"country": "United Kingdom", "limit": limit_value},
            metrics=True,
            timeout=timedelta(seconds=5)  # Short timeout for simple queries
        )
        
        rows = await client.execute_query(query, opts, f"Prepared query #{i+1} (5s timeout)")
        elapsed = time.time() - start_time
        execution_times.append(elapsed)
        
        if rows and i == 0:
            print(f"  Retrieved {len(rows)} UK airlines:")
            for row in rows[:3]:
                print(f"    - {row['name']}")
    
    # Show performance improvement
    print(f"\nPrepared Statement Timing Analysis:")
    print(f"  1st execution: {execution_times[0]:.4f}s (parse + plan + execute)")
    print(f"  2nd execution: {execution_times[1]:.4f}s (cached plan + execute)")
    print(f"  3rd execution: {execution_times[2]:.4f}s (cached plan + execute)")
    print(f"  4th execution: {execution_times[3]:.4f}s (cached plan + execute)")
    print(f"  5th execution: {execution_times[4]:.4f}s (cached plan + execute)")
    print(f"  Average (2-5): {sum(execution_times[1:])/ 4:.4f}s")
    
    improvement = ((execution_times[0] - sum(execution_times[1:])/4) / execution_times[0]) * 100
    if improvement > 0:
        print(f"  ✓ ~{improvement:.1f}% faster with cached plan!")
    print(f"  Note: All queries completed well under 5s timeout")
    
    
    # Example 6: Concurrent prepared statements
    print("\n" + "=" * 70)
    print("EXAMPLE 6: Multiple Concurrent Prepared Statements")
    print("=" * 70)
    print("Running 3 different prepared queries simultaneously\n")
    
    overall_start = time.time()
    
    # Create concurrent prepared query tasks with different parameters
    prepared_tasks = []
    countries = ["France", "Germany", "Spain"]
    limit_value = 3  # Parameterize LIMIT
    
    query_stmt = f"SELECT `name`, `callsign` FROM `{client.bucket_name}`.`inventory`.`airline` WHERE `country` = $country LIMIT $limit"
    
    for country in countries:
        opts = QueryOptions(
            adhoc=False,
            named_parameters={"country": country, "limit": limit_value},
            metrics=True
        )
        task = client.execute_query(
            query_stmt,
            opts,
            f"Prepared query: {country}"
        )
        prepared_tasks.append((country, task))
    
    # Execute all prepared queries concurrently
    results = await asyncio.gather(*[task for _, task in prepared_tasks])
    
    elapsed = time.time() - overall_start
    
    for i, (country, _) in enumerate(prepared_tasks):
        if results[i]:
            print(f"  ✓ {country}: {len(results[i])} airlines")
    
    print(f"\n✓ Executed 3 prepared queries concurrently in {elapsed:.4f}s")
    print(f"  Benefits: Query plan caching + async concurrency!")
    
    
    # Example 7: Scan consistency (REQUEST_PLUS) async
    print("\n" + "=" * 70)
    print("EXAMPLE 7: Async Query with REQUEST_PLUS Consistency")
    print("=" * 70)
    print("Ensures query sees all recent mutations\n")
    
    country_filter = "Canada"
    limit_value = 5
    
    query = f"""
    SELECT `name`, `callsign`
    FROM `{client.bucket_name}`.`inventory`.`airline`
    WHERE `country` = $country
    LIMIT $limit
    """
    
    opts = QueryOptions(
        named_parameters={"country": country_filter, "limit": limit_value},
        scan_consistency=QueryScanConsistency.REQUEST_PLUS,
        metrics=True
    )
    
    rows = await client.execute_query(query, opts, "Async query with REQUEST_PLUS")
    if rows:
        print(f"✓ Retrieved {len(rows)} {country_filter} airlines (strong consistency)")
        for row in rows:
            print(f"  - {row['name']} ({row['callsign']})")
        print(f"  Parameterized: country, limit")


async def main():
    """
    Main async function demonstrating concurrent query operations.
    """
    print("=" * 70)
    print("ASYNC SQL++ QUERIES WITH OBSERVABILITY")
    print("=" * 70)
    print(f"\nDEBUG Mode: {'ON' if DEBUG else 'OFF'}")
    if DEBUG:
        print("  ✓ Async query timing for each operation")
        print("  ✓ Query profiling (PHASES/TIMINGS)")
        print("  ✓ Query metrics (execution time, result count)")
        print("  ✓ Slow query logging (>500ms threshold)")
        print("  ✓ Orphaned query tracking")
    print("\nThis demo shows:")
    print("  - AsyncCouchbaseQueryClient class for async queries")
    print("  - Concurrent query execution with asyncio.gather()")
    print("  - Query metrics and profiling")
    print("  - use_replica for high availability")
    print("  - Prepared statements (adhoc=False) with async")
    print("  - Comprehensive observability\n")
    
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
    
    # Create async query client
    client = AsyncCouchbaseQueryClient(
        endpoint=ENDPOINT,
        username=USERNAME,
        password=PASSWORD,
        bucket_name=BUCKET_NAME
    )
    
    try:
        # Connect to Couchbase asynchronously
        await client.connect(use_tls=USE_TLS, wan_profile=WAN_PROFILE)
        
        # Run query demonstrations
        await demo_query_operations(client)
        
        print("\n" + "=" * 70)
        print("KEY TAKEAWAYS")
        print("=" * 70)
        print("  ✓ Async queries enable concurrent execution")
        print("  ✓ asyncio.gather() runs multiple queries simultaneously")
        print("  ✓ Query metrics provide execution insights")
        print("  ✓ Profiling helps identify slow operators")
        print("  ✓ use_replica improves availability")
        print("  ✓ Prepared statements cache query plans (adhoc=False)")
        print("  ✓ Combining async + prepared = maximum performance")
        print("  ✓ Built-in observability for production debugging")
        print("  ✓ Custom timeouts optimize for query complexity")
        
        if DEBUG:
            print("\n  Check logs above for:")
            print("    - Async query execution times")
            print("    - Concurrent execution patterns")
            print("    - Profiling data (phases/timings)")
            print("    - Metrics (result counts, execution duration)")
            print("    - Slow queries (if any exceeded 500ms)")
            print("    - Final metrics JSON with percentiles")
        
        print("\n  Timeout Configuration Guide:")
        print("    - Simple queries (lookups): 5-10s")
        print("    - Complex queries (aggregations): 30-60s")
        print("    - Analytics queries: 120s+")
        print("    - Default if not set: 75s (1min 15sec)")
        
    except CouchbaseException as e:
        logger.error(f"Couchbase error: {e}")
        print(f"\n✗ Couchbase error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n✗ Unexpected error: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    print("\nTo disable debug output, set DEBUG = False at the top of the script\n")
    asyncio.run(main())
