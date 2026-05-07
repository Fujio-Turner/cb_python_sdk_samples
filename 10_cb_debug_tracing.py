"""
Demonstrates debugging and tracing Couchbase operations using logging and OpenTelemetry.

This script shows how to:
1. Configure Python logging to capture Couchbase SDK logs
2. Enable slow operations logging (threshold tracing)
3. Enable Orphaned Request Reporting (catch responses that arrive after a client timeout)
4. Set up OpenTelemetry for distributed tracing
5. Trace individual operations with custom spans
6. Log errors and exceptions for debugging
7. Export trace data to console (can be changed to Jaeger, Zipkin, etc.)

Logging vs Tracing vs Orphan Reporting:
- Logging: Good for debugging individual events and errors
- Slow Operations Logging: Automatically logs operations exceeding time thresholds
- Orphaned Request Reporting: Logs responses for requests the client already gave up on
  (i.e. the operation timed out client-side, but the server eventually finished it).
  Useful to detect timeouts that are too aggressive, slow nodes, or network lag.
- OpenTelemetry Tracing: Good for understanding request flow and performance across services

Use Cases:
- Debugging slow operations (identify performance bottlenecks)
- Detecting "lag" between client and server (orphaned responses)
- Monitoring application health
- Performance profiling
- Production troubleshooting
- Distributed system observability

Reference docs:
- https://docs.couchbase.com/python-sdk/current/howtos/slow-operations-logging.html
- https://docs.couchbase.com/python-sdk/current/howtos/observability-orphan-logger.html
"""

import logging
import time
import traceback
from datetime import timedelta

# Couchbase SDK imports
import couchbase
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import (
    CouchbaseException,
    TimeoutException,
    AmbiguousTimeoutException,
    UnAmbiguousTimeoutException,
)
from couchbase.options import (
    ClusterOptions,
    WaitUntilReadyOptions,
    ClusterTracingOptions,
    UpsertOptions,
    QueryOptions,
)

# OpenTelemetry imports
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

# Set up logging
# This configures the logging module to write logs to a file
logging.basicConfig(
    filename='couchbase_example.log',  # Name of the log file
    filemode='w',  # 'w' mode overwrites the file on each run
    level=logging.DEBUG,  # Set the logging level to DEBUG (captures all log levels)
    format='%(levelname)s::%(asctime)s::%(message)s',  # Custom log format
    datefmt='%Y-%m-%d %H:%M:%S'  # Custom date format
)
logger = logging.getLogger()  # Get the root logger

# Configure Couchbase SDK to use our logger
# This is the bridge that lets the threshold logger AND the orphan reporter
# emit their JSON forensic reports through Python's logging module.
couchbase.configure_logging(logger.name, level=logger.level)

# Set up OpenTelemetry
# This configures OpenTelemetry to trace our application
trace.set_tracer_provider(TracerProvider())  # Set up a tracer provider
tracer = trace.get_tracer(__name__)  # Get a tracer for this module

# Add a SimpleSpanProcessor that prints to the console
trace.get_tracer_provider().add_span_processor(
    SimpleSpanProcessor(ConsoleSpanExporter())
)

# Connection configuration
# For local/self-hosted Couchbase Server:
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

# For Capella (cloud), uncomment and update these instead:
# ENDPOINT = "cb.your-endpoint.cloud.couchbase.com"
# USERNAME = "your-capella-username"
# PASSWORD = "your-capella-password"

BUCKET_NAME = "travel-sample"
CB_SCOPE = "inventory"
CB_COLLECTION = "airline"

# Configure slow operations logging thresholds AND orphan response reporting.
# `ClusterTracingOptions` controls TWO independent reporters:
#   1. Threshold logger  - any op slower than tracing_threshold_* is logged
#                          as a JSON forensic report (server time, dispatch, etc.)
#   2. Orphan reporter   - any response that arrives AFTER the client gave up
#                          (timed out) is logged as a JSON orphan report.
tracing_opts = ClusterTracingOptions(
    # ---- Slow operation thresholds ----
    tracing_threshold_queue_size=10,  # Log if queue size exceeds 10
    tracing_threshold_kv=timedelta(milliseconds=100),  # Log KV ops slower than 100ms
    tracing_threshold_query=timedelta(milliseconds=500),  # Log queries slower than 500ms
    tracing_threshold_search=timedelta(milliseconds=500),  # Log search ops slower than 500ms
    tracing_threshold_analytics=timedelta(seconds=1),  # Log analytics ops slower than 1s
    tracing_threshold_view=timedelta(seconds=1),  # Log view ops slower than 1s

    # ---- Orphaned request reporter ----
    # Flush the orphan queue every 5s and keep up to 1024 orphan samples.
    # When the client times out but the server later replies, the late
    # response is captured here so you can see "ghost" operations.
    tracing_orphaned_queue_flush_interval=timedelta(seconds=5),
    tracing_orphaned_queue_size=1024,
)


# Main function to perform Couchbase operations
@tracer.start_as_current_span("couchbase_operations")
def perform_couchbase_operations():
    cluster = None
    try:
        logger.info("=== Starting Couchbase Operations ===")
        
        # Connect to the Couchbase cluster
        auth = PasswordAuthenticator(USERNAME, PASSWORD)
        
        # Create ClusterOptions with tracing enabled for slow operations logging
        # AND orphaned request reporting (both come from `tracing_opts`).
        options = ClusterOptions(
            authenticator=auth,
            tracing_options=tracing_opts
        )
        
        # For local/self-hosted Couchbase Server:
        cluster = Cluster(f'couchbase://{ENDPOINT}', options)
        
        # For Capella (cloud), use this instead (uncomment and comment out the line above):
        # options.apply_profile('wan_development')
        # cluster = Cluster(f'couchbases://{ENDPOINT}', options)
        
        logger.info(f"Connecting to cluster at {ENDPOINT}...")
        
        # Wait for the cluster to be ready
        with tracer.start_as_current_span("cluster_connection"):
            cluster.wait_until_ready(
                timedelta(seconds=10),
                WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Query])
            )
        
        logger.info('✓ Cluster is ready for operations')
        
        # Open a bucket and get a reference to a collection
        bucket = cluster.bucket(BUCKET_NAME)
        coll = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)
        logger.info(f"✓ Connected to {BUCKET_NAME}.{CB_SCOPE}.{CB_COLLECTION}")
        
        # Example 1: Get operation that fails (document doesn't exist)
        print("\n--- Example 1: Get Non-Existent Document (with error logging) ---")
        with tracer.start_as_current_span("get_nonexistent_document"):
            try:
                logger.info("Attempting to get non-existent document 'not-a-key'")
                result = coll.get('not-a-key')
                logger.info(f"Document retrieved: {result.content_as[dict]}")
            except CouchbaseException as e:
                logger.error(f"✗ Expected error - Document not found: {e}")
                print(f"✗ Document not found (logged to couchbase_example.log)")
        
        # Example 2: Successful get operation
        print("\n--- Example 2: Successful Get Operation ---")
        with tracer.start_as_current_span("get_existing_document"):
            try:
                doc_key = "airline_10"
                logger.info(f"Attempting to get document '{doc_key}'")
                result = coll.get(doc_key)
                logger.info(f"✓ Successfully retrieved document '{doc_key}'")
                logger.debug(f"Document content: {result.content_as[dict]}")
                print(f"✓ Successfully retrieved '{doc_key}'")
                print(f"  CAS: {result.cas}")
            except CouchbaseException as e:
                logger.error(f"Error retrieving '{doc_key}': {e}")
                print(f"✗ Failed to get document: {e}")
        
        # Example 3: Insert operation
        print("\n--- Example 3: Insert Operation ---")
        with tracer.start_as_current_span("insert_document"):
            try:
                test_key = "test_debug_doc"
                test_doc = {
                    "type": "test",
                    "name": "Debug Test Document",
                    "timestamp": timedelta(seconds=0).total_seconds()
                }
                logger.info(f"Inserting document '{test_key}'")
                result = coll.upsert(test_key, test_doc)
                logger.info(f"✓ Document '{test_key}' inserted with CAS: {result.cas}")
                print(f"✓ Inserted '{test_key}'")
            except CouchbaseException as e:
                logger.error(f"Error inserting document: {e}")
                print(f"✗ Insert failed: {e}")
        
        # Example 4: Query operation
        print("\n--- Example 4: Query Operation ---")
        with tracer.start_as_current_span("query_operation"):
            try:
                query = f"SELECT name, country FROM `{BUCKET_NAME}`.{CB_SCOPE}.{CB_COLLECTION} WHERE country = 'France' LIMIT 3"
                logger.info(f"Executing query: {query}")
                result = cluster.query(query)
                rows = list(result)
                logger.info(f"✓ Query returned {len(rows)} rows")
                print(f"✓ Query returned {len(rows)} results:")
                for row in rows:
                    print(f"  {row}")
            except CouchbaseException as e:
                logger.error(f"Query error: {e}")
                logger.error(traceback.format_exc())
                print(f"✗ Query failed: {e}")
        
        # Example 5: Demonstrate slow operation logging with artificial delay
        print("\n--- Example 5: Slow Operation Detection ---")
        print("(Operations slower than threshold will be logged in JSON format)")
        with tracer.start_as_current_span("slow_operation_demo"):
            try:
                # Force a slow query with SLEEP() to trigger threshold logging
                slow_query = f"""
                SELECT SLEEP(750) AS delay, name, country 
                FROM `{BUCKET_NAME}`.{CB_SCOPE}.{CB_COLLECTION} 
                WHERE country = 'United States' 
                LIMIT 1
                """
                logger.info(f"Executing intentionally slow query (~750ms delay)")
                result = cluster.query(slow_query)
                rows = list(result)
                logger.info(f"✓ Slow query completed")
                print(f"✓ Slow query completed (should appear in slow ops log)")
                print(f"  Check logs for JSON output with operation timing details")
            except CouchbaseException as e:
                logger.error(f"Slow query error: {e}")
                print(f"✗ Slow query failed: {e}")

        # Example 6: Demonstrate ORPHANED REQUEST REPORTING
        # An "orphan" is a response from the server that arrives AFTER the
        # client already gave up on the request (i.e. it timed out client-side).
        # We force this by setting an unrealistically tiny 1-microsecond timeout:
        # the client is guaranteed to time out, but the server still completes
        # the work — that late response becomes an orphan, and the SDK's
        # orphan reporter will flush a JSON report listing those operations.
        print("\n--- Example 6: Orphaned Request Reporting ---")
        print("(Forcing client timeouts to generate orphan responses)")
        with tracer.start_as_current_span("orphan_request_demo"):
            for i in range(10):
                doc_key = f"orphan_demo_{i}"
                try:
                    coll.upsert(
                        doc_key,
                        {"i": i, "demo": "orphan"},
                        UpsertOptions(timeout=timedelta(microseconds=1)),
                    )
                except (TimeoutException,
                        AmbiguousTimeoutException,
                        UnAmbiguousTimeoutException) as ex:
                    # ex.context is the forensic JSON ("r"=remote, "s"=service, etc.)
                    logger.warning(
                        f"Client timed out on '{doc_key}' (expected). "
                        f"context={getattr(ex, 'context', None)}"
                    )

            # Same idea for a query — too-low timeout = orphan candidate.
            try:
                cluster.query(
                    f"SELECT META().id FROM `{BUCKET_NAME}`.{CB_SCOPE}.{CB_COLLECTION} LIMIT 50",
                    QueryOptions(timeout=timedelta(microseconds=1)),
                ).execute()
            except CouchbaseException as ex:
                logger.warning(
                    f"Client timed out on query (expected): {type(ex).__name__}"
                )

        print("✓ Issued ops with 1µs timeout to provoke orphan responses")
        print("  The orphan reporter flushes every 5s. Look for a log line like:")
        print("  'Orphan responses observed: { ... JSON ... }' listing operations")
        print("  the server completed AFTER the client gave up.")

        # Example 7: Cleanup - Remove test document
        print("\n--- Example 7: Cleanup (Remove Test Document) ---")
        with tracer.start_as_current_span("remove_document"):
            try:
                logger.info(f"Removing test document '{test_key}'")
                coll.remove(test_key)
                logger.info(f"✓ Document '{test_key}' removed")
                print(f"✓ Removed test document")
            except CouchbaseException as e:
                logger.error(f"Error removing document: {e}")
                print(f"✗ Remove failed: {e}")
        
        logger.info("=== All operations completed ===")
        print("\n✓ All operations completed successfully")

        # Give the orphan reporter time to flush before we close the cluster.
        # Flush interval is set to 5s above, so we wait a bit longer.
        print("\nWaiting 7s so the orphan reporter can flush its queue...")
        time.sleep(7)
        
    except CouchbaseException as e:
        logger.error(f"Couchbase operation failed: {e}")
        logger.error(traceback.format_exc())
        print(f"\n✗ Couchbase operation failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())
        print(f"\n✗ Unexpected error: {e}")
    finally:
        if cluster:
            cluster.close()
            logger.info("Cluster connection closed")
            print("\nCluster connection closed")

if __name__ == "__main__":
    print("=" * 70)
    print("COUCHBASE DEBUGGING & TRACING DEMO")
    print("=" * 70)
    print("\nThis script demonstrates:")
    print("  1. Logging Couchbase operations to file (couchbase_example.log)")
    print("  2. Slow operations logging (threshold-based automatic detection)")
    print("  3. Orphaned request reporting (responses after client timeout)")
    print("  4. OpenTelemetry tracing with console export")
    print("  5. Error handling and debugging patterns")
    print("\nSlow Operation Thresholds:")
    print("  - KV operations: > 100ms")
    print("  - Query operations: > 500ms")
    print("  - Search/Analytics: > 500ms/1s")
    print("\nOrphan Reporter:")
    print("  - Flush interval: 5s   |   Queue size: 1024")
    print("\nTrace output will appear below, followed by operation results.\n")
    
    perform_couchbase_operations()
    
    print("\n" + "=" * 70)
    print("WHAT TO CHECK:")
    print("=" * 70)
    print("✓ couchbase_example.log - Detailed operation logs")
    print("✓ Slow operations log - JSON format with timing breakdowns:")
    print("    - total_duration_us: Total operation time")
    print("    - last_server_duration_us: Server processing time")
    print("    - operation_name: Type of operation (get, upsert, query, etc.)")
    print("✓ Orphan responses log - JSON listing late server replies for")
    print("  operations the client had already timed out on.")
    print("=" * 70)
