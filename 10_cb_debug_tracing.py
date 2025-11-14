"""
Demonstrates debugging and tracing Couchbase operations using logging and OpenTelemetry.

This script shows how to:
1. Configure Python logging to capture Couchbase SDK logs
2. Enable slow operations logging (threshold tracing)
3. Set up OpenTelemetry for distributed tracing
4. Trace individual operations with custom spans
5. Log errors and exceptions for debugging
6. Export trace data to console (can be changed to Jaeger, Zipkin, etc.)

Logging vs Tracing:
- Logging: Good for debugging individual events and errors
- Slow Operations Logging: Automatically logs operations exceeding time thresholds
- OpenTelemetry Tracing: Good for understanding request flow and performance across services

Use Cases:
- Debugging slow operations (identify performance bottlenecks)
- Monitoring application health
- Performance profiling
- Production troubleshooting
- Distributed system observability
"""

import logging
import traceback
from datetime import timedelta

# Couchbase SDK imports
import couchbase
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions, WaitUntilReadyOptions, ClusterTracingOptions

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

# Configure slow operations logging thresholds
# Operations slower than these thresholds will be automatically logged
tracing_opts = ClusterTracingOptions(
    tracing_threshold_queue_size=10,  # Log if queue size exceeds 10
    tracing_threshold_kv=timedelta(milliseconds=100),  # Log KV ops slower than 100ms
    tracing_threshold_query=timedelta(milliseconds=500),  # Log queries slower than 500ms
    tracing_threshold_search=timedelta(milliseconds=500),  # Log search ops slower than 500ms
    tracing_threshold_analytics=timedelta(seconds=1),  # Log analytics ops slower than 1s
    tracing_threshold_view=timedelta(seconds=1)  # Log view ops slower than 1s
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
        options = ClusterOptions(
            authenticator=auth,
            tracing_options=tracing_opts  # Enable slow operations logging
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
        
        # Example 6: Demonstrate slow operation logging with artificial delay
        print("\n--- Example 6: Slow Operation Detection ---")
        print("(Operations slower than threshold will be logged in JSON format)")
        with tracer.start_as_current_span("slow_operation_demo"):
            try:
                # Force a slow query with sleep to trigger threshold logging
                slow_query = f"""
                SELECT SLEEP(200) AS delay, name, country 
                FROM `{BUCKET_NAME}`.{CB_SCOPE}.{CB_COLLECTION} 
                WHERE country = 'United States' 
                LIMIT 1
                """
                logger.info(f"Executing intentionally slow query (200ms delay)")
                result = cluster.query(slow_query)
                rows = list(result)
                logger.info(f"✓ Slow query completed")
                print(f"✓ Slow query completed (should appear in slow ops log)")
                print(f"  Check logs for JSON output with operation timing details")
            except CouchbaseException as e:
                logger.error(f"Slow query error: {e}")
                print(f"✗ Slow query failed: {e}")
        
        # Example 5: Cleanup - Remove test document
        print("\n--- Example 5: Cleanup (Remove Test Document) ---")
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
    print("  3. OpenTelemetry tracing with console export")
    print("  4. Error handling and debugging patterns")
    print("\nSlow Operation Thresholds:")
    print("  - KV operations: > 100ms")
    print("  - Query operations: > 500ms")
    print("  - Search/Analytics: > 500ms/1s")
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
    print("=" * 70)