"""
This script demonstrates how to connect to a Couchbase cluster using the Couchbase Python SDK,
perform basic operations, and implement logging and OpenTelemetry for monitoring and tracing.

Key Features:
- Connects to a Couchbase cluster and waits for it to be ready.
- Performs a sample get operation on a document.
- Logs information and errors to a file.
- Uses OpenTelemetry to trace operations for better observability.

Make sure to replace 'your-ip' with your actual Couchbase server IP and update the bucket
and collection names as needed.
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
from couchbase.options import ClusterOptions, WaitUntilReadyOptions

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

# Main function to perform Couchbase operations
@tracer.start_as_current_span("couchbase_operations")
def perform_couchbase_operations():
    try:
        # Connect to the Couchbase cluster
        # Replace 'your-ip' with your actual Couchbase server IP
        cluster = Cluster('couchbase://your-ip',
                          ClusterOptions(PasswordAuthenticator("Administrator", "password")))
        
        # Wait for the cluster to be ready
        # This ensures that the KeyValue and Query services are available
        cluster.wait_until_ready(timedelta(seconds=3),
                                 WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Query]))
        
        logger.info('Cluster is ready for operations.')
        
        # Open a bucket and get a reference to a collection
        bucket = cluster.bucket("travel-sample")
        coll = bucket.scope('inventory').collection('airline')
        
        # Perform a get operation with tracing
        with tracer.start_as_current_span("get_operation"):
            try:
                # Attempt to get a document with a non-existent key
                result = coll.get('not-a-key')
            except CouchbaseException as e:
                # Log any Couchbase-specific exceptions
                logger.error(f"Error retrieving key: {str(e)}")
                logger.error(traceback.format_exc())
        
        # Add more Couchbase operations here
        # For example:
        # - Insert a document
        # - Perform a N1QL query
        # - Update a document
        # Each operation should be wrapped in a try-except block and have its own span
        
    except CouchbaseException as e:
        # Catch any Couchbase-specific exceptions that weren't caught earlier
        logger.error(f"Couchbase operation failed: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    perform_couchbase_operations()