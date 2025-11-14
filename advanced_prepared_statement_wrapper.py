"""
Execute a prepared statement in Couchbase using adhoc=False for optimal performance.

This wrapper automatically handles prepared statement caching by leveraging the SDK's
built-in prepared statement management. When adhoc=False, the SDK will prepare and
cache the query plan, significantly improving performance for repeated queries.

Args:
    cluster_or_scope: Couchbase Cluster or Scope object (NOT Collection)
    statement (str): The N1QL query statement
    query_parameters (dict or list, optional): Dictionary of named parameters or list of positional parameters
    retry (int, optional): Number of retry attempts for transient failures (default: 2)
    timeout (int, optional): Query timeout in seconds (default: 75)
    scan_consistency (couchbase.n1ql.QueryScanConsistency, optional): QueryScanConsistency 
        option (default: NOT_BOUNDED)
    read_only (bool, optional): Marks the query as read-only (default: False)
    metrics (bool, optional): Include query metrics in results (default: False)

Returns:
    list: List of query result rows

Raises:
    ValueError: If statement is empty or query_parameters is invalid type
    CouchbaseException: For various Couchbase-related errors
    TimeoutException: If query execution times out after all retries
    QueryException: If query syntax is invalid or execution fails
    
Example:
    # Using named parameters (dict)
    result = run_cb_prepared(
        cluster,
        "SELECT * FROM `bucket_name` WHERE type = $type AND status = $status",
        {"type": "user", "status": "active"},
        scan_consistency=QueryScanConsistency.REQUEST_PLUS
    )
    
    # Using positional parameters (list)
    result = run_cb_prepared(
        cluster,
        "SELECT * FROM `bucket_name` WHERE type = $1 AND status = $2",
        ["user", "active"]
    )
"""
from datetime import timedelta
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (
    CouchbaseException,
    AmbiguousTimeoutException,
    UnAmbiguousTimeoutException,
    AuthenticationException,
    InternalServerFailureException,
    InvalidArgumentException,
    ParsingFailedException
)
from couchbase.n1ql import QueryScanConsistency


def run_cb_prepared(
    cluster_or_scope,
    statement,
    query_parameters=None,
    retry=2,
    timeout=75,
    scan_consistency=QueryScanConsistency.NOT_BOUNDED,
    read_only=False,
    metrics=False
):
    """
    Execute a query with prepared statement optimization (adhoc=False).
    
    The SDK automatically prepares, caches, and manages the prepared statement lifecycle.
    """
    
    # Validate inputs
    if not statement or not isinstance(statement, str):
        raise ValueError("Statement must be a non-empty string")
    
    # Build query options
    opts_dict = {
        'adhoc': False,  # Enable prepared statement optimization
        'timeout': timedelta(seconds=timeout),
        'scan_consistency': scan_consistency,
        'read_only': read_only,
        'metrics': metrics
    }
    
    # Handle query parameters (named or positional)
    if query_parameters is not None:
        if isinstance(query_parameters, dict):
            opts_dict['named_parameters'] = query_parameters
        elif isinstance(query_parameters, (list, tuple)):
            opts_dict['positional_parameters'] = list(query_parameters)
        else:
            raise ValueError(
                "query_parameters must be dict (for named parameters) or "
                "list/tuple (for positional parameters)"
            )
    
    # Create QueryOptions object
    query_opts = QueryOptions(**opts_dict)
    
    # Retry loop for transient failures
    attempts = max(1, int(retry) + 1)
    last_exception = None
    
    for attempt in range(attempts):
        try:
            # Execute query - SDK handles prepared statement caching
            result = cluster_or_scope.query(statement, query_opts)
            
            # Collect all rows
            rows = []
            for row in result:
                rows.append(row)
            
            # Optionally log metrics if enabled
            if metrics:
                metadata = result.metadata()
                if metadata and metadata.metrics():
                    print(f"Query Metrics - Execution Time: {metadata.metrics().execution_time()}, "
                          f"Result Count: {metadata.metrics().result_count()}")
            
            return rows
            
        except (AmbiguousTimeoutException, UnAmbiguousTimeoutException) as e:
            # Timeout - retry if attempts remain
            last_exception = e
            if attempt < attempts - 1:
                print(f"Query timeout on attempt {attempt + 1}/{attempts}, retrying...")
                continue
            else:
                print(f"Query failed after {attempts} timeout attempts")
                raise
        
        except ParsingFailedException as e:
            # Query syntax or planning errors - don't retry, fail immediately
            print(f"Query parsing error: {e}")
            raise
        
        except InternalServerFailureException as e:
            # Server-side error - retry if attempts remain
            last_exception = e
            if attempt < attempts - 1:
                print(f"Server error on attempt {attempt + 1}/{attempts}, retrying...")
                continue
            else:
                print(f"Query failed after {attempts} attempts due to server error")
                raise
        

        
        except AuthenticationException as e:
            # Authentication failure - don't retry
            print(f"Authentication failed: {e}")
            raise
        
        except InvalidArgumentException as e:
            # Invalid argument - don't retry
            print(f"Invalid argument provided: {e}")
            raise
        
        except CouchbaseException as e:
            # Generic Couchbase error - retry if attempts remain
            last_exception = e
            if attempt < attempts - 1:
                print(f"Couchbase error on attempt {attempt + 1}/{attempts}: {e}, retrying...")
                continue
            else:
                print(f"Query failed after {attempts} attempts")
                raise
        
        except Exception as e:
            # Unexpected non-Couchbase error - don't retry
            print(f"Unexpected error: {e}")
            raise
    
    # If we exhausted all retries without success, raise the last exception
    if last_exception:
        raise last_exception


# Example usage:
if __name__ == "__main__":
    # For local/self-hosted Couchbase Server:
    ENDPOINT = "localhost"
    USERNAME = "Administrator"
    PASSWORD = "password"
    
    # For Capella (cloud), uncomment and update these instead:
    # ENDPOINT = "cb.your-endpoint.cloud.couchbase.com"
    # USERNAME = "your-capella-username"
    # PASSWORD = "your-capella-password"
    
    # Connect options - authentication
    auth = PasswordAuthenticator(USERNAME, PASSWORD)
    options = ClusterOptions(auth)
    
    # For local/self-hosted Couchbase Server:
    cluster = Cluster(f'couchbase://{ENDPOINT}', options)
    
    # For Capella (cloud), use this instead (uncomment and comment out the line above):
    # options.apply_profile('wan_development')  # Helps avoid latency issues with Capella
    # cluster = Cluster(f'couchbases://{ENDPOINT}', options)  # Note: couchbaseS (secure)
    
    # Wait until the cluster is ready
    cluster.wait_until_ready(timedelta(seconds=10))
    
    try:
        # Example 1: Named parameters
        result = run_cb_prepared(
            cluster,
            "SELECT name, callsign FROM `travel-sample`.inventory.airline WHERE country = $country LIMIT 5",
            {"country": "United States"},
            scan_consistency=QueryScanConsistency.REQUEST_PLUS,
            metrics=True
        )
        print("\nExample 1 - Named Parameters:")
        for row in result:
            print(row)
        
        # Example 2: Positional parameters
        result2 = run_cb_prepared(
            cluster,
            "SELECT name FROM `travel-sample`.inventory.airline WHERE country = $1 LIMIT $2",
            ["France", 3],
            read_only=True
        )
        print("\nExample 2 - Positional Parameters:")
        for row in result2:
            print(row)
        
    except CouchbaseException as e:
        print(f"Couchbase error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Clean up
        cluster.close()
        print("\nConnection closed.")
