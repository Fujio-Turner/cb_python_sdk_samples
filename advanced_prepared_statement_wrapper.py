"""
Execute a prepared statement in Couchbase or prepare and execute if not found.

Args:
    cb (couchbase.collection.Collection): Couchbase collection object
    name (str): Name prefix for the prepared statement (include version, e.g., "my_query_v1")
    statement (str): The N1QL query statement
    query_parameters (dict, optional): Dictionary of query parameters
    retry (int, optional): Number of retry attempts (default: 3)
    timeout (int, optional): Query timeout in seconds (default: 75)
    scan_consistency (couchbase.options.QueryScanConsistency, optional): QueryScanConsistency 
    option (default: NOT_BOUNDED)

Returns:
    list: List of query result rows or error dictionary
"""
import hashlib
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (
    CouchbaseException,
    TimeoutException,
    AuthenticationException,
    DocumentNotFoundException,
    BucketNotFoundException,
    PreparedStatementException,
    QueryException
)
from couchbase.options import QueryScanConsistency

def run_cb_prepared(cb, name, statement, query_parameters=None, retry=3, timeout=75, scan_consistency=QueryScanConsistency.NOT_BOUNDED):
    
    if not statement:
        return {"error": "No Statement"}

    query_hash = hashlib.md5(statement.encode()).hexdigest()
    prepared = f"{name}_{query_hash}"
    data = []

    query_options = QueryOptions(
        adhoc=False,
        parameters=query_parameters or {},
        timeout=timeout * 1000000,  # Convert seconds to microseconds
        scan_consistency=scan_consistency
    )

    try:
        # Attempt to execute the prepared statement
        result = cb.query(f"EXECUTE {prepared}", **query_options)
        
        for row in result:
            data.append(row)
        return data

    except PreparedStatementException as e:
        if "prepared statement not found" in str(e).lower():
            try:
                # Delete any existing prepared statement with the same name
                cb.query(f'DELETE FROM system:prepared WHERE name = "{prepared}"')
                
                # Prepare and execute the new statement in one step
                prepare_execute_result = cb.query(
                    f"PREPARE {prepared} AS {statement}",
                    **query_options,
                    raw={'auto_execute': True}  # Add auto_execute here
                )
                
                for row in prepare_execute_result:
                    data.append(row)
                return data
            
            except QueryException as inner_e:
                if retry > 0:
                    return run_cb_prepared(cb, name, statement, query_parameters, retry - 1, timeout, scan_consistency)
                else:
                    raise inner_e
        else:
            if retry > 0:
                return run_cb_prepared(cb, name, statement, query_parameters, retry - 1, timeout, scan_consistency)
            else:
                raise e

# Example usage:
# cluster = Cluster('couchbase://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
# bucket = cluster.bucket('bucket_name')
# cb = bucket.default_collection()
# 
# result = run_cb_prepared(cb, "my_query_v1", "SELECT * FROM `bucket_name` WHERE type = $type", {"type": "user"}, scan_consistency=QueryScanConsistency.REQUEST_PLUS)
# print(result)