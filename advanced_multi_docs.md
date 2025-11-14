# Couchbase Python SDK Multi Operations Examples

This document demonstrates batch operations for improved performance when working with multiple documents simultaneously. Multi operations reduce network round-trips and improve throughput.

## Benefits of Multi Operations
- **Reduced Network Overhead**: Single request for multiple documents
- **Better Throughput**: Batch operations are more efficient
- **Simplified Code**: Handle multiple operations in one call
- **Performance**: Especially beneficial for high-volume applications

## Connection Setup

```python
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions

# For local/self-hosted Couchbase Server:
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

# Connect to the cluster
auth = PasswordAuthenticator(USERNAME, PASSWORD)
options = ClusterOptions(auth)
cluster = Cluster(f'couchbase://{ENDPOINT}', options)

# For Capella (cloud), use this instead:
# options.apply_profile('wan_development')
# cluster = Cluster(f'couchbases://{ENDPOINT}', options)

bucket = cluster.bucket('travel-sample')
collection = bucket.default_collection()  # Or use scope/collection

# Perform multi-get operation
keys = ['airline_10', 'airline_11', 'airline_12']
result = collection.get_multi(keys)

for key, value in result.results.items():
    print(f"Key: {key}, Value: {value.content_as[dict]}")
```

### get_multi_replica

```python
# Perform multi-get replica operation
keys = ['airline_10', 'airline_11', 'airline_12']
result = collection.get_multi_replica(keys)

for key, value in result.results.items():
    print(f"Key: {key}, Value: {value.content_as[dict]}")
```

## Multi-Mutation Operations

### upsert_multi

```python
# Prepare documents for upsert
docs = {
    'user_1': {'name': 'Alice', 'age': 30},
    'user_2': {'name': 'Bob', 'age': 35},
    'user_3': {'name': 'Charlie', 'age': 40}
}

# Perform multi-upsert operation
result = collection.upsert_multi(docs)

for key, value in result.results.items():
    print(f"Key: {key}, CAS: {value.cas}")
```

### insert_multi

```python
# Prepare documents for insert
new_docs = {
    'new_user_1': {'name': 'David', 'age': 25},
    'new_user_2': {'name': 'Eva', 'age': 28}
}

# Perform multi-insert operation
result = collection.insert_multi(new_docs)

for key, value in result.results.items():
    print(f"Key: {key}, CAS: {value.cas}")
```

### replace_multi

```python
# Prepare documents for replace
updated_docs = {
    'user_1': {'name': 'Alice', 'age': 31},
    'user_2': {'name': 'Bob', 'age': 36}
}

# Perform multi-replace operation
result = collection.replace_multi(updated_docs)

for key, value in result.results.items():
    print(f"Key: {key}, CAS: {value.cas}")
```

### remove_multi

```python
# Prepare keys for removal
keys_to_remove = ['user_1', 'user_2']

# Perform multi-remove operation
result = collection.remove_multi(keys_to_remove)

for key, value in result.results.items():
    print(f"Key: {key}, CAS: {value.cas}")
```

## Multi-Lookup Operations

### lookup_in_multi

```python
from couchbase.subdocument import SD

# Prepare lookup specifications
specs = {
    'airline_10': [SD.get('name'), SD.get('country')],
    'airline_11': [SD.get('name'), SD.get('country')],
    'airline_12': [SD.get('name'), SD.get('country')]
}

# Perform multi-lookup operation
result = collection.lookup_in_multi(specs)

for key, value in result.results.items():
    print(f"Key: {key}")
    for i, spec in enumerate(value.content_as[list]):
        print(f"  Spec {i}: {spec}")
```

## Multi-Mutation Subdocument Operations

### mutate_in_multi

```python
from couchbase.subdocument import SD

# Prepare mutation specifications
specs = {
    'user_1': [SD.upsert('email', 'alice@example.com'), SD.increment('logins', 1)],
    'user_2': [SD.upsert('email', 'bob@example.com'), SD.increment('logins', 1)]
}

# Perform multi-mutate operation
result = collection.mutate_in_multi(specs)

for key, value in result.results.items():
    print(f"Key: {key}, CAS: {value.cas}")
```

## Error Handling Best Practices

Multi operations return a result object with individual results for each key. Always check for failures:

```python
from couchbase.exceptions import CouchbaseException

# Example with error handling
keys = ['doc1', 'doc2', 'doc3']
try:
    result = collection.get_multi(keys)
    
    # Check each result
    for key, res in result.results.items():
        if res.success:
            print(f"✓ {key}: {res.content_as[dict]}")
        else:
            print(f"✗ {key}: Failed - {res.exception}")
            
except CouchbaseException as e:
    print(f"Multi-operation failed: {e}")
```

## Performance Considerations

1. **Batch Size**: 
   - Optimal batch size: 100-1000 documents
   - Larger batches may hit network limits
   - Split very large operations into chunks

2. **Network Limits**:
   - Total request size should be reasonable (< 10MB)
   - Consider document size when batching

3. **Parallel Processing**:
   - Multi operations are inherently parallel
   - Better than looping with individual operations
   - For even higher throughput, consider async operations (see `11_cb_async_operations.py`)

## When to Use Multi Operations

✅ **Good Use Cases:**
- Fetching related documents together
- Bulk inserts/updates
- Batch deletions
- Consistent subdocument operations across multiple docs

❌ **Avoid When:**
- Documents are unrelated (may cause unnecessary blocking)
- Individual operations need different options/timeouts
- Very large batches (split into smaller chunks)
- Operations require different consistency guarantees

## Related Examples

- **11_cb_async_operations.py** - Async multi-document operations
- **05_cb_exception_handling.py** - Error handling patterns
- **excel_to_json_to_cb.py** - Bulk import example

## Additional Notes

- Multi operations are atomic per-document, not across documents
- For cross-document atomicity, use transactions (see `08a_cb_transaction_kv.py`)
- Results maintain insertion order in most cases
- Failed operations don't stop other operations in the batch

## Resources

- [Couchbase Python SDK Documentation](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html)
- [Bulk Operations Guide](https://docs.couchbase.com/python-sdk/current/howtos/concurrent-async-apis.html)
- [Performance Best Practices](https://docs.couchbase.com/python-sdk/current/concept-docs/performance.html)

---

These examples demonstrate how to use multi operations in the Couchbase Python SDK. Remember to handle exceptions and implement proper error checking in production code.