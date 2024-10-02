# Couchbase Python SDK Multi Operations Examples

## Multi-Get Operations

### get_multi

```python
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions

# Connect to the cluster
cluster = Cluster.connect('couchbase://localhost', ClusterOptions(PasswordAuthenticator('username', 'password')))
bucket = cluster.bucket('travel-sample')
collection = bucket.default_collection()

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

These examples demonstrate how to use the multi operations in the Python Couchbase SDK. 
Remember to handle exceptions and implement proper error checking in your production code.