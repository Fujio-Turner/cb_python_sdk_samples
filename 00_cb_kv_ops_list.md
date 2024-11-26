## Key-Value Operations in Couchbase Python SDK

### Basic CRUD Operations

1. **Insert(ADD)**
   - Adds a new document to the collection.
   - Fails if the document already exists.
   ```python
   result = collection.insert("document-key", {"foo": "bar"})
   ```

2. **Upsert(SET)**
   - Inserts a document or replaces it if it already exists.
   ```python
   result = collection.upsert("document-key", {"foo": "bar"})
   ```

3. **Replace(UPDATE)**
   - Replaces an existing document.
   - Fails if the document doesn't exist.
   ```python
   result = collection.replace("document-key", {"foo": "baz"})
   ```

4. **Get**
   - Retrieves a document by its key.
   ```python
   result = collection.get("document-key")
   ```

5. **Remove(DELETE)**
   - Deletes a document from the collection.
   ```python
   result = collection.remove("document-key")
   ```

### Expiration Operations

6. **Touch**
   - Updates the expiration time on a document.
   ```python
   result = collection.touch("document-key", timedelta(seconds=30))
   ```

7. **Get and Touch**
   - Retrieves a document and updates its expiration time in a single operation.
   ```python
   result = collection.get_and_touch("document-key", timedelta(seconds=30))
   ```

### Atomic Counter Operations

8. **Increment**
   - Atomically increments a counter document.
   ```python
   result = collection.binary().increment("counter-key", delta=1)
   ```

9. **Decrement**
   - Atomically decrements a counter document.
   ```python
   result = collection.binary().decrement("counter-key", delta=1)
   ```

### Durability Operations

10. **Insert with Durability**
    - Inserts a document with specified durability requirements.
    ```python
    result = collection.insert("document-key", {"foo": "bar"}, 
                               UpsertOptions(durability=ServerDurability(Durability.MAJORITY)))
    ```

### Scanning Operations

11. **Scan**
    - Performs a range scan on the collection.
    ```python
    result = collection.scan(RangeScan())
    ```

### Lookup-In Operations

12. **Lookup In(SUBDOC)**
    - Performs a subdocument lookup operation.
    ```python
    result = collection.lookup_in("document-key", [SD.get("path.to.field")])
    ```

### Mutate-In Operations

13. **Mutate In(SUBDOC)**
    - Performs a subdocument mutation operation.
    ```python
    result = collection.mutate_in("document-key", [SD.upsert("path.to.field", "value")])
    ```

These operations cover the main key-value interactions available in the Couchbase Python SDK. Each operation can be further customized with various options like timeouts, expiration times, and durability requirements.


## Limits on Keys in Couchbase Operations

### General Limits

1. **Multi-Get Operations (`get_multi`)**:
   - There is no explicit limit mentioned in the documentation.
   - Practical limitations may arise due to network packet size constraints or server-side processing limits.

2. **Subdocument Operations**:
   - There is a limit of **16 subdocument operations** that can be performed on a document at once.
   - This limit is defined in the protocol file on the server and is not configurable during runtime.

3. **N1QL Queries**:
   - For queries using the `IN` clause with a list of keys, there is a limit on the total size of the keys parameter.
   - The keys parameter cannot be longer than **1772 bytes**.

4. **Array Functions in N1QL**:
   - Some array functions might have a limit of **32K or 16K elements**.

## Document Limits

- The maximum key length for a document is **250 bytes**.
- The maximum value size for a document is **20 MB**.
- There’s a limit of **60,000 concurrent key-value connections** per data node.

## Recommendations

If you need to operate on a large number of keys that exceeds these limits, consider:

- Splitting your operation into multiple batches.
- Using N1QL queries for bulk operations, while keeping in mind the specific limits and performance implications of each method.