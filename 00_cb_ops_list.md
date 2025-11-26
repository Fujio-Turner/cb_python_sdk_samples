# Couchbase Python SDK Sample Scripts Overview

This repository contains comprehensive examples demonstrating various Couchbase operations using the Python SDK. Each script is designed to illustrate specific features and best practices.

---

## 📚 Sample Scripts

### **01a_cb_set_get.py** - Basic Get and Upsert Operations
Demonstrates fundamental key-value operations:
- Connect to Couchbase cluster (local and Capella configurations)
- Upsert documents to a collection
- Retrieve documents by key
- Measure operation performance
- Proper connection management and cleanup

**Key Concepts**: Basic CRUD, connection setup, CAS values, timing operations

---

### **01b_cb_get_update_w_cas.py** - Optimistic Locking with CAS
Shows how to use Compare-And-Swap (CAS) for safe concurrent updates:
- Retrieve document with CAS value
- Update document using CAS for optimistic locking
- Handle CAS mismatch scenarios
- Prevent race conditions in concurrent environments

**Key Concepts**: CAS, optimistic locking, concurrent updates, race condition prevention

---

### **02_cb_upsert_delete.py** - Upsert and Delete Operations
Demonstrates document lifecycle management:
- Upsert documents (insert or update)
- Delete documents from collections
- Handle document-level operations
- Measure operation performance

**Key Concepts**: Upsert, delete, document lifecycle, CAS tracking

---

### **03a_cb_query.py** - SQL++ (N1QL) Queries
Shows how to query Couchbase using SQL++ (formerly N1QL):
- Execute parameterized queries
- Query across scopes and collections
- Use WHERE clauses and filters
- Handle query results and metadata
- Query performance measurement

**Key Concepts**: SQL++/N1QL, parameterized queries, query metadata, scope/collection queries

---

### **03b_cb_query_profile.py** - SQL++ Query Profiling
Demonstrates how to profile N1QL queries:
- Use `QueryProfile.TIMINGS` to see detailed execution steps
- Analyze time spent in each phase of query execution
- Understand the performance impact of profiling (slower, larger payload)
- Debug slow queries by inspecting the profiling data

**Key Concepts**: Query profiling, timings, debugging, performance analysis


---

### **04_cb_sub_doc_ops.py** - Subdocument Operations
Demonstrates efficient partial document updates:
- LookupIn operations (read specific paths)
- MutateIn operations (update specific paths)
- Update nested fields without retrieving entire document
- Subdocument operation limits (max 16 operations per request)
- Atomic subdocument modifications

**Key Concepts**: Subdocument API, partial updates, LookupIn, MutateIn, path operations

---

### **05_cb_exception_handling.py** - Exception Handling
Comprehensive guide to handling Couchbase exceptions:
- **DocumentNotFoundException** - Handle missing documents
- **ParsingFailedException** - Invalid query syntax
- **TimeoutException** - Operation timeouts and retries
- **CASMismatchException** - Optimistic locking conflicts
- **ServiceUnavailableException** - Service availability issues
- Import data from CSV/Excel with error handling
- Production-ready error handling patterns

**Key Concepts**: Exception hierarchy, retry logic, defensive programming, data import

---

### **06_cb_get_retry_replica_read.py** - High Availability with Replica Reads
Demonstrates reading from replica nodes for high availability:
- **get_with_retry()** - Retry logic with replica fallback
- **get_any_replica()** - Read from fastest available replica (load balancing)
- **get_all_replicas()** - Read from all replicas (consistency checking)
- Simulate timeout scenarios to trigger replica reads
- Understand replica lag and data consistency

**Key Concepts**: Replicas, high availability, failover, load balancing, data consistency

---

### **07_cb_query_own_write.py** - Read Your Own Writes Consistency
Shows how to ensure query consistency after writes:
- Scan consistency options (NOT_BOUNDED, REQUEST_PLUS, AT_PLUS)
- MutationState tracking
- Ensure queries see recent writes
- Balance consistency vs. performance

**Key Concepts**: Scan consistency, MutationState, read-your-own-writes, eventual consistency

---

### **08a_cb_transaction_kv.py** - Key-Value Transactions
Demonstrates ACID transactions for key-value operations:
- Multi-document atomic operations
- Transaction commit and rollback
- Handle transaction failures
- Ensure data consistency across multiple documents
- Transaction isolation

**Key Concepts**: ACID transactions, atomicity, isolation, multi-document updates

---

### **08b_cb_transaction_query.py** - Query-based Transactions
Shows transactional query operations:
- Transactional SQL++ queries
- Multi-statement transactions
- Query within transaction context
- Ensure consistency across query operations

**Key Concepts**: Transactional queries, SQL++ transactions, multi-statement consistency

---

### **09_cb_fts_search.py** - Full-Text Search (FTS) - SQL++ and Native SDK
Comprehensive full-text search demonstrating **both approaches**:

**SQL++ SEARCH() Function (3 examples):**
- Basic text search
- Wildcard search (`fran*`)
- Boolean AND search
- Returns full documents
- Works immediately (no scope-level index needed)

**Native SDK Search API (3 examples):**
- `MatchQuery` - Match term in field
- `MatchPhraseQuery` - Exact phrase matching
- `ConjunctionQuery` - AND logic (multiple conditions)
- Uses `cluster.search()` for bucket-level indexes
- Returns document IDs (faster, ~40x)
- Composable, type-safe query objects

**Detailed Comparison:**
- SQL++ Pros: JOINs, aggregations, full documents, quick setup
- SDK Pros: Index aliases, fewer network hops, scan consistency, lower latency
- When to use each approach with real-world guidance
- Performance differences demonstrated

**Key Concepts**: FTS, SQL++ SEARCH(), native SDK API, MatchQuery, ConjunctionQuery, index aliases, cluster.search(), search performance, query composition

---

### **10_cb_debug_tracing.py** - Debugging and Observability
Demonstrates comprehensive debugging and tracing:
- **Python Logging**: File-based operation logs
- **Slow Operations Logging**: Automatic threshold-based detection
  - Configure thresholds for KV, Query, Search, Analytics operations
  - JSON output with detailed timing breakdowns
  - Identify performance bottlenecks
- **OpenTelemetry Tracing**: Distributed tracing
  - Custom spans for operations
  - Trace operation flow and performance
  - Export to console (extendable to Jaeger, Zipkin)
- Error tracking and debugging patterns

**Key Concepts**: Logging, slow ops detection, OpenTelemetry, performance profiling, observability

---

### **11_cb_async_operations.py** - Async Operations with Class-Based Design
Demonstrates high-performance async operations using a production-ready class structure:
- **AsyncCouchbaseClient Class**:
  - `__init__()` - Initialize with connection and retry configuration
  - `async connect()` - Connect with TLS, WAN profile, observability
  - `async upsert_document()` - Single doc upsert with retry
  - `async get_document()` - Single doc get with retry
  - `async remove_document()` - Single doc remove with retry
  - `async close()` - Resource cleanup
- **Exponential backoff retry** - 0.1s → 0.2s → 0.4s for transient failures
- **Concurrent operations** - 20 upserts, 20 gets using asyncio.gather()
- **Mixed operations** - Upserts, gets, removes running simultaneously
- **Slow operations logging** - KV > 100ms threshold
- **Orphaned response tracking** - Timeout detection
- **Performance metrics** - Throughput and timing analysis
- **DEBUG flag** - Toggle detailed logging on/off

**Key Concepts**: Async/await, concurrent operations, acouchbase, asyncio, OOP/class design, exponential backoff, retry logic, observability

---

### **12_cb_async_queries.py** - Async SQL++ Queries with Observability
Demonstrates async query operations with comprehensive best practices:
- **AsyncCouchbaseQueryClient Class**:
  - `async connect()` - Connection with slow query logging
  - `async execute_query()` - Execute with retry, timing, profiling
  - `async close()` - Clean shutdown
- **7 Query Examples**:
  - Parameterized queries ($country, $limit)
  - 5 concurrent queries with asyncio.gather()
  - Query profiling (PHASES/TIMINGS modes)
  - use_replica for high availability
  - Prepared statements (adhoc=False) - 5 executions showing ~80% speedup
  - Concurrent prepared statements
  - REQUEST_PLUS scan consistency
- **Custom timeouts** - 5s, 10s, 30s examples (default: 75s)
- **Exponential backoff retry** - Transient failure handling
- **Backticks** - Field names and bucket.scope.collection
- **Bind variables** - $country, $limit (prevents SQL injection)
- **Slow query logging** - >500ms threshold with JSON metrics
- **Performance comparison** - adhoc=True vs adhoc=False
- **Query metrics** - execution_time, result_count, percentiles

**Key Concepts**: Async queries, SQL++/N1QL, query profiling, prepared statements, use_replica, scan consistency, bind variables, backticks, timeouts, observability, exponential backoff

---

### **advanced_prepared_statement_wrapper.py** - Prepared Statement Optimization
Production-ready query optimization wrapper:
- Automatic prepared statement management with `adhoc=False`
- SDK-managed query plan caching
- Proper error handling and retries
- Support for named and positional parameters
- Timeout and consistency configuration
- Smart retry logic for transient failures

**Key Concepts**: Prepared statements, query optimization, performance, production patterns

---

### **excel_to_json_to_cb.py** - Data Import from Excel/CSV
Demonstrates bulk data import:
- Read data from Excel or CSV files
- Convert to JSON format
- Bulk insert to Couchbase
- Add audit metadata (timestamps, source tracking)
- Handle import errors gracefully

**Key Concepts**: Bulk import, data migration, audit trails, pandas integration

---

## 🔑 Key-Value Operations Reference

### Basic CRUD Operations

1. **Insert (ADD)**
   - Adds a new document to the collection.
   - Fails if the document already exists.
   ```python
   result = collection.insert("document-key", {"foo": "bar"})
   ```

2. **Upsert (SET)**
   - Inserts a document or replaces it if it already exists.
   ```python
   result = collection.upsert("document-key", {"foo": "bar"})
   ```

3. **Replace (UPDATE)**
   - Replaces an existing document.
   - Fails if the document doesn't exist.
   ```python
   result = collection.replace("document-key", {"foo": "baz"})
   ```

4. **Get**
   - Retrieves a document by its key.
   ```python
   result = collection.get("document-key")
   content = result.content_as[dict]
   ```

5. **Remove (DELETE)**
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

### Subdocument Operations

10. **Lookup In (SUBDOC)**
    - Performs a subdocument lookup operation.
    ```python
    result = collection.lookup_in("document-key", [SD.get("path.to.field")])
    ```

11. **Mutate In (SUBDOC)**
    - Performs a subdocument mutation operation.
    ```python
    result = collection.mutate_in("document-key", [SD.upsert("path.to.field", "value")])
    ```

### Replica Operations

12. **Get Any Replica**
    - Retrieves document from any available replica (fastest response).
    ```python
    result = collection.get_any_replica("document-key")
    ```

13. **Get All Replicas**
    - Retrieves document from all replicas for consistency checking.
    ```python
    results = collection.get_all_replicas("document-key")
    for result in results:
        print(f"Replica: {result.is_replica}, CAS: {result.cas}")
    ```

---

## ⚙️ Configuration

### Connection Setup

All scripts support both **local/self-hosted** and **Capella (cloud)** configurations:

**Local/Self-Hosted:**
```python
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

cluster = Cluster(f'couchbase://{ENDPOINT}', options)
```

**Capella (Cloud):**
```python
ENDPOINT = "cb.your-endpoint.cloud.couchbase.com"
USERNAME = "your-capella-username"
PASSWORD = "your-capella-password"

options.apply_profile('wan_development')
cluster = Cluster(f'couchbases://{ENDPOINT}', options)  # Note: couchbaseS (secure)
```

---

## 📊 Limits and Best Practices

### Document Limits

- **Maximum key length**: 250 bytes
- **Maximum document size**: 20 MB
- **Concurrent KV connections per node**: 60,000

### Subdocument Limits

- **Maximum subdocument operations per request**: 16
- This limit is protocol-level and not configurable

### Query Limits

- **N1QL IN clause keys parameter**: Maximum 1,772 bytes
- **Array function elements**: ~32K or 16K elements (function-dependent)

### Multi-Operation Limits

- **Multi-Get operations**: No explicit limit, but subject to network packet size
- **Batch operations**: Consider breaking into smaller batches for large datasets

---

## 🎯 Recommendations

### For Large-Scale Operations

1. **Batch Processing**: Split large operations into manageable batches
2. **Use Subdocuments**: Update only necessary fields for efficiency
3. **Prepared Statements**: Use `adhoc=False` for frequently executed queries
4. **Replica Reads**: Distribute read load across replicas
5. **Error Handling**: Implement comprehensive exception handling with retries
6. **Monitoring**: Enable slow operations logging for production debugging

### For High Availability

1. Configure replica counts appropriately
2. Use `get_any_replica()` for non-critical reads
3. Implement retry logic with exponential backoff
4. Monitor replication lag

### For Performance

1. Use subdocument operations instead of full document updates
2. Enable prepared statements for repeated queries
3. Choose appropriate scan consistency based on requirements
4. Use bulk operations where possible
5. Monitor slow operations with threshold logging

---

## 🚀 Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Update connection settings in each script (ENDPOINT, USERNAME, PASSWORD)

3. Ensure Couchbase Server is running with `travel-sample` bucket loaded

4. Run individual scripts:
   ```bash
   python3 01a_cb_set_get.py
   ```

5. Check logs and output for results

---

## 📖 Additional Resources

- [Couchbase Python SDK Documentation](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html)
- [N1QL Query Language Reference](https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/index.html)
- [Full-Text Search Guide](https://docs.couchbase.com/server/current/fts/fts-introduction.html)
- [Transactions Documentation](https://docs.couchbase.com/python-sdk/current/howtos/distributed-acid-transactions-from-the-sdk.html)
