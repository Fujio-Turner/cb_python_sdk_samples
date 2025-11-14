# Couchbase Python SDK Samples

Welcome to the `cb_python_sdk_samples` repository! This project provides a comprehensive collection of sample code demonstrating how to use the [Couchbase Python SDK](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html) to interact with Couchbase Server. These examples cover everything from basic operations to advanced features like transactions, full-text search, and asynchronous programming.

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Sample Scripts](#sample-scripts)
- [Configuration](#configuration)
- [Running Tests](#running-tests)
- [Contributing](#contributing)
- [License](#license)

## Overview

This repository showcases practical, production-ready examples for working with Couchbase using the Python SDK. Couchbase is a distributed NoSQL database known for its high performance, scalability, and flexibility.

**What You'll Learn:**
- Basic key-value operations (CRUD)
- SQL++ (N1QL) querying
- Subdocument operations
- Exception handling and error recovery
- High availability with replica reads
- Transactions (ACID compliance)
- Full-text search
- Debugging and tracing
- Asynchronous operations
- Production best practices

Whether you're a beginner or an experienced developer, these samples provide practical starting points for integrating Couchbase into your Python projects.

## Prerequisites

To use these samples, you'll need:

1. **Couchbase Server**: 
   - Local/Self-hosted: [Download Community or Enterprise Edition](https://www.couchbase.com/downloads)
   - Cloud: [Couchbase Capella](https://cloud.couchbase.com/) (free tier available)

2. **Python**: Version 3.8 or higher

3. **Couchbase Python SDK**: Version 4.4.0
   - These samples are tested with Couchbase Python SDK 4.4.0
   - For latest SDK version, see [Couchbase Python SDK Releases](https://pypi.org/project/couchbase/)

4. **Couchbase Bucket**: 
   - The samples use the `travel-sample` bucket (included with Couchbase Server)
   - Load it via: Couchbase Web Console → Settings → Sample Buckets → travel-sample

5. **Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   
   **Key Dependencies:**
   - `couchbase==4.4.0` - Couchbase Python SDK
   - `pandas>=1.5.0` - Data processing (for Excel/CSV import examples)
   - `openpyxl>=3.0.0` - Excel file support
   - `opentelemetry-api>=1.15.0` - Tracing/observability
   - `opentelemetry-sdk>=1.15.0` - Tracing SDK

## Quick Start

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Fujio-Turner/cb_python_sdk_samples.git
   cd cb_python_sdk_samples
   ```

2. **Set Up Virtual Environment** (recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Connection**:
   - Edit the connection settings in any script:
   ```python
   ENDPOINT = "localhost"  # Or your Capella hostname
   USERNAME = "Administrator"
   PASSWORD = "password"
   ```

5. **Run Your First Sample**:
   ```bash
   python3 01_cb_set_get.py
   ```

## Sample Scripts

### 📌 Basic Operations

#### **01_cb_set_get.py** - Basic Get and Upsert
Your first Couchbase script! Learn to connect, insert, and retrieve documents.
- Connect to Couchbase (local & Capella configs)
- Upsert documents
- Get documents by key
- Measure performance
- Connection management

**Run**: `python3 01_cb_set_get.py`

---

#### **01a_cb_get_update_w_cas.py** - Optimistic Locking with CAS
Handle concurrent updates safely using Compare-And-Swap.
- CAS-based updates
- Prevent race conditions
- Handle CAS mismatch
- Concurrent update patterns

**Run**: `python3 01a_cb_get_update_w_cas.py`

---

#### **02_cb_upsert_delete.py** - Upsert and Delete
Master document lifecycle management.
- Upsert operations
- Delete operations
- Document lifecycle
- CAS tracking

**Run**: `python3 02_cb_upsert_delete.py`

---

### 📊 Querying

#### **03_cb_query.py** - SQL++ (N1QL) Queries
Query your data using SQL-like syntax.
- Parameterized queries
- WHERE clauses
- Query metadata
- Performance measurement

**Run**: `python3 03_cb_query.py`

---

### 🎯 Advanced Operations

#### **04_cb_sub_doc_ops.py** - Subdocument Operations
Update parts of documents without fetching the whole thing.
- LookupIn operations
- MutateIn operations
- Partial updates
- Path operations
- **Limit**: Max 16 operations per request

**Run**: `python3 04_cb_sub_doc_ops.py`

---

#### **05_cb_exception_handling.py** - Exception Handling
Comprehensive error handling guide with real-world examples.
- DocumentNotFoundException
- ParsingFailedException
- TimeoutException
- CASMismatchException
- CSV/Excel data import
- Production error patterns

**Run**: `python3 05_cb_exception_handling.py`

---

#### **06_cb_get_retry_replica_read.py** - High Availability
Read from replicas for high availability and load balancing.
- Retry with replica fallback
- `get_any_replica()` - fastest response
- `get_all_replicas()` - consistency checking
- Simulated timeout scenarios
- Replica lag considerations

**Run**: `python3 06_cb_get_retry_replica_read.py`

---

#### **07_cb_query_own_write.py** - Read Your Own Writes
Ensure queries see recent writes with scan consistency.
- Scan consistency options
- MutationState tracking
- REQUEST_PLUS consistency
- Consistency vs. performance trade-offs

**Run**: `python3 07_cb_query_own_write.py`

---

### 💳 Transactions

#### **08a_cb_transaction_kv.py** - Key-Value Transactions
ACID transactions for key-value operations.
- Multi-document atomicity
- Transaction commit/rollback
- Isolation guarantees
- Failure handling

**Run**: `python3 08a_cb_transaction_kv.py`

---

#### **08b_cb_transaction_query.py** - Query Transactions
Transactional SQL++ queries.
- Multi-statement transactions
- Query within transaction context
- Transactional consistency

**Run**: `python3 08b_cb_transaction_query.py`

---

### 🔍 Search & Discovery

#### **09_cb_fts_search.py** - Full-Text Search
Powerful text search with automatic index management.
- **Auto-creates FTS indexes** if missing
- Wildcard search (`fran*`)
- Phrase search (`"historic building"`)
- Boolean queries (AND/OR)
- Field-specific search
- Relevance scoring

**Run**: `python3 09_cb_fts_search.py`

---

### 🐛 Debugging & Monitoring

#### **10_cb_debug_tracing.py** - Debugging and Observability
Complete debugging toolkit for production.
- **Python logging** to file
- **Slow operations logging** (threshold-based)
  - Configurable thresholds (KV: 100ms, Query: 500ms)
  - JSON output with timing details
  - Automatic detection
- **OpenTelemetry tracing**
  - Custom spans
  - Distributed tracing
  - Console export (extendable to Jaeger/Zipkin)
- Error tracking patterns

**Run**: `python3 10_cb_debug_tracing.py`  
**Output**: Check `couchbase_example.log`

---

### ⚡ Asynchronous Operations

#### **11_cb_async_operations.py** - Async Operations
High-performance async operations with acouchbase.
- Async connect
- **20 concurrent upserts**
- **20 concurrent gets**
- Performance metrics
- `asyncio` patterns
- Throughput optimization

**Run**: `python3 11_cb_async_operations.py`

---

### 🔧 Advanced Utilities

#### **advanced_prepared_statement_wrapper.py** - Query Optimization
Production-ready prepared statement wrapper.
- Automatic prepared statement caching (`adhoc=False`)
- SDK-managed optimization
- Named & positional parameters
- Retry logic
- Smart error handling

**Run**: `python3 advanced_prepared_statement_wrapper.py`

---

#### **excel_to_json_to_cb.py** - Data Import
Bulk import data from Excel/CSV files.
- Read Excel/CSV
- JSON conversion
- Bulk insert
- Audit metadata
- Error handling

**Run**: `python3 excel_to_json_to_cb.py`

---

## Configuration

### Local Couchbase Server

```python
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

cluster = Cluster(f'couchbase://{ENDPOINT}', options)
```

### Couchbase Capella (Cloud)

```python
ENDPOINT = "cb.xxxxx.cloud.couchbase.com"  # Your Capella endpoint
USERNAME = "your-username"
PASSWORD = "your-password"

options.apply_profile('wan_development')
cluster = Cluster(f'couchbases://{ENDPOINT}', options)  # Note: couchbaseS
```

**Key Differences:**
- Capella uses `couchbases://` (TLS/SSL)
- Capella benefits from `wan_development` profile
- Local uses `couchbase://` (non-encrypted)

## Running Tests

Run the test suite to verify your setup:

```bash
python3 run_tests.py
```

Or run specific test files:

```bash
python3 -m pytest tests/test_10_cb_debug_tracing.py
```

## Documentation Files

- **[00_cb_kv_ops_list.md](00_cb_kv_ops_list.md)** - Comprehensive reference of all scripts and operations
- **[advanced_multi_docs.md](advanced_multi_docs.md)** - Multi-operation patterns (get_multi, upsert_multi, etc.)

## Key Features by Category

### 🎯 High Availability
- Replica reads (06)
- Retry logic (05, 06)
- Failover patterns (06)

### ⚡ Performance
- Async operations (11)
- Prepared statements (advanced_prepared_statement_wrapper.py)
- Subdocument operations (04)
- Batch operations (advanced_multi_docs.md)

### 🔒 Data Consistency
- Transactions (08a, 08b)
- CAS/Optimistic locking (01a)
- Scan consistency (07)

### 🐛 Debugging
- Logging (10)
- Slow operations detection (10)
- OpenTelemetry tracing (10)
- Exception handling (05)

### 🔍 Search
- Full-text search (09)
- SQL++ queries (03, 07)
- Index management (09)

## Best Practices Demonstrated

✅ **Connection Management**: Proper connect/disconnect patterns  
✅ **Error Handling**: Comprehensive exception handling  
✅ **Performance**: Async, prepared statements, subdocs  
✅ **High Availability**: Replica reads, retry logic  
✅ **Data Integrity**: Transactions, CAS  
✅ **Monitoring**: Logging, tracing, metrics  
✅ **Production Ready**: Real-world patterns  

## Common Limits

| Limit | Value |
|-------|-------|
| Max key length | 250 bytes |
| Max document size | 20 MB |
| Subdocument ops per request | 16 |
| Concurrent KV connections/node | 60,000 |

See [00_cb_kv_ops_list.md](00_cb_kv_ops_list.md) for complete limits.

## Contributing

Contributions are welcome! If you have ideas for new samples, improvements, or bug fixes:

1. Fork this repository
2. Create a new branch (`git checkout -b feature/your-idea`)
3. Make your changes and commit them (`git commit -m "Add my feature"`)
4. Push to your fork (`git push origin feature/your-idea`)
5. Open a pull request

Please ensure your code:
- Follows Python best practices
- Includes clear comments
- Has proper error handling
- Works with both local and Capella configurations

## License

This project is licensed under the [MIT License](LICENSE). Feel free to use, modify, and distribute the code as needed.

## Resources

- [Couchbase Python SDK Documentation](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html)
- [Couchbase Server Documentation](https://docs.couchbase.com/server/current/introduction/intro.html)
- [SQL++ (N1QL) Reference](https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/index.html)
- [Full-Text Search Guide](https://docs.couchbase.com/server/current/fts/fts-introduction.html)
- [Transactions Documentation](https://docs.couchbase.com/python-sdk/current/howtos/distributed-acid-transactions-from-the-sdk.html)
- [Slow Operations Logging](https://docs.couchbase.com/python-sdk/current/howtos/slow-operations-logging.html)
- [Async Operations](https://docs.couchbase.com/sdk-api/couchbase-python-client/acouchbase_api/acouchbase_core.html)

## Support

- [Couchbase Forums](https://forums.couchbase.com/)
- [Couchbase Discord](https://discord.gg/couchbase)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/couchbase)
- [GitHub Issues](https://github.com/Fujio-Turner/cb_python_sdk_samples/issues)

---

**Happy Coding with Couchbase! 🚀**
