# AGENTS.md - AI Agent Guide for cb_python_sdk_samples

## Repository Purpose

This repository contains **sample code demonstrating the Couchbase Python SDK** (version 4.4.0). It serves as a learning resource and reference implementation for developers integrating Couchbase NoSQL database with Python applications.

**Target Audience**: Developers learning Couchbase, those migrating to Couchbase, or building production applications with Couchbase.

---

## Repository Structure

### 📁 Main Sample Scripts (Progressive Learning)

All scripts are numbered to show progression from basic to advanced:

1. **01_cb_set_get.py** - Basic get/upsert operations
2. **01a_cb_get_update_w_cas.py** - CAS (Compare-And-Swap) optimistic locking
3. **02_cb_upsert_delete.py** - Upsert and delete operations
4. **03_cb_query.py** - SQL++ (N1QL) querying
5. **04_cb_sub_doc_ops.py** - Subdocument operations (partial updates)
6. **05_cb_exception_handling.py** - Comprehensive exception handling + CSV import
7. **06_cb_get_retry_replica_read.py** - High availability with replica reads
8. **07_cb_query_own_write.py** - Read-your-own-writes consistency
9. **08a_cb_transaction_kv.py** - ACID transactions (key-value)
10. **08b_cb_transaction_query.py** - ACID transactions (queries)
11. **09_cb_fts_search.py** - Full-text search with index management
12. **10_cb_debug_tracing.py** - Logging, slow ops detection, OpenTelemetry
13. **11_cb_async_operations.py** - Async/await concurrent operations

### 📁 Advanced Utilities

- **advanced_prepared_statement_wrapper.py** - Query optimization with prepared statements
- **excel_to_json_to_cb.py** - Bulk data import from Excel/CSV

### 📁 Documentation

- **README.md** - Main documentation
- **00_cb_kv_ops_list.md** - Comprehensive operations reference
- **advanced_multi_docs.md** - Multi-operation patterns (batch operations)

### 📁 Tests

- **tests/** - Unit tests for all scripts (123 tests total)
- **run_tests.py** - Custom test runner with dependency mocking

### 📁 Data

- **demo_data/** - Sample CSV/Excel files for import examples

---

## Common Commands

### Running Scripts

```bash
# Activate virtual environment first
source venv/bin/activate

# Run any sample script
python3 01_cb_set_get.py
python3 11_cb_async_operations.py

# Scripts output to console and some create log files
# Check couchbase_example.log for logging examples
```

### Running Tests

```bash
# Run all tests (custom runner with mocking)
python3 run_tests.py

# Run with pytest (if installed)
python3 -m pytest tests/ -v

# Run specific test file
python3 -m pytest tests/test_11_cb_async_operations.py -v

# Run with unittest
python3 -m unittest discover -s tests -p "test_*.py"
```

### Type Checking / Linting

```bash
# No specific linter configured
# Python files follow PEP 8 style guidelines
# Use your preferred linter (pylint, flake8, black, etc.)
```

---

## Configuration Patterns

### ⚙️ Connection Configuration

**All scripts support two configurations:**

#### Local/Self-Hosted Couchbase:
```python
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

cluster = Cluster(f'couchbase://{ENDPOINT}', options)  # Non-TLS
```

#### Couchbase Capella (Cloud):
```python
ENDPOINT = "cb.xxxxx.cloud.couchbase.com"
USERNAME = "your-username"
PASSWORD = "your-password"

options.apply_profile('wan_development')  # WAN optimization
cluster = Cluster(f'couchbases://{ENDPOINT}', options)  # TLS required
```

**Key Differences:**
- Capella uses `couchbases://` (with 's' for TLS)
- Capella uses `wan_development` profile for latency optimization
- Local uses `couchbase://` (non-encrypted by default)

### 🔧 Standard Connection Pattern

Every script follows this pattern:

```python
from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

# Configuration
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"
BUCKET_NAME = "travel-sample"
CB_SCOPE = "inventory"
CB_COLLECTION = "airline"

# Connect
auth = PasswordAuthenticator(USERNAME, PASSWORD)
options = ClusterOptions(auth)
cluster = Cluster(f'couchbase://{ENDPOINT}', options)
cluster.wait_until_ready(timedelta(seconds=10))

# Get collection reference
bucket = cluster.bucket(BUCKET_NAME)
collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)

# Perform operations...

# Always close connection
cluster.close()
```

---

## Code Patterns and Conventions

### Exception Handling

All scripts use comprehensive exception handling:

```python
from couchbase.exceptions import (
    CouchbaseException,
    DocumentNotFoundException,
    TimeoutException
)

try:
    result = collection.get(key)
except DocumentNotFoundException:
    # Handle missing document
except TimeoutException:
    # Handle timeout (maybe retry)
except CouchbaseException as e:
    # Handle other Couchbase errors
```

### Async Operations

Async scripts use the `acouchbase` module:

```python
from acouchbase.cluster import Cluster
import asyncio

async def main():
    cluster = await Cluster.connect(f'couchbase://{ENDPOINT}', options)
    await cluster.wait_until_ready(timedelta(seconds=10))
    
    # Async operations
    result = await collection.get(key)
    
    await cluster.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Content Access Pattern

Documents are retrieved with type specification:

```python
result = collection.get(key)
content = result.content_as[dict]  # For JSON documents
# Or: result.content_as[str], result.content_as[bytes]
```

---

## Testing Strategy

### Test Structure

- **Unit tests**: Mock Couchbase SDK to test logic without live cluster
- **run_tests.py**: Custom runner that mocks unavailable dependencies
- **123 tests** covering all 11 main scripts + utilities

### Running Tests

```bash
# Preferred method (handles mocking automatically)
python3 run_tests.py

# Expected output:
# Ran 123 tests in ~0.1s
# OK (all passing)
```

### Test Files Map

| Script | Test File |
|--------|-----------|
| 01_cb_set_get.py | test_01_cb_set_get.py |
| 01a_cb_get_update_w_cas.py | test_01a_cb_get_update_w_cas.py |
| 02_cb_upsert_delete.py | test_02_cb_upsert_delete.py |
| 03_cb_query.py | test_03_cb_query.py |
| 04_cb_sub_doc_ops.py | test_04_cb_sub_doc_ops.py |
| 05_cb_exception_handling.py | test_05_cb_exception_handling.py |
| 06_cb_get_retry_replica_read.py | test_06_cb_get_retry_replica_read.py |
| 07_cb_query_own_write.py | test_07_cb_query_own_write.py |
| 08a_cb_transaction_kv.py | test_08a_cb_transaction_kv.py |
| 08b_cb_transaction_query.py | test_08b_cb_transaction_query.py |
| 09_cb_fts_search.py | test_09_cb_fts_search.py |
| 10_cb_debug_tracing.py | test_10_cb_debug_tracing.py |
| 11_cb_async_operations.py | test_11_cb_async_operations.py |
| advanced_prepared_statement_wrapper.py | test_advanced_prepared_statement_wrapper.py |
| excel_to_json_to_cb.py | test_excel_to_json_to_cb.py |

---

## Key Concepts Demonstrated

### 🔑 Key-Value Operations
- **CRUD**: Insert, upsert, replace, get, remove
- **CAS**: Optimistic locking with Compare-And-Swap
- **Subdocuments**: Partial updates (LookupIn, MutateIn)
- **Replicas**: High availability reads
- **Expiration**: Document TTL

### 📊 Querying
- **SQL++/N1QL**: SQL-like queries
- **Parameterization**: Named and positional parameters
- **Prepared Statements**: `adhoc=False` for performance
- **Scan Consistency**: NOT_BOUNDED, REQUEST_PLUS, AT_PLUS

### 💳 Transactions
- **ACID Guarantees**: Atomicity, Consistency, Isolation, Durability
- **Multi-Document**: Atomic operations across documents
- **Rollback**: Automatic on failure

### 🔍 Search
- **FTS**: Full-text search with SEARCH() function
- **Index Management**: Automatic creation and waiting
- **Wildcards**: Pattern matching (`fran*`)
- **Boolean**: AND/OR/NOT queries

### ⚡ Performance
- **Async Operations**: Concurrent I/O with `acouchbase`
- **Prepared Statements**: Query plan caching
- **Subdocuments**: Efficient partial updates
- **Batch Operations**: Multi-get, multi-upsert

### 🐛 Debugging
- **Logging**: Python logging to file
- **Slow Operations**: Threshold-based auto-detection
- **OpenTelemetry**: Distributed tracing
- **Exception Handling**: Comprehensive error recovery

---

## Important Limits

| Limit | Value | Notes |
|-------|-------|-------|
| Max key length | 250 bytes | Per document |
| Max document size | 20 MB | Per document |
| Subdocument ops/request | 16 | Protocol limit |
| N1QL IN clause keys | 1,772 bytes | Total size |
| Concurrent KV connections | 60,000 | Per node |

---

## Common Issues & Solutions

### Issue: Script hangs on startup
**Cause**: Connecting to wrong endpoint or cluster not running  
**Solution**: 
- Verify Couchbase is running: `http://localhost:8091`
- Check ENDPOINT, USERNAME, PASSWORD in script
- Ensure using `couchbase://` for local (not `couchbases://`)

### Issue: BucketNotFoundException
**Cause**: Bucket doesn't exist  
**Solution**: Load travel-sample bucket via Couchbase Web Console

### Issue: Pandas import slow (10+ seconds)
**Cause**: Normal on M1/M2 Macs with certain numpy builds  
**Solution**: Wait ~15 seconds, or skip scripts that use pandas

### Issue: NetworkException doesn't exist
**Cause**: Old exception name  
**Solution**: Use `ServiceUnavailableException` instead

### Issue: Tests fail with import errors
**Cause**: Couchbase SDK not installed or wrong path  
**Solution**: Run `python3 run_tests.py` which handles mocking

---

## Adding New Samples

When adding new sample scripts:

1. **Follow numbering convention**: `12_cb_new_feature.py`
2. **Include docstring**: Explain what the script demonstrates
3. **Add configuration section**:
   ```python
   # For local/self-hosted Couchbase Server:
   ENDPOINT = "localhost"
   # For Capella (cloud), uncomment...
   ```
4. **Add exception handling**: Use try/except with specific exceptions
5. **Close connections**: Always call `cluster.close()`
6. **Create unit test**: Add `tests/test_12_cb_new_feature.py`
7. **Update documentation**: Add to README.md and 00_cb_kv_ops_list.md

---

## Development Workflow

### Making Changes

1. Update script in root directory
2. Test manually: `python3 XX_script.py`
3. Update corresponding test in `tests/`
4. Run tests: `python3 run_tests.py`
5. Update documentation if needed

### Before Committing

```bash
# Ensure all tests pass
python3 run_tests.py

# Verify scripts work with travel-sample bucket
python3 01_cb_set_get.py

# Check for common issues
grep -r "couchbases://" *.py  # Ensure local uses couchbase://
grep -r "NetworkException" *.py  # Should use ServiceUnavailableException
```

---

## SDK Version Compatibility

**Current Version**: Couchbase Python SDK 4.4.0

**Key Features in 4.4.0:**
- Improved async support with `acouchbase`
- Enhanced slow operations logging with `ClusterTracingOptions`
- Better transaction support
- Full-text search improvements
- Replica read enhancements

**Upgrading SDK:**
```bash
pip install --upgrade couchbase
# Update requirements.txt if pinning new version
```

---

## Important Notes for AI Agents

### When Helping Users:

1. **Always check connection config first**: Most issues stem from wrong endpoint or using `couchbases://` for local
2. **travel-sample bucket required**: All samples use this bucket
3. **Exception names matter**: `NetworkException` doesn't exist, use `ServiceUnavailableException`
4. **Async imports**: Use `from acouchbase.cluster import Cluster` for async
5. **Content access**: Always use `result.content_as[dict]` not `result.content_as[str]` for JSON docs
6. **Test runner**: Always use `python3 run_tests.py` not direct pytest (handles mocking)

### Common Fix Patterns:

**Connection hangs:**
```python
# Change from:
cluster = Cluster('couchbases://localhost', options)
# To:
cluster = Cluster('couchbase://localhost', options)
```

**Wrong content access:**
```python
# Change from:
print(result.content_as[str])
# To:
print(result.content_as[dict])
```

**Missing exception:**
```python
# Change from:
from couchbase.exceptions import NetworkException
# To:
from couchbase.exceptions import ServiceUnavailableException
```

### SDK API Patterns:

1. **Cluster-level operations**: `cluster.query()`, `cluster.bucket()`
2. **Collection-level operations**: `collection.get()`, `collection.upsert()`
3. **Prepared statements**: Query on `Cluster` or `Scope` (not `Collection`)
4. **Timeouts**: Always use `timedelta`, never raw integers or microseconds
5. **Parameters**: Use `named_parameters=` or `positional_parameters=` (not `parameters=`)

---

## Dependencies

**Required:**
- `couchbase==4.4.0` - Core SDK

**Optional (for specific examples):**
- `pandas>=1.5.0` - CSV/Excel import (05, excel_to_json_to_cb)
- `openpyxl>=3.0.0` - Excel support
- `opentelemetry-api>=1.15.0` - Tracing (10)
- `opentelemetry-sdk>=1.15.0` - Tracing SDK (10)

---

## Testing Notes

- **123 total tests** across 15 test files
- Tests use mocking to avoid requiring live Couchbase cluster
- `run_tests.py` handles dependency mocking automatically
- All tests pass when run with `python3 run_tests.py`
- Tests validate logic, not actual Couchbase operations

---

## Troubleshooting Commands

```bash
# Check if Couchbase is running
curl http://localhost:8091

# Verify Python version
python3 --version  # Should be 3.8+

# Check installed packages
pip list | grep couchbase

# Verify travel-sample bucket loaded
# Visit: http://localhost:8091 → Buckets

# Test connection
python3 01_cb_set_get.py

# Run all tests
python3 run_tests.py
```

---

## Reference Documentation

- [Couchbase Python SDK 4.4.0 Docs](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html)
- [N1QL Query API](https://docs.couchbase.com/sdk-api/couchbase-python-client/couchbase_api/couchbase_n1ql.html)
- [Exception Handling](https://docs.couchbase.com/sdk-api/couchbase-python-client/couchbase_api/exceptions.html)
- [Async Operations](https://docs.couchbase.com/sdk-api/couchbase-python-client/acouchbase_api/acouchbase_core.html)
- [Slow Operations Logging](https://docs.couchbase.com/python-sdk/current/howtos/slow-operations-logging.html)

---

## Quick Reference: Exception Types

```python
# Import these as needed:
from couchbase.exceptions import (
    CouchbaseException,           # Base exception
    DocumentNotFoundException,    # Get on missing doc
    DocumentExistsException,      # Insert on existing doc
    TimeoutException,             # Operation timeout
    AmbiguousTimeoutException,    # Uncertain timeout
    UnAmbiguousTimeoutException,  # Definite timeout
    AuthenticationException,      # Auth failure
    CASMismatchException,         # Optimistic lock conflict
    ParsingFailedException,       # Bad query syntax
    ServiceUnavailableException,  # Service/network issue
    InternalServerFailureException # Server error
)
```

---

## Quick Reference: Key Methods

### Collection Operations
```python
collection.get(key)
collection.upsert(key, doc)
collection.insert(key, doc)
collection.replace(key, doc)
collection.remove(key)
collection.get_any_replica(key)
collection.get_all_replicas(key)
collection.lookup_in(key, [SD.get("path")])
collection.mutate_in(key, [SD.upsert("path", value)])
```

### Cluster Operations
```python
cluster.query(statement, QueryOptions(...))
cluster.bucket(name)
cluster.wait_until_ready(timedelta(seconds=10))
cluster.search_indexes()  # For FTS index management
cluster.close()
```

### Async Operations
```python
await Cluster.connect(...)
await cluster.wait_until_ready(...)
await collection.get(key)
await collection.upsert(key, doc)
await cluster.close()
```

---

## File Naming Convention

- **##_cb_*.py** - Numbered samples (progressive learning)
- **advanced_*.py** - Advanced/production patterns
- **test_*.py** - Unit tests
- **##_*.md** - Documentation files
- **demo_data/** - Sample data files

---

## Environment

- **Python**: 3.8+
- **Couchbase SDK**: 4.4.0
- **Test Framework**: unittest (not pytest by default)
- **Async**: asyncio + acouchbase
- **Data Processing**: pandas (optional)
- **Tracing**: OpenTelemetry (optional)

---

**Last Updated**: 2025-11-13  
**SDK Version**: 4.4.0  
**Python Version**: 3.8+  
**Maintained By**: Fujio Turner
