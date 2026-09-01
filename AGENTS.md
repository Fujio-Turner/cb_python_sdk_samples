# AGENTS.md - AI Agent Guide for cb_python_sdk_samples

## Repository Purpose

This repository contains **sample code demonstrating the Couchbase Python SDK** (version **4.6.3**, API spec 3.9). It is a learning resource and copy-paste reference for integrating Couchbase with Python.

**Target Audience**: Developers learning Couchbase, migrating a 4.4-era client to 4.6, or building production applications.

---

## Repository Structure

### Main Sample Scripts (Progressive Learning)

Numbered scripts go from basic KV to search, async, and counters. Filenames are the source of truth:

1. **01a_cb_set_get.py** — Basic get/upsert
2. **01b_cb_get_update_w_cas.py** — CAS optimistic locking (`CASMismatchException`)
3. **02_cb_upsert_delete.py** — Upsert and delete
4. **03a_cb_query.py** — SQL++ (N1QL) querying
5. **03b_cb_query_profile.py** — SQL++ query profiling
6. **04_cb_sub_doc_ops.py** — Subdocument LookupIn / MutateIn
7. **05_cb_exception_handling.py** — Exception handling + CSV/Excel import
8. **06_cb_get_retry_replica_read.py** — Replica reads + zone-aware `ReadPreference`
9. **07_cb_query_own_write.py** — Read-your-own-writes consistency
10. **08a_cb_transaction_kv.py** — ACID transactions (key-value)
11. **08b_cb_transaction_query.py** — ACID transactions (queries)
12. **09_cb_fts_search.py** — Full-text search (SQL++ `SEARCH()` + `scope.search()`)
13. **10_cb_debug_tracing.py** — Logging, slow ops, native OpenTelemetry
14. **11_cb_async_operations.py** — Async KV (`acouchbase`)
15. **12_cb_async_queries.py** — Async SQL++ queries
16. **13_cb_increment.py** — Binary increment / decrement

There is no `01_cb_set_get.py`. The first script is **01a**.

### Advanced Utilities

- **advanced_prepared_statement_wrapper.py** — Prepared statements (`adhoc=False`)
- **excel_to_json_to_cb.py** — Bulk import (`upsert_multi`, not `mutate_in_batch`)
- **fts/create_hotels_index.py** + **fts/hotels-index.json** — scoped FTS index for script 09
- **ai_vector_sample/** — vector search on `cake.us.orders` (FTS on 7.6, GSI on 8.0)

### Documentation

- **README.md** — Main documentation
- **00_cb_ops_list.md** — Operations reference for samples 01–13
- **advanced_multi_docs.md** — Multi-operation patterns
- **ai_vector_sample/README.md** — Vector index + query walkthrough
- **tests/README.md** — Mock test runner
- **work/v3_to_4.md** — Branch working log for the 4.4.0 → 4.6.3 upgrade (not end-user docs)

### Tests

- **tests/** — 20 test files (226 collected cases on disk)
- **run_tests.py** — Supported runner: **165 tests, 0 failures** (mocks Couchbase)
- **tests/test_sample_syntax.py** — Compile-checks every sample `.py`
- Scripts **07 / 08a / 08b / 13** have tests on disk but are **not** in `run_tests.py` (`assertRaises` vs MagicMock)

### Data

- **demo_data/** — Sample CSV/Excel for import examples
- **ai_vector_sample/01_vector_docs.json** — 4 country docs with 128-dim embeddings

---

## Common Commands

### Running Scripts

```bash
source venv/bin/activate

python3 01a_cb_set_get.py
python3 11_cb_async_operations.py

# FTS (needs Search Service + travel-sample)
python3 fts/create_hotels_index.py
python3 09_cb_fts_search.py

# Vector (needs cake.us.orders + Search Service)
python3 ai_vector_sample/create_vector_indexes.py
python3 ai_vector_sample/04_vector_search_using_python_sdk.py
```

### Running Tests

```bash
# Preferred (handles mocking; 165 tests)
python3 run_tests.py

# Single file
python3 -m unittest tests.test_11_cb_async_operations -v

# Full discover (includes 07/08/13; those files may fail under MagicMock)
python3 -m unittest discover -s tests -p "test_*.py"
```

Do **not** treat raw pytest as the supported runner. Use `python3 run_tests.py`.

---

## Configuration Patterns

### Connection Configuration

All scripts support two configurations.

#### Local / self-hosted

```python
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

cluster = Cluster.connect(f'couchbase://{ENDPOINT}', options)  # Non-TLS
```

#### Couchbase Capella (cloud)

```python
ENDPOINT = "cb.xxxxx.cloud.couchbase.com"
USERNAME = "your-username"
PASSWORD = "your-password"

options.apply_profile('wan_development')
cluster = Cluster.connect(f'couchbases://{ENDPOINT}', options)  # TLS required
```

**Key differences:**
- Capella uses `couchbases://` (TLS)
- Capella uses `wan_development` for WAN latency
- Local uses `couchbase://`

### Standard Connection Pattern (SDK 4.6)

```python
from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"
BUCKET_NAME = "travel-sample"
CB_SCOPE = "inventory"
CB_COLLECTION = "airline"

auth = PasswordAuthenticator(USERNAME, PASSWORD)
options = ClusterOptions(auth)
cluster = Cluster.connect(f'couchbase://{ENDPOINT}', options)
cluster.wait_until_ready(timedelta(seconds=10))

bucket = cluster.bucket(BUCKET_NAME)
collection = bucket.scope(CB_SCOPE).collection(CB_COLLECTION)

# ... operations ...

cluster.close()
# After close(), create a new Cluster.connect(...) — 4.6 will not reconnect
```

---

## Code Patterns and Conventions

### Exception Handling

```python
from couchbase.exceptions import (
    CouchbaseException,
    DocumentNotFoundException,
    TimeoutException,
    CASMismatchException,
)

try:
    result = collection.get(key)
except DocumentNotFoundException:
    pass
except TimeoutException:
    pass
except CASMismatchException:
    pass
except CouchbaseException as e:
    pass
```

Use `CASMismatchException` as the documented name (`CasMismatchException` is an alias). There is no `NetworkException` — use `ServiceUnavailableException`.

### Async Operations

```python
from acouchbase.cluster import Cluster
import asyncio

async def main():
    cluster = await Cluster.connect(f'couchbase://{ENDPOINT}', options)
    await cluster.wait_until_ready(timedelta(seconds=10))
    bucket = cluster.bucket(BUCKET_NAME)
    await bucket.on_connect()

    result = await collection.get(key)

    await cluster.close()

if __name__ == "__main__":
    asyncio.run(main())
```

The import name is `Cluster` from `acouchbase.cluster` (not `AsyncCluster`).

### Content Access Pattern

```python
result = collection.get(key)
content = result.content_as[dict]  # JSON documents
```

### Full-Text Search (script 09)

- Index is **scoped**: `travel-sample.inventory.hotels-index`
- Create with `python3 fts/create_hotels_index.py` (PUT `:8094/api/bucket/{bucket}/scope/{scope}/index/{name}`; no `uuid` / `sourceUUID` on create)
- Native SDK: `scope.search(INDEX_NAME, SearchRequest.create(...))` — **not** `cluster.search()` (that is for global indexes)
- SQL++ `SEARCH()` must pass the fully-qualified name: `{"index": "travel-sample.inventory.hotels-index"}`. Bare `hotels-index` or `inventory.hotels-index` fails with n1fty “index mapping not found”
- `ConjunctionQuery([q1, q2])` list form is valid (SDK 4.5+)
- Type mapping: `inventory.hotel`; stored fields: `name`, `country`, `city`, `description`

### Vector Search (`ai_vector_sample/`)

- Data: `cake.us.orders` (not travel-sample)
- **FTS vector** `cake.us.vect`: Server **7.6+** — this is what runs on 7.6.5
- **GSI `CREATE VECTOR INDEX` / `APPROX_VECTOR_DISTANCE`**: Server **8.0+** — skip / catch on 7.6
- SDK: `SearchRequest.create(VectorSearch.from_vector_query(VectorQuery("embedding", vec, num_candidates=...)))` then `scope.search("vect", request, ...)`
- Pre-filter: `VectorQuery.create(..., prefilter=MatchQuery(...))` (SDK 4.4+ / Server 7.6.4+)
- SQL++ FTS knn uses index `cake.us.vect`

### Zone-aware replica reads (script 06)

`Collection` has **no** `get_replica_from_preferred_server_group()`. That method is on transaction `AttemptContext`.

KV path:

```python
from couchbase.options import GetAnyReplicaOptions
from couchbase.replica_reads import ReadPreference

collection.get_any_replica(
    key,
    GetAnyReplicaOptions(read_preference=ReadPreference.SELECTED_SERVER_GROUP),
)
```

Set `ClusterOptions(preferred_server_group=...)` at connect. On a 1-node / 0-replica cluster this raises `DocumentUnretrievableException`.

---

## Testing Strategy

- **Unit tests**: Mock the SDK. Most tests reimplement sample logic and do **not** import numbered scripts (those connect at import time).
- **run_tests.py**: Custom runner; mocks missing deps; patches `Cluster.connect` / `await Cluster.connect`.
- **165 tests, 0 failures** is the supported green run.
- **test_sample_syntax.py** compile-checks samples so a syntax error fails the suite.
- The vector test is the only one that `exec`s a sample file.

### Test Files Map

| Script | Test File |
|--------|-----------|
| 01a_cb_set_get.py | test_01_cb_set_get.py |
| 01b_cb_get_update_w_cas.py | test_01a_cb_get_update_w_cas.py |
| 02_cb_upsert_delete.py | test_02_cb_upsert_delete.py |
| 03a_cb_query.py | test_03a_cb_query.py |
| 03b_cb_query_profile.py | test_03b_cb_query_profile.py |
| 04_cb_sub_doc_ops.py | test_04_cb_sub_doc_ops.py |
| 05_cb_exception_handling.py | test_05_cb_exception_handling.py |
| 06_cb_get_retry_replica_read.py | test_06_cb_get_retry_replica_read.py |
| 07_cb_query_own_write.py | test_07_cb_query_own_write.py (not in run_tests.py) |
| 08a_cb_transaction_kv.py | test_08a_cb_transaction_kv.py (not in run_tests.py) |
| 08b_cb_transaction_query.py | test_08b_cb_transaction_query.py (not in run_tests.py) |
| 09_cb_fts_search.py | test_09_cb_fts_search.py |
| 10_cb_debug_tracing.py | test_10_cb_debug_tracing.py |
| 11_cb_async_operations.py | test_11_cb_async_operations.py |
| 12_cb_async_queries.py | test_12_cb_async_queries.py |
| 13_cb_increment.py | test_13_cb_increment.py (not in run_tests.py) |
| advanced_prepared_statement_wrapper.py | test_advanced_prepared_statement_wrapper.py |
| excel_to_json_to_cb.py | test_excel_to_json_to_cb.py |
| ai_vector_sample/04_*.py | test_ai_vector_search.py |
| all sample `.py` | test_sample_syntax.py |

---

## Key Concepts Demonstrated

### Key-Value Operations
- **CRUD**: Insert, upsert, replace, get, remove
- **CAS**: Optimistic locking (`CASMismatchException`)
- **Subdocuments**: LookupIn, MutateIn (max 16 specs)
- **Replicas**: `get_any_replica` / `get_all_replicas` / zone-aware reads
- **Counters**: `collection.binary().increment` / `decrement`
- **Multi-ops**: `get_multi`, `upsert_multi`, `lookup_in_multi`, `mutate_in_multi`

### Querying
- **SQL++/N1QL**: Named and positional parameters
- **Prepared statements**: `adhoc=False`
- **Scan consistency**: NOT_BOUNDED, REQUEST_PLUS, AT_PLUS
- **Profiling**: `QueryProfile.TIMINGS` / `PHASES`

### Transactions
- ACID multi-document KV and query transactions
- Close the cluster in `finally`; do not reconnect the same instance

### Search
- Scoped FTS + SQL++ `SEARCH()` vs SDK `scope.search()`
- Vector FTS KNN (7.6) vs GSI vector (8.0)

### Performance
- Async with `acouchbase`
- Prepared statements and subdocuments
- Multi-ops (do not mix client + server durability in one multi-op)

### Debugging
- Python logging + slow-ops thresholds
- Native OTel: `get_otel_tracer(provider)` on `ClusterOptions`
- `ClusterMetricsOptions` / `LoggingMeter`
- `GetOptions(parent_span=...)`

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

### Script hangs on startup
**Cause**: Wrong endpoint or cluster not running  
**Solution**: Verify `http://localhost:8091`; use `couchbase://` for local (not `couchbases://`)

### BucketNotFoundException
**Cause**: Bucket missing  
**Solution**: Load travel-sample (or create `cake` for vector)

### Pandas import slow (10+ seconds)
**Cause**: Normal on some Mac numpy builds  
**Solution**: Wait, or skip pandas scripts

### NetworkException doesn't exist
**Solution**: Use `ServiceUnavailableException`

### Tests fail with import errors
**Solution**: `python3 run_tests.py` (handles mocking). Ensure `tests/__init__.py` exists so local tests are not shadowed by site-packages `tests`.

### 09 FTS “index mapping not found”
**Cause**: SQL++ `SEARCH()` used a short index name  
**Solution**: Use `travel-sample.inventory.hotels-index`. Create the index with `fts/create_hotels_index.py`. REST PUT needs an explicit `Authorization: Basic ...` header (urllib `HTTPBasicAuthHandler` can 403).

### 06 DocumentUnretrievableException on replica read
**Cause**: No replica (typical 1-node / 0-replica)  
**Solution**: Expected. Durability examples in 05 also need replicas.

### GSI CREATE VECTOR INDEX syntax error / APPROX_VECTOR_DISTANCE unknown
**Cause**: Couchbase Server 7.6  
**Solution**: Use FTS vector index `cake.us.vect`. GSI vector is Server 8.0+.

---

## Adding New Samples

1. Follow numbering: `14_cb_new_feature.py`
2. Include a docstring for what the script demonstrates
3. Use `Cluster.connect(...)` (async: `await Cluster.connect` + `await bucket.on_connect()`)
4. Add local vs Capella comments
5. Catch specific exceptions, then `CouchbaseException`
6. Close once; never reuse a closed cluster
7. Add `tests/test_14_cb_new_feature.py` (mock `Cluster.connect`)
8. Update README.md, 00_cb_ops_list.md, and this file
9. Paths: `get_hermes_home()` is not used here — this is a samples repo, not Hermes

---

## Development Workflow

```bash
python3 run_tests.py
python3 01a_cb_set_get.py

# Local connect strings should be couchbase:// not couchbases://
# No NetworkException
```

---

## SDK Version Compatibility

**Current Version**: Couchbase Python SDK **4.6.3** (pin; do not use 4.6.0 — PYCBC-1758 / PYCBC-1753)

**Key features in 4.6.3:**
- Native OpenTelemetry (`get_otel_tracer`) and `LoggingMeter` (`ClusterMetricsOptions`)
- `Cluster.connect()` is the documented connect path; a closed cluster cannot be reused
- Python **3.10–3.14** (3.9 wheels dropped)
- Zone-aware KV reads via `get_any_replica` + `ReadPreference.SELECTED_SERVER_GROUP`
- Vector search pre-filters (4.4+) and GSI/hyperscale vector query (4.5+ / Server 8.0)
- `CASMismatchException` as the documented CAS exception name
- `QueryIndexManagement.create_index()` uses `keys=`, not `fields=`
- Multi-ops cannot mix client durability and server durability in one call
- `ConjunctionQuery` / `DisjunctionQuery` accept a list of queries (4.5+)

**Upgrading SDK:**
```bash
pip install --upgrade 'couchbase==4.6.3'
# Keep requirements.txt pinned
```

---

## Important Notes for AI Agents

1. **Connection config first**: most hangs are `couchbases://localhost` or a down cluster
2. **travel-sample** for 01–13; **cake.us.orders** for vector
3. **Exception names**: `CASMismatchException`, `ServiceUnavailableException` (not `NetworkException`)
4. **Async**: `from acouchbase.cluster import Cluster` then `await Cluster.connect` and `await bucket.on_connect()`
5. **JSON content**: `result.content_as[dict]`
6. **Tests**: `python3 run_tests.py`, not pytest by default
7. **Connect**: `Cluster.connect(connstr, options)` — never `Cluster(connstr, options)` in new code
8. **FTS**: `scope.search` for scoped indexes; SQL++ needs the fully-qualified index name
9. **Replicas**: Collection has no `get_replica_from_preferred_server_group`

---

## Dependencies

**Required:**
- `couchbase==4.6.3`

**Optional:**
- `pandas>=1.5.0` / `openpyxl>=3.0.0` / `tqdm>=4.66.0` — CSV/Excel import
- `opentelemetry-api~=1.22` / `opentelemetry-sdk~=1.22` — tracing (10); or `pip install 'couchbase[otel]==4.6.3'`

---

## Troubleshooting Commands

```bash
curl http://localhost:8091
python3 --version   # 3.10–3.14
pip list | grep couchbase   # 4.6.3

# Buckets UI: http://localhost:8091
python3 01a_cb_set_get.py
python3 run_tests.py
```

---

## Reference Documentation

- [Python SDK 4.6 hello-world](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html)
- [Release notes](https://docs.couchbase.com/python-sdk/current/project-docs/sdk-release-notes.html)
- [Error handling](https://docs.couchbase.com/python-sdk/current/howtos/error-handling.html)
- [Async APIs](https://docs.couchbase.com/python-sdk/current/howtos/concurrent-async-apis.html)
- [Tracing](https://docs.couchbase.com/python-sdk/current/howtos/observability-tracing.html)
- [Vector search](https://docs.couchbase.com/python-sdk/current/howtos/vector-searching-with-sdk.html)
- [Create Search index (REST)](https://docs.couchbase.com/server/current/search/create-search-index-rest-api.html)

---

## Quick Reference: Exception Types

```python
from couchbase.exceptions import (
    CouchbaseException,
    DocumentNotFoundException,
    DocumentExistsException,
    TimeoutException,
    AmbiguousTimeoutException,
    UnAmbiguousTimeoutException,
    AuthenticationException,
    CASMismatchException,
    ParsingFailedException,
    ServiceUnavailableException,
    InternalServerFailureException,
    DocumentUnretrievableException,
    DurabilitySyncWriteAmbiguousException,
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
collection.get_any_replica(key, GetAnyReplicaOptions(read_preference=ReadPreference.SELECTED_SERVER_GROUP))
collection.get_all_replicas(key)
collection.lookup_in(key, [SD.get("path")])
collection.mutate_in(key, [SD.upsert("path", value)])
collection.upsert_multi({key: doc, ...})
collection.binary().increment(key, IncrementOptions(...))
```

### Cluster / Scope Operations
```python
cluster.query(statement, QueryOptions(...))
cluster.bucket(name)
cluster.wait_until_ready(timedelta(seconds=10))
scope.search(index_name, SearchRequest.create(...), SearchOptions(...))  # scoped FTS
cluster.search(...)  # global indexes only
cluster.close()
```

### Async Operations
```python
await Cluster.connect(...)
await cluster.wait_until_ready(...)
await bucket.on_connect()
await collection.get(key)
await collection.upsert(key, doc)
await cluster.close()
```

---

## File Naming Convention

- **##_cb_*.py** / **##a_** / **##b_** — Numbered samples
- **advanced_*.py** — Production patterns
- **test_*.py** — Unit tests
- **fts/** — Search index helper for script 09
- **ai_vector_sample/** — Vector demo
- **demo_data/** — Sample data files

---

## Environment

- **Python**: 3.10–3.14
- **Couchbase SDK**: 4.6.3
- **Couchbase Server**: 7.6.x is enough for KV, SQL++, FTS, FTS vector; **8.0+** for GSI vector
- **Test Framework**: unittest via `run_tests.py`
- **Async**: asyncio + acouchbase
- **Data Processing**: pandas (optional)
- **Tracing**: OpenTelemetry (optional)

---

**Last Updated**: 2026-09-01  
**SDK Version**: 4.6.3  
**Python Version**: 3.10+  
**Maintained By**: Fujio Turner
