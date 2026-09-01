# Unit Tests for Couchbase Python SDK Samples

Mock unit tests for the Couchbase **Python SDK 4.6.3** samples. They do not need a live cluster.

## Overview

The suite covers numbered samples 01–13, the prepared-statement wrapper, Excel import, the vector search script, and a syntax compile-check:

- **Basic Operations**: Set, get, upsert, delete, CAS
- **Query Operations**: SQL++ queries, profiling, prepared statements
- **Transaction Support**: Key-value and SQL++ transactions (tests exist; not all are in the runner)
- **Error Handling**: Exception types including `CASMismatchException` and `QueryErrorContext`
- **Search Operations**: FTS (`scope.search`, `ConjunctionQuery` list form)
- **Vector Search**: `SearchRequest` + `VectorQuery` (execs the sample under mocks)
- **Data Import**: CSV/Excel and `upsert_multi`
- **Debugging**: Native OTel tracer / `ClusterMetricsOptions`
- **Replica Operations**: `get_any_replica`, `get_all_replicas`, zone-aware `ReadPreference`
- **Async**: `await Cluster.connect` + `await bucket.on_connect()`
- **Syntax**: Compile-check every sample `.py`

## Test Files

| Test File | Source File | Description |
|-----------|-------------|-------------|
| `test_01_cb_set_get.py` | `01a_cb_set_get.py` | Basic set/get with timing |
| `test_01a_cb_get_update_w_cas.py` | `01b_cb_get_update_w_cas.py` | CAS updates (`CASMismatchException`) |
| `test_02_cb_upsert_delete.py` | `02_cb_upsert_delete.py` | Upsert and delete |
| `test_03a_cb_query.py` | `03a_cb_query.py` | SQL++ query execution |
| `test_03b_cb_query_profile.py` | `03b_cb_query_profile.py` | Query profiling |
| `test_04_cb_sub_doc_ops.py` | `04_cb_sub_doc_ops.py` | lookup_in / mutate_in |
| `test_05_cb_exception_handling.py` | `05_cb_exception_handling.py` | Exception handling |
| `test_06_cb_get_retry_replica_read.py` | `06_cb_get_retry_replica_read.py` | Retry + replica reads |
| `test_07_cb_query_own_write.py` | `07_cb_query_own_write.py` | Scan consistency (not in `run_tests.py`) |
| `test_08a_cb_transaction_kv.py` | `08a_cb_transaction_kv.py` | KV transactions (not in `run_tests.py`) |
| `test_08b_cb_transaction_query.py` | `08b_cb_transaction_query.py` | Query transactions (not in `run_tests.py`) |
| `test_09_cb_fts_search.py` | `09_cb_fts_search.py` | FTS SQL++ + SDK search |
| `test_10_cb_debug_tracing.py` | `10_cb_debug_tracing.py` | Logging + native OTel |
| `test_11_cb_async_operations.py` | `11_cb_async_operations.py` | Async KV |
| `test_12_cb_async_queries.py` | `12_cb_async_queries.py` | Async SQL++ |
| `test_13_cb_increment.py` | `13_cb_increment.py` | Binary counters (not in `run_tests.py`) |
| `test_advanced_prepared_statement_wrapper.py` | `advanced_prepared_statement_wrapper.py` | Prepared statements |
| `test_excel_to_json_to_cb.py` | `excel_to_json_to_cb.py` | CSV/Excel bulk import |
| `test_ai_vector_search.py` | `ai_vector_sample/04_vector_search_using_python_sdk.py` | Vector search script under mocks |
| `test_sample_syntax.py` | all sample `.py` files | Compile-check so syntax errors fail the suite |

There are **20 test files** and **226** collected cases on disk. The supported runner executes a subset (see below).

## Running the Tests

### Prerequisites

```bash
pip install -r requirements.txt
```

Python **3.10+**. `unittest` / `unittest.mock` are stdlib.

SDK 4.6 samples use `Cluster.connect()` / `await Cluster.connect()`. Tests mock that **classmethod**, not the constructor.

`tests/__init__.py` must exist so the local package is imported instead of a site-packages `tests` module.

### Supported runner

```bash
python3 run_tests.py
```

**Current result: 165 tests, 0 failures.**

That runner always loads:

- `test_01_cb_set_get`
- `test_11_cb_async_operations`
- `test_12_cb_async_queries`
- `test_advanced_prepared_statement_wrapper`

and then the “safe” list (excel, 01b CAS, 02–06, 09, 10, vector, syntax).

**Not in `run_tests.py`:** `test_07_*`, `test_08a_*`, `test_08b_*`, `test_13_*` (MagicMock / `assertRaises` issues). They remain on disk for later.

### Other invocations

```bash
# Single file
python3 -m unittest tests.test_01_cb_set_get -v
python3 -m unittest tests.test_excel_to_json_to_cb -v

# Single method
python3 -m unittest tests.test_01_cb_set_get.TestCbSetGet.test_upsert_document -v

# Full discover (includes 07/08/13; expect failures there)
python3 -m unittest discover -s tests -p "test_*.py" -v
```

## Test Architecture

### Mocking Strategy

Most unit tests mock Couchbase and reimplement sample logic. They do **not** import `01a_cb_set_get.py` (and siblings) because those scripts connect at import time. Treat them as API-shape docs, not as a guarantee that every sample file still runs live.

`tests/test_sample_syntax.py` compile-checks every sample `.py` so a syntax error fails `python3 run_tests.py`. The vector test is the only one that `exec`s a sample file.

1. **Couchbase SDK Mocking**: Operations mocked with `unittest.mock` when the SDK is absent; with the SDK installed, real exception classes are used
2. **Connect shape**: `Cluster.connect` / `await Cluster.connect`, plus `await bucket.on_connect()` on async clients
3. **External Dependencies**: pandas, OpenTelemetry, tqdm mocked as needed
4. **File I/O**: `mock_open`

### Common Test Patterns

1. `setUp()` initializes mocks
2. Time mocked with `@patch('time.time')` where timing is asserted
3. Print mocked to verify output
4. Exception classes must inherit `BaseException` (the runner installs real-shaped stubs if the SDK is missing)

## Test Status

| Check | Status |
|-------|--------|
| Live Couchbase required | No (mocks) |
| External files required | No |
| `python3 run_tests.py` | **165 tests, 0 failures** |
| Python | 3.10–3.14 |
| SDK pin under test | 4.6.3 |
| 07 / 08a / 08b / 13 in runner | No (skipped on purpose) |
| Live cluster E2E | Separate; run the sample scripts against localhost |

Do not quote older “10/15 tests passing” figures — that predates the 4.6 runner.

## Debugging Test Issues

1. **Module Import Errors**: Parent directory on `sys.path`; `tests/__init__.py` present
2. **Mock path**: Match the import used in the test (4.6 is `Cluster.connect`)
3. **Dependencies**: `pip install -r requirements.txt`
4. **Shadowed `tests` package**: Without `tests/__init__.py`, Python can import site-packages `tests` instead of this directory

## Contributing

When adding tests:

1. Name them `test_[source_file_name].py`
2. Mock `Cluster.connect` (not the constructor)
3. Include success and failure paths
4. Do not require a live cluster
5. Add the module to `run_tests.py` only if it stays green under the runner’s mocks
6. Prefer behavior/invariants over snapshots of SDK internals

## Notes

- These tests check code structure, call shapes, and error handling — not a live database
- Integration against Couchbase is done by running the sample scripts
- All external dependencies can be mocked for isolation
