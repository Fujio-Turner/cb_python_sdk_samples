# Couchbase Python SDK

A comprehensive collection of production-ready code samples demonstrating the [Couchbase Python SDK 4.6](https://docs.couchbase.com/python-sdk/current/hello-world/overview.html). Learn everything from CRUD operations to transactions, full-text search, native OpenTelemetry, vector search, and async programming.

Connect with `Cluster.connect()` (async: `await Cluster.connect()` then `await bucket.on_connect()`). A closed cluster cannot be reused.

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/Fujio-Turner/cb_python_sdk_samples.git
cd cb_python_sdk_samples

# Set up virtual environment
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run your first sample
python3 01a_cb_set_get.py
```

## 📋 Prerequisites

- **Couchbase Server** (local) or **Capella** (cloud) — [Download here](https://www.couchbase.com/downloads)
- **Python 3.10+** (3.10–3.14; SDK 4.6 dropped 3.9 wheels)
- **Couchbase Python SDK 4.6.3** (pinned in `requirements.txt`)
- **travel-sample** bucket (load via Couchbase Web Console) for scripts 01–13
- **Search Service** (port 8094) for `09_cb_fts_search.py` and the vector sample
- **`cake.us.orders`** collection for [ai_vector_sample/](ai_vector_sample/) (4 country docs with 128-dim embeddings)

## 📚 What's Included

### Core Operations (01-04)
- **01a** - Basic get/upsert operations
- **01b** - Optimistic locking with CAS
- **02** - Upsert and delete
- **03a** - SQL++ (N1QL) queries
- **03b** - SQL++ query profiling
- **04** - Subdocument operations

### Advanced Features (05–13)
- **05** - Exception handling + CSV/Excel import (`upsert_multi`; durability examples need replicas)
- **06** - High availability with replica reads (`get_any_replica` + zone-aware `ReadPreference.SELECTED_SERVER_GROUP`)
- **07** - Read-your-own-writes consistency
- **08a/08b** - ACID transactions (KV & Query)
- **09** - Full-text search (SQL++ `SEARCH()` + SDK `scope.search()`)
- **10** - Debugging, logging, slow ops, orphaned request reporting, **native OpenTelemetry** (`get_otel_tracer`)
- **11** - Async KV operations with class-based design
- **12** - Async SQL++ queries (profiling, prepared statements, `use_replica`)
- **13** - Binary increment / decrement counters

### AI & Vector Search
- **[ai_vector_sample/](ai_vector_sample/)** — vector search on `cake.us.orders`
  - **FTS vector index** `cake.us.vect` — Couchbase Server **7.6+** (this is the path that runs on 7.6.x)
  - **GSI `CREATE VECTOR INDEX` / `APPROX_VECTOR_DISTANCE`** — Server **8.0+** (skipped on 7.6)
  - SDK: `SearchRequest.create` + `VectorQuery` / pre-filter; SQL++ `SEARCH(..., knn)`

### Utilities
- **fts/create_hotels_index.py** — PUT scoped `hotels-index` for script 09
- **ai_vector_sample/create_vector_indexes.py** — PUT `cake.us.vect` (and try GSI on 8.0)
- **advanced_prepared_statement_wrapper.py** — Query optimization
- **excel_to_json_to_cb.py** — Bulk data import (`upsert_multi`)

## 🎯 Key Features

| Feature | Script | Highlights |
|---------|--------|------------|
| **Async/Concurrent** | 11, 12 | Class-based async client; `await Cluster.connect` + `await bucket.on_connect()` |
| **Vector Search** | ai_vector_sample/ | FTS KNN on 7.6; GSI vector on 8.0; SDK `VectorQuery` + pre-filter |
| **Transactions** | 08a, 08b | ACID compliance, multi-doc atomicity |
| **Full-Text Search** | 09 | Scoped `hotels-index`, SQL++ `SEARCH()`, `scope.search()`, `ConjunctionQuery([q1, q2])` |
| **High Availability** | 06 | Replica reads, retry logic, zone-aware `SELECTED_SERVER_GROUP` |
| **Debugging** | 10 | Slow ops, orphan reporter, native OTel tracer + `LoggingMeter` |
| **Error Handling** | 05 | `CASMismatchException`, `QueryErrorContext`, durability notes |
| **Performance** | 11, 12, advanced_* | Prepared statements, async, subdocs, multi-ops |

## ⚙️ Configuration

All scripts support **local** and **Capella** (cloud) configurations:

```python
# Local/Self-hosted
ENDPOINT = "localhost"
cluster = Cluster.connect(f'couchbase://{ENDPOINT}', options)

# Capella (cloud)  
ENDPOINT = "cb.xxxxx.cloud.couchbase.com"
options.apply_profile('wan_development')
cluster = Cluster.connect(f'couchbases://{ENDPOINT}', options)  # Note: couchbaseS
```

## 🧪 Testing

```bash
# Mock unit suite (no live cluster required)
python3 run_tests.py
# Expected: 165 tests, 0 failures

# Run a specific test file
python3 -m unittest tests.test_11_cb_async_operations
```

`run_tests.py` mocks Couchbase. Samples that connect at import time are compile-checked by `tests/test_sample_syntax.py`, not executed against a cluster.

**Live cluster** (optional): Couchbase at `http://localhost:8091` (`Administrator` / `password`).

| Sample | Extra setup |
|--------|-------------|
| 01–08, 10–13 | `travel-sample` loaded |
| 05 durability / 06 replicas | replica count ≥ 1 (a 1-node / 0-replica cluster raises `DocumentUnretrievableException` on replica reads) |
| 09 | Search Service + `python3 fts/create_hotels_index.py` |
| vector | Import `ai_vector_sample/01_vector_docs.json` into `cake.us.orders`, then `python3 ai_vector_sample/create_vector_indexes.py` |

## 📖 Documentation

- **[00_cb_ops_list.md](00_cb_ops_list.md)** — Guide for numbered samples 01–13 plus KV reference
- **[advanced_multi_docs.md](advanced_multi_docs.md)** — Batch / multi-op patterns (SDK 4.6 durability mixing rule)
- **[ai_vector_sample/README.md](ai_vector_sample/README.md)** — FTS vector (7.6) vs GSI vector (8.0)
- **[AGENTS.md](AGENTS.md)** — AI agent reference
- **[tests/README.md](tests/README.md)** — Mock test runner

## 🔑 Key Concepts

**Basic**: CRUD, connections, CAS, queries  
**Intermediate**: Subdocuments, exceptions, replicas, consistency  
**Advanced**: Transactions, FTS, vector search, async, observability  
**Production**: Error handling, retry logic, prepared statements, monitoring

## 📊 Important Limits

- Max document size: **20 MB**
- Max key length: **250 bytes**
- Subdocument ops per request: **16**
- Concurrent connections per node: **60,000**

## 🛠️ Common Issues

| Issue | Solution |
|-------|----------|
| Script hangs on connect | Use `couchbase://` (not `couchbases://`) for local |
| Pandas slow import | Normal on M1/M2 Macs (~15s wait) |
| NetworkException error | Use `ServiceUnavailableException` instead |
| BucketNotFoundException | Load travel-sample bucket in Couchbase UI |
| 09 FTS “index mapping not found” | Create `hotels-index` (`python3 fts/create_hotels_index.py`); SQL++ `SEARCH()` must use index `travel-sample.inventory.hotels-index` |
| 06 `DocumentUnretrievableException` | No replica answered — typical on 1-node / 0-replica clusters |
| GSI `CREATE VECTOR INDEX` syntax error | Server 7.6 — use the FTS vector index instead (Server 8.0+ for GSI) |

## 🤝 Contributing

Contributions welcome! See contribution guidelines in the full documentation.

1. Fork the repository
2. Create feature branch
3. Follow existing code patterns
4. Add tests for new features
5. Submit pull request

## 📄 License

MIT License - see [LICENSE](LICENSE)

## 🔗 Resources

- [Couchbase Python SDK Docs](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html)
- [Release notes (4.6.x)](https://docs.couchbase.com/python-sdk/current/project-docs/sdk-release-notes.html)
- [Vector search with the SDK](https://docs.couchbase.com/python-sdk/current/howtos/vector-searching-with-sdk.html)
- [SQL++ Reference](https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/index.html)
- [Create a Search index (REST)](https://docs.couchbase.com/server/current/search/create-search-index-rest-api.html)
- [Couchbase Forums](https://forums.couchbase.com/)
- [SDK API Reference](https://docs.couchbase.com/sdk-api/couchbase-python-client/)

---

**SDK Version**: 4.6.3 | **Python**: 3.10+ | **Author**: Fujio Turner

