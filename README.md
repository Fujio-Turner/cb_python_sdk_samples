# Couchbase Python SDK

A comprehensive collection **production-ready code samples demonstrating the [Couch Python SDK 4..0](https://.couchbase.com/python/current/hello-world/start-using.html). Learn everything from CRUD operations to advanced features transactions, full-text search and async programming.
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

- **Couchbase Server** (local) or **Capella** (cloud) - [Download here](https://www.couchbase.com/downloads)
- **Python 3.8+**
- **Couchbase Python SDK 4.4.0** (auto-installed via requirements.txt)
- **travel-sample bucket** (load via Couchbase Web Console)

## 📚 What's Included

### Core Operations (01-04)
- **01a** - Basic get/upsert operations
- **01b** - Optimistic locking with CAS
- **02** - Upsert and delete
- **03a** - SQL++ (N1QL) queries
- **03b** - SQL++ query profiling
- **04** - Subdocument operations

### Advanced Features (05-11)
- **05** - Exception handling + CSV/Excel import
- **06** - High availability with replica reads
- **07** - Read-your-own-writes consistency
- **08a/08b** - ACID transactions (KV & Query)
- **09** - Full-text search with auto-index creation
- **10** - Debugging, logging, slow ops detection, **orphaned request reporting**, OpenTelemetry
- **11** - **Async operations with class-based design** ⚡

### AI & Vector Search
- **[ai_vector_sample/](ai_vector_sample/)** - **New!** Vector Search Demo (Requires Couchbase Server 8.x+)
  - Create Vector Indexes (Standard & Covering)
  - Perform Similarity Search (KNN)
  - Compare GSI vs FTS Vector Search

### Utilities
- **advanced_prepared_statement_wrapper.py** - Query optimization
- **excel_to_json_to_cb.py** - Bulk data import

## 🎯 Key Features

| Feature | Script | Highlights |
|---------|--------|------------|
| **Async/Concurrent** | 11 | Class-based async client, 20 concurrent ops |
| **Vector Search** | ai_vector_sample/ | Semantic search, RAG, vector embeddings |
| **Transactions** | 08a, 08b | ACID compliance, multi-doc atomicity |
| **Full-Text Search** | 09 | Auto-index creation, wildcards, boolean queries |
| **High Availability** | 06 | Replica reads, retry logic, failover |
| **Debugging** | 10 | Slow ops logging, orphan response reporter, OpenTelemetry tracing |
| **Error Handling** | 05 | All exception types with examples |
| **Performance** | 11, advanced_* | Prepared statements, async, subdocs |

## ⚙️ Configuration

All scripts support **local** and **Capella** (cloud) configurations:

```python
# Local/Self-hosted
ENDPOINT = "localhost"
cluster = Cluster(f'couchbase://{ENDPOINT}', options)

# Capella (cloud)  
ENDPOINT = "cb.xxxxx.cloud.couchbase.com"
options.apply_profile('wan_development')
cluster = Cluster(f'couchbases://{ENDPOINT}', options)  # Note: couchbaseS
```

## 🧪 Testing

```bash
# Run all 123 unit tests
python3 run_tests.py

# Run specific test
python3 -m unittest tests.test_11_cb_async_operations
```

All tests use mocking - **no live Couchbase cluster required**.

## 📖 Documentation

- **[00_cb_ops_list.md](00_cb_ops_list.md)** - Detailed guide for all 12 sample scripts
- **[advanced_multi_docs.md](advanced_multi_docs.md)** - Batch operation patterns
- **[AGENTS.md](AGENTS.md)** - AI agent reference guide

## 🔑 Key Concepts

**Basic**: CRUD, connections, CAS, queries  
**Intermediate**: Subdocuments, exceptions, replicas, consistency  
**Advanced**: Transactions, FTS, async, observability  
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
- [SQL++ Reference](https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/index.html)
- [Couchbase Forums](https://forums.couchbase.com/)
- [SDK API Reference](https://docs.couchbase.com/sdk-api/couchbase-python-client/)

---

**SDK Version**: 4.4.0 | **Python**: 3.8+ | **Author**: Fujio Turner

