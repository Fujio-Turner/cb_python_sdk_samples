# Couchbase Vector Search Demo

Hands-on vector search with the **Couchbase Python SDK 4.6.3** on bucket **`cake`**, scope **`us`**, collection **`orders`**.

**Server versions:**

| Path | Couchbase Server | What to run |
|------|------------------|-------------|
| **FTS vector index** `cake.us.vect` | **7.6+** | `create_vector_indexes.py` + `04_vector_search_using_python_sdk.py` |
| **GSI `CREATE VECTOR INDEX` / `APPROX_VECTOR_DISTANCE`** | **8.0+** | Same helper tries GSI and skips it on 7.6 |

On Server **7.6.5**, GSI `CREATE VECTOR INDEX` is a syntax error (`VECTOR` is reserved) and `APPROX_VECTOR_DISTANCE` is not a function. Use the Search (FTS) vector index.

---

## What This Demo Does

Four country documents (Belgium, France, Germany, United States) each have a pre-computed **128-dimension** embedding. Belgium’s vector is the query. Search ranks nearest neighbors — the same idea as recommendations, semantic search, and RAG.

| Index Type | Description | Server |
|------------|-------------|--------|
| **FTS Vector Index** | Search index with KNN (`knn` + optional text pre-filter) | 7.6+ |
| **GSI Hyperscale Vector Index** | SQL++ `APPROX_VECTOR_DISTANCE` (optional covering `INCLUDE`) | 8.0+ |

---

## What You'll Learn

1. Load **4 country docs** with 128-dim embeddings into `cake.us.orders`
2. Create the **FTS vector index** `vect` (and try GSI on 8.0)
3. Run **SDK** `VectorQuery` / `SearchRequest` + **pre-filter**
4. Run **SQL++ `SEARCH(..., knn)`** against `cake.us.vect` (7.6)
5. On 8.0, compare with **GSI** `APPROX_VECTOR_DISTANCE`

---

## Overview

| Index Type | Best For | ANN Pruning | Score Filter | Covering Support |
|------------|----------|-------------|--------------|------------------|
| **GSI Vector Index** (w/ Covering) | Pure vector top-k | Yes (`ORDER BY … LIMIT`) | Post-filter (after top-k) | **Yes** (`INCLUDE` fields) |
| **FTS Vector Index** | Hybrid text + vector, high recall | Yes (`knn`) | Pushed-down (`filter.min` / prefilter) | Partial (via `fields` param) |

---

## File Layout

```
ai_vector_sample/
├── README.md
├── 00_query_vector_input.json           ← 128-dim Belgium query vector
├── 01_vector_docs.json                  ← 4 country docs to import
├── 02_vector_search_index.json          ← FTS vector index definition (128-dim cosine)
├── 03_vector_search_query_curl.txt      ← FTS REST knn example (port 8094)
├── 04_vector_search_using_python_sdk.py ← SDK 4.6 FTS + SQL++ SEARCH knn (+ GSI try)
└── create_vector_indexes.py             ← PUT cake.us.vect; try GSI VECTOR INDEX
```

---

## Prerequisites

- Python **3.10+**, `couchbase==4.6.3`
- Couchbase with **Search Service** (port **8094**)
- Bucket **`cake`**, scope **`us`**, collection **`orders`**
- Default sample credentials: `Administrator` / `password` on `localhost`

---

## Step 1 – Import Sample Documents

Import **`01_vector_docs.json`** into **Buckets → `cake` → `us.orders`**.

Four keys: `country::belgium`, `country::france`, `country::germany`, `country::united-states`. Each has `name`, `capital`, `region`, and `embedding` (128 floats).

---

## Step 2 – Create Indexes

```bash
python3 ai_vector_sample/create_vector_indexes.py
```

That script:

1. PUTs `02_vector_search_index.json` to
   `http://localhost:8094/api/bucket/cake/scope/us/index/vect`
   (128-dim **cosine** on `embedding`; stored `name` / `capital`; type mapping `us.orders`).
   Strips `uuid` / `sourceUUID`. Uses an explicit `Authorization: Basic ...` header.
2. Waits until the index reports documents (`/count` can 500 `no planPIndexes` for a couple of seconds while the plan builds).
3. Attempts GSI:

   ```sql
   CREATE VECTOR INDEX idx_country_embedding_v1
   ON `cake`.`us`.`orders`(embedding VECTOR)
   WITH { "dimension": 128, "similarity": "COSINE" };
   ```

   On 7.6 this is skipped. On 8.0+ you can also add a covering index:

   ```sql
   CREATE VECTOR INDEX idx_country_embedding_cover_v1
   ON `cake`.`us`.`orders`(embedding VECTOR)
     INCLUDE (`name`, `capital`)
   WITH { "dimension": 128, "similarity": "COSINE" };
   ```

### Non-covering vs covering (GSI, Server 8.0+)

| Aspect | Non-covering | Covering (`INCLUDE`) |
|--------|----------------|----------------------|
| **Index size** | Smaller | Larger |
| **Projected fields** | Fetch from KV | Return from index |
| **Best for** | IDs + scores | Returning `name`, `capital`, … |

`dimension` must match the embedding model (this demo is **128**). `similarity`: **COSINE** for text semantics; L2 / DOT for other cases.

---

## Step 3 – Query Vector

`00_query_vector_input.json` is Belgium’s 128-dim embedding (same array inlined in `04_vector_search_using_python_sdk.py`).

In the Query Workbench, bind it as named parameter `$query_vector` / `$vec` if you run SQL++ by hand.

---

## Step 4 – FTS vector search (Server 7.6+) — Python SDK 4.6

```bash
python3 ai_vector_sample/04_vector_search_using_python_sdk.py
```

Connect is `Cluster.connect(...)`. Search uses the scoped index name `vect`:

```python
from couchbase.search import SearchRequest, MatchQuery
from couchbase.vector_search import VectorQuery, VectorSearch

vector_search = VectorSearch.from_vector_query(
    VectorQuery("embedding", query_vector, num_candidates=5)
)
request = SearchRequest.create(vector_search)
result = scope.search(
    "vect",
    request,
    SearchOptions(limit=5, fields=["name", "capital"]),
)
```

Pre-filter (SDK 4.4+ / Server 7.6.4+): run a non-vector query first, then KNN:

```python
prefilter = MatchQuery("Belgium", field="name")
vector_query = VectorQuery.create(
    "embedding", query_vector, num_candidates=5, prefilter=prefilter
)
request = SearchRequest.create(VectorSearch.from_vector_query(vector_query))
result = scope.search("vect", request, SearchOptions(limit=5, fields=["name", "capital"]))
```

**Live ranking** (Belgium query, FTS cosine, 7.6.5): Belgium **1.0**, Germany **~0.941**, United States **~0.907**, France **~0.906**. Pre-filter on `name=Belgium` returns only Belgium.

SQL++ on 7.6 uses `SEARCH(..., knn)` against the **fully-qualified** index `cake.us.vect`:

```sql
SELECT META(t).id AS id, t.name, t.capital
FROM `cake`.`us`.`orders` AS t
WHERE SEARCH(t, $search, {"index": "cake.us.vect"})
```

`$search` is a JSON object with `knn: [{ "k": 4, "field": "embedding", "vector": [...] }]` and `fields`. On 7.6, `SEARCH_SCORE()` / `SEARCH_META()` may be null for knn; rank comes from the SDK FTS API.

`scope.search()` may emit a CouchbaseDeprecationWarning that option `scope_name` is deprecated (SDK internals). The call still succeeds.

---

## Step 5 – FTS Vector Search (cURL)

See `03_vector_search_query_curl.txt`. Endpoint:

`http://localhost:8094/api/bucket/cake/scope/us/index/vect/query`

Use real Basic auth (the sample header in that file is a placeholder). Body: `knn` on `embedding` plus `fields` for projection.

---

## Step 6 – GSI top-k (Server 8.0+ only)

```sql
SELECT
    META().id AS doc_id,
    `name`, `capital`,
    APPROX_VECTOR_DISTANCE(embedding, $query_vector, "COSINE") AS similarity
FROM `cake`.`us`.`orders`
ORDER BY similarity
LIMIT 2;
```

Covering index (`INCLUDE`) avoids KV fetches for `name` / `capital`. Threshold + top-k:

```sql
SELECT META().id AS doc_id, `name`, `capital`,
       APPROX_VECTOR_DISTANCE(embedding, $query_vector, "COSINE") AS similarity
FROM `cake`.`us`.`orders`
WHERE APPROX_VECTOR_DISTANCE(embedding, $query_vector, "COSINE") <= 0.08
ORDER BY similarity
LIMIT 2;
```

The Python script calls this via `cluster.query(...)` and prints `GSI vector query skipped (needs Server 8.0+ ...)` on 7.6.

---

## Which Index Should You Choose?

| Criteria | **GSI Vector (8.0, covering)** | **FTS Vector (7.6+)** |
|----------|--------------------------------|------------------------|
| **Pure top-k** | Best (`ORDER BY … LIMIT`) | Good (`k` / `num_candidates`) |
| **Score threshold** | Post-filter | Pushed down / prefilter |
| **Hybrid text + vector** | Not supported | Supported |
| **This repo on 7.6.x** | Not available | **Use this** |
| **Use-case** | High-QPS ANN, recommendations | Search + similarity, RAG, hybrid |

**Rule of thumb:** On 7.6, start with FTS `cake.us.vect`. On 8.0, add covering GSI for pure vector top-k.

---

## References

- [Python SDK vector search](https://docs.couchbase.com/python-sdk/current/howtos/vector-searching-with-sdk.html)
- [CREATE VECTOR INDEX](https://docs.couchbase.com/cloud/n1ql/n1ql-language-reference/createvectorindex.html) (Server 8.0+)
- [APPROX_VECTOR_DISTANCE](https://docs.couchbase.com/cloud/n1ql/n1ql-language-reference/vectorfun.html)
- [FTS knn](https://docs.couchbase.com/cloud/search/search-request-params.html#knn)
- [Create a Search index (REST)](https://docs.couchbase.com/server/current/search/create-search-index-rest-api.html)
- [Search index params](https://docs.couchbase.com/server/current/search/search-index-params.html)

---

## You’re Done!

- **4 docs** in `cake.us.orders`
- **FTS index** `cake.us.vect` (7.6+)
- **SDK** `VectorQuery` + pre-filter + SQL++ `SEARCH` knn
- **GSI vector** only if the cluster is 8.0+

**Next steps:** hybrid FTS (text + knn), more INCLUDE fields on 8.0, or a real embedding model in place of the canned 128-dim vectors.
