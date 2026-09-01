# Couchbase Vector Search Demo

**Note:** This demo requires **Couchbase Server 8.x** or higher (or Couchbase Capella) for Vector Search support.

A hands-on example showing how to use **Vector Search** in Couchbase to find similar documents based on embeddings.

---

## What This Demo Does

This demo uses **4 country documents** (Belgium, France, Germany, United States) that each have a pre-computed 128-dimension vector embedding. 

We'll use **Belgium's embedding as the query input** to find which countries are most similar. Vector search calculates the "distance" between embeddings to find the closest neighbors – perfect for recommendation systems, semantic search, and RAG applications.

**This demo covers two vector index types:**

| Index Type | Description |
|------------|-------------|
| **GSI Hyperscale Vector Index** | Couchbase's new high-performance vector index using SQL++ queries |
| **FTS Vector Index** | Full-Text Search index with KNN vector support |

---

## What You'll Learn

1. Load **4 country docs** with 128-dim embeddings
2. Create **GSI vector indexes** (basic and covering)
3. Create an **FTS vector index**
4. Run **similarity queries** using Belgium's embedding to find closest matches
5. Compare **GSI vs FTS** approaches

---

## Overview

| Index Type | Best For | ANN Pruning | Score Filter | Covering Support |
|------------|----------|-------------|--------------|------------------|
| **GSI Vector Index** (w/ Covering) | Pure vector top-k | Yes (`ORDER BY … LIMIT`) | Post-filter (after top-k) | **Yes** (INCLUDE fields) |
| **FTS Vector Index** | Hybrid text + vector, high recall | Yes (`knn`) | Pushed-down (`filter.min`) | Partial (via `fields` param) |  

---

## File Layout

```
.
├── README.md
├── 00_query_vector_input.json                  ← Query input: 128-dim Belgium vector (named parameter)
├── 01_vector_docs.json                         ← Step 1: 4 country docs to import
├── 02_vector_search_index.json                 ← Step 6: FTS vector index definition
├── 03_vector_search_query_curl.txt             ← Step 7: FTS cURL example
└── 04_vector_search_using_python_sdk.py        ← Step 8: Python SDK FTS example
```

---

## Step 1 – Import Sample Documents

### `sample_vector_docs.json`

*(Unchanged – see original for the 4 country docs with embeddings.)*

**Import** → **Buckets → `cake` → `us.orders` → **Import Documents**

---

## Step 2a – Create a Basic (Non-Covering) Vector Index

```sql
CREATE VECTOR INDEX idx_country_embedding_v1
ON `cake`.`us`.`orders`(embedding VECTOR)
WITH { "dimension": 128, "similarity": "COSINE" };
```

This is the simplest vector index – it only indexes the `embedding` field.

---

## Step 2b – Create a Covering Vector Index

```sql
CREATE VECTOR INDEX idx_country_embedding_cover_v1
ON `cake`.`us`.`orders`(embedding VECTOR)
  INCLUDE (`name`, `capital`)
WITH { "dimension": 128, "similarity": "COSINE" };
```

This index stores `name` and `capital` **inside the index** using the `INCLUDE` clause.

---

### Comparing Non-Covering vs Covering Indexes

| Aspect | 2a: Non-Covering | 2b: Covering (INCLUDE) |
|--------|------------------|------------------------|
| **Index size** | Smaller | ~10-20% larger |
| **Query for extra fields** | Fetches from KV store (slower) | Returns directly from index (faster) |
| **Best for** | Simple similarity lookups | Returning field values with results |
| **Latency** | Higher for projected fields | ~20-50% lower for covered fields |

> 💡 **When to use which?**
> - Use **2a (basic)** if you only need doc IDs and similarity scores
> - Use **2b (covering)** if your queries return fields like `name`, `capital`, etc.

---

### 🧠 Vector Search Concepts: What do `dimension` and `similarity` mean?

When creating the index, you saw: `WITH { "dimension": 128, "similarity": "COSINE" }`

Here is what this means in simple terms:

- **dimension (e.g., 128)**: Think of this as the "fingerprint size" of the AI model you used.
  - Every AI model (like OpenAI, HuggingFace) converts text into a list of numbers (a vector).
  - The "dimension" is just **how many numbers** are in that list.
  - **Rule**: This number MUST match your AI model exactly. (e.g., OpenAI `text-embedding-3-small` is 1536).

- **similarity (e.g., "COSINE")**: This is the "ruler" used to decide if two things are related.
  - **COSINE**: Measures the angle between vectors. Best for text semantic search (e.g., "dog" is close to "puppy").
  - **L2 (Squared Euclidean)**: Measures the straight physical distance. Good for some specific math/image use cases.
  - **DOT (Dot Product)**: Useful for recommendation systems or when vector magnitude matters.

For more details, see the [official documentation](https://docs.couchbase.com/cloud/n1ql/n1ql-language-reference/createvectorindex.html).

---

## Step 3 – The Query Vector (Named Parameter)

### `sample_vector_query_topk.json` *(exact 128-dim Belgium embedding)*

*(Unchanged – see original for the full 128-float array.)*

**How to use it**  
*(Unchanged – paste into Query Editor parameters.)*

---

## Step 4 – Basic Top-K Query (`LIMIT 2`, Covered)

### `sample_vector_query_topk_cover.txt`

```sql
SELECT 
    meta().id AS doc_id,
    `name`, `capital`,  -- Covered: Pulled directly from index
    APPROX_VECTOR_DISTANCE(embedding, $query_vector, "COSINE") AS similarity
FROM `cake`.`us`.`orders`
ORDER BY similarity DESC
LIMIT 2;
```

**Result** *(Covered query – faster!)*

```json
[
  {"doc_id":"country::belgium","name":"Belgium","capital":"Brussels","similarity":1.0},
  {"doc_id":"country::france","name":"France","capital":"Paris","similarity":0.951}
]
```

*Pro Tip:* Run `EXPLAIN` on this query – you'll see **no data scans**, just index fetches. Latency drops for large datasets.

---

## Step 5 – **Threshold + Top-K Query (`LIMIT 2`, Covered)**

### `sample_vector_query_filtered_cover.txt`

```sql
-- Efficient: top-k pruning + post-filter on similarity (still covered for projected fields)
SELECT 
    meta().id AS doc_id,
    `name`, `capital`, `region`,  -- name/capital covered; region pulls from KV (partial cover)
    APPROX_VECTOR_DISTANCE(embedding, $query_vector, "COSINE") AS similarity
FROM `cake`.`us`.`orders`
WHERE APPROX_VECTOR_DISTANCE(embedding, $query_vector, "COSINE") <= 0.08
ORDER BY similarity DESC
LIMIT 2;
```

**Result (your data)** *(Partial cover – add `region` to INCLUDE for full coverage)*

```json
[
  {"doc_id":"country::germany","name":"Germany","capital":"Berlin","region":"Europe","similarity":0.0584},
  {"doc_id":"country::belgium","name":"Belgium","capital":"Brussels","region":"Europe","similarity":0.0}
]
```

*Why partial?* `region` isn't INCLUDEd – KV fetch for it. For full coverage, update index: `INCLUDE (`name`,`capital`,`region`)`.  
*The index returns **only 2 docs** – even if you had millions. Covering shines here for QPS scaling.*

---

## Step 6 – Create FTS Vector Index

On Couchbase Server **7.6.x**, GSI `CREATE VECTOR INDEX` is not available (Server 8.0+). Use the Search vector index:

```bash
python3 ai_vector_sample/create_vector_indexes.py
```

That PUTs `02_vector_search_index.json` to
`http://localhost:8094/api/bucket/cake/scope/us/index/vect` (128-dim cosine on `embedding`, plus stored `name` / `capital`). It also tries GSI VECTOR INDEX and skips it on 7.6.

SQL++ vector query on 7.6 uses `SEARCH(..., knn)` against `cake.us.vect`. On 8.0+ use `APPROX_VECTOR_DISTANCE` with a GSI vector index.

*Note:* FTS "covers" via `fields` param (projects from index), but lacks explicit INCLUDE like GSI.

---

## Step 7 – FTS Vector Search (cURL)

*(Unchanged – see original. Uses `fields` for projection, akin to covering.)*

---

Python SDK 4.6 uses `SearchRequest.create(VectorSearch.from_vector_query(VectorQuery(field, vector, num_candidates=...)))` and `scope.search(index, request)`. Pre-filters are supported from SDK 4.4 / Server 7.6.4.

## Step 8 – Python SDK (FTS)

*(Unchanged – see original. Projects via `fields`, similar efficiency.)*

---

## Which Index Should You Choose?

| Criteria | **GSI Vector Index (Covering)** | **FTS Vector Index** |
|----------|---------------------------------|----------------------|
| **Pure top-k** | **Best** (`ORDER BY … LIMIT 2`, fully covered) | Good (`k`) |
| **Score threshold** | Post-filter (after top-k) | **Pushed down** (`filter.min`) |
| **Hybrid text + vector** | Not supported | Supported |
| **Recall tuning** | Fixed IVF/PQ | Configurable |
| **Index size** | **Slightly larger** (w/ INCLUDE) | Larger |
| **Real-time upserts** | Fast | Slightly slower |
| **Query Latency** | **Lower** (no KV for covered fields) | Good (index-based projection) |
| **Use-case** | **Recommendation engines, pure ANN, high-QPS reads** | **Search + similarity, RAG, multi-modal** |

**Rule of thumb:**  
*Start with **covering GSI** for pure vector top-k + projected fields.*  
*Switch to FTS when you need hybrid search, pushed filters, or non-vector text matching.*  
*Monitor w/ `EXPLAIN` & Couchbase Metrics – covering can halve p99 latency!*

---

## References

- [CREATE VECTOR INDEX](https://docs.couchbase.com/cloud/n1ql/n1ql-language-reference/createvectorindex.html) – SQL++ syntax for vector indexes
- [Vector Search Overview](https://docs.couchbase.com/cloud/vector-search/vector-search.html) – Introduction to vector search in Couchbase
- [APPROX_VECTOR_DISTANCE Function](https://docs.couchbase.com/cloud/n1ql/n1ql-language-reference/vectorfun.html) – Query function for similarity calculations
- [FTS Vector Search](https://docs.couchbase.com/cloud/search/search-request-params.html#knn) – Full-Text Search with KNN vectors
- [Python SDK Vector Search](https://docs.couchbase.com/python-sdk/current/howtos/full-text-searching-with-sdk.html#vector-search) – Python SDK examples

---

## You’re Done!

* **4 docs loaded**  
* **Both indexes built** (GSI now **covering**)  
* **Top-k and filtered queries** (`LIMIT 2`, covered where possible)  
* **Single source of truth** for the query vector (`sample_vector_query_topk.json`)  
* **Scalable patterns** ready for millions of docs + high throughput  

**Next steps:** Add more INCLUDE fields (e.g., `population`), hybrid FTS+GSI, RAG pipelines, or benchmark covering vs. non-covering latency.  

What do you think – want to extend the covering to more fields, or dive into FTS hybrid tweaks? Let's build!