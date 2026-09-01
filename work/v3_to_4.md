# Migration plan: Couchbase Python SDK 4.4.0 → 4.6.3

This file is a **branch working log** for `v4.6`, not end-user documentation. Drop it from `main` after merge, or move it under contributor notes.

**Status (2026-09-01):** Implementation is done. Samples pin `couchbase==4.6.3`, use `Cluster.connect()`, and the user-facing markdown (README, AGENTS, 00_cb_ops_list, tests/README, ai_vector_sample/README) matches that. §3 rows below are the original plan, not a remaining todo list — see **§9**.

**Branch:** `v4.6` (from `main` @ `220204f`)  
**Plan file:** `work/v3_to_4.md`  
**Target package:** `couchbase==4.6.3` (latest 4.6 patch as of 2026-08-25)  
**SDK API spec:** 3.7 (Python 4.4) → **3.9** (Python 4.6)

This repo is already on the **4.x client**, not 3.x. The filename is historical (`v3.8` snapshot branch vs this `v4.6` work). The job is a **4.4.0 → 4.6.3** upgrade plus aligning samples with current docs, not a 3.x rewrite.

Official sources used for this plan:

- [Python SDK 4.6 overview](https://docs.couchbase.com/python-sdk/current/hello-world/overview.html)
- [Hello World / `Cluster.connect`](https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html)
- [Release notes (4.6.0–4.6.3)](https://docs.couchbase.com/python-sdk/current/project-docs/sdk-release-notes.html)
- [Compatibility (Python 3.10–3.14)](https://docs.couchbase.com/python-sdk/current/project-docs/compatibility.html)
- [Async APIs (`acouchbase`)](https://docs.couchbase.com/python-sdk/current/howtos/concurrent-async-apis.html)
- [Error handling](https://docs.couchbase.com/python-sdk/current/howtos/error-handling.html)
- [Request tracing + native OTel](https://docs.couchbase.com/python-sdk/current/howtos/observability-tracing.html)
- [Metrics / `LoggingMeter`](https://docs.couchbase.com/python-sdk/current/howtos/observability-metrics.html)
- [Vector search](https://docs.couchbase.com/python-sdk/current/howtos/vector-searching-with-sdk.html)

---

## 1. What actually changed between 4.4.0 and 4.6.3

Most KV / query / transaction samples already use the 4.x collections API (`Cluster` → `bucket` → `scope` → `collection`, `content_as[dict]`, `wait_until_ready`). Those keep working. The upgrade is **pin + connect style + 4.6 observability + a few real API bugs**.

### Behavioral / API (must handle)

| Change | First seen | Impact on this repo |
|---|---|---|
| Preferred connect is `Cluster.connect(connstr, options)`, not `Cluster(connstr, options)` | docs (4.x hello-world) | Almost every sample still uses the constructor |
| Closed cluster **cannot be reconnected**; create a new instance | 4.6.0 | Samples that `close()` then keep using the same object; docs/comments |
| Python **3.9 wheels dropped**; supported: **3.10–3.14** | 4.6.0 | README / AGENTS still say 3.8+ |
| Native tracing + OTel: pass `tracer=get_otel_tracer(provider)` on `ClusterOptions` | 4.6.0 | `10_cb_debug_tracing.py` only traces *app* spans; SDK ops are not exported |
| Native `LoggingMeter` via `ClusterMetricsOptions` | 4.6.0 | Not demonstrated |
| `QueryIndexManagement.create_index()` uses `keys=`, not `fields=` | 4.6.0 | No current sample, but document so we do not add the old API |
| Multi-ops cannot mix client + server durability in one call | 4.6.0 | `advanced_multi_docs.md` if durability is shown |
| Scoped search must pass `scope_name` / `bucket_name` | 4.6.1 | `09_cb_fts_search.py`, vector sample |
| `MutateInResult.content_as()` TypeError fixed | 4.6.3 | `04_cb_sub_doc_ops.py` |
| `ConjunctionQuery` / `DisjunctionQuery` accept a list of queries | 4.5.0 | `09_cb_fts_search.py` (already list-like; confirm) |
| Zone-aware replica reads (`get_replica_from_preferred_server_group`) | 4.4.0 (underused) | `06_cb_get_retry_replica_read.py` |
| Vector pre-filters | 4.4.0 | Vector sample uses a non-canonical `VectorQuery(field=, k=)` constructor |
| Vector **GSI / hyperscale** queries via SQL++ | 4.5.0 | Vector README already describes GSI; Python file only does FTS |
| Async: `await Cluster.connect(...)` then `await bucket.on_connect()` | docs | `11` / `12` connect but never `on_connect()` |
| Official exception name is `CASMismatchException` | error-handling howto | Mix of `CasMismatchException` and `CASMismatchException` |

### Pin policy

Pin **`couchbase==4.6.3`**, not `4.6.0`.

- 4.6.0 known issues: ClusterOptions not fully propagated (PYCBC-1758), scoped search names dropped (PYCBC-1753) — both fixed in 4.6.1.
- 4.6.3 is the current patch (C-extension hardening, `MutateInResult.content_as()` fix).

OTel extra (docs): `pip install couchbase[otel]` which pulls `opentelemetry-api~=1.22` and `opentelemetry-sdk~=1.22`. Replace the repo’s loose `>=1.15.0` pins.

---

## 2. Cross-cutting code pattern (apply to every sample)

### 2.1 Connect (sync)

**Before (4.4 samples):**

```python
cluster = Cluster(f'couchbase://{ENDPOINT}', options)
cluster.wait_until_ready(timedelta(seconds=10))
```

**After (4.6 docs):**

```python
cluster = Cluster.connect(f'couchbase://{ENDPOINT}', options)
cluster.wait_until_ready(timedelta(seconds=10))
# Capella:
# options.apply_profile('wan_development')
# cluster = Cluster.connect(f'couchbases://{ENDPOINT}', options)
```

Keep local vs Capella comments. Do **not** invent a shared helper module — these samples are meant to be copy-pasteable.

### 2.2 Connect (async)

**After:**

```python
from acouchbase.cluster import Cluster

cluster = await Cluster.connect(connection_string, options)
await cluster.wait_until_ready(timedelta(seconds=10))
bucket = cluster.bucket(bucket_name)
await bucket.on_connect()
```

Docs import `Cluster` from `acouchbase.cluster` and call `Cluster.connect` (the page also writes `AsyncCluster.connect` once — that is a docs typo; the import name is `Cluster`).

### 2.3 Close

After `cluster.close()`, never call APIs on that instance. `13_cb_increment.py` already uses try/finally; others should close once at the end and not reconnect.

### 2.4 Exceptions

Standardize on the names in the [error handling](https://docs.couchbase.com/python-sdk/current/howtos/error-handling.html) page:

| Use | Do not use as primary |
|---|---|
| `CASMismatchException` | `CasMismatchException` (keep as alias only if the SDK still exports it) |
| `DocumentNotFoundException` | — |
| `DocumentExistsException` | — |
| `TimeoutException` / `AmbiguousTimeoutException` / `UnAmbiguousTimeoutException` | — |
| `ServiceUnavailableException` | `NetworkException` (does not exist) |
| `DurabilitySyncWriteAmbiguousException` | — |
| `QueryErrorContext` via `ex.context` | string-matching error text |

Catch **specific** exceptions first, then `CouchbaseException`. For query errors, inspect `isinstance(ex.context, QueryErrorContext)`.

### 2.5 Python version

Document **Python 3.10+** (3.10–3.14). Remove “3.8+” / “3.6+” claims.

---

## 3. Per-file plan

Status key: **Must change** / **Light touch** / **No SDK change**.

### 3.1 Root config and docs

#### `requirements.txt` — Must change

- `couchbase==4.4.0` → `couchbase==4.6.3`
- Add comment that `couchbase[otel]` is the supported extra for tracing/metrics
- `opentelemetry-api>=1.15.0` → `opentelemetry-api~=1.22`
- `opentelemetry-sdk>=1.15.0` → `opentelemetry-sdk~=1.22`
- Keep `pandas` / `openpyxl`
- Add `tqdm` (used by `excel_to_json_to_cb.py` but currently undeclared)

#### `README.md` — Must change

- Title blurb currently says “SDK 4..0” (typo) → **Python SDK 4.6**
- Prerequisites: Python **3.10+**, SDK **4.6.3**
- Footer: `SDK Version: 4.6.3 | Python: 3.10+`
- Link hello-world + release notes
- Mention native OTel (script 10) and vector pre-filters / GSI vector query

#### `AGENTS.md` — Must change

- All `4.4.0` strings → `4.6.3`
- “Key Features in 4.4.0” → 4.6.3 list: native OTel tracer/meter, no reconnect-after-close, Python 3.14, JWT/mTLS refresh (mention only; no sample required), zone-aware replicas, vector pre-filters
- Connection snippet: `Cluster.connect`
- Exception note: `CASMismatchException`
- Dependencies block

#### `00_cb_ops_list.md` — Light touch

- Version / connect examples if they show `Cluster(`
- Point script 06 at preferred-server-group reads
- Point script 10 at native OTel

#### `advanced_multi_docs.md` — Light touch

- Add 4.6.0 rule: do not mix client durability and server durability in one multi-op
- Prefer `Cluster.connect` in any snippet

#### `LICENSE` — No SDK change

#### `.gitignore` — No SDK change

(`work/` is committed; it is the migration record.)

---

### 3.2 Numbered samples

#### `01a_cb_set_get.py` — Must change (connect)

- `Cluster(...)` → `Cluster.connect(...)`
- Keep upsert/get + `content_as[dict]` (already correct; tests that print `content_as["str"]` are wrong)
- Close once at end (already does)

#### `01b_cb_get_update_w_cas.py` — Must change (connect + exception name)

- `Cluster.connect`
- `CasMismatchException` → `CASMismatchException` (docs retry example uses this name)
- Keep get → replace-with-CAS → expected mismatch on stale CAS
- Optional: show `ReplaceOptions(cas=result.cas)` explicitly (docs pattern)

#### `02_cb_upsert_delete.py` — Must change (connect)

- `Cluster.connect` only unless exceptions are added

#### `03a_cb_query.py` — Must change (connect)

- `Cluster.connect`
- Keep `QueryOptions(named_parameters=...)` / `positional_parameters=...`
- Optional: catch `ParsingFailedException` / print `QueryErrorContext` on failure (ties to error-handling howto)

#### `03b_cb_query_profile.py` — Must change (connect)

- `Cluster.connect`
- Keep `QueryProfile` / `ReadFromReplica` (4.x query replica reads)

#### `04_cb_sub_doc_ops.py` — Must change (connect + result access)

- `Cluster.connect`
- Lookup results: prefer `result.content_as[dict](0)` (docs) over `content_as[str](index)` where the field is JSON
- Comment: 16-spec limit still applies to mutate/lookup **specs**; 4.6.2 relaxed *projected get* >16 paths (different API)
- 4.6.3: `MutateInResult.content_as()` no longer raises `TypeError` — tests can assert content after mutate if we add it

#### `05_cb_exception_handling.py` — Must change (connect + error-handling howto)

Already the exception showcase. Align with the howto:

- `Cluster.connect` + `wait_until_ready`
- Keep `DocumentExistsException`, `DocumentNotFoundException`, `TimeoutException`, `ServiceUnavailableException`, `CASMismatchException`
- Add a short **ambiguous durability** example (`DurabilitySyncWriteAmbiguousException` + retry insert) *or* a comment + stub if we do not want extra deps on durability config
- Add `QueryErrorContext` example (`ex.context.statement`, `first_error_code`, `first_error_message`, `client_context_id`)
- Retry decorator already matches the docs pattern — keep it
- Do not use `NetworkException`

#### `06_cb_get_retry_replica_read.py` — Must change (connect + 4.4/4.6 replica APIs)

- `Cluster.connect`
- Keep `get` / `get_any_replica` / `get_all_replicas` + retry
- **Add Example 5:** `get_replica_from_preferred_server_group` (zone-aware replica reads; Server 7.6+ / SDK 4.4+)
- Document that replica data can be stale; `DocumentUnretrievableException` if no replica answers
- 4.5+: `access_deleted` for replica reads — mention only, do not make it the default path

#### `07_cb_query_own_write.py` — Must change (connect)

- `Cluster.connect`
- Keep `QueryScanConsistency.REQUEST_PLUS`

#### `08a_cb_transaction_kv.py` — Must change (connect + exception names)

- `Cluster.connect`
- `CasMismatchException` → `CASMismatchException` if referenced
- 4.6.3: lazy `Transactions` init is concurrency-safe; teardown on cluster close — still `cluster.close()` in `finally`
- Optional comment: `get_multi` (ExtGetMulti, 4.4+) exists; do not expand the sample unless we add a focused extra example
- Do not reconnect after close

#### `08b_cb_transaction_query.py` — Must change (connect)

- Same as 08a
- Transaction query options: `flex_index` exists in C++ core (4.5) — optional, not required for this sample

#### `09_cb_fts_search.py` — Must change (connect + scoped search)

- `Cluster.connect`
- Prefer `scope.search(index, request)` for scoped indexes; `cluster.search()` only for **global** indexes
- 4.6.1 fix: SDK now passes `scope_name` / `bucket_name` for scoped indexes — our calls must use the scope object when the index is scoped
- `ConjunctionQuery([...])` list form is valid as of 4.5
- `SearchOptions(fields=[...])` stays
- Keep SQL++ `SEARCH()` vs SDK comparison; that is still valid

#### `10_cb_debug_tracing.py` — Must change (native 4.6 observability)

This is the **largest** functional sample change.

Today: Python `logging` + `couchbase.configure_logging` + `ClusterTracingOptions` (threshold + orphan) + **app-level** OpenTelemetry spans. SDK KV/query spans are **not** sent to OTel.

**After (4.6 native):**

1. Keep `configure_logging` + `ClusterTracingOptions` (threshold logger is native to the Python SDK as of 4.6; same option names).
2. Wire SDK tracer:

   ```python
   from couchbase.observability.otel_tracing import get_otel_tracer
   couchbase_tracer = get_otel_tracer(tracer_provider)
   options = ClusterOptions(..., tracer=couchbase_tracer, tracing_options=tracing_opts)
   ```

3. Add `LoggingMeter` via `ClusterMetricsOptions(emit_interval=...)` (default emit 10 min — use a short interval in the demo, e.g. 10s, so the sample actually prints).
4. Optional second path: `from couchbase.observability.otel_metrics import get_otel_meter` + `meter=...` on `ClusterOptions`.
5. Parent span on ops: `GetOptions(parent_span=parent_span)` so SDK spans nest under the demo span.
6. `Cluster.connect`
7. Docs links: tracing + metrics howtos, not only slow-ops / orphan pages.
8. Extra: `opentelemetry-exporter-otlp-proto-grpc` is **not** required for the console demo; keep console exporter. Mention `couchbase[otel]` in comments.

#### `11_cb_async_operations.py` — Must change (async connect)

- Already uses `await Cluster.connect` — keep
- Add `await self.bucket.on_connect()` after `cluster.bucket(...)`
- Tracing options remain valid
- `await cluster.close()` — do not reuse
- Retry set (timeouts, `ServiceUnavailableException`, `InternalServerFailureException`) matches the howto
- `async for` not needed for KV

#### `12_cb_async_queries.py` — Must change (async connect + iteration)

- Same connect/`on_connect` as 11
- Query iteration must stay `async for row in result` (not `for`)
- Catch `ParsingFailedException` then `CouchbaseException`

#### `13_cb_increment.py` — Must change (connect)

- `Cluster.connect`
- Keep `collection.binary().increment` / `decrement` with `IncrementOptions` / `DeltaValue` / `SignedInt64`
- 4.4.1+/4.3.x: CAS is honored on append/prepend; counters still use binary API
- try/finally close is already correct

#### `advanced_prepared_statement_wrapper.py` — Must change (connect)

- `Cluster.connect` in the `__main__` demo
- Keep `adhoc=False`, `named_parameters` / `positional_parameters`
- 4.4 C++ core allows **both** named and positional parameters on one query — optional comment, do not change the wrapper’s either/or validation unless we explicitly support both

#### `excel_to_json_to_cb.py` — Must change (connect + types + connstr)

- `from couchbase.collection import OrderedCollection` is **not** a 4.x public type for this use. Annotate as `Collection` (or drop the alias).
- `Cluster.connect`
- Connection string currently `couchbase://{host}:{port}` with default port **8091** (HTTP/UI). The SDK bootstrap port is **11210** (or omit the port). Fix default: `couchbase://{host}` and only append a port if the user passed `--kv-port`.
- `preserve_expiry=True` on first insert of new docs is a no-op / footgun — leave behavior unless tests depend on it; note in plan comments
- `tqdm` must be in `requirements.txt`

---

### 3.3 Vector sample

#### `ai_vector_sample/04_vector_search_using_python_sdk.py` — Must change (canonical 4.6 search API)

Current constructor is not the documented 4.6 shape (`field=`, `k=` vs positional `field_name` + `num_candidates`).

**Replace with:**

```python
from couchbase.search import SearchRequest, MatchQuery
from couchbase.vector_search import VectorQuery, VectorSearch

vector_search = VectorSearch.from_vector_query(
    VectorQuery('embedding', query_vector, num_candidates=5)
)
request = SearchRequest.create(vector_search)
result = scope.search('vect', request, SearchOptions(limit=5))
```

Then add a **pre-filter** example (4.4+ / Server 7.6.4+):

```python
prefilter = MatchQuery('primary', field='some_field')  # field that exists on demo docs
vector_query = VectorQuery.create('embedding', query_vector, num_candidates=5, prefilter=prefilter)
```

Optional third block (4.5+ / Server 8.0): one SQL++ hyperscale/GSI vector query via `cluster.query(...)` so the Python file matches the README’s GSI story.

- `Cluster.connect`
- Close the cluster
- Catch `CouchbaseException`

#### `ai_vector_sample/README.md` — Light touch

- SDK 4.6.3; `SearchRequest` + `VectorQuery` + pre-filters
- Server 8.x still required for GSI/hyperscale vector query
- Point at [vector searching with SDK](https://docs.couchbase.com/python-sdk/current/howtos/vector-searching-with-sdk.html)

#### `ai_vector_sample/00_*.json`, `01_*.json`, `02_*.json`, `03_*.txt` — No SDK change

---

### 3.4 Tests (must stay green; mocks follow new call shapes)

Tests are mock-based (`run_tests.py` stubs couchbase if missing). After connect/API changes they **will fail** unless updated. Do not snapshot SDK internals; assert call shapes and exception types.

#### `run_tests.py` — Must change

- Exception mocks: keep `CASMismatchException`; add alias `CasMismatchException = CASMismatchException` so either import works
- Mock modules to add if the SDK is absent:
  - `couchbase.observability`
  - `couchbase.observability.otel_tracing`
  - `couchbase.observability.otel_metrics`
  - `couchbase.vector_search`
  - `couchbase.collection`
- `test_13_cb_increment` / `test_08a` / `test_08b` are not in `safe_tests` / `working_tests` — add them if they import cleanly after the upgrade (they exist on disk but are skipped today)

#### `tests/README.md` — Must change

- Python 3.6+ → 3.10+
- Source filename table: `01_cb_set_get.py` is wrong (file is `01a_cb_set_get.py`); `01a` CAS file is `01b_cb_get_update_w_cas.py`
- Document `python3 run_tests.py` as the supported runner
- Note 4.6: tests mock `Cluster.connect` / `await Cluster.connect`

#### `tests/test_01_cb_set_get.py` — Must change

- Source is `01a_cb_set_get.py` (uses `content_as[dict]`)
- Fix mocked get to `result.content_as[dict]`, not `content_as["str"]`
- If the test starts importing the module, patch `Cluster.connect`

#### `tests/test_01a_cb_get_update_w_cas.py` — Must change

- Local stub `CasMismatchException` → `CASMismatchException`
- `assertRaises(CASMismatchException)`
- Filename vs source: this file tests **01b**

#### `tests/test_02_cb_upsert_delete.py` — Light touch

- Patch `Cluster.connect` if the module is imported at collection time

#### `tests/test_03a_cb_query.py` / `tests/test_03b_cb_query_profile.py` — Light touch

- Connect patch if needed
- Keep query option assertions (`named_parameters`, profile)

#### `tests/test_04_cb_sub_doc_ops.py` — Must change

- `CasMismatchException` → `CASMismatchException`
- Assert `content_as[dict](index)` call pattern if we change the sample
- Optional: mutate_in result `content_as` no longer TypeError (4.6.3)

#### `tests/test_05_cb_exception_handling.py` — Must change

- Keep local exception stubs; rename is already `CASMismatchException`
- `Cluster("couchbase://localhost", ...)` → `Cluster.connect(...)`
- Add tests for `QueryErrorContext` handling if the sample gains it
- Still no live cluster

#### `tests/test_06_cb_get_retry_replica_read.py` — Must change

- Add unit test for `get_replica_from_preferred_server_group` (mock collection method, success + `DocumentUnretrievableException`)
- Existing `get_any_replica` / `get_all_replicas` tests stay

#### `tests/test_07_cb_query_own_write.py` — Light touch

- Connect patch if imported

#### `tests/test_08a_cb_transaction_kv.py` / `tests/test_08b_cb_transaction_query.py` — Light touch

- Exception name if the sample switches
- Assert `cluster.close()` and that we do **not** call connect again on the same mock

#### `tests/test_09_cb_fts_search.py` — Must change

- If search moves to `scope.search(index, request)`, assert positional `(index, request)` rather than only kwargs
- ConjunctionQuery list input still valid

#### `tests/test_10_cb_debug_tracing.py` — Must change

- Today several tests only check config dicts and never import the sample (weak)
- Add tests that:
  - `get_otel_tracer` is used and passed as `ClusterOptions(..., tracer=...)`
  - `ClusterTracingOptions` still set
  - `Cluster.connect` called (not constructor)
  - `ClusterMetricsOptions` present if we add LoggingMeter
- Mock `couchbase.observability.otel_tracing.get_otel_tracer`

#### `tests/test_11_cb_async_operations.py` — Must change

- Already patches `Cluster.connect` — keep
- Add assertion that `bucket.on_connect` is awaited in the client `connect()` path (patch `AsyncCouchbaseClient` or extract connect for test)
- Mock `acouchbase` remains

#### `tests/test_12_cb_async_queries.py` — Must change

- Same `on_connect` assertion
- Query tests must use `async for` semantics (iterator is async)

#### `tests/test_13_cb_increment.py` — Must change

- Patch `Cluster.connect`
- Keep binary increment/decrement option tests
- Include in `run_tests.py` working list

#### `tests/test_advanced_prepared_statement_wrapper.py` — Light touch

- Wrapper itself has no Cluster constructor; demo `if __name__` connect change should not break unit tests of `run_cb_prepared`

#### `tests/test_excel_to_json_to_cb.py` — Must change

- `OrderedCollection` type gone → collection mock only
- `connect_couchbase` uses `Cluster.connect`
- Connection string **without** `:8091` by default
- Keep pandas/excel parsing tests

#### `tests/test_ai_vector_search.py` — Must change

This test `exec`s the script and asserts:

- `Cluster()` constructor called
- `scope.search(..., index_name=..., search_request=..., options=...)` kwargs

**After:**

- Mock `Cluster.connect` (class method), not constructor
- Mock `couchbase.search.SearchRequest` and `VectorQuery` / `VectorSearch.from_vector_query`
- Assert `scope.search` called with index `'vect'` and a `SearchRequest` (positional or kwargs — match whatever the sample uses)
- If we add a pre-filter example, assert a second `scope.search` **or** split the script so import does not run both demos (prefer a `if __name__ == "__main__"` guard so tests can import helpers without executing search). **Do this guard as part of the 4.6 cleanup** — running side effects on import is why this test uses `exec`.

---

### 3.5 Data / misc

| File | Action |
|---|---|
| `demo_data/*` | No SDK change |
| `run_tests.py` | See tests section |
| `venv/` | Not committed; recreate with 4.6.3 after merge (`pip install -r requirements.txt`) |

---

## 4. Implementation order

Do not upgrade in random file order. Connect + pin first so tests can be updated against a stable pattern.

1. **Pin + docs:** `requirements.txt`, `README.md`, `AGENTS.md`
2. **Shared connect/exception pass** on all numbered samples + wrapper + excel
3. **Behavior upgrades:** `05` (error context), `06` (preferred server group), `09` (scoped search), `10` (native OTel + meter), `11`/`12` (`on_connect`), `04` (content_as), vector script
4. **Tests + `run_tests.py` mocks**
5. **Docs pass:** `00_cb_ops_list.md`, `advanced_multi_docs.md`, `tests/README.md`, `ai_vector_sample/README.md`
6. **Verify:** `python3 run_tests.py` from repo root (mock suite; no cluster)

---

## 5. File checklist

| File | Connect | Exceptions | New 4.6 API | Tests |
|---|---|---|---|---|
| `requirements.txt` | n/a | n/a | pin 4.6.3 + otel ~=1.22 | n/a |
| `README.md` | docs | — | 3.10+, 4.6.3 | — |
| `AGENTS.md` | `Cluster.connect` | `CASMismatchException` | features list | — |
| `00_cb_ops_list.md` | snippets | — | 06/10 notes | — |
| `advanced_multi_docs.md` | snippets | — | durability mixing | — |
| `01a_cb_set_get.py` | `connect` | — | — | `test_01_cb_set_get.py` |
| `01b_cb_get_update_w_cas.py` | `connect` | `CASMismatchException` | — | `test_01a_cb_get_update_w_cas.py` |
| `02_cb_upsert_delete.py` | `connect` | — | — | `test_02_*` |
| `03a_cb_query.py` | `connect` | optional QueryErrorContext | — | `test_03a_*` |
| `03b_cb_query_profile.py` | `connect` | — | — | `test_03b_*` |
| `04_cb_sub_doc_ops.py` | `connect` | — | `content_as[dict](i)` | `test_04_*` |
| `05_cb_exception_handling.py` | `connect` | howto alignment | QueryErrorContext | `test_05_*` |
| `06_cb_get_retry_replica_read.py` | `connect` | `DocumentUnretrievableException` | preferred server group | `test_06_*` |
| `07_cb_query_own_write.py` | `connect` | — | — | `test_07_*` |
| `08a_cb_transaction_kv.py` | `connect` | names | close/no-reconnect | `test_08a_*` |
| `08b_cb_transaction_query.py` | `connect` | names | close/no-reconnect | `test_08b_*` |
| `09_cb_fts_search.py` | `connect` | — | scoped search | `test_09_*` |
| `10_cb_debug_tracing.py` | `connect` | — | `get_otel_tracer` + meter | `test_10_*` |
| `11_cb_async_operations.py` | + `on_connect` | retry set | async docs | `test_11_*` |
| `12_cb_async_queries.py` | + `on_connect` | `ParsingFailedException` | `async for` | `test_12_*` |
| `13_cb_increment.py` | `connect` | — | — | `test_13_*` |
| `advanced_prepared_statement_wrapper.py` | `connect` | — | — | `test_advanced_*` |
| `excel_to_json_to_cb.py` | `connect` | — | drop OrderedCollection; fix port | `test_excel_*` |
| `ai_vector_sample/04_*.py` | `connect` | `CouchbaseException` | SearchRequest + prefilter | `test_ai_vector_search.py` |
| `run_tests.py` | n/a | aliases + observability mocks | include skipped tests if green | self |

---

## 6. Out of scope (do not add in this pass)

- JWT authenticator / mTLS cert refresh samples (4.6 features, no current file)
- Twisted / `txcouchbase`
- MapReduce views (deprecated in 4.6)
- Enterprise Analytics / Capella Analytics SDKs
- New numbered sample files (06/10/vector absorb the new APIs)
- Live cluster integration tests (suite is mock-only by design)

---

## 7. Verification

```bash
cd ~/Documents/GitHub/cb_python_sdk_samples
source venv/bin/activate   # or recreate venv
pip install -r requirements.txt
python3 -c "import couchbase; print(couchbase.__version__)"   # expect 4.6.3
python3 run_tests.py
```

Success criteria:

- `couchbase==4.6.3` installed
- No remaining `Cluster('couchbase://` constructor calls in samples (except comments)
- No `CasMismatchException` as the primary catch (use `CASMismatchException`)
- `10_cb_debug_tracing.py` passes an SDK `tracer=` into `ClusterOptions`
- Vector sample uses `SearchRequest.create` + `VectorQuery(field_name, vector, ...)`
- `python3 run_tests.py` exits 0

---

## 8. Execution log

| Step | Status |
|---|---|
| Branch `v4.6` from `main` | done |
| This plan | done |
| Implement pin, samples, docs, tests | done |
| Remaining plan items (05 durability, 09 scoped search, 08 close, GSI vector, docs) | done |
| `python3 run_tests.py` | done — 165 tests, 0 failures |
| Smoke-import venv `couchbase==4.6.3` (QueryErrorContext, get_otel_tracer, upsert_multi) | done |
| Live cluster 7.6.5 (`localhost:8091`, travel-sample + cake.us.orders) | done |
| Scoped FTS `hotels-index` + `scope.search()` | done (`fts/`, 09) |
| Vector FTS `cake.us.vect` + SQL++ knn; GSI skipped on 7.6 | done |
| Markdown pass aligned to 4.6 (connect, FTS, vector 7.6 vs 8.0, Python 3.10+, 165 tests) | done |

---

## 9. What landed vs this plan (read this, not the unchecked rows above)

The per-file plan in §3 was written **before** implementation. A few items changed on purpose:

| Plan said | What shipped |
|---|---|
| Collection `get_replica_from_preferred_server_group` in 06 | **Does not exist** on `Collection`. KV uses `get_any_replica(GetAnyReplicaOptions(read_preference=ReadPreference.SELECTED_SERVER_GROUP))` plus `ClusterOptions(preferred_server_group=...)`. The txn API stays on `AttemptContext`. |
| 09 SQL++ “no scope-level index needed” | Both SQL++ and SDK use scoped **`hotels-index`**. SQL++ `SEARCH()` needs index **`travel-sample.inventory.hotels-index`**. |
| Vector README “Server 8.x required” | **FTS vector works on 7.6**. GSI `CREATE VECTOR INDEX` / `APPROX_VECTOR_DISTANCE` need **8.0**. |
| README / AGENTS still on Python 3.8+ and `Cluster()` | Samples and docs now **3.10+** and **`Cluster.connect()`**. |
| `run_tests.py` includes 07/08/13 | Left **out** of the runner (MagicMock `assertRaises`). Suite is **165 / 0**. |
| Docs pass last | README, AGENTS, 00_cb_ops_list, tests/README, ai_vector_sample/README, advanced_multi_docs, this file. |

This file is a **branch working log**. Do not treat unchecked §3 rows as remaining work.
