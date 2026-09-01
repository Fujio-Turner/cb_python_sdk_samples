"""
Demonstrates Full-Text Search (FTS) in Couchbase using both methods:
1. SQL++ SEARCH() function (query-based approach)  
2. Native SDK Search API (object-based approach)

This script shows how to:
- Perform FTS using SQL++ SEARCH() function (works immediately)
- Perform FTS using native SDK methods (requires the scoped index in fts/hotels-index.json)
- Compare both approaches with detailed pros/cons
- Understand when to use each method

Full-Text Search is useful for:
- Text-based searches with wildcards, fuzzy matching, and relevance scoring
- Searching across multiple fields simultaneously
- Geographic queries (proximity searches)
- Complex boolean queries with AND/OR/NOT logic
- Phrase matching and term boosting

PROS & CONS OF EACH APPROACH:

SQL++ SEARCH() Function Pros:
- ✓ Can combine with SQL JOINs for related data in one query
- ✓ Data manipulation (aggregation, filtering, sorting) done by SQL++, not app code
- ✓ Returns full documents with all fields
- ✓ Familiar SQL syntax for developers
- ✓ Single query can search + join + aggregate
- ✓ Example: Search hotels + JOIN reviews + GROUP BY rating - all in one query
- ✓ Works with cluster-level indexes (no scope-level index needed)
- ✗ Requires hop: Client → SQL++ Server → FTS Server (extra latency)
- ✗ No FTS Search index alias support

Native SDK Search API Pros:
- ✓ Index alias support: Change FTS indexes without modifying application code
  - Create alias "hotels-search" pointing to "hotels-index-v1"
  - Deploy new index "hotels-index-v2", update alias
  - Zero downtime, no code changes needed!
- ✓ Fewer network hops: Client → FTS Server directly (lower latency)
- ✓ FTS scan consistency control (not available in SQL++ SEARCH)
- ✓ Type-safe query construction
- ✓ Composable query objects (build complex queries programmatically)
- ✓ More control over search-specific options
- ✗ Returns keys/fields only - may need separate KV gets for full documents
- ✗ Aggregation/joins must be done in application code
- ✗ Requires scope-level FTS index (more setup)

Recommendation:
- Use SQL++ SEARCH() when: JOINs, aggregations, complex data manipulation, quick setup
- Use Native SDK when: Aliases, lowest latency, scan consistency, programmatic queries
"""
from datetime import timedelta
import time
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions, SearchOptions
from couchbase.exceptions import CouchbaseException
from couchbase.search import (
    SearchRequest,
    MatchQuery,
    TermQuery,
    QueryStringQuery,
    MatchPhraseQuery,
    WildcardQuery,
    ConjunctionQuery
)

# Update this to your cluster
# For local/self-hosted Couchbase Server:
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

# For Capella (cloud), uncomment and update these instead:
# ENDPOINT = "cb.your-endpoint.cloud.couchbase.com"
# USERNAME = "your-capella-username"
# PASSWORD = "your-capella-password"

BUCKET_NAME = "travel-sample"
CB_SCOPE = "inventory"
INDEX_NAME = "hotels-index"  # scoped SDK name
# SQL++ SEARCH() requires the fully-qualified scoped index name:
SQL_INDEX_NAME = f"{BUCKET_NAME}.{CB_SCOPE}.{INDEX_NAME}"

# Create the index once:
#   python3 fts/create_hotels_index.py
# Definition: fts/hotels-index.json (inventory.hotel: name, country, city, description)

# Native SDK examples use scope.search() (correct for this scoped index).
USE_NATIVE_SDK_EXAMPLES = True
# cluster.search() is only for *global* indexes. Leave False for hotels-index.
USE_CLUSTER_SEARCH_EXAMPLE = False

# User Input ends here.

# Connect options - authentication
auth = PasswordAuthenticator(USERNAME, PASSWORD)
options = ClusterOptions(auth)

# For local/self-hosted Couchbase Server:
cluster = Cluster.connect(f'couchbase://{ENDPOINT}', options)

# For Capella (cloud), use this instead:
# options.apply_profile('wan_development')
# cluster = Cluster.connect(f'couchbases://{ENDPOINT}', options)

cluster.wait_until_ready(timedelta(seconds=10))

# Get references
bucket = cluster.bucket(BUCKET_NAME)
scope = bucket.scope(CB_SCOPE)

print("=" * 70)
print("FULL-TEXT SEARCH (FTS) EXAMPLES")
print("=" * 70)
print("\nDemonstrating SQL++ SEARCH() and Native SDK approaches\n")


# ============================================================================
# PART 1: SQL++ SEARCH() Function Approach (Works Immediately)
# ============================================================================
print("=" * 70)
print("PART 1: SQL++ SEARCH() Function Approach")
print("=" * 70)
print("SQL++ SEARCH() uses the same scoped FTS index (fts/hotels-index.json).\n")
print("Create it with: python3 fts/create_hotels_index.py\n")


# Example 1: Basic SEARCH() with SQL++
print("--- Example 1 (SQL++): Basic Text Search ---")
query = f"""
SELECT `name`, `country`
FROM `{BUCKET_NAME}`.`inventory`.`hotel`
WHERE SEARCH(`hotel`, "paris", {{"index": "{SQL_INDEX_NAME}"}})
LIMIT $limit
"""

try:
    start_time = time.time()
    result = cluster.query(query, QueryOptions(named_parameters={"limit": 5}))
    rows = list(result)
    elapsed = time.time() - start_time
    
    print(f"✓ Found {len(rows)} hotels with 'paris'")
    for row in rows[:3]:
        print(f"  - {row['name']} ({row['country']})")
    print(f"  Time: {elapsed:.4f}s | Method: SQL++ SEARCH()")
except Exception as e:
    print(f"✗ Error: {e}")


# Example 2: SEARCH() with wildcards
print("\n--- Example 2 (SQL++): Wildcard Search ---")
query = f"""
SELECT `name`, `country`
FROM `{BUCKET_NAME}`.`inventory`.`hotel`
WHERE SEARCH(`hotel`, "country:fran*", {{"index": "{SQL_INDEX_NAME}"}})
LIMIT $limit
"""

try:
    start_time = time.time()
    result = cluster.query(query, QueryOptions(named_parameters={"limit": 5}))
    rows = list(result)
    elapsed = time.time() - start_time
    
    print(f"✓ Found {len(rows)} hotels in France (wildcard: fran*)")
    for row in rows[:3]:
        print(f"  - {row['name']}")
    print(f"  Time: {elapsed:.4f}s | Method: SQL++ wildcard syntax")
except Exception as e:
    print(f"✗ Error: {e}")


# Example 3: SEARCH() with boolean AND
print("\n--- Example 3 (SQL++): Boolean AND Search ---")
query = f"""
SELECT `name`, `city`, `country`
FROM `{BUCKET_NAME}`.`inventory`.`hotel`
WHERE SEARCH(`hotel`, "country:france AND city:paris", {{"index": "{SQL_INDEX_NAME}"}})
LIMIT $limit
"""

try:
    start_time = time.time()
    result = cluster.query(query, QueryOptions(named_parameters={"limit": 5}))
    rows = list(result)
    elapsed = time.time() - start_time
    
    print(f"✓ Found {len(rows)} hotels in Paris, France (AND logic)")
    for row in rows[:3]:
        print(f"  - {row['name']}")
    print(f"  Time: {elapsed:.4f}s | Method: SQL++ boolean AND")
except Exception as e:
    print(f"✗ Error: {e}")


# ============================================================================
# PART 2: Native SDK Search API Approach (Requires Scope-Level Index)
# ============================================================================
print("\n" + "=" * 70)
print("PART 2: Native SDK Search API Approach")
print("=" * 70)

if not USE_NATIVE_SDK_EXAMPLES:
    print("\n⚠️  Native SDK examples DISABLED by default")
    print("Set USE_NATIVE_SDK_EXAMPLES = True to enable (requires FTS index)")
    print("\nYour current index setup will work - it's bucket-level, not scope-level.")
    print("The examples use cluster.search() which works with bucket-level indexes.\n")
else:
    print("\nNative SDK examples ENABLED - using scoped FTS index via scope.search()\n")
    
    # Example 4: MatchQuery - scope.search() for a scoped index (SDK 4.6.1+)
    print("--- Example 4 (SDK): MatchQuery for 'paris' ---")
    try:
        start_time = time.time()
        
        query = MatchQuery("paris", field="name")
        request = SearchRequest.create(query)
        
        search_result = scope.search(INDEX_NAME, request, SearchOptions(limit=5, fields=["name", "country"]))
        
        rows = list(search_result.rows())
        elapsed = time.time() - start_time
        
        print(f"✓ Found {len(rows)} hotels with 'paris' in name")
        for row in rows[:3]:
            # Access fields safely
            if hasattr(row, 'fields') and row.fields:
                print(f"  - {row.fields.get('name', 'N/A')} ({row.fields.get('country', 'N/A')})")
            else:
                print(f"  - ID: {row.id}")
        print(f"  Time: {elapsed:.4f}s | Method: SDK MatchQuery + scope.search()")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    
    # Example 5: MatchPhraseQuery
    print("\n--- Example 5 (SDK): MatchPhraseQuery for Exact Phrase ---")
    try:
        start_time = time.time()
        
        query = MatchPhraseQuery("historic building", field="description")
        request = SearchRequest.create(query)
        
        search_result = scope.search(INDEX_NAME, request, SearchOptions(limit=3, fields=["name", "description"]))
        
        rows = list(search_result.rows())
        elapsed = time.time() - start_time
        
        print(f"✓ Found {len(rows)} hotels with 'historic building' phrase")
        for row in rows:
            if hasattr(row, 'fields') and row.fields:
                name = row.fields.get('name', 'N/A')
                desc = row.fields.get('description', '')
                print(f"  - {name}: {desc[:60]}...")
            else:
                print(f"  - ID: {row.id}")
        print(f"  Time: {elapsed:.4f}s | Method: SDK MatchPhraseQuery + scope.search()")
    except Exception as e:
        print(f"✗ Error: {e}")
    
    
    # Example 6: ConjunctionQuery (AND logic)
    print("\n--- Example 6 (SDK): ConjunctionQuery (AND Logic) ---")
    try:
        start_time = time.time()
        
        query1 = MatchQuery("france", field="country")
        query2 = MatchQuery("paris", field="city")
        
        # 4.5+: ConjunctionQuery also accepts a list of queries
        conjunction = ConjunctionQuery([query1, query2])
        request = SearchRequest.create(conjunction)
        
        search_result = scope.search(INDEX_NAME, request, SearchOptions(limit=5, fields=["name", "city", "country"]))
        
        rows = list(search_result.rows())
        elapsed = time.time() - start_time
        
        print(f"✓ Found {len(rows)} hotels in Paris, France (AND logic)")
        for row in rows[:3]:
            if hasattr(row, 'fields') and row.fields:
                print(f"  - {row.fields.get('name', 'N/A')}")
            else:
                print(f"  - ID: {row.id}")
        print(f"  Time: {elapsed:.4f}s | Method: SDK ConjunctionQuery + scope.search()")
    except Exception as e:
        print(f"✗ Error: {e}")

    # Example 7: cluster.search() is for *global* indexes only
    print("\n--- Example 7 (SDK): cluster.search() vs scope.search() ---")
    print("  hotels-index is scoped → examples 4–6 use scope.search().")
    if not USE_CLUSTER_SEARCH_EXAMPLE:
        print("  Skipped cluster.search() (USE_CLUSTER_SEARCH_EXAMPLE = False).")
        print("  Enable only for a cluster-level (global) FTS index.")
    else:
        try:
            query = MatchQuery("paris", field="name")
            request = SearchRequest.create(query)
            search_result = cluster.search(INDEX_NAME, request, SearchOptions(limit=5, fields=["name", "country"]))
            rows = list(search_result.rows())
            print(f"✓ cluster.search() returned {len(rows)} rows")
        except Exception as e:
            print(f"✗ cluster.search() error (expected for a scoped index): {e}")


# ============================================================================
# COMPARISON SUMMARY
# ============================================================================
print("\n" + "=" * 70)
print("COMPARISON: SQL++ SEARCH() vs Native SDK")
print("=" * 70)

print("\n🔹 SQL++ SEARCH() Approach:")
print("  ✓ Works immediately - no scope-level index needed")
print("  ✓ Combine with SQL JOINs (e.g., search hotels + join reviews)")
print("  ✓ Aggregation in SQL++ (GROUP BY, ORDER BY, COUNT)")
print("  ✓ Returns full documents")
print("  ✓ Familiar SQL syntax")
print("  ✗ Extra network hop (Client → SQL++ → FTS)")
print("  ✗ No index alias support")

print("\n🔹 Native SDK Search API:")
print("  ✓ Index aliases for zero-downtime index updates")
print("  ✓ Direct to FTS server (fewer hops = lower latency)")
print("  ✓ FTS scan consistency control")
print("  ✓ Type-safe, composable query objects")
print("  ✗ Requires scope-level index setup")
print("  ✗ Returns fields only (may need KV gets for full docs)")
print("  ✗ Aggregation/joins in application code")

print("\n💡 When to Use Each:")
print("  - SQL++ SEARCH(): JOINs, aggregations, full documents, quick setup")
print("  - Native SDK: Aliases, lowest latency, scan consistency, production flexibility")

print("\n" + "=" * 70)
print("FTS SEARCH EXAMPLES COMPLETE")
print("=" * 70)

print("\nLearn More:")
print("  - Search API: https://docs.couchbase.com/sdk-api/couchbase-python-client/couchbase_api/couchbase_search.html")
print("  - SQL++ SEARCH: https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/searchfun.html")
print("  - Index Aliases: https://docs.couchbase.com/server/current/fts/fts-index-aliases.html")

# Close the connection
print("\nClosing connection...")
cluster.close()
print("Connection closed.")
