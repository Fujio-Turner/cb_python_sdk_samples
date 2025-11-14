"""
Demonstrates Full-Text Search (FTS) in Couchbase using the SEARCH() function.

This script shows how to:
1. Check if a search index exists
2. Create a search index if it doesn't exist
3. Wait for the index to be ready
4. Perform various FTS queries using SEARCH()

Full-Text Search is useful for:
- Text-based searches with wildcards, fuzzy matching, and relevance scoring
- Searching across multiple fields simultaneously
- Geographic queries (proximity searches)
- Complex boolean queries with AND/OR/NOT logic
- Phrase matching and term boosting

Note: FTS indexes must be created before they can be used in queries.
"""
from datetime import timedelta
import time
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.exceptions import CouchbaseException, SearchIndexNotFoundException
from couchbase.management.search import SearchIndex
import json

# Update this to your cluster
# For local/self-hosted Couchbase Server:
ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"

# For Capella (cloud), uncomment and update these instead:
# ENDPOINT = "cb.your-endpoint.cloud.couchbase.com"  # Your Capella hostname
# USERNAME = "your-capella-username"
# PASSWORD = "your-capella-password"

BUCKET_NAME = "travel-sample"
INDEX_NAME = "hotels-index"
# User Input ends here.

# Connect options - authentication
auth = PasswordAuthenticator(USERNAME, PASSWORD)

# get a reference to our cluster
options = ClusterOptions(auth)

# For local/self-hosted Couchbase Server:
cluster = Cluster('couchbase://{}'.format(ENDPOINT), options)

# For Capella (cloud), use this instead (uncomment and comment out the line above):
# options.apply_profile('wan_development')  # Helps avoid latency issues with Capella
# cluster = Cluster('couchbases://{}'.format(ENDPOINT), options)  # Note: couchbaseS (secure)

cluster.wait_until_ready(timedelta(seconds=10))

# Get a reference to the bucket
cb = cluster.bucket(BUCKET_NAME)

# Get search index manager
search_mgr = cluster.search_indexes()


def check_and_create_index(index_name, bucket_name):
    """
    Check if a search index exists, create it if it doesn't, and wait for it to be ready.
    """
    print(f"\n--- Checking for FTS index: {index_name} ---")
    
    try:
        # Check if index exists
        existing_index = search_mgr.get_index(index_name)
        print(f"✓ Index '{index_name}' already exists")
        return True
    except SearchIndexNotFoundException:
        print(f"Index '{index_name}' not found. Creating it now...")
        
        # Create index definition
        # This creates a simple default index on all fields in the hotel collection
        index_definition = {
            "type": "fulltext-index",
            "name": index_name,
            "sourceType": "gocbcore",
            "sourceName": bucket_name,
            "planParams": {
                "maxPartitionsPerPIndex": 1024,
                "indexPartitions": 1
            },
            "params": {
                "doc_config": {
                    "docid_prefix_delim": "",
                    "docid_regexp": "",
                    "mode": "scope.collection.type_field",
                    "type_field": "type"
                },
                "mapping": {
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "dynamic": True,
                        "enabled": False
                    },
                    "default_type": "_default",
                    "docvalues_dynamic": False,
                    "index_dynamic": True,
                    "store_dynamic": False,
                    "type_field": "_type",
                    "types": {
                        "inventory.hotel": {
                            "dynamic": True,
                            "enabled": True
                        }
                    }
                },
                "store": {
                    "indexType": "scorch",
                    "segmentVersion": 16
                }
            },
            "sourceParams": {}
        }
        
        try:
            # Create the index
            search_mgr.upsert_index(SearchIndex(
                name=index_name,
                source_name=bucket_name,
                idx_type='fulltext-index',
                params=index_definition['params']
            ))
            
            print(f"✓ Index '{index_name}' created successfully")
            print(f"  Waiting for index to be ready...")
            
            # Wait for index to be ready (check every 2 seconds, max 60 seconds)
            max_wait = 60
            elapsed = 0
            while elapsed < max_wait:
                time.sleep(2)
                elapsed += 2
                try:
                    idx = search_mgr.get_index(index_name)
                    # Index exists, assume it's ready
                    print(f"✓ Index is ready (waited {elapsed}s)")
                    return True
                except:
                    print(f"  Still waiting... ({elapsed}s)")
                    continue
            
            print(f"⚠ Index creation may still be in progress after {max_wait}s")
            return False
            
        except Exception as e:
            print(f"✗ Failed to create index: {e}")
            return False
    except Exception as e:
        print(f"✗ Error checking index: {e}")
        return False


# Check and create index before running queries
index_ready = check_and_create_index(INDEX_NAME, BUCKET_NAME)

if not index_ready:
    print("\n⚠ Warning: Index may not be ready. Queries might fail.")
    print("You can also create the index manually via Couchbase UI:")
    print(f"  1. Go to Search > Add Index")
    print(f"  2. Name: {INDEX_NAME}")
    print(f"  3. Bucket: {BUCKET_NAME}")
    print(f"  4. Type Mappings: Add 'inventory.hotel'")
    print(f"  5. Click 'Create Index'\n")


print("\n" + "=" * 70)
print("FTS SEARCH EXAMPLES")
print("=" * 70)


# Example 1: Basic SEARCH() - no index required (searches default fields)
print("\n--- Example 1: Basic SEARCH() (no specific index) ---")
query = """
SELECT name, country
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, "paris")
LIMIT 5
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    rows = list(result)
    for row in rows:
        print(f"  {row}")
    print(f"✓ Found {len(rows)} results in {time.time() - start_time:.4f}s")
except Exception as e:
    print(f"✗ Error: {e}")


# Example 2: SEARCH() with specific field using FTS index
print(f"\n--- Example 2: Field-specific search using '{INDEX_NAME}' ---")
query = f"""
SELECT name, country
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, "country:france", {{"index": "{INDEX_NAME}"}})
LIMIT 5
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    rows = list(result)
    for row in rows:
        print(f"  {row}")
    print(f"✓ Found {len(rows)} results in {time.time() - start_time:.4f}s")
except Exception as e:
    print(f"✗ Error: {e}")


# Example 3: Wildcard search
print(f"\n--- Example 3: Wildcard search (fran*) ---")
query = f"""
SELECT name, country
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, "country:fran*", {{"index": "{INDEX_NAME}"}})
LIMIT 5
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    rows = list(result)
    for row in rows:
        print(f"  {row}")
    print(f"✓ Found {len(rows)} results in {time.time() - start_time:.4f}s")
except Exception as e:
    print(f"✗ Error: {e}")


# Example 4: Phrase search
print(f"\n--- Example 4: Phrase search ---")
query = f"""
SELECT name, description
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, 'description:"historic building"', {{"index": "{INDEX_NAME}"}})
LIMIT 3
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    rows = list(result)
    for row in rows:
        print(f"  {row['name']}: {row['description'][:80]}...")
    print(f"✓ Found {len(rows)} results in {time.time() - start_time:.4f}s")
except Exception as e:
    print(f"✗ Error: {e}")


# Example 5: Boolean query (AND/OR)
print(f"\n--- Example 5: Boolean query (country AND city) ---")
query = f"""
SELECT name, country, city
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, "country:france AND city:paris", {{"index": "{INDEX_NAME}"}})
LIMIT 5
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    rows = list(result)
    for row in rows:
        print(f"  {row}")
    print(f"✓ Found {len(rows)} results in {time.time() - start_time:.4f}s")
except Exception as e:
    print(f"✗ Error: {e}")


print("\n" + "=" * 70)
print("FTS SEARCH EXAMPLES COMPLETE")
print("=" * 70)

print("\nNote: For more advanced FTS features:")
print("  - Fuzzy matching: Use ~N (e.g., 'name:paris~2')")
print("  - Range queries: Use field:[start TO end]")
print("  - Boost terms: Use ^N (e.g., 'name:paris^2.0')")
print("  - Regex: Use field:/pattern/")
print("  - Geographic: Use geo queries with lat/lon")

# Close the connection
print("\nClosing connection to Couchbase cluster...")
cluster.close()
print("Connection closed successfully.")
