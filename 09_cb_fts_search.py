"""
This code demonstrates how to use the Couchbase Python SDK to perform full-text search (FTS) queries on a Couchbase cluster.

The code first connects to a Couchbase cluster using the provided endpoint, username, and password. It then retrieves a reference to the "travel-sample" bucket.

The code then provides several examples of using the `SEARCH()` function to perform FTS queries:

1. A basic example that searches for the term "paris" in the "hotel" field.
2. An example that searches for hotels with a country starting with "fran*" using the "hotels-index" FTS index.
3. An advanced example that uses a conjunctive query with multiple conditions, including field matches, wildcards, ranges, and boosting. This query searches for hotels in France with a luxury description, free parking, vacancy between 50 and 100, and gives a boost to hotels with "paris" in the name.
4. A geo-based query that searches for hotels in the United States within a 10-mile radius of the specified coordinates.

Each example measures the execution time of the query and prints the results.

Finally, the code closes the connection to the Couchbase cluster.
"""
from datetime import timedelta
import time
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions

# Update this to your cluster
ENDPOINT = "localhost"
USERNAME = "demo"
PASSWORD = "password"
BUCKET_NAME = "travel-sample"

# Connect to the cluster
auth = PasswordAuthenticator(USERNAME, PASSWORD)
options = ClusterOptions(auth)
options.apply_profile('wan_development')
cluster = Cluster('couchbases://{}'.format(ENDPOINT), options)
cluster.wait_until_ready(timedelta(seconds=10))

# Get a reference to the bucket
cb = cluster.bucket(BUCKET_NAME)

## Basic SEARCH() example with no FTS index
print("Basic SEARCH() example:")
query = """
SELECT name, country
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, "paris")
LIMIT 5
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    for row in result:
        print(row)
    print(f"Query executed in {time.time() - start_time:.4f} seconds")
except Exception as e:
    print(f"An error occurred: {e}")

## SEARCH() with specific field and FTS index
print("\nSEARCH() with specific field and FTS index:")
query = """
SELECT name, country
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, "country:fran*", {"index": "hotels-index"})
LIMIT 5
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    for row in result:
        print(row)
    print(f"Query executed in {time.time() - start_time:.4f} seconds")
except Exception as e:
    print(f"An error occurred: {e}")

## Advanced SEARCH() with conjunctive query using conjuncts array
print("\nAdvanced SEARCH() with conjunctive query using conjuncts array:")
query = """
SELECT name, country, description
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, 
    {
        "conjuncts": [
            {"field": "country", "match": "franc*"},
            {"field": "description", "match": "luxur*"},
            {"field": "free_parking", "bool": true},
            {"field": "vacancy", "min": 50, "max": 100},
            {"field": "name", "match": "paris", "boost": 2.0}
        ]
    },
    {"index": "hotels-index"})
LIMIT 5
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    for row in result:
        print(row)
    print(f"Query executed in {time.time() - start_time:.4f} seconds")
except Exception as e:
    print(f"An error occurred: {e}")

## Geo query with SEARCH()
print("\nGeo query with SEARCH():")
query = """
SELECT name, country, geo
FROM `travel-sample`.inventory.hotel
WHERE SEARCH(hotel, 
    "country:\"united states\" AND geo:[-33.8688, 151.2093, 10mi]", 
    {"index": "hotels-geo-index"})
LIMIT 5
"""
try:
    start_time = time.time()
    result = cluster.query(query)
    for row in result:
        print(row)
    print(f"Query executed in {time.time() - start_time:.4f} seconds")
except Exception as e:
    print(f"An error occurred: {e}")

# Close the connection
cluster.close()