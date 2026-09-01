"""
Vector search with the Couchbase Python SDK 4.6.

Uses the Search Service (FTS) vector index. For GSI / hyperscale vector
query (Server 8.0+, SDK 4.5+), run SQL++ via cluster.query() — see README.
"""
from datetime import timedelta
import json

from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, SearchOptions, QueryOptions
from couchbase.exceptions import CouchbaseException
from couchbase.search import SearchRequest, MatchQuery
from couchbase.vector_search import VectorQuery, VectorSearch

ENDPOINT = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"
BUCKET_NAME = "cake"
SCOPE_NAME = "us"
COLLECTION_NAME = "orders"
INDEX_NAME = "vect"  # scoped FTS vector index
SQL_INDEX_NAME = f"{BUCKET_NAME}.{SCOPE_NAME}.{INDEX_NAME}"

# 128-dim query vector (Belgium) — same payload as 00_query_vector_input.json
query_vector = [
    0.307843137254902, 0.40588235294117647, 0.4117647058823529, 0.1803921568627451,
    0.12941176470588237, 0.32745098039215687, 0.4137254901960784, 0.43529411764705883,
    0.2803921568627451, 0.14901960784313725, 0.2607843137254902, 0.40980392156862744,
    0.43333333333333335, 0.4019607843137255, 0.1803921568627451, 0.12941176470588237,
    0.35294117647058826, 0.4549019607843137, 0.4235294117647059, 0.4372549019607843,
    0.15294117647058825, 0.12941176470588237, 0.4372549019607843, 0.44901960784313727,
    0.4019607843137255, 0.43333333333333335, 0.43333333333333335, 0.1803921568627451,
    0.1588235294117647, 0.2, 0.22156862745098038, 0.20392156862745098,
    0.18627450980392157, 0.12941176470588237, 0.4137254901960784, 0.38823529411764707,
    0.1803921568627451, 0.1627450980392157, 0.1980392156862745, 0.20784313725490197,
    0.14901960784313725, 0.2607843137254902, 0.45294117647058824, 0.4215686274509804,
    0.40980392156862744, 0.30392156862745096, 0.17647058823529413, 0.2019607843137255,
    0.45294117647058824, 0.28431372549019607, 0.14901960784313725, 0.2784313725490196,
    0.40588235294117647, 0.43137254901960786, 0.39215686274509803, 0.4235294117647059,
    0.1803921568627451, 0.2411764705882353, 0.20392156862745098, 0.40980392156862744,
    0.4196078431372549, 0.4294117647058823, 0.15294117647058825, 0.12941176470588237,
    0.3607843137254902, 0.4137254901960784, 0.3980392156862745, 0.15294117647058825,
    0.12941176470588237, 0.33725490196078434, 0.4372549019607843, 0.40588235294117647,
    0.24901960784313726, 0.14901960784313725, 0.2901960784313726, 0.4,
    0.4235294117647059, 0.2823529411764706, 0.17647058823529413, 0.2019607843137255,
    0.45294117647058824, 0.4372549019607843, 0.2647058823529412, 0.14901960784313725,
    0.29215686274509806, 0.4215686274509804, 0.4215686274509804, 0.40784313725490196,
    0.43333333333333335, 0.1803921568627451, 0.12941176470588237, 0.3686274509803922,
    0.45294117647058824, 0.4215686274509804, 0.2784313725490196, 0.36470588235294116,
    0.4411764705882353, 0.4176470588235294, 0.15294117647058825, 0.12941176470588237,
    0.4117647058823529, 0.39215686274509803, 0.1803921568627451, 0.12941176470588237,
    0.43137254901960786, 0.4470588235294118, 0.3392156862745098, 0.1843137254901961,
    0.44901960784313727, 0.4294117647058823, 0.3862745098039216, 0.3235294117647059,
    0.41568627450980394, 0.4196078431372549, 0.3941176470588235, 0.396078431372549,
    0.307843137254902, 0.42549019607843136, 0.3254901960784314, 0.41568627450980394,
    0.42549019607843136, 0.3941176470588235, 0.396078431372549, 0.28627450980392155,
    0.43137254901960786, 0.43137254901960786, 0.4411764705882353, 0.1980392156862745
]


def connect():
    cluster = Cluster.connect(
        f"couchbase://{ENDPOINT}",
        ClusterOptions(PasswordAuthenticator(USERNAME, PASSWORD)),
    )
    cluster.wait_until_ready(timedelta(seconds=10))
    bucket = cluster.bucket(BUCKET_NAME)
    scope = bucket.scope(SCOPE_NAME)
    collection = scope.collection(COLLECTION_NAME)
    return cluster, bucket, scope, collection


def print_hits(result, heading):
    print(heading)
    print("-" * 60)
    for row in result.rows():
        print(f"ID: {row.id}")
        print(f"Score (cosine similarity): {row.score:.6f}")
        fields = getattr(row, "fields", None) or {}
        print(f"Fields: {json.dumps(fields, indent=2)}")
        print("-" * 40)


def run_knn(scope):
    vector_search = VectorSearch.from_vector_query(
        VectorQuery("embedding", query_vector, num_candidates=5)
    )
    request = SearchRequest.create(vector_search)
    result = scope.search(
        INDEX_NAME,
        request,
        SearchOptions(limit=5, fields=["name", "capital"]),
    )
    print_hits(result, "SDK FTS Vector Search (Top 5):")
    return result


def run_knn_with_prefilter(scope):
    # Pre-filter (Server 7.6.4+ / SDK 4.4+): non-vector query first, then KNN.
    prefilter = MatchQuery("Belgium", field="name")
    vector_query = VectorQuery.create(
        "embedding",
        query_vector,
        num_candidates=5,
        prefilter=prefilter,
    )
    request = SearchRequest.create(VectorSearch.from_vector_query(vector_query))
    result = scope.search(
        INDEX_NAME,
        request,
        SearchOptions(limit=5, fields=["name", "capital"]),
    )
    print_hits(result, "SDK FTS Vector Search + pre-filter (name matches Belgium):")
    return result


def _print_query_rows(result, heading):
    print(heading)
    print("-" * 60)
    count = 0
    rows_iter = result.rows() if hasattr(result, "rows") else result
    for row in rows_iter:
        print(f"  {row}")
        count += 1
        if count >= 10:
            break
    if count == 0:
        print("  (no rows)")
    return count


def run_sql_fts_knn(cluster):
    """SQL++ SEARCH() knn against the FTS vector index (Server 7.6+)."""
    search_body = {
        "fields": ["name", "capital"],
        "knn": [
            {
                "k": 4,
                "field": "embedding",
                "vector": query_vector,
            }
        ],
    }
    result = cluster.query(
        "SELECT META(t).id AS id, t.name, t.capital "
        "FROM `cake`.`us`.`orders` AS t "
        f"WHERE SEARCH(t, $search, {{'index': '{SQL_INDEX_NAME}'}})",
        QueryOptions(named_parameters={"search": search_body}),
    )
    _print_query_rows(result, "SQL++ SEARCH() knn (FTS vector index):")


def run_gsi_vector_query(cluster):
    """Hyperscale / composite GSI vector query via SQL++ (Server 8.0+)."""
    print("\nGSI / hyperscale vector query (SQL++ APPROX_VECTOR_DISTANCE):")
    result = cluster.query(
        "SELECT META().id AS id, name, capital, "
        "APPROX_VECTOR_DISTANCE(embedding, $vec, 'cosine') AS distance "
        "FROM `cake`.`us`.`orders` "
        "ORDER BY distance "
        "LIMIT 5",
        QueryOptions(named_parameters={"vec": query_vector}),
    )
    _print_query_rows(result, "GSI results:")


def main():
    cluster = None
    try:
        cluster, _bucket, scope, _collection = connect()
        run_knn(scope)
        print()
        run_knn_with_prefilter(scope)
        print()
        try:
            run_sql_fts_knn(cluster)
        except CouchbaseException as e:
            print(f"SQL++ SEARCH knn failed: {e}")
        print()
        try:
            run_gsi_vector_query(cluster)
        except CouchbaseException as e:
            print(f"GSI vector query skipped (needs Server 8.0+ CREATE VECTOR INDEX): {e}")
    except CouchbaseException as e:
        print(f"Search failed: {e}")
    finally:
        if cluster:
            cluster.close()


if __name__ == "__main__":
    main()
