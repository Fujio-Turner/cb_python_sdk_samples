#!/usr/bin/env python3
"""Create vector indexes on cake.us.orders.

1. FTS Search Vector Index `vect` (Server 7.6+) — used by SQL++ SEARCH() knn
   and by 04_vector_search_using_python_sdk.py.
2. GSI VECTOR INDEX (Server 8.0+ / hyperscale). This cluster is often 7.6.x;
   CREATE VECTOR INDEX is attempted and skipped if the server rejects it.

    python3 ai_vector_sample/create_vector_indexes.py
"""
from __future__ import annotations

import base64
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HOST = "localhost"
USERNAME = "Administrator"
PASSWORD = "password"
BUCKET = "cake"
SCOPE = "us"
FTS_INDEX = "vect"
N1QL_URL = f"http://{HOST}:8093/query/service"

HERE = Path(__file__).resolve().parent
DEFINITION_PATH = HERE / "02_vector_search_index.json"
PUT_URL = f"http://{HOST}:8094/api/bucket/{BUCKET}/scope/{SCOPE}/index/{FTS_INDEX}"
COUNT_URL = f"http://{HOST}:8094/api/bucket/{BUCKET}/scope/{SCOPE}/index/{FTS_INDEX}/count"


def _auth_header() -> str:
    token = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _http(method: str, url: str, body: bytes | None = None) -> tuple[int, dict | str]:
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", _auth_header())
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                return resp.status, raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = raw
        return exc.code, payload


def n1ql(statement: str) -> dict:
    body = json.dumps({"statement": statement, "timeout": "120s"}).encode("utf-8")
    status, payload = _http("POST", N1QL_URL, body)
    if not isinstance(payload, dict):
        return {"status": "fail", "http": status, "raw": payload}
    payload["_http"] = status
    return payload


def create_fts_index() -> None:
    definition = json.loads(DEFINITION_PATH.read_text())
    definition.pop("uuid", None)
    definition.pop("sourceUUID", None)
    print(f"PUT {PUT_URL}")
    status, payload = _http("PUT", PUT_URL, json.dumps(definition).encode("utf-8"))
    if status not in (200, 201):
        print(f"FTS index create failed HTTP {status}: {payload}", file=sys.stderr)
        sys.exit(1)
    print(f"FTS vector index: {payload}")


def wait_fts_ready(timeout_s: int = 90) -> None:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        status, payload = _http("GET", COUNT_URL)
        if status == 200 and isinstance(payload, dict):
            count = payload.get("count", 0)
            last = count
            print(f"  FTS indexed docs: {count}")
            if isinstance(count, int) and count > 0:
                print(f"Index {BUCKET}.{SCOPE}.{FTS_INDEX} ready ({count} docs).")
                return
        else:
            last = payload
            print(f"  waiting FTS... {status} {payload}")
        time.sleep(2)
    print(f"Timed out waiting for FTS index (last={last})", file=sys.stderr)
    sys.exit(1)


def try_gsi_vector_indexes() -> None:
    statements = [
        """CREATE VECTOR INDEX idx_country_embedding_v1 IF NOT EXISTS
           ON `cake`.`us`.`orders`(embedding VECTOR)
           WITH {"dimension": 128, "similarity": "COSINE"}""",
        """CREATE VECTOR INDEX idx_country_embedding_cover_v1 IF NOT EXISTS
           ON `cake`.`us`.`orders`(embedding VECTOR)
           INCLUDE (`name`, `capital`)
           WITH {"dimension": 128, "similarity": "COSINE"}""",
    ]
    print("\nAttempting GSI VECTOR INDEX (Server 8.0+ hyperscale/composite)...")
    for stmt in statements:
        result = n1ql(stmt)
        errors = result.get("errors") or []
        if result.get("status") == "success" and not errors:
            print(f"  GSI created: {stmt.split()[3]}")
        else:
            msg = errors[0].get("msg", result) if errors else result
            print(f"  GSI skipped ({stmt.split()[3]}): {msg}")
            print("  (Expected on Couchbase Server 7.6.x — use FTS vector + SQL++ SEARCH knn.)")


def main() -> None:
    if not DEFINITION_PATH.is_file():
        print(f"Missing {DEFINITION_PATH}", file=sys.stderr)
        sys.exit(1)
    create_fts_index()
    print("Waiting for FTS indexing...")
    wait_fts_ready()
    try_gsi_vector_indexes()


if __name__ == "__main__":
    main()
