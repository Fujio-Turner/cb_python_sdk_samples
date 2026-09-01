#!/usr/bin/env python3
"""Create the scoped FTS index used by 09_cb_fts_search.py.

PUT fts/hotels-index.json to the Search REST API (port 8094), then wait until
the index reports documents. Requires Search Service and travel-sample.

    python3 fts/create_hotels_index.py

Docs:
  https://docs.couchbase.com/server/current/search/create-search-index-rest-api.html
  https://docs.couchbase.com/server/current/search/search-index-params.html
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
BUCKET = "travel-sample"
SCOPE = "inventory"
INDEX = "hotels-index"

HERE = Path(__file__).resolve().parent
DEFINITION_PATH = HERE / "hotels-index.json"

PUT_URL = f"http://{HOST}:8094/api/bucket/{BUCKET}/scope/{SCOPE}/index/{INDEX}"
COUNT_URL = f"http://{HOST}:8094/api/bucket/{BUCKET}/scope/{SCOPE}/index/{INDEX}/count"
STATS_URL = f"http://{HOST}:8094/api/bucket/{BUCKET}/scope/{SCOPE}/index/{INDEX}"


def _request(method: str, url: str, body: bytes | None = None) -> tuple[int, dict | str]:
    token = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Basic {token}")
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


def create_index() -> None:
    definition = json.loads(DEFINITION_PATH.read_text())
    # REST create must not send uuid / sourceUUID (those update an existing index).
    definition.pop("uuid", None)
    definition.pop("sourceUUID", None)
    status, payload = _request("PUT", PUT_URL, json.dumps(definition).encode("utf-8"))
    if status not in (200, 201):
        print(f"Failed to create index HTTP {status}: {payload}", file=sys.stderr)
        sys.exit(1)
    print(f"Created/updated scoped index: {payload}")


def wait_until_ready(timeout_s: int = 90) -> None:
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        status, payload = _request("GET", COUNT_URL)
        if status == 200 and isinstance(payload, dict):
            count = payload.get("count", payload)
            last = count
            print(f"  indexed docs: {count}")
            if isinstance(count, int) and count > 0:
                print(f"Index {BUCKET}.{SCOPE}.{INDEX} is ready ({count} docs).")
                return
        else:
            last = payload
            print(f"  waiting... {status} {payload}")
        time.sleep(2)
    print(f"Timed out waiting for index docs (last={last})", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    if not DEFINITION_PATH.is_file():
        print(f"Missing {DEFINITION_PATH}", file=sys.stderr)
        sys.exit(1)
    print(f"PUT {PUT_URL}")
    create_index()
    print("Waiting for indexing...")
    wait_until_ready()


if __name__ == "__main__":
    main()
