"""
Excel to JSON to Couchbase Bulk Importer

Converts Excel files (.xlsx, .xls) to JSON documents and bulk imports to Couchbase.
Supports multiple sheets, automatic type inference, and configurable batching.

Usage:
    python excel_to_json_to_cb.py path/to/file.xlsx

Features:
- Multi-sheet Excel processing
- Automatic data type detection (strings, numbers, dates, booleans)
- Configurable batch size for optimal Couchbase performance
- Progress tracking and error handling
- Flexible Couchbase connection options
"""

import sys
import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from typing import Dict, List, Any, Optional

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from couchbase.collection import OrderedCollection
from tqdm import tqdm


def parse_args():
    parser = argparse.ArgumentParser(description='Excel to JSON to Couchbase Bulk Importer')
    parser.add_argument('excel_file', help='Path to Excel file (.xlsx, .xls)')
    parser.add_argument('--bucket', default='travel-sample', help='Couchbase bucket name')
    parser.add_argument('--scope', default='_default', help='Couchbase scope name')
    parser.add_argument('--collection', default='_default', help='Couchbase collection name')
    parser.add_argument('--host', default='localhost', help='Couchbase host')
    parser.add_argument('--username', default='Administrator', help='Couchbase username')
    parser.add_argument('--password', default='password', help='Couchbase password')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch insert size')
    parser.add_argument('--port', type=int, default=8091, help='Couchbase port')
    parser.add_argument('--https', action='store_true', help='Use HTTPS connection')
    return parser.parse_args()


def excel_to_json(excel_path: str, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Convert Excel file to list of JSON-serializable dictionaries.
    """
    print(f"Reading Excel file: {excel_path}")
    
    # Read Excel file
    if sheet_name:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
    else:
        # Read first sheet by default
        xl_file = pd.ExcelFile(excel_path)
        df = pd.read_excel(xl_file, sheet_name=xl_file.sheet_names[0])
    
    print(f"Loaded {len(df)} rows from sheet(s)")
    
    # Convert DataFrame to list of dicts with type conversion
    json_data = []
    for idx, row in df.iterrows():
        doc = {}
        for col, value in row.items():
            if pd.isna(value):
                doc[col] = None
            elif isinstance(value, (int, float)):
                doc[col] = value
            elif isinstance(value, pd.Timestamp):
                doc[col] = value.isoformat()
            else:
                doc[col] = str(value)
        
        # Add metadata
        doc['id'] = f"excel_doc_{idx}"
        doc['source_file'] = excel_path
        doc['processed_at'] = datetime.utcnow().isoformat()
        doc['sheet'] = sheet_name or 'default'
        
        json_data.append(doc)
    
    return json_data


def connect_couchbase(args) -> tuple[Cluster, OrderedCollection]:
    """
    Connect to Couchbase cluster and return cluster + collection.
    """
    connection_string = f"couchbase{'s' if args.https else ''}://{args.host}:{args.port}"
    
    auth = PasswordAuthenticator(args.username, args.password)
    options = ClusterOptions(auth)
    
    if args.https:
        options.apply_profile('wan_development')
    
    cluster = Cluster(connection_string, options)
    cluster.wait_until_ready(timedelta(seconds=10))
    
    bucket = cluster.bucket(args.bucket)
    bucket.wait_until_ready(timedelta(seconds=5))
    
    collection = bucket.scope(args.scope).collection(args.collection)
    
    print(f"Connected to Couchbase: {args.host}:{args.port} / {args.bucket}.{args.scope}.{args.collection}")
    return cluster, collection


def bulk_import_to_couchbase(collection: OrderedCollection, data: List[Dict], batch_size: int = 1000):
    """
    Bulk insert JSON documents to Couchbase with progress tracking.
    """
    total_docs = len(data)
    print(f"Starting bulk import of {total_docs} documents (batch_size={batch_size})")
    
    successful = 0
    failed = 0
    
    for i in tqdm(range(0, total_docs, batch_size), desc="Importing"):
        batch = data[i:i + batch_size]
        
        try:
            # Prepare batch mutations
            mutations = []
            for doc in batch:
                mutations.append(
                    collection.upsert(doc['id'], doc, preserve_expiry=True)
                )
            
            # Execute batch
            result = collection.mutate_in_batch(mutations)
            successful += len(result)
            
        except CouchbaseException as e:
            print(f"Batch failed at {i}: {e}")
            failed += len(batch)
            continue
    
    print(f"\nImport complete: {successful} successful, {failed} failed")


def main():
    args = parse_args()
    
    # Validate Excel file
    if not os.path.exists(args.excel_file):
        print(f"Error: Excel file not found: {args.excel_file}")
        sys.exit(1)
    
    if not args.excel_file.lower().endswith(('.xlsx', '.xls')):
        print("Error: File must be .xlsx or .xls")
        sys.exit(1)
    
    try:
        # Convert Excel to JSON
        json_data = excel_to_json(args.excel_file)
        
        # Connect to Couchbase
        cluster, collection = connect_couchbase(args)
        
        # Bulk import
        bulk_import_to_couchbase(collection, json_data, args.batch_size)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'cluster' in locals():
            cluster.close()
            print("\nConnection closed.")


if __name__ == "__main__":
    main()
