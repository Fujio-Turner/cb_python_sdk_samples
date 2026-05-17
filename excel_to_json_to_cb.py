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
- Comprehensive logging to file

For local/self-hosted Couchbase Server:
    ENDPOINT = "localhost"

For Couchbase Capella (cloud), uncomment and configure the Capella settings below.
"""

import sys
import os
import logging
from datetime import datetime, timedelta
import argparse
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (
    CouchbaseException,
    DocumentExistsException,
    TimeoutException,
    ServiceUnavailableException
)
from couchbase.collection import Collection
from tqdm import tqdm


# Configure logging
LOG_FILE = "excel_import.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='Excel to JSON to Couchbase Bulk Importer')
    parser.add_argument('excel_file', help='Path to Excel file (.xlsx, .xls)')
    parser.add_argument('--bucket', default='travel-sample', help='Couchbase bucket name')
    parser.add_argument('--scope', default='_default', help='Couchbase scope name')
    parser.add_argument('--collection', default='_default', help='Couchbase collection name')
    parser.add_argument('--host', default='localhost', help='Couchbase host')
    parser.add_argument('--username', default='Administrator', help='Couchbase username')
    parser.add_argument('--password', default='password', help='Couchbase password')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch insert size')
    parser.add_argument('--port', type=int, default=11210, help='Couchbase KV port (default: 11210)')
    parser.add_argument('--https', action='store_true', help='Use HTTPS/TLS connection (for Capella)')
    parser.add_argument('--all-sheets', action='store_true', help='Process all sheets in Excel file')
    parser.add_argument('--dry-run', action='store_true', help='Parse Excel only, do not connect to Couchbase')
    return parser.parse_args()


def setup_logging():
    """Ensure logging directory exists and log startup."""
    logger.info("=" * 60)
    logger.info("Excel to Couchbase Import Started")
    logger.info("=" * 60)


def convert_value(value: Any) -> Any:
    """Convert pandas/numpy value to JSON-serializable Python type."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        # Handle numpy types
        if hasattr(value, 'item'):
            return value.item()
        return value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    # Default to string for other types
    return str(value)


def excel_to_json(excel_path: str, all_sheets: bool = False) -> List[Dict[str, Any]]:
    """
    Convert Excel file to list of JSON-serializable dictionaries.
    Supports single or multiple sheet processing.
    """
    logger.info(f"Reading Excel file: {excel_path}")

    try:
        xl_file = pd.ExcelFile(excel_path)
        sheet_names = xl_file.sheet_names
        logger.info(f"Found {len(sheet_names)} sheet(s): {sheet_names}")
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        raise

    all_json_data = []

    # Determine which sheets to process
    sheets_to_process = sheet_names if all_sheets else [sheet_names[0]]

    for sheet_name in sheets_to_process:
        logger.info(f"Processing sheet: '{sheet_name}'")

        try:
            df = pd.read_excel(xl_file, sheet_name=sheet_name)
        except Exception as e:
            logger.error(f"Failed to read sheet '{sheet_name}': {e}")
            continue

        logger.info(f"Loaded {len(df)} rows from sheet '{sheet_name}'")

        # Convert DataFrame to list of dicts with type conversion
        for idx, row in df.iterrows():
            doc = {}
            for col, value in row.items():
                # Sanitize column names (replace spaces/special chars)
                clean_col = str(col).strip().replace(' ', '_').replace('.', '_')
                doc[clean_col] = convert_value(value)

            # Generate unique document ID
            doc_id = f"excel_{sheet_name}_{idx}".replace(' ', '_').lower()

            # Add metadata
            doc['_id'] = doc_id
            doc['_source_file'] = os.path.basename(excel_path)
            doc['_sheet_name'] = sheet_name
            doc['_row_index'] = int(idx)
            doc['_processed_at'] = datetime.utcnow().isoformat()

            all_json_data.append((doc_id, doc))

    logger.info(f"Total documents prepared: {len(all_json_data)}")
    return all_json_data


def connect_couchbase(args) -> Tuple[Cluster, Collection]:
    """
    Connect to Couchbase cluster and return cluster + collection.
    Supports both local and Capella (cloud) configurations.
    """
    protocol = "couchbases" if args.https else "couchbase"
    connection_string = f"{protocol}://{args.host}"

    # Only append port if not using default
    if args.port not in (11210, 11207):
        connection_string = f"{protocol}://{args.host}:{args.port}"

    logger.info(f"Connecting to Couchbase at: {connection_string}")

    try:
        auth = PasswordAuthenticator(args.username, args.password)
        options = ClusterOptions(auth)

        # Apply WAN profile for Capella/cloud connections
        if args.https:
            options.apply_profile('wan_development')
            logger.info("Applied 'wan_development' profile for cloud connection")

        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=15))

        bucket = cluster.bucket(args.bucket)
        bucket.wait_until_ready(timedelta(seconds=10))

        collection = bucket.scope(args.scope).collection(args.collection)

        logger.info(f"Successfully connected to: {args.bucket}.{args.scope}.{args.collection}")
        return cluster, collection

    except ServiceUnavailableException as e:
        logger.error(f"Could not connect to Couchbase server: {e}")
        logger.error("Check that Couchbase is running and the host/port are correct")
        raise
    except TimeoutException as e:
        logger.error(f"Connection timeout: {e}")
        logger.error("Check network connectivity and firewall settings")
        raise
    except Exception as e:
        logger.error(f"Unexpected connection error: {e}")
        raise


def bulk_import_to_couchbase(collection: Collection, data: List[Tuple[str, Dict]],
                              batch_size: int = 100, dry_run: bool = False):
    """
    Bulk insert JSON documents to Couchbase with progress tracking.
    Uses individual upserts with batching for reliability.
    """
    total_docs = len(data)
    logger.info(f"Starting bulk import of {total_docs} documents (batch_size={batch_size})")

    if dry_run:
        logger.info("DRY RUN MODE: No documents will be written to Couchbase")
        for doc_id, doc in tqdm(data, desc="Dry run processing"):
            pass  # Just iterate to show progress
        logger.info(f"Dry run complete: {total_docs} documents would be imported")
        return

    successful = 0
    failed = 0
    errors: List[str] = []

    for i in tqdm(range(0, total_docs, batch_size), desc="Importing"):
        batch = data[i:i + batch_size]

        for doc_id, doc in batch:
            try:
                collection.upsert(doc_id, doc)
                successful += 1
            except DocumentExistsException:
                # This shouldn't happen with upsert, but handle gracefully
                logger.warning(f"Document already exists (unexpected): {doc_id}")
                successful += 1
            except TimeoutException as e:
                logger.warning(f"Timeout upserting {doc_id}: {e}")
                failed += 1
                errors.append(f"{doc_id}: timeout")
            except CouchbaseException as e:
                logger.error(f"Failed to upsert {doc_id}: {e}")
                failed += 1
                errors.append(f"{doc_id}: {str(e)[:100]}")
            except Exception as e:
                logger.error(f"Unexpected error upserting {doc_id}: {e}")
                failed += 1
                errors.append(f"{doc_id}: {str(e)[:100]}")

    logger.info(f"\nImport complete: {successful} successful, {failed} failed")

    if errors:
        logger.warning(f"First 10 errors: {errors[:10]}")

    # Write error summary to file if there were failures
    if failed > 0:
        error_log = f"import_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(error_log, 'w') as f:
            f.write("\n".join(errors))
        logger.info(f"Error details written to: {error_log}")


def validate_excel_file(excel_path: str) -> bool:
    """Validate that the file exists and has correct extension."""
    if not os.path.exists(excel_path):
        logger.error(f"Excel file not found: {excel_path}")
        return False

    if not excel_path.lower().endswith(('.xlsx', '.xls')):
        logger.error(f"File must be .xlsx or .xls: {excel_path}")
        return False

    return True


def main():
    setup_logging()
    args = parse_args()
    
    # Validate Excel file
    if not validate_excel_file(args.excel_file):
        sys.exit(1)
    
    cluster = None
    try:
        # Convert Excel to JSON
        json_data = excel_to_json(args.excel_file, all_sheets=args.all_sheets)

        if not json_data:
            logger.error("No data extracted from Excel file")
            sys.exit(1)

        # Dry run mode - don't connect to Couchbase
        if args.dry_run:
            bulk_import_to_couchbase(None, json_data, args.batch_size, dry_run=True)
            return

        # Connect to Couchbase
        cluster, collection = connect_couchbase(args)
        
        # Bulk import
        bulk_import_to_couchbase(collection, json_data, args.batch_size)
        
    except KeyboardInterrupt:
        logger.warning("\nImport interrupted by user")
        sys.exit(130)
    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        sys.exit(1)
    except ServiceUnavailableException as e:
        logger.error(f"Service unavailable: {e}")
        logger.error("Ensure Couchbase Server is running and accessible")
        sys.exit(1)
    except TimeoutException as e:
        logger.error(f"Operation timeout: {e}")
        sys.exit(1)
    except CouchbaseException as e:
        logger.error(f"Couchbase error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if cluster is not None:
            try:
                cluster.close()
                logger.info("Connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")

    logger.info("Import process completed")


if __name__ == "__main__":
    main()
