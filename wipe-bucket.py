#!/usr/bin/env python3

import boto3
import csv
import os
import sys
import logging
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config

# Parse args
args = sys.argv[1:]
DRY_RUN = '--dry-run' in args
ONLY_UNLOCK = '--only-unlock' in args
VERBOSE = '--verbose' in args
args = [a for a in args if not a.startswith('--')]

if not args:
    print("Usage: python3 wipe-bucket.py <bucket-name> [--dry-run] [--only-unlock] [--verbose]")
    sys.exit(1)

BUCKET = args[0]

# Logging setup
log_level = logging.DEBUG if VERBOSE else logging.WARNING
logging.basicConfig(format='[%(asctime)s] %(message)s', level=log_level, datefmt='%H:%M:%S')

# Boto3 config with large connection pool
boto_config = Config(max_pool_connections=100)
s3 = boto3.client('s3', config=boto_config)

# Globals and locks
lock = threading.Lock()
total_processed = 0
unlocked_count = 0
deleted_count = 0
error_occurred = False
object_lock_enabled = False

CSV_FILE = 'object_versions.csv'

def detect_object_lock():
    global object_lock_enabled
    try:
        response = s3.get_object_lock_configuration(Bucket=BUCKET)
        object_lock_enabled = response.get('ObjectLockConfiguration', {}).get('ObjectLockEnabled') == 'Enabled'
    except s3.exceptions.ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code == 'ObjectLockConfigurationNotFoundError':
            object_lock_enabled = False
            return
        logging.error(f"Failed to read Object Lock configuration: {e.response.get('Error', {}).get('Message', str(e))}")
        raise

def mark_error(message):
    global error_occurred
    logging.error(message)
    with lock:
        error_occurred = True

def export_versions():
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Key', 'VersionId'])
        paginator = s3.get_paginator('list_object_versions')
        for page in paginator.paginate(Bucket=BUCKET):
            for v in page.get('Versions', []) + page.get('DeleteMarkers', []):
                writer.writerow([v['Key'], v['VersionId']])

def process_object(key, version_id):
    global total_processed, unlocked_count, deleted_count
    ident = f"{key}@{version_id}"

    if DRY_RUN:
        if VERBOSE:
            logging.debug(f"[dry-run] Would unlock and delete: {ident}")
        with lock:
            total_processed += 1
            unlocked_count += 1
            if not ONLY_UNLOCK:
                deleted_count += 1
        return

    if object_lock_enabled:
        try:
            s3.put_object_legal_hold(
                Bucket=BUCKET,
                Key=key,
                VersionId=version_id,
                LegalHold={'Status': 'OFF'}
            )
        except s3.exceptions.ClientError:
            pass  # No hold set or access denied silently ignored

        try:
            s3.put_object_retention(
                Bucket=BUCKET,
                Key=key,
                VersionId=version_id,
                Retention={
                    'Mode': 'GOVERNANCE',
                    'RetainUntilDate': (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat()
                },
                BypassGovernanceRetention=True
            )
            with lock:
                unlocked_count += 1
        except s3.exceptions.ClientError as e:
            if 'AccessDenied' in str(e):
                mark_error(f"AccessDenied on unlock: {ident}")
            else:
                mark_error(f"Unlock failed: {ident} — {e.response.get('Error', {}).get('Message', str(e))}")
            with lock:
                total_processed += 1
            return
    else:
        with lock:
            unlocked_count += 1

    if ONLY_UNLOCK:
        with lock:
            total_processed += 1
        if VERBOSE:
            logging.debug(f"Unlocked: {ident}")
        return

    try:
        delete_kwargs = {
            'Bucket': BUCKET,
            'Key': key,
            'VersionId': version_id
        }
        if object_lock_enabled:
            delete_kwargs['BypassGovernanceRetention'] = True
        s3.delete_object(**delete_kwargs)
        if VERBOSE:
            logging.debug(f"Deleted: {ident}")
        with lock:
            deleted_count += 1
            total_processed += 1
    except s3.exceptions.ClientError as e:
        mark_error(f"Delete failed: {ident} — {e.response.get('Error', {}).get('Message', str(e))}")
        with lock:
            total_processed += 1

def main():
    had_exception = False
    try:
        detect_object_lock()
        if object_lock_enabled:
            logging.info("Object Lock is enabled: governance bypass will be used.")
        else:
            logging.info("Object Lock is NOT enabled: retention and bypass headers will be skipped.")

        logging.info(f"Exporting object versions from bucket '{BUCKET}'...")
        export_versions()

        logging.info(f"Starting parallel unlock/delete with threads...")
        with open(CSV_FILE, newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            total = len(rows)

            with ThreadPoolExecutor(max_workers=50) as executor:
                futures = [executor.submit(process_object, row['Key'], row['VersionId']) for row in rows]

                if not VERBOSE:
                    import time
                    while any(not f.done() for f in futures):
                        with lock:
                            print(
                                f"\rProgress: {unlocked_count}/{deleted_count}/{total} "
                                f"(unlocked/deleted/total)", end='', flush=True
                            )
                        time.sleep(0.5)
                    print(f"\rProgress: {unlocked_count}/{deleted_count}/{total} (unlocked/deleted/total)")

                for future in futures:
                    future.result()

        logging.info(f"✅ Finished: {unlocked_count} unlocked, {deleted_count} deleted, out of {total} objects.")
    except Exception:
        had_exception = True
        raise
    finally:
        if not had_exception and not error_occurred:
            try:
                os.remove(CSV_FILE)
            except FileNotFoundError:
                pass

if __name__ == '__main__':
    main()
