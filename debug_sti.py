#!/usr/bin/env python3
import os
import sys
import time
import hashlib
import tempfile
import argparse
import logging
import concurrent.futures
import threading

import boto3
import xarray as xr

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("debug_sti")

BUCKET = "pangu-mvp-data"

def get_file_hash(path):
    sha256_hash = hashlib.sha256()
    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def diagnose_s3_object(bucket, key):
    logger.info(f"--- Diagnosing S3 Object: s3://{bucket}/{key} ---")
    s3 = boto3.client("s3")
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Size: {head['ContentLength']} bytes")
        logger.info(f"ETag: {head['ETag']}")
        logger.info(f"LastModified: {head['LastModified']}")
        return True
    except Exception as e:
        logger.error(f"S3 Head Object Failed: {e}")
        return False

def download_and_inspect(bucket, key):
    ts = int(time.time())
    local_path = os.path.join(tempfile.gettempdir(), f"debug_sti_{ts}.nc")
    
    logger.info(f"--- Downloading to {local_path} ---")
    s3 = boto3.client("s3")
    try:
        s3.download_file(bucket, key, local_path)
        size = os.path.getsize(local_path)
        logger.info(f"Downloaded successfully. Local Size: {size} bytes")
        
        checksum = get_file_hash(local_path)
        logger.info(f"SHA256: {checksum}")
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return None

    return local_path

def test_engines(local_path):
    logger.info("--- Testing Xarray Engines ---")
    engines = ["h5netcdf", "netcdf4", "scipy"]
    
    for eng in engines:
        logger.info(f"Trying engine='{eng}'...")
        try:
            ds = xr.open_dataset(local_path, engine=eng)
            logger.info(f"  [SUCCESS] Opened with {eng}")
            logger.info(f"  Dims: {dict(ds.dims)}")
            logger.info(f"  Data Vars: {list(ds.data_vars)}")
            logger.info(f"  Coords: {list(ds.coords)}")
            ds.close()
        except ImportError:
            logger.warning(f"  [SKIP] Engine {eng} not installed.")
        except Exception as e:
            logger.error(f"  [FAIL] Failed with {eng}: {e}")

def stress_test_local_read(local_path, concurrency=10):
    logger.info(f"--- Starting Local Read Stress Test (Threads={concurrency}) ---")
    
    def worker(idx):
        try:
            # Randomly pick an engine to stress test generic stability, or stick to h5netcdf
            ds = xr.open_dataset(local_path, engine="h5netcdf", cache=False)
            # Force read
            _ = list(ds.data_vars)
            ds.close()
            return "OK"
        except Exception as e:
            return f"FAIL: {e}"

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(worker, i): i for i in range(concurrency * 2)}
        
        results = []
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            
    failures = [r for r in results if r != "OK"]
    if failures:
        logger.error(f"Stress Test Failed! {len(failures)} errors.")
        logger.error(f"Sample error: {failures[0]}")
    else:
        logger.info("Stress Test Passed: All threads opened the file successfully.")

def main():
    parser = argparse.ArgumentParser(description="Debug STI NetCDF issues")
    parser.add_argument("--run", default="2025111500", help="Run ID")
    parser.add_argument("--step", default="072", help="Step ID")
    args = parser.parse_args()

    # Construct Key
    # Format: indices/sti/run=YYYYMMDDHH/step=XXX/sti_chile_run=YYYYMMDDHH_step=XXX.nc
    filename = f"sti_chile_run={args.run}_step={args.step}.nc"
    key = f"indices/sti/run={args.run}/step={args.step}/{filename}"

    if diagnose_s3_object(BUCKET, key):
        local_path = download_and_inspect(BUCKET, key)
        if local_path:
            test_engines(local_path)
            stress_test_local_read(local_path)
            
            # Cleanup
            try:
                os.remove(local_path)
                logger.info(f"Cleaned up {local_path}")
            except:
                pass

if __name__ == "__main__":
    main()
