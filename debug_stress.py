import concurrent.futures
import time
import os
import logging
from s3_helpers import load_dataset

# Configure logging to see what's happening
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')

def worker(run, step):
    try:
        ds = load_dataset(run, step)
        # Force a read to trigger lazy loading issues if any
        val = ds['sti'].values[0] 
        ds.close()
        return "OK"
    except Exception as e:
        return f"ERROR: {e}"

def run_stress_test():
    run = "2025111500"
    step = "072"
    
    # Clean up before starting to test download race too
    import tempfile
    run = "2025121400"
    step = "048"
    local_file = os.path.join(tempfile.gettempdir(), f"sti_{run}_{step}.nc")
    if os.path.exists(local_file):
        os.remove(local_file)
        print(f"Cleaned up {local_file}")

    print("--- Starting Stress Test (10 concurrent threads) ---")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Launch 20 tasks
        futures = [executor.submit(worker, run, step) for _ in range(20)]
        
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            print(f"Result: {res}")
            if "ERROR" in res:
                print("!!! CONCURRENCY FAILURE DETECTED !!!")

if __name__ == "__main__":
    run_stress_test()
