import requests
import sys
import time

BASE_URL = "http://localhost:8000"

def verify_api():
    print(f"Checking API at {BASE_URL}...")
    
    # Retry loop for health check (server might be starting)
    for i in range(5):
        try:
            r = requests.get(f"{BASE_URL}/health", timeout=2)
            if r.status_code == 200:
                print("Health Check: OK")
                break
        except requests.exceptions.ConnectionError:
            print("Server not ready, retrying...")
            time.sleep(1)
    else:
        print("Could not connect to backend.")
        sys.exit(1)

    # 2. List Runs
    print("\n--- Listing Runs ---")
    r = requests.get(f"{BASE_URL}/sti/runs")
    if r.status_code != 200:
        print(f"Failed to list runs: {r.status_code} - {r.text}")
        sys.exit(1)
        
    data = r.json()
    runs = data.get("runs", [])
    print(f"Found runs: {runs}")
    
    if not runs:
        print("No runs available to test.")
        sys.exit(0)
        
    last_run = runs[-1]
    print(f"Selected Run: {last_run}")
    
    # 3. List Steps
    print(f"\n--- Listing Steps for {last_run} ---")
    r = requests.get(f"{BASE_URL}/sti/{last_run}/steps")
    if r.status_code != 200:
        print(f"Failed to list steps: {r.status_code} - {r.text}")
        sys.exit(1)
        
    steps = r.json().get("steps", [])
    print(f"Found steps: {steps}")
    
    if not steps:
        print("No steps available.")
        sys.exit(0)
        
    first_step = steps[0]
    print(f"Selected Step: {first_step}")
    
    # 4. Request Subset
    print(f"\n--- Requesting Subset for {last_run}/{first_step} ---")
    # Using bounds that cover Chile roughly
    params = {
        "lat_min": -56.0,
        "lat_max": -17.0,
        "lon_min": -76.0,
        "lon_max": -66.0
    }
    
    url = f"{BASE_URL}/sti/{last_run}/{first_step}/subset"
    print(f"GET {url} with params {params}")
    
    try:
        r = requests.get(url, params=params, timeout=30)
        print(f"Status Code: {r.status_code}")
        
        if r.status_code == 200:
            subset_data = r.json()
            keys = list(subset_data.keys())
            print("Subset Response Keys:", keys)
            
            # Check data integrity
            n_lats = len(subset_data.get("latitudes", []))
            n_lons = len(subset_data.get("longitudes", []))
            n_sti = len(subset_data.get("sti", []))
            
            print(f"Data Points: {n_sti}")
            print(f"Latitudes: {n_lats}, Longitudes: {n_lons}")
            
            if n_sti == n_lats and n_sti == n_lons:
                print("SUCCESS: Data arrays are aligned.")
            else:
                print("ERROR: Data arrays invalid lengths.")
        else:
            print("FAILED Request.")
            print("Response length:", len(r.text))
            
    except Exception as e:
        print(f"Exception during request: {e}")

if __name__ == "__main__":
    verify_api()
