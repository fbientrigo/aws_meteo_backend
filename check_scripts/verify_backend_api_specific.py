import requests
import sys
import time

BASE_URL = "http://localhost:8000"

def verify_api():
    print(f"Checking API at {BASE_URL}...")
    
    # Force specific run mentioned by user if available
    target_run = "2025111500"
    target_step = "072"
    
    print(f"\n--- Requesting Subset for {target_run}/{target_step} ---")
    
    params = {
        "lat_min": -56.0,
        "lat_max": -17.0,
        "lon_min": -76.0,
        "lon_max": -66.0
    }
    
    url = f"{BASE_URL}/sti/{target_run}/{target_step}/subset"
    print(f"GET {url} with params {params}")
    
    try:
        r = requests.get(url, params=params, timeout=30)
        print(f"Status Code: {r.status_code}")
        
        if r.status_code == 200:
            subset_data = r.json()
            n_sti = len(subset_data.get("sti", []))
            print(f"SUCCESS. Data Points: {n_sti}")
        else:
            print(f"FAILED Request.")
            print(f"Status: {r.status_code}")
            print("Response:", r.text)
            
    except Exception as e:
        print(f"Exception during request: {e}")

if __name__ == "__main__":
    verify_api()
