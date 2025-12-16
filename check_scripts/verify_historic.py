import sys
import os
import requests
import json
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("verify_historic")

URL = "http://localhost:8000/historic/t2m"

def test_endpoint():
    payload = {
        "points": [
            {"lat": -33.45, "lon": -70.66}, # Santiago
             {"lat": -53.0, "lon": -70.0}   # Magallanes
        ],
        "units": "C"
    }
    
    logger.info(f"Sending POST to {URL} with payload: {json.dumps(payload)}")
    
    try:
        resp = requests.post(URL, json=payload, timeout=30)
        logger.info(f"Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            logger.info("Response received successfully")
            
            # Basic validation
            items = data.get("data", [])
            logger.info(f"Received {len(items)} items")
            
            for item in items:
                logger.info(f"Point: {item.get('lat_requested')}, {item.get('lon_requested')}")
                if "error" in item:
                    logger.error(f"  Error: {item['error']}")
                else:
                    series = item.get("series", [])
                    logger.info(f"  Series length: {len(series)}")
                    if series:
                        logger.info(f"  First: {series[0]}")
                        logger.info(f"  Last:  {series[-1]}")
                        logger.info(f"  Units: {item.get('units')}")
        else:
            logger.error(f"Failed response: {resp.text}")
            
    except Exception as e:
        logger.error(f"Request failed: {e}")

if __name__ == "__main__":
    test_endpoint()
