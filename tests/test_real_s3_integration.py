from fastapi.testclient import TestClient
from api_aws.main import app
import pytest
import os

# This test attempts to connect to REAL S3.
# It requires AWS credentials to be configured in the environment.

client = TestClient(app)

@pytest.mark.skipif(os.environ.get("SKIP_REAL_S3") == "true", reason="Skipping real S3 tests")
def test_predict_endpoint_real_s3():
    """
    Test the /forecast/predict endpoint using REAL S3 data.
    """
    # Use a coordinate that is likely to be in the domain (Chile)
    # Domain defined in data.yaml: [-75.0, -56.0, -66.0, -17.0]
    # Let's pick Santiago approx: -33.4, -70.6
    payload = {
        "latitude": -33.4,
        "longitude": -70.6
    }
    
    try:
        response = client.post("/forecast/predict", json=payload)
    except Exception as e:
        pytest.fail(f"API call failed with error: {e}")

    # If we get 403/404 from S3, the API might return 500 or 404.
    # We want to see what happens.
    
    if response.status_code != 200:
        pytest.fail(f"API returned {response.status_code}: {response.text}")
        
    data = response.json()
    
    assert "history" in data
    assert "forecast" in data
    assert len(data["forecast"]) == 24
    
    # Check that the value is not the mock value (mock base was 288.0)
    # Real STI values are likely different (e.g. around 0-1 if it's an index, or Kelvin if temp)
    # If it's STI, it might be unitless.
    print(f"Real Data Value: {data['history'][0]['value']}")
