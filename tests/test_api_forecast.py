import sys
from unittest.mock import MagicMock

# Mock s3_helpers BEFORE importing app to avoid S3 connection/dependency
mock_s3 = MagicMock()
sys.modules["s3_helpers"] = mock_s3

# Configure the mock to return valid structures
mock_s3.list_runs.return_value = ["2025010100"]
mock_s3.list_steps.return_value = ["000"]

# Mock Dataset
mock_ds = MagicMock()
# When sel() is called, return a mock that has .values
mock_point = MagicMock()
mock_point.__contains__.side_effect = lambda key: key == "sti" # simulate "sti" in ds
mock_point.__getitem__.return_value.values = 300.0 # Mock value (Kelvin)
# Handle .sel()...
mock_ds.sel.return_value = mock_point
mock_s3.load_dataset.return_value = mock_ds

from fastapi.testclient import TestClient
from main import app
import pytest

client = TestClient(app)

def test_predict_endpoint_success():
    """
    Test the /forecast/predict endpoint with valid coordinates.
    Expects a 200 OK response with history and forecast data.
    """
    payload = {
        "latitude": 0.0,
        "longitude": 0.0
    }
    response = client.post("/forecast/predict", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    
    # Check structure
    assert "history" in data
    assert "forecast" in data
    
    # Check forecast length
    assert len(data["forecast"]) == 24
    
    # Check forecast fields
    first_step = data["forecast"][0]
    assert "mean" in first_step
    assert "p05" in first_step
    assert "p95" in first_step
    
    # Check logic (basic)
    assert first_step["p05"] < first_step["p95"]

def test_predict_endpoint_out_of_bounds():
    """
    Test the /forecast/predict endpoint with coordinates that might be out of bounds
    (depending on the mock grid).
    Our mock grid is 10x10 covering global, but let's try to find a point 
    that 'nearest' might pick up, or if we had strict bounds.
    Actually, xarray 'nearest' will always find something unless we restrict it.
    But let's ensure it returns 200 for a random location too.
    """
    payload = {
        "latitude": 45.0,
        "longitude": 180.0
    }
    response = client.post("/forecast/predict", json=payload)
    assert response.status_code == 200

def test_predict_endpoint_invalid_payload():
    """
    Test with missing fields.
    """
    payload = {
        "latitude": 0.0
        # Missing longitude
    }
    response = client.post("/forecast/predict", json=payload)
    assert response.status_code == 422 # Validation Error
