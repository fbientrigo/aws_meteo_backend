
import sys
from unittest.mock import MagicMock

# 1. Mock S3 Dependencies hard
# This prevents 's3_helpers' from trying to import boto3/fsspec if they are missing,
# or from trying to connect to AWS.
mock_s3 = MagicMock()
sys.modules["s3_helpers"] = mock_s3
mock_s3.list_runs.return_value = ["2025010100"]

# 2. Mock lib if necessary (though it should be there now)
# But let's assume we want to test that 'lib' IS importable.
# So we don't mock 'lib' unless we suspect it invokes heavy things on import.

from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_health_check():
    """
    Verifies that the API starts and responds to /health.
    This proves that all imports in main.py (including lib) succeeded.
    """
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_list_runs_mocked():
    """
    Verifies that the API can call the mocked s3_helpers.
    """
    response = client.get("/sti/runs")
    assert response.status_code == 200
    assert "runs" in response.json()
    assert response.json()["runs"] == ["2025010100"]

if __name__ == "__main__":
    # Allow running this script directly
    try:
        test_health_check()
        test_list_runs_mocked()
        print("✅ Standalone Smoke Test Passed!")
    except Exception as e:
        print(f"❌ Test Failed: {e}")
        sys.exit(1)
