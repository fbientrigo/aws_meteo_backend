from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
import sys
import os

# Ensure root is in path so we can import app
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Patching s3_helpers before importing main
# We need to mock .s3_helpers because relative import in main points to app.s3_helpers
# AND main itself does `from .s3_helpers import ...`
# The easiest way is to mock `app.s3_helpers` if possible, but `app` is a package.
# Let's import main and then patch.

# But wait, `from main import app` will trigger imports.
# If `app/main.py` does `from .s3_helpers`, it tries to import `s3_helpers.py` from `app/`.
# `s3_helpers.py` might try to connect to S3 via global code:
# `s3_fs = fsspec.filesystem("s3")`
# We should probably mock `fsspec` and `boto3` to avoid real connection attempts during import.

with patch.dict(sys.modules, {"fsspec": MagicMock(), "boto3": MagicMock()}):
    # We import app.main
    # But since we are running from root, and app dir is a package...
    # We need to make sure `app` is imported as a module.
    # The `sys.path` append above adds `../app`? No, `..` adds root.
    # Root contains `app/`. So `import app.main` is valid.
    from app.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@patch("app.main.list_runs")
def test_get_runs(mock_list_runs):
    mock_list_runs.return_value = ["2025010100", "2025010200"]
    response = client.get("/sti/runs")
    assert response.status_code == 200
    assert response.json() == {"runs": ["2025010100", "2025010200"]}

if __name__ == "__main__":
    # verification run
    test_health()
    print("Health check passed!")
