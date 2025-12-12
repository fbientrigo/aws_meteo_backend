import sys
from unittest.mock import MagicMock
import unittest
import numpy as np
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Mock s3_helpers BEFORE importing app or main
mock_s3 = MagicMock()
sys.modules["s3_helpers"] = mock_s3

# Mock api_aws.routers.forecast to avoid importing lib/matplotlib
mock_router = MagicMock()
sys.modules["api_aws.routers"] = MagicMock()
sys.modules["api_aws.routers.forecast"] = mock_router
mock_router.router = MagicMock()

# Configure mocks
mock_s3.list_runs.return_value = ["2025010100"]
mock_s3.list_steps.return_value = ["000"]

# Import get_subset directly
from api_aws.main import get_subset

class TestSTIEndpoint(unittest.TestCase):
    def test_get_subset_flattened_structure(self):
        """
        Verify that get_subset returns flattened 1D arrays
        for latitudes, longitudes, and sti, and that they have equal length.
        """
        # 1. Setup Mock Dataset
        mock_ds = MagicMock()
        mock_sub = MagicMock()
        
        # Simulate a 2x2 grid
        # Lats: [-33.0, -33.25]
        # Lons: [-71.0, -70.75]
        # Expected output size: 4
        
        lats_input = np.array([-33.0, -33.25])
        lons_input = np.array([-71.0, -70.75])
        # Values as (2,2) matrix
        sti_values = np.array([[0.1, 0.2], [0.3, 0.4]])
        
        # Mock ds.data_vars to satisfy "if 'sti' not in ds.data_vars" check
        mock_ds.data_vars = ["sti"]

        mock_sub.__contains__.side_effect = lambda key: key in ["latitude", "longitude"]
        # Mock .values access for lat/lon/sti
        mock_sub.values = sti_values
        mock_sub.__getitem__.side_effect = lambda key: MagicMock(values=lats_input) if key == "latitude" else MagicMock(values=lons_input)
        
        # Mock ds['sti'].sel(...) -> returns mock_sub
        mock_ds.__getitem__.return_value.sel.return_value = mock_sub
        mock_s3.load_dataset.return_value = mock_ds

        # 2. Call Function directly
        # get_subset(run, step, lat_min, lat_max, lon_min, lon_max)
        data = get_subset(
            run="2025010100",
            step="000",
            lat_min=-34.0, 
            lat_max=-32.0,
            lon_min=-72.0, 
            lon_max=-70.0
        )
        
        # 3. Verify Response
        self.assertIn("latitudes", data)
        self.assertIn("longitudes", data)
        self.assertIn("sti", data)
        
        lats = data["latitudes"]
        lons = data["longitudes"]
        sti = data["sti"]
        
        # Check types (should be lists, not dicts or nesting)
        self.assertIsInstance(lats, list)
        self.assertIsInstance(lons, list)
        self.assertIsInstance(sti, list)
        
        # Check length
        expected_len = 4 # 2 * 2
        self.assertEqual(len(lats), expected_len)
        self.assertEqual(len(lons), expected_len)
        self.assertEqual(len(sti), expected_len)
        
        # Check values (flattened structure)
        # meshgrid with indexing='xy' (default for numpy)
        # For lats=[-33.0, -33.25], lons=[-71.0, -70.75]
        # lat_grid:
        # [[-33.0,  -33.0],
        #  [-33.25, -33.25]] -> flatten -> [-33.0, -33.0, -33.25, -33.25]
        # lon_grid:
        # [[-71.0,  -70.75],
        #  [-71.0,  -70.75]] -> flatten -> [-71.0, -70.75, -71.0, -70.75]
        # sti (row-major flatten): [0.1, 0.2, 0.3, 0.4]
        
        print(f"\nLats received: {lats}")
        print(f"Lons received: {lons}")
        print(f"STI received: {sti}")

        self.assertEqual(lats, [-33.0, -33.0, -33.25, -33.25])
        self.assertEqual(lons, [-71.0, -70.75, -71.0, -70.75])
        self.assertEqual(sti, [0.1, 0.2, 0.3, 0.4])

        # Clean up mock
        mock_ds.close.assert_called()

if __name__ == "__main__":
    unittest.main()
