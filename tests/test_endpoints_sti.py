import sys
import unittest
from unittest.mock import MagicMock, patch
import numpy as np
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

# Import imported get_subset. Since we are patching "app.main.load_dataset", 
# we don't worry about previous imports of s3_helpers.
from app.main import get_subset

class TestSTIEndpoint(unittest.TestCase):
    @patch("app.main.load_dataset")
    def test_get_subset_flattened_structure(self, mock_load_dataset):
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
        lats_input = np.array([-33.0, -33.25])
        lons_input = np.array([-71.0, -70.75])
        sti_values = np.array([[0.1, 0.2], [0.3, 0.4]])
        
        mock_ds.data_vars = ["sti"]

        # Setup mock_sub behavior
        mock_sub.__contains__.side_effect = lambda key: key in ["latitude", "longitude"]
        mock_sub.values = sti_values
        mock_sub.__getitem__.side_effect = lambda key: MagicMock(values=lats_input) if key == "latitude" else MagicMock(values=lons_input)
        
        # Mock ds['sti'] to return mock_sub
        mock_ds.__getitem__.return_value = mock_sub
        
        # Configure the patch
        # Note: get_subset calls load_dataset(run, step)
        mock_load_dataset.return_value = mock_ds
    
        # 2. Call Function directly
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
        
        # Check types
        self.assertIsInstance(lats, list)
        self.assertIsInstance(lons, list)
        self.assertIsInstance(sti, list)
        
        # Check length
        expected_len = 4 # 2 * 2
        self.assertEqual(len(lats), expected_len)
        self.assertEqual(len(lons), expected_len)
        self.assertEqual(len(sti), expected_len)
        
        # Check values
        self.assertEqual(lats, [-33.0, -33.0, -33.25, -33.25])
        self.assertEqual(lons, [-71.0, -70.75, -71.0, -70.75])
        self.assertEqual(sti, [0.1, 0.2, 0.3, 0.4])

        # Verify load_dataset was called
        mock_load_dataset.assert_called_with("2025010100", "000")
        mock_ds.close.assert_called()

if __name__ == "__main__":
    unittest.main()
