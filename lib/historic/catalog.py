from pathlib import Path
from typing import List

# Base directory relative to this file
# Assumes structure: .../lib/historic/catalog.py -> .../historic/
HISTORIC_DIR = Path(__file__).resolve().parent.parent.parent / "historic"

# Defined filenames
FILE_1991_2024 = "ERA5_T2M_monthly_1991_2024_chile.nc"
FILE_1991_2025 = "ERA5_T2M_monthly_1991_2025_chile.nc"
FILE_2025_UPDATE = "ERA5_T2M_monthly_2025_01_10_chile.nc"

def get_ordered_sources() -> List[Path]:
    """
    Returns the list of NetCDF file paths to load, in order of 'layering'.
    
    Strategy:
    1. Base: Prefer 1991-2025 if exists, else 1991-2024.
    2. Update: If 2025 update exists, add it to the list.
    
    The loader is responsible for merging these and deduplicating time.
    """
    sources = []
    
    path_91_25 = HISTORIC_DIR / FILE_1991_2025
    path_91_24 = HISTORIC_DIR / FILE_1991_2024
    path_25_up = HISTORIC_DIR / FILE_2025_UPDATE
    
    # 1. Choose Base
    if path_91_25.exists():
        sources.append(path_91_25)
    elif path_91_24.exists():
        sources.append(path_91_24)
    else:
        # Fallback: if no base exists, we might return empty or just the update if it exists.
        # But for strictly historic series, we surely expect a base.
        # We'll just continue and see if update exists.
        pass

    # 2. Add Update if exists
    if path_25_up.exists():
        sources.append(path_25_up)
        
    return sources
