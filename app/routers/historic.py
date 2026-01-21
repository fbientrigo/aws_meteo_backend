from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field
from typing import List, Optional
import logging

from app.lib.historic.extract import extract_points

router = APIRouter()
logger = logging.getLogger(__name__)

class Point(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=360) # Allow 0..360 input gracefully

class HistoricRequest(BaseModel):
    points: Optional[List[Point]] = None
    polygon: Optional[List[Point]] = None
    units: Optional[str] = Field("C", pattern="^(C|K)$")
    start: Optional[str] = None
    end: Optional[str] = None

@router.post("/t2m")
async def get_historic_t2m(payload: HistoricRequest):
    """
    Get historic monthly temperature series for a list of points or a polygon (centroid).
    """
    try:
        pts = []
        
        # 1. Determine points source
        if payload.polygon and len(payload.polygon) > 0:
            # Calculate Centroid
            lats = [p.lat for p in payload.polygon]
            lons = [p.lon for p in payload.polygon]
            
            avg_lat = sum(lats) / len(lats)
            avg_lon = sum(lons) / len(lons)
            
            logger.info(f"Polygon provided. Calculated centroid: ({avg_lat}, {avg_lon})")
            pts = [{"lat": avg_lat, "lon": avg_lon}]
            
        elif payload.points:
            pts = [p.dict() for p in payload.points]
        else:
            raise ValueError("Must provide either 'points' or 'polygon'")

        # 2. Extract Data
        data = extract_points(
            points=pts,
            units=payload.units,
            # date_start=payload.start,
            # date_end=payload.end
        )
        
        # 3. Request requirement: "El output debe ser imprimirlo por la consola de DEBUG, los datos RAW obtenidos"
        import json
        # We try to pretty print it to stdout for the user to see in their terminal
        print("\n" + "="*40)
        print("DEBUG: RAW HISTORIC DATA OUTPUT")
        print("="*40)
        print(json.dumps(data, default=str, indent=2))
        print("="*40 + "\n")

        return {"data": data}
        
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        logger.error(f"Error in historic t2m: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error processing historic data.")
