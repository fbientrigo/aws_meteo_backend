from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field
from typing import List, Optional
import logging

from lib.historic.extract import extract_points

router = APIRouter()
logger = logging.getLogger(__name__)

class Point(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=360) # Allow 0..360 input gracefully

class HistoricRequest(BaseModel):
    points: List[Point]
    units: Optional[str] = Field("C", pattern="^(C|K)$")
    start: Optional[str] = None
    end: Optional[str] = None

@router.post("/t2m")
async def get_historic_t2m(payload: HistoricRequest):
    """
    Get historic monthly temperature series for a list of points.
    """
    try:
        # Convert pydantic models to dicts for the library function
        pts = [p.dict() for p in payload.points]
        
        data = extract_points(
            points=pts,
            units=payload.units,
            # date_start=payload.start,
            # date_end=payload.end
        )
        return {"data": data}
        
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        logger.error(f"Error in historic t2m: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error processing historic data.")
