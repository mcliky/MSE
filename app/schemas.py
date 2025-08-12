from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime

class ForecastCreate(BaseModel):
    part_code: str
    forecasted_usage: int
    job_id: Optional[str] = None
    job_start_date: Optional[datetime] = None
    job_end_date: Optional[datetime] = None

class ForecastResponse(ForecastCreate):
    id: int
    model_config = ConfigDict(from_attributes=True)
