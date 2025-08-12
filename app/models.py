from sqlalchemy import Column, Integer, String, DateTime
from .db import Base

class Forecast(Base):
    __tablename__ = "forecasts"
    id = Column(Integer, primary_key=True, index=True)
    part_code = Column(String, index=True)
    forecasted_usage = Column(Integer)
    job_id = Column(String, nullable=True)
    job_start_date = Column(DateTime, nullable=True)
    job_end_date = Column(DateTime, nullable=True)
