from datetime import datetime
from .db import SessionLocal
from .models import Forecast

if __name__ == "__main__":
    db = SessionLocal()
    db.add(Forecast(part_code="P-001", forecasted_usage=30, job_id="J-789",
                    job_start_date=datetime(2025,8,3), job_end_date=datetime(2025,8,10)))
    db.add(Forecast(part_code="P-002", forecasted_usage=10, job_id="J-790",
                    job_start_date=datetime(2025,8,4), job_end_date=datetime(2025,8,8)))
    db.commit(); db.close()
    print("Seeded MES forecasts.")
