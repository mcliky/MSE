# mse_api/app/main.py — MES API (FastAPI)
# Stage 4 / Step 1: reads ERP /inventory (now includes part_code),
# stores forecasts, computes reorder candidates, and creates POs in ERP.

import os
import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Dict

import httpx
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, conint
from sqlalchemy import Column, Integer, String, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

APP_TITLE = "MES API (Stage 4 / Step 1)"
ERP_API_URL = os.getenv("ERP_API_URL", "http://erp:8100")   # set to http://localhost:8100 if running locally outside compose
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/mes.db")

# SQLite threading flag
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, future=True, echo=False, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI(title=APP_TITLE, version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------ Models ------------------
class Forecast(Base):
    __tablename__ = "forecasts"
    id = Column(Integer, primary_key=True, index=True)
    part_code = Column(String, nullable=False, index=True)
    forecasted_usage = Column(Integer, nullable=False, default=0)
    job_id = Column(String, nullable=True)
    job_start_date = Column(DateTime, nullable=True)
    job_end_date = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# ------------------ DB dep ------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ------------------ Schemas ------------------
class ForecastIn(BaseModel):
    part_code: str = Field(..., min_length=1)
    forecasted_usage: conint(ge=0)
    job_id: Optional[str] = None
    job_start_date: Optional[datetime] = None
    job_end_date: Optional[datetime] = None

class ForecastOut(ForecastIn):
    id: int

class ReorderCandidate(BaseModel):
    part_id: int
    part_code: Optional[str]
    part_name: str
    current_stock: int
    reorder_point: int
    lead_time_days: int
    usage_rate_per_day: float
    window_demand: int
    urgency: str
    recommendedQuantity: int
    depletionDate: str

class CreatePORequest(BaseModel):
    part_id: int
    quantity: conint(gt=0)
    urgency: Optional[str] = None
    reason: Optional[str] = "Created by MES planner"
    correlation_id: Optional[str] = None
    lookback_hours: int = 24
    allow_duplicate: bool = False

# ------------------ Forecast CRUD ------------------
@app.get("/forecast/", response_model=List[ForecastOut], tags=["Forecast"])
def list_forecast(db: Session = Depends(get_db)):
    rows = db.query(Forecast).order_by(Forecast.id).all()
    return [ForecastOut(**r.__dict__) for r in rows]

@app.post("/forecast/", response_model=ForecastOut, status_code=201, tags=["Forecast"])
def create_forecast(payload: ForecastIn, db: Session = Depends(get_db)):
    row = Forecast(**payload.dict())
    db.add(row); db.commit(); db.refresh(row)
    return ForecastOut(**row.__dict__)

@app.put("/forecast/{fid}", response_model=ForecastOut, tags=["Forecast"])
def update_forecast(fid: int, payload: ForecastIn, db: Session = Depends(get_db)):
    row = db.get(Forecast, fid)
    if not row:
        raise HTTPException(404, "forecast not found")
    for k, v in payload.dict().items():
        setattr(row, k, v)
    db.commit(); db.refresh(row)
    return ForecastOut(**row.__dict__)

@app.delete("/forecast/{fid}", status_code=204, tags=["Forecast"])
def delete_forecast(fid: int, db: Session = Depends(get_db)):
    row = db.get(Forecast, fid)
    if not row:
        return
    db.delete(row); db.commit()

# ------------------ Planning ------------------
def _urgency(stock: int, usage_per_day: float, lead_time_days: int, reorder_point: int, window: int) -> str:
    if stock < usage_per_day * lead_time_days:
        return "Critical"
    if stock < reorder_point:
        return "High"
    if stock <= reorder_point + window:
        return "Medium"
    return "Low"

def _recommend_qty(stock: int, reorder_point: int, window: int, max_threshold: Optional[int]) -> int:
    target = max_threshold if max_threshold is not None else (reorder_point + window)
    return max(0, int(target) - int(stock))

@app.get("/planning/reorder-candidates", response_model=List[ReorderCandidate], tags=["Planning"])
def reorder_candidates(
    horizon_days: int = Query(7, ge=1, le=90),   # reserved for future per-day enhancement
    db: Session = Depends(get_db),
):
    # 1) Fetch ERP inventory (it includes part_code now)
    try:
        with httpx.Client(timeout=20) as client:
            r = client.get(f"{ERP_API_URL}/inventory/")
            if r.status_code != 200:
                raise HTTPException(502, f"ERP inventory fetch failed: HTTP {r.status_code} {r.text}")
            inv = r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"ERP inventory fetch failed: {e}")

    # 2) Sum forecast by part_code
    usage_by_code: Dict[str, int] = {}
    for f in db.query(Forecast).all():
        code = f.part_code
        usage_by_code[code] = usage_by_code.get(code, 0) + int(f.forecasted_usage or 0)

    # 3) Merge & compute
    out: List[ReorderCandidate] = []
    for m in inv:
        code = m.get("part_code")
        if not code:
            # If a material lacks a part_code, skip; forecasts are keyed by code.
            continue

        window = usage_by_code.get(code, 0)
        stock = int(m.get("current_stock") or 0)
        rp    = int(m.get("reorder_point") or 0)
        ltd   = int(m.get("lead_time_days") or 0)
        upd   = float(m.get("usage_rate_per_day") or 0.0)
        maxthr = m.get("max_threshold")

        urg = _urgency(stock, upd, ltd, rp, window)
        rec = _recommend_qty(stock, rp, window, maxthr)

        # rough depletion date (protect divide-by-zero)
        per_day = max(1e-9, upd)
        days_left = stock / per_day
        depl = (datetime.utcnow() + timedelta(days=days_left)).date().isoformat()

        out.append(ReorderCandidate(
            part_id=int(m["part_id"]),
            part_code=code,
            part_name=m["part_name"],
            current_stock=stock,
            reorder_point=rp,
            lead_time_days=ltd,
            usage_rate_per_day=upd,
            window_demand=window,
            urgency=urg,
            recommendedQuantity=rec,
            depletionDate=depl,
        ))

    # Sort by urgency and recommended quantity
    order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    out.sort(key=lambda x: (order.get(x.urgency, 9), -x.recommendedQuantity))
    return out

# ------------------ Create PO in ERP ------------------
@app.post("/planning/create-po", tags=["Planning"])
def create_po(req: CreatePORequest):
    corr = req.correlation_id or f"mes-{uuid.uuid4().hex[:8]}"
    params = {"lookback_hours": str(req.lookback_hours)}
    body = {
        "part_id": req.part_id,
        "quantity": req.quantity,
        "urgency": req.urgency,
        "reason": req.reason,
        "correlation_id": corr,
    }
    try:
        with httpx.Client(timeout=20) as client:
            r = client.post(f"{ERP_API_URL}/purchase-orders/", params=params, json=body)
            if r.status_code == 409 and not req.allow_duplicate:
                # bubble up duplicate guard feedback from ERP
                raise HTTPException(409, r.text)
            r.raise_for_status()
            return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"ERP PO create failed: {e}")

# ------------------ Ops & Seeds ------------------
@app.get("/_info", tags=["Ops"])
def info():
    return {
        "erp_api_url": ERP_API_URL,
        "database_url": DATABASE_URL,
        "tables": ["forecasts"],
        "now_utc": datetime.utcnow().isoformat() + "Z",
    }

@app.get("/health", tags=["Ops"])
def health():
    return {"ok": True}

@app.post("/_seed", tags=["Ops"])
def seed(db: Session = Depends(get_db)):
    rows = [
        Forecast(part_code="P-001", forecasted_usage=50, job_id="J-1001"),
        Forecast(part_code="P-001", forecasted_usage=25, job_id="J-1002"),
        Forecast(part_code="P-002", forecasted_usage=10, job_id="J-1003"),
        Forecast(part_code="P-003", forecasted_usage=12, job_id="J-1004"),
    ]
    for r in rows:
        db.add(r)
    db.commit()
    return {"ok": True, "seeded": len(rows)}
