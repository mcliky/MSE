# mes_api/app/main.py
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime, timedelta
import os, httpx, uuid

from .db import Base, engine, SessionLocal
from .models import Forecast
from .schemas import ForecastCreate, ForecastResponse

# --- App (define before any decorators) ---
app = FastAPI(title="MES API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# --- DB bootstrap ---
Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -----------------------
# Health
# -----------------------
@app.get("/")
def health():
    return {"status": "MES OK"}

# -----------------------
# MES Forecast CRUD
# -----------------------
@app.get("/forecast/", response_model=List[ForecastResponse])
def list_forecast(db: Session = Depends(get_db)):
    return db.query(Forecast).all()

@app.get("/forecast/{forecast_id}", response_model=ForecastResponse)
def get_forecast_item(forecast_id: int, db: Session = Depends(get_db)):
    row = db.get(Forecast, forecast_id)
    if not row:
        raise HTTPException(status_code=404, detail="Forecast not found")
    return row

@app.post("/forecast/", response_model=ForecastResponse)
def create_forecast(item: ForecastCreate, db: Session = Depends(get_db)):
    f = Forecast(**item.dict())
    db.add(f); db.commit(); db.refresh(f)
    return f

@app.put("/forecast/{forecast_id}", response_model=ForecastResponse)
def update_forecast(forecast_id: int, item: ForecastCreate, db: Session = Depends(get_db)):
    row = db.get(Forecast, forecast_id)
    if not row:
        raise HTTPException(status_code=404, detail="Forecast not found")
    for k, v in item.dict(exclude_unset=True).items():
        setattr(row, k, v)
    db.commit(); db.refresh(row)
    return row

@app.delete("/forecast/{forecast_id}", status_code=204)
def delete_forecast(forecast_id: int, db: Session = Depends(get_db)):
    row = db.get(Forecast, forecast_id)
    if not row:
        raise HTTPException(status_code=404, detail="Forecast not found")
    db.delete(row); db.commit()
    return None

# -----------------------
# Planning (joins ERP + Catalog + MES)
# -----------------------
ERP_API_URL = os.getenv("ERP_API_URL", "http://host.docker.internal:8100")
CATALOG_API_URL = os.getenv("CATALOG_API_URL", "http://host.docker.internal:8200")

class ReorderCandidate(BaseModel):
    part_id: int
    part_name: str
    part_code: Optional[str] = None
    current_stock: int
    reorder_point: Optional[int] = None
    usage_rate_per_day: Optional[float] = None
    lead_time_days: Optional[int] = None
    forecasted_usage_window: int
    urgency: str
    reason: str
    # extras
    recommendedQuantity: int
    depletionDate: Optional[datetime] = None

@app.get("/planning/reorder-candidates", response_model=List[ReorderCandidate])
def reorder_candidates(
    horizon_days: int = Query(7, ge=1, le=90, description="Forecast window in days"),
    db: Session = Depends(get_db),
):
    try:
        with httpx.Client(timeout=15) as client:
            inv = client.get(f"{ERP_API_URL}/inventory/").json()
            parts = client.get(f"{CATALOG_API_URL}/parts/").json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream fetch failed: {e}")

    partcode_by_material = {
        p.get("material_id"): p.get("part_code")
        for p in parts if p.get("material_id")
    }

    usage_by_code: dict[str, int] = {}
    for f in db.query(Forecast).all():
        code = f.part_code
        usage_by_code[code] = usage_by_code.get(code, 0) + int(f.forecasted_usage or 0)

    out: List[ReorderCandidate] = []
    for m in inv:
        pid = int(m["part_id"])
        pname = m["part_name"]
        current = int(m.get("current_stock") or 0)
        rpoint = int(m.get("reorder_point") or 0)
        lead = int(m.get("lead_time_days") or 0)
        usage = float(m.get("usage_rate_per_day") or 0.0)
        minthr = m.get("min_threshold")
        maxthr = m.get("max_threshold")

        pcode = partcode_by_material.get(pid)
        window_usage = usage_by_code.get(pcode, 0)

        urgency, reason = "Low", "Sufficient stock vs. demand."
        if usage and lead and (current < usage * lead):
            urgency, reason = "Critical", "Stock may deplete before supplier lead time."
        elif rpoint and current < rpoint:
            urgency, reason = "High", "Stock below reorder point."
        elif window_usage > 0 and current <= (rpoint + window_usage):
            urgency, reason = "Medium", "Forecasted usage may push stock near reorder threshold."

        depletionDate = None
        if usage and usage > 0:
            days_left = current / usage
            depletionDate = datetime.utcnow() + timedelta(days=days_left)

        if isinstance(maxthr, int):
            target = maxthr
        elif rpoint or window_usage:
            target = rpoint + window_usage
        else:
            target = current
        recommended = max(int(target - current), 0)

        out.append(ReorderCandidate(
            part_id=pid, part_name=pname, part_code=pcode,
            current_stock=current, reorder_point=rpoint or None,
            usage_rate_per_day=usage or None, lead_time_days=lead or None,
            forecasted_usage_window=window_usage, urgency=urgency, reason=reason,
            recommendedQuantity=recommended, depletionDate=depletionDate
        ))

    rank = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    out.sort(key=lambda r: (rank.get(r.urgency, 9), -(r.forecasted_usage_window or 0)))
    return out

# -----------------------
# One-click PO (calls ERP)
# -----------------------
class CreatePORequest(BaseModel):
    part_id: int
    quantity: int
    urgency: Optional[str] = "High"
    reason: Optional[str] = None
    # guardrails
    lookback_hours: int = 24
    allow_duplicate: bool = False

class POResponse(BaseModel):
    id: int
    part_id: int
    quantity: int
    urgency: str
    reason: Optional[str] = None
    created_at: Optional[datetime] = None

def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # tolerate "Z" UTC suffix
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _erp_list_purchase_orders(client: httpx.Client) -> List[dict]:
    r = client.get(f"{ERP_API_URL}/purchase-orders/")
    r.raise_for_status()
    return r.json()

@app.post("/planning/create-po", response_model=POResponse)
def planning_create_po(payload: CreatePORequest):
    body = payload.dict()
    lookback_hours = body.pop("lookback_hours")
    allow_duplicate = body.pop("allow_duplicate")

    # Always send a correlation_id so ERP can enforce de-dupe
    body["correlation_id"] = f"mes-{uuid.uuid4().hex}"

    if not body.get("reason"):
        body["reason"] = "Created by MES planner"

    try:
        with httpx.Client(timeout=15) as client:
            # (Optional) client-side duplicate guard
            if not allow_duplicate and lookback_hours > 0:
                existing = _erp_list_purchase_orders(client)
                cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
                for po in existing:
                    created = _parse_dt(po.get("created_at"))
                    if po.get("part_id") == payload.part_id and created and created >= cutoff:
                        raise HTTPException(
                            status_code=409,
                            detail=f"PO {po.get('id')} already exists for part {payload.part_id} in the last {lookback_hours}h"
                        )

            # Also ask ERP to enforce the lookback window server-side
            qs = f"?lookback_hours={lookback_hours}" if lookback_hours > 0 else ""
            r = client.post(f"{ERP_API_URL}/purchase-orders/{qs}", json=body)
            if r.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"ERP error: {r.text}")
            return r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to create PO: {e}")
