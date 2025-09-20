from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.database import get_db, InsightDB
from shared.models import Insight

router = APIRouter(prefix="/leads", tags=["insights"])

@router.get("/{lead_id}/insight", response_model=Insight)
async def get_lead_insight(lead_id: str, db: Session = Depends(get_db)):
    """Получает последний инсайт для лида"""
    
    insight_db = (
        db.query(InsightDB)
        .filter(InsightDB.lead_id == lead_id)
        .order_by(InsightDB.created_at.desc())
        .first()
    )
    
    if not insight_db:
        raise HTTPException(status_code=404, detail="Insight not found")
    
    tags = insight_db.tags.split(",") if insight_db.tags else []
    
    insight = Insight(
        id=insight_db.id,
        lead_id=insight_db.lead_id,
        intent=insight_db.intent,
        priority=insight_db.priority,
        next_action=insight_db.next_action,
        confidence=insight_db.confidence,
        tags=tags,
        created_at=insight_db.created_at
    )
    
    return insight