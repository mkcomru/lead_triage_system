from fastapi import APIRouter, Depends, Header
from sqlalchemy.orm import Session

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.database import get_db
from shared.models import LeadRequest, Lead
from services.lead_service import LeadService

router = APIRouter(prefix="/leads", tags=["leads"])

@router.post("", response_model=Lead, status_code=201)
async def create_lead(
    lead_request: LeadRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    db: Session = Depends(get_db)
):
    """Создает новый лид с идемпотентностью"""
    lead_service = LeadService(db)
    return await lead_service.create_lead(lead_request, idempotency_key)

@router.get("/{lead_id}", response_model=Lead)
async def get_lead(lead_id: str, db: Session = Depends(get_db)):
    """Получает лид по ID"""
    lead_service = LeadService(db)
    return await lead_service.get_lead(lead_id)