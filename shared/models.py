# shared/models.py
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime
from uuid import UUID

class LeadRequest(BaseModel):
    """Входящий запрос на создание лида"""
    email: Optional[str] = None
    phone: Optional[str] = None
    name: Optional[str] = None
    note: str = Field(..., min_length=1)
    source: Optional[str] = None

class Lead(BaseModel):
    """Модель лида для API ответов"""
    id: str
    email: Optional[str] = None
    phone: Optional[str] = None
    name: Optional[str] = None
    note: str
    source: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True  # Для конвертации из SQLAlchemy объектов

class InsightPayload(BaseModel):
    """Результат работы LLM адаптера"""
    intent: Literal["buy", "support", "spam", "job", "other"]
    priority: Literal["P0", "P1", "P2", "P3"]
    next_action: Literal["call", "email", "ignore", "qualify"]
    confidence: float = Field(..., ge=0.0, le=1.0)
    tags: Optional[List[str]] = []

class Insight(BaseModel):
    """Модель инсайта для API ответов"""
    id: str
    lead_id: str
    intent: Literal["buy", "support", "spam", "job", "other"]
    priority: Literal["P0", "P1", "P2", "P3"]
    next_action: Literal["call", "email", "ignore", "qualify"]
    confidence: float = Field(..., ge=0.0, le=1.0)
    tags: Optional[List[str]] = None
    created_at: datetime

    class Config:
        from_attributes = True

class QueueEvent(BaseModel):
    """Событие в очереди"""
    event_id: str
    type: Literal["lead.created"] = "lead.created"
    lead_id: str
    content_hash: str
    occurred_at: datetime