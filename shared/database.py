from sqlalchemy import create_engine, String, Float, DateTime, Text, UniqueConstraint, ForeignKey
from sqlalchemy.orm import sessionmaker, mapped_column, Mapped, relationship, declarative_base
from typing import Optional, List
import uuid
from datetime import datetime
from .config import config
from datetime import datetime, timezone

DATABASE_URL = config.DATABASE_URL
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def create_str_primary_key():
    return mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

class LeadDB(Base):
    __tablename__ = "leads"
    
    id: Mapped[str] = create_str_primary_key()
    email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    note: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    insights: Mapped[List["InsightDB"]] = relationship("InsightDB", back_populates="lead")

class InsightDB(Base):
    __tablename__ = "insights"
    
    id: Mapped[str] = create_str_primary_key()
    lead_id: Mapped[str] = mapped_column(String, ForeignKey("leads.id"), nullable=False)  # ✅ Добавили ForeignKey
    intent: Mapped[str] = mapped_column(String, nullable=False)
    priority: Mapped[str] = mapped_column(String, nullable=False)
    next_action: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    tags: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_hash: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    lead: Mapped["LeadDB"] = relationship("LeadDB", back_populates="insights")
    
    __table_args__ = (
        UniqueConstraint('lead_id', 'content_hash', name='uq_lead_content'),
    )

class IdempotencyKeyDB(Base):
    __tablename__ = "idempotency_keys"
    
    key: Mapped[str] = mapped_column(String, primary_key=True)
    response_data: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    Base.metadata.create_all(bind=engine)