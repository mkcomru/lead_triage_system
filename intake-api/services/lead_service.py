import json
import uuid
from datetime import datetime
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.database import LeadDB, IdempotencyKeyDB
from shared.models import LeadRequest, Lead, QueueEvent
from shared.queue import queue
from shared.utils import generate_content_hash

class LeadService:
    def __init__(self, db: Session):
        self.db = db

    async def create_lead(self, lead_request: LeadRequest, idempotency_key: str) -> Lead:
        """Создает лид с проверкой идемпотентности"""
        
        print(f"Processing request with idempotency key: {idempotency_key}")
        
        existing_key = self.db.query(IdempotencyKeyDB).filter(
            IdempotencyKeyDB.key == idempotency_key
        ).first()
        
        if existing_key:
            print(f"Found existing idempotency key: {idempotency_key}")
            try:
                stored_data = json.loads(existing_key.response_data)
                stored_request = stored_data["request"]
                current_request = lead_request.model_dump()
                
                if stored_request == current_request:
                    print("Request matches - returning cached response")
                    return Lead(**stored_data["lead"])
                else:
                    print("Request differs - conflict!")
                    raise HTTPException(status_code=409, detail="Idempotency key conflict")
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing stored data: {e}")
                raise HTTPException(status_code=500, detail="Invalid stored idempotency data")
        
        print("Creating new lead...")
        
        lead_id = str(uuid.uuid4())
        lead_db = LeadDB(
            id=lead_id,
            email=lead_request.email,
            phone=lead_request.phone,
            name=lead_request.name,
            note=lead_request.note,
            source=lead_request.source,
            created_at=datetime.utcnow()
        )
        
        try:
            self.db.add(lead_db)
            self.db.flush()  
            
            lead_response = Lead.model_validate(lead_db)
            
            response_data = {
                "request": lead_request.model_dump(),
                "lead": lead_response.model_dump()
            }
            
            idempotency_record = IdempotencyKeyDB(
                key=idempotency_key,
                response_data=json.dumps(response_data, default=str),
                created_at=datetime.utcnow()
            )
            self.db.add(idempotency_record)
            
            content_hash = generate_content_hash(lead_request.note)
            event = QueueEvent(
                event_id=str(uuid.uuid4()),
                type="lead.created",
                lead_id=lead_id,
                content_hash=content_hash,
                occurred_at=datetime.utcnow()
            )
            
            await queue.publish_event(event)
            print(f"Published event for lead {lead_id}")
            
            self.db.commit()
            print(f"Successfully created lead {lead_id}")
            
            return lead_response
            
        except IntegrityError as e:
            self.db.rollback()
            print(f"Integrity error: {e}")
            raise HTTPException(status_code=400, detail="Failed to create lead")
        except Exception as e:
            self.db.rollback()
            print(f"Unexpected error: {e}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

    async def get_lead(self, lead_id: str) -> Lead:
        """Получает лид по ID"""
        lead_db = self.db.query(LeadDB).filter(LeadDB.id == lead_id).first()
        
        if not lead_db:
            raise HTTPException(status_code=404, detail="Lead not found")
        
        return Lead.model_validate(lead_db)