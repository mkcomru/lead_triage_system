import asyncio
import uuid
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from shared.database import engine, LeadDB, InsightDB
from shared.message_queue import queue
from shared.llm import get_llm_adapter

class TriageWorker:
    def __init__(self):
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        self.llm_adapter = get_llm_adapter()
        self.consumer_name = f"worker-{uuid.uuid4().hex[:8]}"
        
    async def run(self):
        """Основной цикл обработки событий"""
        print(f"Worker {self.consumer_name} started")
        
        while True:
            try:
                events = queue.consume_events(
                    consumer_name=self.consumer_name,
                    count=1,
                    block=1000  
                )
                
                for message_id, event in events:
                    await self.process_event(message_id, event)
                    
            except Exception as e:
                print(f"Error in worker loop: {e}")
                await asyncio.sleep(1)
    
    async def process_event(self, message_id: str, event):
        """Обрабатывает одно событие"""
        print(f"Processing event {event.event_id} for lead {event.lead_id}")
        
        db = self.SessionLocal()
        try:
            lead = db.query(LeadDB).filter(LeadDB.id == event.lead_id).first()
            if not lead:
                print(f"Lead {event.lead_id} not found")
                queue.ack_message(message_id)
                return
            
            existing_insight = db.query(InsightDB).filter(
                InsightDB.lead_id == event.lead_id,
                InsightDB.content_hash == event.content_hash
            ).first()
            
            if existing_insight:
                print(f"Insight already exists for lead {event.lead_id} with hash {event.content_hash}")
                queue.ack_message(message_id)
                return
            
            insight_payload = await self.llm_adapter.triage(lead.note)
            
            insight = InsightDB(
                id=str(uuid.uuid4()),
                lead_id=event.lead_id,
                intent=insight_payload.intent,
                priority=insight_payload.priority,
                next_action=insight_payload.next_action,
                confidence=insight_payload.confidence,
                tags=",".join(insight_payload.tags) if insight_payload.tags else None,
                content_hash=event.content_hash,
                created_at=datetime.utcnow()
            )
            
            db.add(insight)
            db.commit()
            
            print(f"Created insight {insight.id} for lead {event.lead_id}")
            
            queue.ack_message(message_id)
            
        except IntegrityError:
            db.rollback()
            print(f"Duplicate insight for lead {event.lead_id}, skipping")
            queue.ack_message(message_id)
            
        except Exception as e:
            db.rollback()
            print(f"Error processing event {event.event_id}: {e}")
            
        finally:
            db.close()