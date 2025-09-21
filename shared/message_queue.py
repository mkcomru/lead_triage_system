import redis
import json
import uuid
from typing import Optional, Dict, Any
from datetime import datetime
from .config import config
from .models import QueueEvent

class RedisQueue:
    def __init__(self):
        self.redis = redis.from_url(config.REDIS_URL, decode_responses=True)
        self.stream_name = config.QUEUE_STREAM_NAME
        self.consumer_group = config.CONSUMER_GROUP
        
    async def publish_event(self, event: QueueEvent) -> str:
        """Публикует событие в Redis Stream"""
        event_data = event.model_dump()
        # Конвертируем datetime в ISO string для Redis
        event_data["occurred_at"] = event_data["occurred_at"].isoformat()
        
        message_id = self.redis.xadd(
            self.stream_name,
            event_data
        )
        return message_id
    
    def create_consumer_group(self, consumer_group: Optional[str] = None):
        """Создает consumer group если не существует"""
        group = consumer_group or self.consumer_group
        try:
            self.redis.xgroup_create(self.stream_name, group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
    
    def consume_events(self, consumer_name: str, count: int = 1, block: int = 1000):
        """Читает события из Redis Stream"""
        try:
            messages = self.redis.xreadgroup(
                self.consumer_group,
                consumer_name,
                {self.stream_name: ">"},
                count=count,
                block=block
            )
            
            events = []
            for stream, msgs in messages:
                for msg_id, fields in msgs:
                    # Конвертируем ISO string обратно в datetime
                    if "occurred_at" in fields:
                        fields["occurred_at"] = datetime.fromisoformat(fields["occurred_at"])
                    
                    event = QueueEvent(**fields)
                    events.append((msg_id, event))
            
            return events
        except Exception as e:
            print(f"Error consuming events: {e}")
            return []
    
    def ack_message(self, message_id: str):
        """Подтверждает обработку сообщения"""
        self.redis.xack(self.stream_name, self.consumer_group, message_id)

# Singleton instance
queue = RedisQueue()