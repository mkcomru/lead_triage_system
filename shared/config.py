import os
from typing import Literal

class Config:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{PROJECT_ROOT}/database.sqlite")
    
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
    QUEUE_STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "lead_events")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "triage_workers")
    
    LLM_ADAPTER: Literal["rule_based", "openai_like"] = os.getenv("LLM_ADAPTER", "rule_based")

config = Config()