import os
from typing import Literal


class Config:
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./database.sqlite")
    
    # Redis/Queue
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
    QUEUE_STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "lead_events")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "triage_workers")
    
    # LLM
    LLM_ADAPTER: Literal["rule_based", "openai_like"] = os.getenv("LLM_ADAPTER", "rule_based")


config = Config()