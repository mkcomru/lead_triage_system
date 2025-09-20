from abc import ABC, abstractmethod
from typing import Dict, Optional
from ..models import InsightPayload

class LLMAdapter(ABC):
    @abstractmethod
    async def triage(self, note: str, context: Optional[Dict] = None) -> InsightPayload:
        """Анализирует заметку лида и возвращает структурированный инсайт"""
        pass