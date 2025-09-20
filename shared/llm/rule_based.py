import re
from typing import Dict, Optional
from .base import LLMAdapter
from ..models import InsightPayload

class RuleBasedLLM(LLMAdapter):
    async def triage(self, note: str, context: Optional[Dict] = None) -> InsightPayload:
        note_lower = note.lower()
        
        # Определяем intent
        intent = self._detect_intent(note_lower)
        
        # Определяем priority
        priority = self._detect_priority(note_lower, intent)
        
        # Определяем next_action
        next_action = self._detect_next_action(intent, priority)
        
        # Определяем confidence (базовая логика)
        confidence = self._calculate_confidence(note_lower, intent)
        
        # Извлекаем теги
        tags = self._extract_tags(note_lower)
        
        return InsightPayload(
            intent=intent,
            priority=priority,
            next_action=next_action,
            confidence=confidence,
            tags=tags
        )
    
    def _detect_intent(self, note: str) -> str:
        # Паттерны для определения намерений
        buy_patterns = [
            r'\b(price|pricing|стоимость|купить|счёт|invoice|purchase|buy|order)\b',
            r'\b(trial|discount|скидка|пробная|демо)\b'
        ]
        
        support_patterns = [
            r'\b(support|поддержка|помощь|не работает|bug|error|проблема|issue)\b',
            r'\b(сломан|broken|fix|repair|troubleshoot)\b'
        ]
        
        job_patterns = [
            r'\b(вакансия|резюме|собеседование|job|career|vacancy|cv|resume|interview)\b',
            r'\b(работа|position|hiring|recruit)\b'
        ]
        
        spam_patterns = [
            r'\b(spam|реклама|продам|купим|массовая|рассылка)\b',
            r'\b(win|won|lottery|prize|congratulations)\b'
        ]
        
        if any(re.search(pattern, note) for pattern in buy_patterns):
            return "buy"
        elif any(re.search(pattern, note) for pattern in support_patterns):
            return "support"
        elif any(re.search(pattern, note) for pattern in job_patterns):
            return "job"
        elif any(re.search(pattern, note) for pattern in spam_patterns):
            return "spam"
        else:
            return "other"
    
    def _detect_priority(self, note: str, intent: str) -> str:
        urgent_patterns = [
            r'\b(urgent|срочно|asap|emergency|critical|немедленно)\b',
            r'\b(сегодня|today|now|right now|сейчас)\b'
        ]
        
        high_patterns = [
            r'\b(next week|на следующей неделе|завтра|tomorrow)\b',
            r'\b(important|важно|приоритет)\b'
        ]
        
        if intent == "spam":
            return "P3"
        elif any(re.search(pattern, note) for pattern in urgent_patterns):
            return "P0"
        elif any(re.search(pattern, note) for pattern in high_patterns):
            return "P1"
        elif intent == "buy":
            return "P1"
        elif intent == "support":
            return "P2"
        else:
            return "P3"
    
    def _detect_next_action(self, intent: str, priority: str) -> str:
        if intent == "spam":
            return "ignore"
        elif intent == "buy" and priority in ["P0", "P1"]:
            return "call"
        elif intent == "support":
            return "email"
        elif intent == "job":
            return "email"
        elif priority == "P0":
            return "call"
        else:
            return "qualify"
    
    def _calculate_confidence(self, note: str, intent: str) -> float:
        # Простая логика расчета уверенности
        base_confidence = 0.7
        
        # Увеличиваем уверенность для четких паттернов
        clear_patterns = {
            "buy": [r'\b(price|pricing|купить|invoice)\b'],
            "support": [r'\b(support|не работает|bug)\b'],
            "job": [r'\b(резюме|interview|вакансия)\b'],
            "spam": [r'\b(spam|реклама|win)\b']
        }
        
        if intent in clear_patterns:
            pattern_matches = sum(1 for pattern in clear_patterns[intent] 
                                if re.search(pattern, note))
            if pattern_matches > 0:
                base_confidence = min(0.95, base_confidence + 0.1 * pattern_matches)
        
        return round(base_confidence, 2)
    
    def _extract_tags(self, note: str) -> list:
        tags = []
        
        # Определяем размер компании
        if re.search(r'\b(\d+)\s*(seat|license|user|пользователь)', note):
            tags.append("enterprise")
        elif re.search(r'\b(small|startup|начинающ)', note):
            tags.append("small_business")
        
        # Определяем источник urgency
        if re.search(r'\b(urgent|срочно|asap)', note):
            tags.append("urgent")
        
        # Определяем технические термины
        if re.search(r'\b(api|integration|техническ)', note):
            tags.append("technical")
        
        return tags