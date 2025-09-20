from .base import LLMAdapter
from .rule_based import RuleBasedLLM

def get_llm_adapter() -> LLMAdapter:
    """Возвращает rule-based LLM адаптер"""
    return RuleBasedLLM()