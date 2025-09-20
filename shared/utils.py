import hashlib
from datetime import datetime

def generate_content_hash(content: str) -> str:
    """Генерирует SHA-256 хеш для контента"""
    return hashlib.sha256(content.encode()).hexdigest()

def mask_email(email: str) -> str:
    """Маскирует email для логов"""
    if not email or '@' not in email:
        return email
    
    local, domain = email.split('@', 1)
    if len(local) <= 2:
        masked_local = '*' * len(local)
    else:
        masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
    
    return f"{masked_local}@{domain}"

def mask_phone(phone: str) -> str:
    """Маскирует телефон для логов"""
    if not phone or len(phone) < 4:
        return '*' * len(phone) if phone else ''
    
    return phone[:2] + '*' * (len(phone) - 4) + phone[-2:]