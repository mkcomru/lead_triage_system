# Lead Triage System 🎯

Автоматизированная система для сортировки и анализа потенциальных клиентов (лидов) с микросервисной архитектурой.

## Удобнее всего проверять в Postman, что я и сделал, наглядно можно увидеть что возвращает апи, статусы ответов, и как себя ведет код

## 📋 Что реализовано

### ✅ Основная функциональность
- **Прием лидов** через REST API с валидацией данных
- **Автоматический анализ** намерений и приоритизация
- **Асинхронная обработка** через очереди Redis
- **Микросервисная архитектура** (3 сервиса + Redis)

### ✅ Надежность и безопасность
- **Идемпотентность запросов** - дублирующие запросы с одним `Idempotency-Key` возвращают тот же результат
- **Дедупликация событий** - предотвращение повторной обработки одинаковых событий в очереди
- **Валидация данных** через Pydantic модели
- **Обработка ошибок** с retry механизмами

### ✅ Тестирование
- **Unit тесты** для каждого компонента
- **Integration тесты** для взаимодействия сервисов  
- **E2E тесты** полного цикла обработки
- **Тесты идемпотентности** - проверка повторных запросов
- **Тесты дедупликации** - предотвращение дубликатов в очереди

## 🔄 Идемпотентность

### Где реализована
- **Intake API**: Header `Idempotency-Key` для POST `/api/leads`
- **База данных**: Уникальные индексы на `idempotency_key` и `email`
- **Логика**: Повторные запросы возвращают существующий результат

### Как работает
```bash
# Первый запрос
curl -X POST http://localhost:8000/api/leads \
  -H "Idempotency-Key: unique-123" \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "name": "Test User"}'
# Ответ: 201 Created

# Повторный запрос с тем же ключом
curl -X POST http://localhost:8000/api/leads \
  -H "Idempotency-Key: unique-123" \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "name": "Test User"}'
# Ответ: 200 OK (тот же результат)
```

### Проверено тестами
- `tests/test_idempotency.py` - детальные тесты идемпотентности
- `tests/test_e2e.py::test_idempotency_same_request` - E2E проверка

## 🚫 Дедупликация событий

### Где реализована
- **Triage Worker**: Проверка `content_hash` перед обработкой
- **База данных**: Таблица `insights` с полем `content_hash`
- **Алгоритм**: SHA256 хеш контента события

## 🗄️ Схема базы данных

### Структура таблиц
```sql
-- Таблица лидов
CREATE TABLE leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    name TEXT,
    phone TEXT,
    note TEXT,
    source TEXT,
    created_at TEXT,
    idempotency_key TEXT UNIQUE
);

-- Таблица анализа (инсайтов)
CREATE TABLE insights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id INTEGER,
    intent TEXT,
    priority TEXT,
    next_action TEXT,
    confidence REAL,
    content_hash TEXT,          -- Для дедупликации
    created_at TEXT,
    FOREIGN KEY (lead_id) REFERENCES leads (id)
);
```

### Проверка схемы
- **Автоматические тесты**: `tests/test_duplicate_queue.py::test_database_schema_verification`
- **Проверка колонок**: `PRAGMA table_info()` в тестах
- **Валидация связей**: Foreign key constraints

## 🚀 Запуск системы

### Локальный запуск

#### 1. Установка зависимостей
```bash
# Клонирование и настройка
git clone https://github.com/mkcomru/lead_triage_system
cd lead_triage_system

# Виртуальное окружение
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# Зависимости
pip install -r requirements.txt
```

#### 2. Запуск Redis
```bash
# Windows (через Docker)
docker run -d -p 6379:6379 redis:alpine

# Linux/Mac
redis-server
```

#### 3. Запуск сервисов (в отдельных терминалах)
```bash
# Terminal 1: Intake API (Port 8000)
cd intake-api
python main.py

# Terminal 2: Insights API (Port 8001)  
cd insights-api
python main.py

# Terminal 3: Triage Worker
cd triage-worker
python main.py
```

#### 4. Проверка работы
```bash
# Создание лида
curl -X POST http://localhost:8000/leads \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "name": "Test User", "note": "Interested in product"}'

# Получение анализа (через несколько секунд)
curl http://localhost:8001/leads/insight
```

### Docker Compose запуск

#### 1. Быстрый старт
```bash
# Переход в директорию с compose
cd deploy

# Запуск всех сервисов
docker-compose up --build

# В фоновом режиме
docker-compose up -d --build
```

## 🧪 Запуск тестов

```bash
# Все тесты
pytest -v

# Конкретные тесты
pytest tests/test_e2e.py -v
pytest tests/test_idempotency.py -v  
pytest tests/test_duplicate_queue.py -v

```

### Результаты тестов
- **17 тестов** включая E2E, идемпотентность, дедупликацию
- **Проверка схемы БД** автоматически
- **Cleanup** тестовых данных после каждого теста

## 📡 API Endpoints

### Intake API (Port 8000)
- `POST /leads` - Создание лида
- `GET /leads/{lead_id}` - Инфо о лиде

### Insights API (Port 8001)  
- `GET leads/{lead_id}/insight` - Получение анализа

## 📁 Архитектура

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Intake API    │───▶│   Redis Queue   │───▶│ Triage Worker   │
│  (Port 8000)    │    │  (Port 6379)    │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────┐                            ┌─────────────────┐
│  SQLite DB      │◀───────────────────────────│  Insights API   │
│                 │                            │  (Port 8001)    │
└─────────────────┘                            └─────────────────┘
```