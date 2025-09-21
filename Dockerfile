FROM python:3.13-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /app

# Копируем и устанавливаем основные зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект
COPY . .

# Создаем директории для логов и данных
RUN mkdir -p /app/logs /app/data

# Экспортируем порты
EXPOSE 8000 8001

# Устанавливаем переменные окружения
ENV PYTHONPATH=/app
ENV DATABASE_PATH=/app/data/database.sqlite

# Команда по умолчанию (запуск тестов)
CMD ["pytest", "-v"]