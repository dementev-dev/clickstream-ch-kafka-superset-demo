FROM python:3.12-slim

# Установка uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Копируем зависимости отдельно для кэширования слоёв
COPY requirements.txt .

# Установка зависимостей через uv (компактный venv)
RUN uv venv /app/.venv && \
    uv pip install --python /app/.venv/bin/python -r requirements.txt

# Копируем код
COPY generator.py .

# Не-root пользователь для безопасности
RUN useradd -m -u 1000 generator && chown -R generator:generator /app
USER generator

# Активируем venv через PATH
ENV PATH="/app/.venv/bin:$PATH"

# Запуск генератора
CMD ["python", "generator.py"]
