FROM python:3.13-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .


ENV REDIS_HOST=my-redis-stream
ENV REDIS_PORT=6379
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]