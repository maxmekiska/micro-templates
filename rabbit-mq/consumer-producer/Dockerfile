FROM python:3.13-slim-bookworm

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV RABBITMQ_HOST=rabbitmq-broker
ENV RABBITMQ_PORT=5672

CMD ["python", "main.py"]