FROM python:3.13-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y librdkafka-dev build-essential && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]