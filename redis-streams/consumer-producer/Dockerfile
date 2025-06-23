FROM python:3.13-slim-bookworm

WORKDIR /app


# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Set environment variables
ENV REDIS_HOST=my-redis-stream
ENV REDIS_PORT=6379
ENV PYTHONUNBUFFERED=1

# Run the producer
CMD ["python", "main.py"]